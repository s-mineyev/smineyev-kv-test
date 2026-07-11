package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/redis/rueidis"
)

const (
	defaultAddr = "amr1-test1.centralus.redis.azure.net:10000"
	// Entra ID scope for Azure Managed Redis / Azure Cache for Redis.
	redisScope = "https://redis.azure.com/.default"

	defaultCosmosEndpoint = "https://smineyev-kv-cosmos-cus.documents.azure.com:443/"
	defaultCosmosDB       = "kvdb"
	defaultCosmosCont     = "kvcache"

	// numSessions concurrent sessions run in parallel, each with its own AMR
	// and Cosmos DB client and its own distinct block of keysPerSession keys.
	numSessions    = 5
	keysPerSession = 20
	// numIterations is how many times each session repeats its block of keys.
	numIterations = 4
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func randValue(r *rand.Rand) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 16)
	for i := range b {
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}

// item is the durable document stored in Cosmos DB. The partition key path is
// "/id", so the id field also serves as the partition key value.
type item struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

// config holds the shared connection settings resolved from the environment.
type config struct {
	addr           string
	host           string
	objectID       string
	cosmosEndpoint string
	cosmosDB       string
	cosmosCont     string
}

// sessionResult carries the latencies collected by a single session.
type sessionResult struct {
	totalLat  []time.Duration
	commitLat []time.Duration
	err       error
}

func main() {
	cfg := config{
		addr:           getenv("AMR_ADDR", defaultAddr),
		objectID:       os.Getenv("AMR_OBJECT_ID"),
		cosmosEndpoint: getenv("COSMOS_ENDPOINT", defaultCosmosEndpoint),
		cosmosDB:       getenv("COSMOS_DB", defaultCosmosDB),
		cosmosCont:     getenv("COSMOS_CONTAINER", defaultCosmosCont),
	}
	if cfg.objectID == "" {
		log.Fatal("AMR_OBJECT_ID env var is required (Entra object ID of the signed-in principal)")
	}
	cfg.host = cfg.addr
	if i := indexByte(cfg.addr, ':'); i >= 0 {
		cfg.host = cfg.addr[:i]
	}

	// A single credential is shared by all sessions; azidentity credentials
	// are safe for concurrent use and cache tokens internally.
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("failed to create Azure credential: %v", err)
	}

	log.Printf("launching %d concurrent sessions x %d iterations x %d keys (%d mutations total)",
		numSessions, numIterations, keysPerSession, numSessions*numIterations*keysPerSession)
	log.Printf("AMR %s | Cosmos %s db=%s container=%s", cfg.addr, cfg.cosmosEndpoint, cfg.cosmosDB, cfg.cosmosCont)

	results := make([]sessionResult, numSessions)
	var wg sync.WaitGroup
	overallStart := time.Now()
	for s := 0; s < numSessions; s++ {
		wg.Add(1)
		go func(sessionID int) {
			defer wg.Done()
			results[sessionID] = runSession(cred, cfg, sessionID)
		}(s)
	}
	wg.Wait()
	elapsed := time.Since(overallStart)

	// Aggregate results across all sessions.
	var allTotal, allCommit []time.Duration
	for s := 0; s < numSessions; s++ {
		if results[s].err != nil {
			log.Fatalf("session %d failed: %v", s, results[s].err)
		}
		allTotal = append(allTotal, results[s].totalLat...)
		allCommit = append(allCommit, results[s].commitLat...)
	}

	log.Printf("all sessions complete: %d mutations in %v (wall clock)", len(allTotal), elapsed)
	printStats("total mutation (steps 1-3), all sessions", allTotal)
	printStats("durable commit (Cosmos upsert), all sessions", allCommit)
}

// runSession opens its own AMR and Cosmos clients (an independent "session")
// and applies numIterations rounds of the 6.4 Mutation Algorithm over its own
// distinct block of keysPerSession keys.
func runSession(cred azcore.TokenCredential, cfg config, sessionID int) sessionResult {
	ctx := context.Background()

	amr, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{cfg.addr},
		TLSConfig:   &tls.Config{ServerName: cfg.host, MinVersion: tls.VersionTLS12},
		AuthCredentialsFn: func(ctx rueidis.AuthCredentialsContext) (rueidis.AuthCredentials, error) {
			tk, err := cred.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{redisScope}})
			if err != nil {
				return rueidis.AuthCredentials{}, err
			}
			return rueidis.AuthCredentials{Username: cfg.objectID, Password: tk.Token}, nil
		},
	})
	if err != nil {
		return sessionResult{err: fmt.Errorf("create redis client: %w", err)}
	}
	defer amr.Close()

	cosmosClient, err := azcosmos.NewClient(cfg.cosmosEndpoint, cred, nil)
	if err != nil {
		return sessionResult{err: fmt.Errorf("create cosmos client: %w", err)}
	}
	container, err := cosmosClient.NewContainer(cfg.cosmosDB, cfg.cosmosCont)
	if err != nil {
		return sessionResult{err: fmt.Errorf("get cosmos container: %w", err)}
	}

	if err := amr.Do(ctx, amr.B().Ping().Build()).Error(); err != nil {
		return sessionResult{err: fmt.Errorf("AMR PING: %w", err)}
	}

	// Each session owns a distinct, non-overlapping block of keys.
	firstKey := sessionID * keysPerSession
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(sessionID)))

	totalLat := make([]time.Duration, 0, keysPerSession*numIterations)
	commitLat := make([]time.Duration, 0, keysPerSession*numIterations)

	log.Printf("session %d started (keys %03d-%03d)", sessionID, firstKey, firstKey+keysPerSession-1)
	for iter := 0; iter < numIterations; iter++ {
		for k := 0; k < keysPerSession; k++ {
			key := fmt.Sprintf("app:test:key:%03d", firstKey+k)
			val := randValue(r)

			start := time.Now()
			commit, err := mutatePut(ctx, amr, container, key, val)
			total := time.Since(start)
			if err != nil {
				return sessionResult{err: fmt.Errorf("mutation for %s: %w", key, err)}
			}
			totalLat = append(totalLat, total)
			commitLat = append(commitLat, commit)
		}
	}
	log.Printf("session %d complete (%d mutations)", sessionID, len(totalLat))

	return sessionResult{totalLat: totalLat, commitLat: commitLat}
}

// mutatePut applies the 6.4 Mutation Algorithm for a Put mutation, excluding
// Step 4 (CDC Reconciliation):
//
//	Step 1: Best-Effort AMR Invalidation (log & continue on failure).
//	Step 2: Commit the Durable Mutation to Cosmos DB (authoritative; must succeed).
//	Step 3: Opportunistic Cache Update in AMR (log & continue on failure).
//
// It returns the Cosmos commit latency and a fatal error only if Step 2 fails.
func mutatePut(ctx context.Context, amr rueidis.Client, container *azcosmos.ContainerClient, key, val string) (time.Duration, error) {
	// Step 1: Best-Effort AMR Invalidation.
	if err := amr.Do(ctx, amr.B().Del().Key(key).Build()).Error(); err != nil {
		log.Printf("[step1] AMR invalidation failed for %s (continuing): %v", key, err)
	}

	// Step 2: Commit the Durable Mutation to Cosmos DB.
	doc := item{ID: key, Value: val}
	body, err := json.Marshal(doc)
	if err != nil {
		return 0, fmt.Errorf("marshal item: %w", err)
	}
	pk := azcosmos.NewPartitionKeyString(key)
	commitStart := time.Now()
	if _, err := container.UpsertItem(ctx, pk, body, nil); err != nil {
		return 0, fmt.Errorf("cosmos upsert: %w", err)
	}
	commit := time.Since(commitStart)

	// Step 3: Opportunistic Cache Update.
	if err := amr.Do(ctx, amr.B().Set().Key(key).Value(val).Build()).Error(); err != nil {
		log.Printf("[step3] AMR cache update failed for %s (continuing): %v", key, err)
	}

	return commit, nil
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func printStats(label string, latencies []time.Duration) {
	if len(latencies) == 0 {
		return
	}
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total time.Duration
	for _, l := range sorted {
		total += l
	}
	avg := total / time.Duration(len(sorted))
	pct := func(p float64) time.Duration {
		idx := int(p * float64(len(sorted)-1))
		return sorted[idx]
	}

	log.Printf("---- latency summary: %s ----", label)
	log.Printf("count : %d", len(sorted))
	log.Printf("min   : %v", sorted[0])
	log.Printf("avg   : %v", avg)
	log.Printf("p50   : %v", pct(0.50))
	log.Printf("p90   : %v", pct(0.90))
	log.Printf("p99   : %v", pct(0.99))
	log.Printf("max   : %v", sorted[len(sorted)-1])
}

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
	"time"

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

	numKeys = 100
	// numIterations is how many times the full set of numKeys mutations is repeated.
	numIterations = 10
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

func main() {
	addr := getenv("AMR_ADDR", defaultAddr)
	objectID := os.Getenv("AMR_OBJECT_ID")
	if objectID == "" {
		log.Fatal("AMR_OBJECT_ID env var is required (Entra object ID of the signed-in principal)")
	}
	cosmosEndpoint := getenv("COSMOS_ENDPOINT", defaultCosmosEndpoint)
	cosmosDB := getenv("COSMOS_DB", defaultCosmosDB)
	cosmosCont := getenv("COSMOS_CONTAINER", defaultCosmosCont)

	host := addr
	if i := indexByte(addr, ':'); i >= 0 {
		host = addr[:i]
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("failed to create Azure credential: %v", err)
	}

	// --- AMR (rueidis) client, authenticated with Entra ID ---
	amr, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{addr},
		TLSConfig:   &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12},
		AuthCredentialsFn: func(ctx rueidis.AuthCredentialsContext) (rueidis.AuthCredentials, error) {
			tk, err := cred.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{redisScope}})
			if err != nil {
				return rueidis.AuthCredentials{}, err
			}
			return rueidis.AuthCredentials{Username: objectID, Password: tk.Token}, nil
		},
	})
	if err != nil {
		log.Fatalf("failed to create redis client: %v", err)
	}
	defer amr.Close()

	// --- Cosmos DB client, authenticated with Entra ID ---
	cosmosClient, err := azcosmos.NewClient(cosmosEndpoint, cred, nil)
	if err != nil {
		log.Fatalf("failed to create cosmos client: %v", err)
	}
	container, err := cosmosClient.NewContainer(cosmosDB, cosmosCont)
	if err != nil {
		log.Fatalf("failed to get cosmos container: %v", err)
	}

	ctx := context.Background()

	if err := amr.Do(ctx, amr.B().Ping().Build()).Error(); err != nil {
		log.Fatalf("AMR PING failed: %v", err)
	}
	log.Printf("connected to AMR %s as %s", addr, objectID)
	log.Printf("using Cosmos %s db=%s container=%s", cosmosEndpoint, cosmosDB, cosmosCont)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	totalLat := make([]time.Duration, 0, numKeys*numIterations)
	commitLat := make([]time.Duration, 0, numKeys*numIterations)

	log.Printf("mutating %d keys x %d iterations via 6.4 Mutation Algorithm (steps 1-3)...", numKeys, numIterations)
	for iter := 0; iter < numIterations; iter++ {
		iterStart := time.Now()
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("app:test:key:%03d", i)
			val := randValue(r)

			start := time.Now()
			commit, err := mutatePut(ctx, amr, container, key, val)
			total := time.Since(start)
			if err != nil {
				log.Fatalf("mutation for %s failed at durable commit: %v", key, err)
			}
			totalLat = append(totalLat, total)
			commitLat = append(commitLat, commit)
			log.Printf("iter %02d/%02d PUT %s -> %s | commit(cosmos): %v | total: %v", iter+1, numIterations, key, val, commit, total)
		}
		log.Printf("iteration %d/%d complete (%d mutations) in %v", iter+1, numIterations, numKeys, time.Since(iterStart))
	}

	log.Printf("completed %d mutations (%d keys x %d iterations)", numKeys*numIterations, numKeys, numIterations)
	printStats("total mutation (steps 1-3)", totalLat)
	printStats("durable commit (Cosmos upsert)", commitLat)
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

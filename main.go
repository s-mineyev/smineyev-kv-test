package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
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
	// It is configurable via the -sessions flag (default 1).
	defaultSessions = 1
	keysPerSession  = 20
	// numIterations is how many times each session repeats its block of keys.
	numIterations = 4
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func randValue(r *rand.Rand, size int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, size)
	for i := range b {
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}

// item is the durable document stored in Cosmos DB. The partition key path is
// "/id", so the id field also serves as the partition key value.
//
// Per doc 6.6/6.7, ExpireAt (absolute unix-ms) controls visibility and acts as
// the generation discriminator for expiration/recreation races. TTL (relative
// seconds) drives Cosmos physical cleanup. WrittenAtMs is the client commit
// timestamp used by the CDC consumer to measure end-to-end stream lag.
type item struct {
	ID          string `json:"id"`
	Value       string `json:"value"`
	ExpireAt    int64  `json:"expire_at"`
	WrittenAtMs int64  `json:"written_at_ms"`
	TTL         int32  `json:"ttl"`
}

// cacheEntry is the JSON value stored in AMR for a key. It carries expire_at so
// both the CDC consumer (doc 6.7 generation compare) and the read path (6.3
// expire_at validation) can reason about it. The Go app and the TypeScript CDC
// consumer must keep this shape in sync.
type cacheEntry struct {
	Value    string `json:"value"`
	ExpireAt int64  `json:"expire_at"`
}

// config holds the shared connection settings resolved from the environment.
type config struct {
	addr           string
	host           string
	objectID       string
	cosmosEndpoint string
	cosmosDB       string
	cosmosCont     string
	ttlSeconds     int32
	keyPrefix      string
	valueSize      int
}

// sessionResult carries the latencies collected by a single session.
type sessionResult struct {
	totalLat  []time.Duration
	commitLat []time.Duration
	rus       []float64
	err       error
}

func main() {
	sessions := flag.Int("sessions", defaultSessions, "number of concurrent sessions")
	ttl := flag.Int("ttl", 3600, "per-item TTL in seconds (drives expire_at and Cosmos physical cleanup)")
	keyPrefix := flag.String("keyprefix", "app:test", "key namespace prefix; keys are <prefix>:s<NN>:key:<KKK>")
	cdcLag := flag.Bool("cdclag", false, "CDC-lag mode: write only to Cosmos (bypass AMR), then poll AMR to measure change-feed reconciliation lag")
	valueSize := flag.Int("valuesize", 16, "size in bytes of the random value payload")
	flag.Parse()
	numSessions := *sessions
	if numSessions < 1 {
		log.Fatalf("-sessions must be >= 1, got %d", numSessions)
	}

	cfg := config{
		addr:           getenv("AMR_ADDR", defaultAddr),
		objectID:       os.Getenv("AMR_OBJECT_ID"),
		cosmosEndpoint: getenv("COSMOS_ENDPOINT", defaultCosmosEndpoint),
		cosmosDB:       getenv("COSMOS_DB", defaultCosmosDB),
		cosmosCont:     getenv("COSMOS_CONTAINER", defaultCosmosCont),
		ttlSeconds:     int32(*ttl),
		keyPrefix:      *keyPrefix,
		valueSize:      *valueSize,
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

	if *cdcLag {
		runCdcLagTest(cred, cfg, numSessions)
		return
	}

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
	var allRU []float64
	for s := 0; s < numSessions; s++ {
		if results[s].err != nil {
			log.Fatalf("session %d failed: %v", s, results[s].err)
		}
		allTotal = append(allTotal, results[s].totalLat...)
		allCommit = append(allCommit, results[s].commitLat...)
		allRU = append(allRU, results[s].rus...)
	}

	log.Printf("all sessions complete: %d mutations in %v (wall clock)", len(allTotal), elapsed)
	printStats("total mutation (steps 1-3), all sessions", allTotal)
	printStats("durable commit (Cosmos upsert), all sessions", allCommit)
	printRUStats("Cosmos RU charge per upsert, all sessions", allRU, elapsed)
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

	// Each session owns its own key namespace, keyed by the session number so
	// keys are guaranteed unique per session regardless of key count.
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(sessionID)))

	totalLat := make([]time.Duration, 0, keysPerSession*numIterations)
	commitLat := make([]time.Duration, 0, keysPerSession*numIterations)
	rus := make([]float64, 0, keysPerSession*numIterations)

	log.Printf("session %d started (keys %s:s%02d:key:000-%03d)", sessionID, cfg.keyPrefix, sessionID, keysPerSession-1)
	for iter := 0; iter < numIterations; iter++ {
		for k := 0; k < keysPerSession; k++ {
			key := fmt.Sprintf("%s:s%02d:key:%03d", cfg.keyPrefix, sessionID, k)
			val := randValue(r, cfg.valueSize)

			start := time.Now()
			commit, ru, err := mutatePut(ctx, amr, container, key, val, cfg.ttlSeconds)
			total := time.Since(start)
			if err != nil {
				return sessionResult{err: fmt.Errorf("mutation for %s: %w", key, err)}
			}
			totalLat = append(totalLat, total)
			commitLat = append(commitLat, commit)
			rus = append(rus, float64(ru))
		}
	}
	log.Printf("session %d complete (%d mutations)", sessionID, len(totalLat))

	return sessionResult{totalLat: totalLat, commitLat: commitLat, rus: rus}
}

// mutatePut applies the 6.4 Mutation Algorithm for a Put mutation, excluding
// Step 4 (CDC Reconciliation):
//
//	Step 1: Best-Effort AMR Invalidation (log & continue on failure).
//	Step 2: Commit the Durable Mutation to Cosmos DB (authoritative; must succeed).
//	Step 3: Opportunistic Cache Update in AMR (log & continue on failure).
//
// It returns the Cosmos commit latency, the RU charge for the upsert, and a
// fatal error only if Step 2 fails.
func mutatePut(ctx context.Context, amr rueidis.Client, container *azcosmos.ContainerClient, key, val string, ttlSeconds int32) (time.Duration, float32, error) {
	// Step 1: Best-Effort AMR Invalidation.
	if err := amr.Do(ctx, amr.B().Del().Key(key).Build()).Error(); err != nil {
		log.Printf("[step1] AMR invalidation failed for %s (continuing): %v", key, err)
	}

	// Step 2: Commit the Durable Mutation to Cosmos DB.
	// expire_at (absolute) controls visibility/generation; ttl (relative) drives
	// Cosmos physical cleanup; written_at_ms lets the CDC consumer measure lag.
	nowMs := time.Now().UnixMilli()
	doc := item{
		ID:          key,
		Value:       val,
		ExpireAt:    nowMs + int64(ttlSeconds)*1000,
		WrittenAtMs: nowMs,
		TTL:         ttlSeconds,
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return 0, 0, fmt.Errorf("marshal item: %w", err)
	}
	pk := azcosmos.NewPartitionKeyString(key)
	commitStart := time.Now()
	resp, err := container.UpsertItem(ctx, pk, body, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("cosmos upsert: %w", err)
	}
	commit := time.Since(commitStart)
	ru := resp.RequestCharge

	// Step 3: Opportunistic Cache Update. Store {value, expire_at} so the CDC
	// consumer and read path can compare generations (doc 6.7), and set the
	// Redis key to auto-expire at expire_at.
	cacheBody, err := json.Marshal(cacheEntry{Value: val, ExpireAt: doc.ExpireAt})
	if err == nil {
		if err := amr.Do(ctx, amr.B().Set().Key(key).Value(string(cacheBody)).ExatTimestamp(doc.ExpireAt/1000).Build()).Error(); err != nil {
			log.Printf("[step3] AMR cache update failed for %s (continuing): %v", key, err)
		}
	} else {
		log.Printf("[step3] marshal cache entry failed for %s (continuing): %v", key, err)
	}

	return commit, ru, nil
}

// runCdcLagTest measures CDC reconciliation lag under numSessions concurrent
// writers. Each session writes its keys *directly to Cosmos only* (bypassing the
// AMR invalidate/update steps), then polls AMR until the change-feed consumer
// has reconciled each key. Because the client never writes AMR itself, the
// observed AMR arrival is attributable solely to CDC. lag = AMR-seen - Cosmos-commit.
func runCdcLagTest(cred azcore.TokenCredential, cfg config, numSessions int) {
	log.Printf("CDC-lag mode: %d concurrent session(s) x %d keys, writing Cosmos-only then polling AMR", numSessions, keysPerSession)

	lagCh := make(chan time.Duration, numSessions*keysPerSession)
	missCh := make(chan int, numSessions)
	var wg sync.WaitGroup
	for s := 0; s < numSessions; s++ {
		wg.Add(1)
		go func(sessionID int) {
			defer wg.Done()
			misses := cdcLagSession(cred, cfg, sessionID, lagCh)
			missCh <- misses
		}(s)
	}
	wg.Wait()
	close(lagCh)
	close(missCh)

	lags := make([]time.Duration, 0, numSessions*keysPerSession)
	for l := range lagCh {
		lags = append(lags, l)
	}
	misses := 0
	for m := range missCh {
		misses += m
	}
	log.Printf("CDC-lag: %d reconciled, %d not seen within timeout", len(lags), misses)
	printStats("CDC reconciliation lag", lags)
}

// cdcLagSession writes keysPerSession keys to Cosmos only, then polls AMR for
// each until reconciled by CDC, sending per-key lag on lagCh. Returns the count
// of keys not reconciled within the timeout.
func cdcLagSession(cred azcore.TokenCredential, cfg config, sessionID int, lagCh chan<- time.Duration) int {
	ctx := context.Background()
	amr, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{cfg.addr},
		TLSConfig:   &tls.Config{ServerName: cfg.host, MinVersion: tls.VersionTLS12},
		AuthCredentialsFn: func(rueidis.AuthCredentialsContext) (rueidis.AuthCredentials, error) {
			tk, err := cred.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{redisScope}})
			if err != nil {
				return rueidis.AuthCredentials{}, err
			}
			return rueidis.AuthCredentials{Username: cfg.objectID, Password: tk.Token}, nil
		},
	})
	if err != nil {
		log.Fatalf("session %d: create redis client: %v", sessionID, err)
	}
	defer amr.Close()

	cosmosClient, err := azcosmos.NewClient(cfg.cosmosEndpoint, cred, nil)
	if err != nil {
		log.Fatalf("session %d: create cosmos client: %v", sessionID, err)
	}
	container, err := cosmosClient.NewContainer(cfg.cosmosDB, cfg.cosmosCont)
	if err != nil {
		log.Fatalf("session %d: get cosmos container: %v", sessionID, err)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(sessionID)))
	type pending struct {
		val string
		t0  time.Time
	}
	pend := make(map[string]pending, keysPerSession)

	// Phase 1: write each key directly to Cosmos (no AMR), pre-clearing AMR so a
	// stale value can't be mistaken for a CDC reconciliation.
	for k := 0; k < keysPerSession; k++ {
		key := fmt.Sprintf("%s:s%02d:key:%03d", cfg.keyPrefix, sessionID, k)
		val := randValue(r, cfg.valueSize)
		_ = amr.Do(ctx, amr.B().Del().Key(key).Build()).Error()

		nowMs := time.Now().UnixMilli()
		doc := item{ID: key, Value: val, ExpireAt: nowMs + int64(cfg.ttlSeconds)*1000, WrittenAtMs: nowMs, TTL: cfg.ttlSeconds}
		body, _ := json.Marshal(doc)
		if _, err := container.UpsertItem(ctx, azcosmos.NewPartitionKeyString(key), body, nil); err != nil {
			log.Fatalf("session %d: cosmos upsert %s: %v", sessionID, key, err)
		}
		pend[key] = pending{val: val, t0: time.Now()}
	}

	// Phase 2: poll AMR until each key is reconciled by CDC (or times out).
	const timeout = 60 * time.Second
	deadline := time.Now().Add(timeout)
	for len(pend) > 0 && time.Now().Before(deadline) {
		for key, p := range pend {
			got, err := amr.Do(ctx, amr.B().Get().Key(key).Build()).ToString()
			if err == nil && got != "" {
				var ce cacheEntry
				if json.Unmarshal([]byte(got), &ce) == nil && ce.Value == p.val {
					lagCh <- time.Since(p.t0)
					delete(pend, key)
				}
			}
		}
		if len(pend) > 0 {
			time.Sleep(150 * time.Millisecond)
		}
	}
	return len(pend)
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

// printRUStats reports the per-request RU charge distribution (from the Cosmos
// x-ms-request-charge response header) plus total and effective RU/s consumed.
func printRUStats(label string, rus []float64, elapsed time.Duration) {
	if len(rus) == 0 {
		return
	}
	sorted := make([]float64, len(rus))
	copy(sorted, rus)
	sort.Float64s(sorted)

	var sum float64
	for _, v := range sorted {
		sum += v
	}
	avg := sum / float64(len(sorted))
	pct := func(p float64) float64 {
		idx := int(p * float64(len(sorted)-1))
		return sorted[idx]
	}
	effectiveRUps := sum / elapsed.Seconds()

	log.Printf("---- RU summary: %s ----", label)
	log.Printf("count       : %d", len(sorted))
	log.Printf("min RU      : %.2f", sorted[0])
	log.Printf("avg RU      : %.2f", avg)
	log.Printf("p50 RU      : %.2f", pct(0.50))
	log.Printf("p99 RU      : %.2f", pct(0.99))
	log.Printf("max RU      : %.2f", sorted[len(sorted)-1])
	log.Printf("total RU    : %.2f", sum)
	log.Printf("effective RU/s : %.2f (over %v wall clock)", effectiveRUps, elapsed)
}

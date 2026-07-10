package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/redis/rueidis"
)

const (
	defaultAddr = "amr1-test1.centralus.redis.azure.net:10000"
	// Entra ID scope for Azure Managed Redis / Azure Cache for Redis.
	redisScope = "https://redis.azure.com/.default"
	numKeys    = 100
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

func main() {
	addr := getenv("AMR_ADDR", defaultAddr)
	// The Entra username must be the object ID of the principal.
	objectID := os.Getenv("AMR_OBJECT_ID")
	if objectID == "" {
		log.Fatal("AMR_OBJECT_ID env var is required (Entra object ID of the signed-in principal)")
	}

	host := addr
	if i := indexByte(addr, ':'); i >= 0 {
		host = addr[:i]
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("failed to create Azure credential: %v", err)
	}

	// Fetch an Entra access token to use as the Redis password.
	tokenFn := func(ctx context.Context) (string, error) {
		tk, err := cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{redisScope}})
		if err != nil {
			return "", err
		}
		return tk.Token, nil
	}

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{addr},
		TLSConfig:   &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12},
		AuthCredentialsFn: func(ctx rueidis.AuthCredentialsContext) (rueidis.AuthCredentials, error) {
			token, err := tokenFn(context.Background())
			if err != nil {
				return rueidis.AuthCredentials{}, err
			}
			return rueidis.AuthCredentials{Username: objectID, Password: token}, nil
		},
	})
	if err != nil {
		log.Fatalf("failed to create redis client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Verify connectivity.
	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		log.Fatalf("PING failed: %v", err)
	}
	log.Printf("connected to %s as %s", addr, objectID)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	latencies := make([]time.Duration, 0, numKeys)

	log.Printf("setting %d keys...", numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("app:test:key:%03d", i)
		val := randValue(r)

		start := time.Now()
		err := client.Do(ctx, client.B().Set().Key(key).Value(val).Build()).Error()
		elapsed := time.Since(start)

		if err != nil {
			log.Fatalf("SET %s failed: %v", key, err)
		}
		latencies = append(latencies, elapsed)
		log.Printf("SET %s -> %s | latency: %v", key, val, elapsed)
	}

	printStats(latencies)
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func printStats(latencies []time.Duration) {
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

	log.Println("---- latency summary (client-side, SET operations) ----")
	log.Printf("count : %d", len(sorted))
	log.Printf("min   : %v", sorted[0])
	log.Printf("avg   : %v", avg)
	log.Printf("p50   : %v", pct(0.50))
	log.Printf("p90   : %v", pct(0.90))
	log.Printf("p99   : %v", pct(0.99))
	log.Printf("max   : %v", sorted[len(sorted)-1])
}

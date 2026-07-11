# smineyev-kv-test

A small Go test app that performs key mutations against a KV platform composed of
**Azure Managed Redis (AMR)** as the cache and **Azure Cosmos DB for NoSQL** as the durable
source of truth. Each mutation follows the **6.4 Mutation Algorithm** (steps 1–3) and the app
logs client-side latency per operation. Step 4 (CDC Reconciliation) is implemented by the
TypeScript Azure Function in [`cdc-consumer/`](cdc-consumer/).

## Mutation Algorithm (section 6.4, steps 1–3)

For every key the app performs a Put mutation:

1. **Best-Effort AMR Invalidation** — `DEL` the key from AMR. On failure, log and continue.
2. **Commit the Durable Mutation** — upsert the item into Cosmos DB. This is authoritative
   and must succeed; a failure aborts the mutation.
3. **Opportunistic Cache Update** — `SET` the committed value in AMR as a JSON
   `{value, expire_at}` entry with a Redis expiry at `expire_at`. On failure, log and continue.

Step 4 (CDC Reconciliation) is implemented separately by the TypeScript Azure Function in
[`cdc-consumer/`](cdc-consumer/).

## Data model

Each Cosmos document is `{id, value, expire_at, written_at_ms, ttl}`:

- `expire_at` — absolute unix-ms; controls visibility and is the generation discriminator for
  expiration/recreation races (doc 6.6/6.7).
- `ttl` — relative seconds (from `-ttl`, default 3600); drives Cosmos physical cleanup.
- `written_at_ms` — client commit timestamp, used by the CDC consumer to measure stream lag.

The AMR value mirrors `{value, expire_at}` so the cache carries the generation for reconciliation.

## Authentication

Both AMR and Cosmos DB authenticate with **Microsoft Entra ID** via `DefaultAzureCredential`
(no keys/secrets on the host):

- **AMR**: acquires a token for scope `https://redis.azure.com/.default` and supplies it to
  rueidis via `AuthCredentialsFn`, with the username set to the principal's Entra **object ID**
  and the password set to the access token (AMR access-key auth is disabled).
- **Cosmos DB**: the `azcosmos` client uses the same credential. The principal must hold a
  Cosmos DB data-plane role (e.g. *Cosmos DB Built-in Data Contributor*) on the account.

## Usage

```bash
AMR_OBJECT_ID=<your-entra-object-id> go run .
```

Environment variables:

| Variable           | Required | Default                                          | Description                                  |
|--------------------|----------|--------------------------------------------------|----------------------------------------------|
| `AMR_OBJECT_ID`    | yes      | —                                                | Entra object ID of the signed-in principal   |
| `AMR_ADDR`         | no       | `amr1-test1.centralus.redis.azure.net:10000`     | AMR cluster `host:port`                       |
| `COSMOS_ENDPOINT`  | no       | `https://smineyev-kv-cosmos-cus.documents.azure.com:443/` | Cosmos DB account endpoint          |
| `COSMOS_DB`        | no       | `kvdb`                                            | Cosmos database name                         |
| `COSMOS_CONTAINER` | no       | `kvcache`                                         | Cosmos container name (partition key `/id`)  |

## What it does

1. Launches **N concurrent sessions** (goroutines; `-sessions N`, default `1`), each with
   its own AMR and Cosmos DB client (both via Entra ID), and runs `PING` to verify AMR.
2. Each session owns its own key namespace, keyed by session number
   (session _s_ uses `app:test:s<ss>:key:000` … `app:test:s<ss>:key:019`), so keys are
   unique per session.
3. Each session repeats over **4 iterations**, performing a Put mutation per key following
   section 6.4 steps 1–3 (invalidate AMR → commit to Cosmos → update AMR). This yields
   N × 4 × 20 mutations total.
4. Prints latency summaries (min / avg / p50 / p90 / p99 / max) aggregated across all
   sessions, for total mutation and durable commit.

Run with, e.g., `./kvtest -sessions 5 -ttl 3600` (defaults to a single session, 1h TTL).

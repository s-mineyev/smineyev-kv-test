# cdc-consumer — CDC Reconciliation (doc §6.4 step 4)

A TypeScript Azure Function that implements **Step 4 (CDC Reconciliation)** of the platform's
6.4 Mutation Algorithm. It consumes the Azure Cosmos DB change feed in
**All Versions and Deletes** mode and reconciles **Azure Managed Redis (AMR)** toward the
committed durable state — the authoritative cache-reconciliation mechanism described in the design.

## What it does

- Triggers on the Cosmos DB change feed for `kvdb/kvcache` in `AllVersionsAndDeletes` mode
  (managed leases in a `leases` container, auto-created).
- For each ordered change event (per key):
  - **Create / replace (upsert)** → `SET` AMR `{value, expire_at}` with a Redis expiry at
    `expire_at`; if the committed record is already expired, remove it instead.
  - **Explicit delete** → unconditionally `DEL` the key from AMR.
  - **TTL expiration** → `DEL`, applying the §6.7 race rule: if the current AMR entry carries a
    *newer* `expire_at`, the key was recreated and the stale expiry is ignored.
  - **Out-of-order upsert** → ignored if AMR already holds a newer `expire_at` generation.
- `expire_at` is the only generation discriminator (per §6.6/6.7) — no version fields, tombstones,
  or sequence numbers. Per-key change-feed ordering guarantees eventual convergence.
- Uses `WATCH`/`MULTI` optimistic concurrency so a concurrent opportunistic cache update
  (mutation step 3) or another event cannot be clobbered.

## Authentication

Both Cosmos DB and AMR use the Function App's **system-assigned managed identity** (no keys):

- **Cosmos trigger**: identity-based connection (`COSMOS__accountEndpoint` +
  `COSMOS__credential=managedidentity`); the MI holds the *Cosmos DB Built-in Data Contributor*
  data-plane role (covers the monitored container and the leases container).
- **AMR**: `@redis/entraid` acquires Entra tokens (scope `https://redis.azure.com/.default`),
  maps the object ID to the Redis username, and refreshes automatically; the MI holds the AMR
  `default` data-access policy.

## Prerequisites (Cosmos account)

All Versions and Deletes mode requires, on the Cosmos account:

- **Continuous backup (PITR)** enabled (already set on `smineyev-kv-cosmos-cus`).
- **`enableAllVersionsAndDeletesChangeFeed = true`** at the account level (set via ARM PATCH).
- Do **not** set a container `changeFeedPolicy.retentionDuration` when continuous backup is
  enabled — retention is governed by the backup retention, and setting it is rejected.

## Hosting

Deployed to a **Basic (B1) Linux App Service plan with Always On** — *not* Consumption/Flex.
A CDC consumer must run continuously to own the change-feed leases and keep converging the cache;
a scale-to-zero plan lets the trigger listener stop and leases go unowned.

## Deploy

```bash
./deploy.sh
```

Creates storage + Application Insights + the Function App (system MI, Always On) in Central US,
grants the MI Cosmos + AMR access, pre-creates the leases container, sets app settings, builds and
publishes. Deployment package is ~11 MB (the App Insights SDK is intentionally not bundled).

## Observability

Structured logs (`context.log`, JSON) flow to the Function App's logs and Application Insights:

- `cdc_invocation_start` / `cdc_invocation_end` — batch size, duration.
- `cdc_reconcile` — per event: `key`, `op`, `action` (`set`/`del`/`del-expired`/
  `ignored-older-generation`), `expire_at`, `lag_ms`, `commit_lag_ms`.
- `metric` lines for `cdc_lag_ms` and `cdc_commit_lag_ms` (emitted as logs rather than via the
  App Insights SDK, to keep the package small).

View them:

```bash
# live tail (App Service plan)
az webapp log tail -n smineyev-kv-cdc-cus -g test1

# Application Insights (Kusto)
traces
| where message has 'cdc_reconcile'
| extend d = parse_json(message)
| summarize count(),
    p50 = percentile(toint(d.lag_ms), 50),
    p90 = percentile(toint(d.lag_ms), 90),
    p99 = percentile(toint(d.lag_ms), 99)
  by tostring(d.op), tostring(d.action)
```

## Lag measurement

Three complementary measures (see `src/functions/cdcReconcile.ts`):

1. **End-to-end lag** `cdc_lag_ms` = `now − written_at_ms` (client commit → cache-visible).
2. **Commit-to-process lag** `cdc_commit_lag_ms` = `now − crts` (Cosmos commit → processed).
3. **Backlog** — the change-feed estimator (documents pending) indicates whether the consumer
   keeps up or builds a backlog.

**Observed (Central US, B1 plan):**

| Load | end-to-end `lag_ms` | notes |
|------|--------------------:|-------|
| Single mutation | ~3.1 s | dominated by the change-feed poll interval (`feedPollDelay`, default 5 s) |
| 5-session sweep (100-doc batch) | ~6.9 s | batch of 100 reconciled in ~1.3 s once delivered |

Lag is **poll-interval-bound**, not throughput-bound: reconciliation itself is milliseconds per
event. Lower `feedPollDelay` in the trigger to reduce steady-state lag at the cost of more polling.

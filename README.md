# smineyev-kv-test

A small Go test app that connects to an **Azure Managed Redis (AMR)** cluster using the
[`github.com/redis/rueidis`](https://github.com/redis/rueidis) client, sets 100 predefined
keys with random values, and logs the client-side latency of each operation.

## Authentication

The target AMR database has access-key authentication **disabled**, so the app authenticates
with **Microsoft Entra ID**:

- Uses `DefaultAzureCredential` to acquire a token for scope `https://redis.azure.com/.default`.
- Supplies the token to rueidis via `AuthCredentialsFn`, with the username set to the
  principal's Entra **object ID** and the password set to the access token.

## Usage

```bash
AMR_OBJECT_ID=<your-entra-object-id> go run .
```

Environment variables:

| Variable         | Required | Default                                          | Description                                  |
|------------------|----------|--------------------------------------------------|----------------------------------------------|
| `AMR_OBJECT_ID`  | yes      | —                                                | Entra object ID of the signed-in principal   |
| `AMR_ADDR`       | no       | `amr1-test1.centralus.redis.azure.net:10000`     | AMR cluster `host:port`                       |

## What it does

1. Connects over TLS to the AMR cluster.
2. Runs `PING` to verify connectivity.
3. Sets 100 keys (`app:test:key:000` … `app:test:key:099`) with random 16-char values,
   logging each `SET` latency.
4. Prints a latency summary (min / avg / p50 / p90 / p99 / max).

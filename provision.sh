#!/usr/bin/env bash
#
# provision.sh — stands up the ENTIRE Azure infrastructure for the KV platform
# test bed in one shot:
#
#   * Resource group
#   * Azure Managed Redis (AMR) cluster (Entra-ID auth only)
#   * Serverless Cosmos DB for NoSQL account with continuous backup and the
#     account-level "All Versions and Deletes" change-feed capability, plus the
#     kvcache (point-KV, no-index, TTL) and leases containers
#   * Ubuntu VM (ephemeral OS disk, system-assigned managed identity) as the
#     in-region test client, bootstrapped with Go + the repo
#   * Data-plane role grants for the VM identity (AMR + Cosmos)
#   * The CDC reconciliation Azure Function (via cdc-consumer/deploy.sh)
#
# Idempotent-ish: safe to re-run; existing resources are reused where possible.
# Region defaults to Central US. Requires: az CLI (logged in), Azure Functions
# Core Tools (func), Node 20+, Go (only if you want the local build; the VM
# builds its own copy).
#
# Usage: ./provision.sh
set -euo pipefail

# ---- configuration ----------------------------------------------------------
RG="${RG:-test1}"
LOCATION="${LOCATION:-centralus}"

AMR_CLUSTER="${AMR_CLUSTER:-amr1-test1}"
AMR_SKU="${AMR_SKU:-Balanced_B0}"

COSMOS_ACCOUNT="${COSMOS_ACCOUNT:-smineyev-kv-cosmos-sl}"
COSMOS_DB="${COSMOS_DB:-kvdb}"
COSMOS_CONTAINER="${COSMOS_CONTAINER:-kvcache}"
LEASE_CONTAINER="${LEASE_CONTAINER:-leases}"

VM_NAME="${VM_NAME:-kvvm}"
VM_SIZE="${VM_SIZE:-Standard_D2ds_v4}"
VM_IMAGE="${VM_IMAGE:-Ubuntu2204}"
VM_ADMIN="${VM_ADMIN:-azureuser}"

REPO_URL="${REPO_URL:-https://github.com/s-mineyev/smineyev-kv-test.git}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COSMOS_DATA_ROLE="00000000-0000-0000-0000-000000000002" # Cosmos DB Built-in Data Contributor
REDIS_SCOPE="https://redis.azure.com/.default"

az config set extension.use_dynamic_install=yes_without_prompt >/dev/null 2>&1 || true
SUB_ID="$(az account show --query id -o tsv)"
echo ">>> subscription: $SUB_ID | RG: $RG | region: $LOCATION"

# ---- 0: resource providers --------------------------------------------------
echo ">>> [0/8] registering resource providers"
for ns in Microsoft.DocumentDB Microsoft.Cache Microsoft.Compute Microsoft.Web \
          Microsoft.Insights microsoft.operationalinsights Microsoft.Network; do
  az provider register --namespace "$ns" >/dev/null 2>&1 || true
done

# ---- 1: resource group ------------------------------------------------------
echo ">>> [1/8] resource group"
az group create --name "$RG" --location "$LOCATION" --only-show-errors -o none

# ---- 2: AMR cluster ---------------------------------------------------------
echo ">>> [2/8] Azure Managed Redis cluster '$AMR_CLUSTER' ($AMR_SKU) — this takes ~10-15 min"
if az redisenterprise show -n "$AMR_CLUSTER" -g "$RG" -o none 2>/dev/null; then
  echo "    cluster already exists, skipping create"
else
  az redisenterprise create \
    --name "$AMR_CLUSTER" --resource-group "$RG" --location "$LOCATION" \
    --sku "$AMR_SKU" --public-network-access Enabled \
    --only-show-errors -o none
fi
AMR_HOST="$(az redisenterprise show -n "$AMR_CLUSTER" -g "$RG" --query hostName -o tsv)"
echo "    AMR host: $AMR_HOST (access-key auth disabled -> Entra ID only)"

# ---- 3: serverless Cosmos account ------------------------------------------
echo ">>> [3/8] serverless Cosmos DB account '$COSMOS_ACCOUNT'"
if az cosmosdb show -n "$COSMOS_ACCOUNT" -g "$RG" -o none 2>/dev/null; then
  echo "    account already exists, skipping create"
else
  az cosmosdb create \
    --name "$COSMOS_ACCOUNT" --resource-group "$RG" \
    --locations regionName="$LOCATION" failoverPriority=0 isZoneRedundant=False \
    --default-consistency-level Session \
    --capabilities EnableServerless \
    --backup-policy-type Continuous --continuous-tier Continuous7Days \
    --only-show-errors -o none
fi

# ---- 3b: enable account-level All Versions and Deletes change feed ----------
echo ">>> [3b/8] enabling All Versions and Deletes change feed (account flag; can take ~15-20 min)"
ACCT_URL="https://management.azure.com/subscriptions/$SUB_ID/resourceGroups/$RG/providers/Microsoft.DocumentDB/databaseAccounts/$COSMOS_ACCOUNT?api-version=2024-12-01-preview"
CUR_FF="$(az rest --method get --url "$ACCT_URL" --query "properties.enableAllVersionsAndDeletesChangeFeed" -o tsv 2>/dev/null || echo false)"
if [ "$CUR_FF" != "true" ]; then
  az rest --method patch --url "$ACCT_URL" \
    --headers "Content-Type=application/json" \
    --body '{"properties":{"enableAllVersionsAndDeletesChangeFeed":true}}' -o none 2>/dev/null || true
  for i in $(seq 1 60); do
    sleep 20
    ST="$(az rest --method get --url "$ACCT_URL" --query "{p:properties.provisioningState,f:properties.enableAllVersionsAndDeletesChangeFeed}" -o tsv 2>/dev/null || true)"
    echo "    [$((i*20))s] state=$ST"
    echo "$ST" | grep -qi "Succeeded.*True" && { echo "    change-feed capability enabled"; break; }
  done
else
  echo "    already enabled"
fi

# ---- 4: Cosmos database + containers ---------------------------------------
echo ">>> [4/8] Cosmos database '$COSMOS_DB' + containers"
az cosmosdb sql database create --account-name "$COSMOS_ACCOUNT" -g "$RG" --name "$COSMOS_DB" --only-show-errors -o none 2>/dev/null || true
# kvcache: partition key /id, per-item TTL enabled, indexing disabled (pure KV, no queries).
cat > /tmp/kv_noidx.json <<'JSON'
{ "indexingMode": "consistent", "automatic": true, "includedPaths": [], "excludedPaths": [{ "path": "/*" }] }
JSON
az cosmosdb sql container create --account-name "$COSMOS_ACCOUNT" -g "$RG" \
  --database-name "$COSMOS_DB" --name "$COSMOS_CONTAINER" \
  --partition-key-path "/id" --ttl -1 --idx @/tmp/kv_noidx.json \
  --only-show-errors -o none 2>/dev/null || echo "    ($COSMOS_CONTAINER already exists)"
az cosmosdb sql container create --account-name "$COSMOS_ACCOUNT" -g "$RG" \
  --database-name "$COSMOS_DB" --name "$LEASE_CONTAINER" \
  --partition-key-path "/id" \
  --only-show-errors -o none 2>/dev/null || echo "    ($LEASE_CONTAINER already exists)"
rm -f /tmp/kv_noidx.json
COSMOS_ENDPOINT="$(az cosmosdb show -n "$COSMOS_ACCOUNT" -g "$RG" --query documentEndpoint -o tsv)"

# ---- 5: VM (ephemeral OS disk + system-assigned identity) -------------------
echo ">>> [5/8] test client VM '$VM_NAME' ($VM_SIZE, ephemeral OS disk)"
if az vm show -n "$VM_NAME" -g "$RG" -o none 2>/dev/null; then
  echo "    VM already exists, skipping create"
else
  az vm create \
    --resource-group "$RG" --name "$VM_NAME" \
    --image "$VM_IMAGE" --size "$VM_SIZE" --location "$LOCATION" \
    --ephemeral-os-disk true --ephemeral-os-disk-placement ResourceDisk \
    --os-disk-caching ReadOnly \
    --assign-identity '[system]' \
    --admin-username "$VM_ADMIN" --generate-ssh-keys \
    --public-ip-sku Standard \
    --only-show-errors -o none
fi
VM_MI_OID="$(az vm identity show -n "$VM_NAME" -g "$RG" --query principalId -o tsv)"
echo "    VM managed identity object id: $VM_MI_OID"

# ---- 6: data-plane role grants for the VM identity --------------------------
echo ">>> [6/8] granting VM identity data-plane access (AMR + Cosmos)"
az redisenterprise database access-policy-assignment create \
  --cluster-name "$AMR_CLUSTER" --resource-group "$RG" --database-name default \
  --name "vm$(echo "$VM_MI_OID" | tr -d '-' | cut -c1-8)" \
  --access-policy-name default --object-id "$VM_MI_OID" \
  --only-show-errors -o none 2>/dev/null || echo "    (AMR policy already assigned)"
COSMOS_SCOPE="$(az cosmosdb show -n "$COSMOS_ACCOUNT" -g "$RG" --query id -o tsv)"
az cosmosdb sql role assignment create \
  --account-name "$COSMOS_ACCOUNT" -g "$RG" \
  --role-definition-id "$COSMOS_DATA_ROLE" --principal-id "$VM_MI_OID" --scope "$COSMOS_SCOPE" \
  --only-show-errors -o none 2>/dev/null || echo "    (Cosmos role already assigned)"

# ---- 7: bootstrap the VM (Go + git + repo build) ----------------------------
echo ">>> [7/8] bootstrapping VM (install Go + git, clone + build the repo on local SSD)"
az vm run-command invoke --resource-group "$RG" --name "$VM_NAME" \
  --command-id RunShellScript --only-show-errors \
  --scripts "export HOME=/root DEBIAN_FRONTEND=noninteractive GOCACHE=/mnt/gocache GOPATH=/mnt/gopath PATH=\$PATH:/usr/local/go/bin
apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq git curl >/dev/null 2>&1
GOVER=\$(curl -fsSL 'https://go.dev/VERSION?m=text' | head -1)
curl -fsSL \"https://go.dev/dl/\${GOVER}.linux-amd64.tar.gz\" -o /mnt/go.tgz
rm -rf /usr/local/go && tar -C /usr/local -xzf /mnt/go.tgz
rm -rf /mnt/kv-src && git clone --depth 1 $REPO_URL /mnt/kv-src >/dev/null 2>&1
cd /mnt/kv-src && /usr/local/go/bin/go build -o kvtest . && echo BUILD_OK" \
  --query "value[0].message" -o tsv 2>&1 | tail -3

# ---- 8: deploy the CDC reconciliation function ------------------------------
echo ">>> [8/8] deploying CDC reconciliation function (cdc-consumer/deploy.sh)"
RG="$RG" LOCATION="$LOCATION" AMR_CLUSTER="$AMR_CLUSTER" AMR_HOST="$AMR_HOST" \
COSMOS_ACCOUNT="$COSMOS_ACCOUNT" COSMOS_DB="$COSMOS_DB" COSMOS_CONTAINER="$COSMOS_CONTAINER" \
LEASE_CONTAINER="$LEASE_CONTAINER" \
  bash "$SCRIPT_DIR/cdc-consumer/deploy.sh"

echo ""
echo "=============================================================="
echo " Provisioning complete."
echo " Cosmos (serverless) : $COSMOS_ENDPOINT"
echo " AMR                 : $AMR_HOST:10000"
echo " VM                  : $VM_NAME ($VM_SIZE)  identity OID: $VM_MI_OID"
echo ""
echo " Run a test from the VM (in-region):"
echo "   az vm run-command invoke -g $RG -n $VM_NAME --command-id RunShellScript --scripts \\"
echo "     'export PATH=\$PATH:/usr/local/go/bin; cd /mnt/kv-src && AMR_OBJECT_ID=$VM_MI_OID ./kvtest -sessions 5'"
echo "=============================================================="

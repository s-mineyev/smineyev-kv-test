#!/usr/bin/env bash
#
# Deploys the CDC reconciliation Azure Function (doc 6.4 step 4).
#
# Creates (in resource group `test1`, region Central US): a storage account,
# an Application Insights component, and a Flex Consumption Function App with a
# system-assigned managed identity. Grants that identity data-plane access to
# Cosmos DB and AMR, wires app settings (identity-based Cosmos connection), then
# builds and deploys the TypeScript function.
#
# Prereqs: az CLI (logged in), Azure Functions Core Tools (func), Node 20+.
# The Cosmos account, container, and AMR cluster must already exist.
#
# Usage: ./deploy.sh
set -euo pipefail

# ---- configuration ----------------------------------------------------------
RG="${RG:-test1}"
LOCATION="${LOCATION:-centralus}"

COSMOS_ACCOUNT="${COSMOS_ACCOUNT:-smineyev-kv-cosmos-cus}"
COSMOS_DB="${COSMOS_DB:-kvdb}"
COSMOS_CONTAINER="${COSMOS_CONTAINER:-kvcache}"
LEASE_CONTAINER="${LEASE_CONTAINER:-leases}"

AMR_CLUSTER="${AMR_CLUSTER:-amr1-test1}"
AMR_HOST="${AMR_HOST:-amr1-test1.centralus.redis.azure.net}"
AMR_PORT="${AMR_PORT:-10000}"

FUNC_APP="${FUNC_APP:-smineyev-kv-cdc-cus}"
APP_INSIGHTS="${APP_INSIGHTS:-smineyev-kv-cdc-ai}"
# App Service plan for the function. A CDC reconciliation consumer must run
# continuously (own the change-feed leases and keep converging the cache), so
# we host it on a Basic Linux plan with Always On rather than a scale-to-zero
# Consumption/Flex plan (where the trigger listener would not stay alive).
PLAN="${PLAN:-smineyev-kv-cdc-plan}"
PLAN_SKU="${PLAN_SKU:-B1}"
# Storage account names are global + <=24 lowercase alphanumerics.
STORAGE="${STORAGE:-kvcdc$(openssl rand -hex 4)sa}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

az config set extension.use_dynamic_install=yes_without_prompt >/dev/null 2>&1 || true
SUB_ID="$(az account show --query id -o tsv)"

echo "== ensuring resource providers registered =="
az provider register --namespace Microsoft.Insights >/dev/null 2>&1 || true
az provider register --namespace Microsoft.Web >/dev/null 2>&1 || true
az provider register --namespace microsoft.operationalinsights >/dev/null 2>&1 || true

echo "== pre-creating leases container (avoids needing control-plane rights for the MI) =="
az cosmosdb sql container create \
  --account-name "$COSMOS_ACCOUNT" --resource-group "$RG" \
  --database-name "$COSMOS_DB" --name "$LEASE_CONTAINER" \
  --partition-key-path "/id" --throughput 400 \
  --only-show-errors -o none 2>/dev/null || echo "   (leases container already exists)"

echo "== creating storage account: $STORAGE =="
az storage account create \
  --name "$STORAGE" --resource-group "$RG" --location "$LOCATION" \
  --sku Standard_LRS --allow-blob-public-access false \
  --only-show-errors -o none

echo "== creating Application Insights: $APP_INSIGHTS =="
az monitor app-insights component create \
  --app "$APP_INSIGHTS" --resource-group "$RG" --location "$LOCATION" \
  --kind web --only-show-errors -o none
AI_CONN="$(az monitor app-insights component show --app "$APP_INSIGHTS" --resource-group "$RG" --query connectionString -o tsv)"

echo "== creating Linux App Service plan: $PLAN ($PLAN_SKU) =="
az appservice plan create \
  --name "$PLAN" --resource-group "$RG" --location "$LOCATION" \
  --is-linux --sku "$PLAN_SKU" \
  --only-show-errors -o none

echo "== creating Function App (always-on): $FUNC_APP =="
az functionapp create \
  --name "$FUNC_APP" --resource-group "$RG" \
  --storage-account "$STORAGE" \
  --plan "$PLAN" \
  --runtime node --runtime-version 24 \
  --functions-version 4 \
  --assign-identity '[system]' \
  --app-insights "$APP_INSIGHTS" \
  --only-show-errors -o none

echo "== enabling Always On (keeps the change-feed listener running) =="
az functionapp config set \
  --name "$FUNC_APP" --resource-group "$RG" \
  --always-on true --only-show-errors -o none

MI_OID="$(az functionapp identity show --name "$FUNC_APP" --resource-group "$RG" --query principalId -o tsv)"
echo "   function managed identity object id: $MI_OID"

echo "== granting the MI the Cosmos DB built-in Data Contributor role (items + leases) =="
COSMOS_SCOPE="$(az cosmosdb show -n "$COSMOS_ACCOUNT" -g "$RG" --query id -o tsv)"
az cosmosdb sql role assignment create \
  --account-name "$COSMOS_ACCOUNT" --resource-group "$RG" \
  --role-definition-id "00000000-0000-0000-0000-000000000002" \
  --principal-id "$MI_OID" --scope "$COSMOS_SCOPE" \
  --only-show-errors -o none 2>/dev/null || echo "   (Cosmos role assignment already exists)"

echo "== granting the MI the AMR default data-access policy =="
az redisenterprise database access-policy-assignment create \
  --cluster-name "$AMR_CLUSTER" --resource-group "$RG" --database-name default \
  --name "cdcfn$(echo "$MI_OID" | tr -d '-' | cut -c1-8)" \
  --access-policy-name default --object-id "$MI_OID" \
  --only-show-errors -o none 2>/dev/null || echo "   (AMR access policy already exists)"

echo "== configuring app settings (identity-based Cosmos connection) =="
az functionapp config appsettings set \
  --name "$FUNC_APP" --resource-group "$RG" --only-show-errors -o none \
  --settings \
    "COSMOS__accountEndpoint=https://${COSMOS_ACCOUNT}.documents.azure.com:443/" \
    "COSMOS__credential=managedidentity" \
    "COSMOS_DB=${COSMOS_DB}" \
    "COSMOS_CONTAINER=${COSMOS_CONTAINER}" \
    "AMR_HOST=${AMR_HOST}" \
    "AMR_PORT=${AMR_PORT}" \
    "APPLICATIONINSIGHTS_CONNECTION_STRING=${AI_CONN}"

echo "== building and publishing the function =="
pushd "$SCRIPT_DIR" >/dev/null
npm install --no-fund --no-audit
npm run build
# Wait for role/setting propagation before the app cold-starts.
sleep 30
# .funcignore excludes typescript/@types/core-tools and *.ts from the package,
# so only the compiled dist/ + runtime deps are shipped.
func azure functionapp publish "$FUNC_APP" --typescript
popd >/dev/null

echo ""
echo "=============================================================="
echo " Deployment complete."
echo " Function App : $FUNC_APP  ($PLAN_SKU App Service plan, Always On, $LOCATION)"
echo " Identity OID : $MI_OID"
echo " App Insights : $APP_INSIGHTS"
echo ""
echo " Watch invocations live:"
echo "   func azure functionapp logstream $FUNC_APP"
echo ""
echo " Portal - invocations:"
echo "   https://portal.azure.com/#@/resource/subscriptions/$SUB_ID/resourceGroups/$RG/providers/Microsoft.Web/sites/$FUNC_APP/functionsList"
echo ""
echo " App Insights (Kusto) - reconciliation events & lag percentiles:"
cat <<'KUSTO'
   traces
   | where message has 'cdc_reconcile'
   | extend d = parse_json(message)
   | summarize count(),
       p50=percentile(toint(d.lag_ms),50),
       p90=percentile(toint(d.lag_ms),90),
       p99=percentile(toint(d.lag_ms),99)
     by tostring(d.op), tostring(d.action)

   // custom metrics:
   customMetrics
   | where name in ('cdc_lag_ms','cdc_commit_lag_ms')
   | summarize p50=percentile(value,50), p90=percentile(value,90), p99=percentile(value,99) by name
KUSTO
echo "=============================================================="

#!/bin/bash
# T4 (D7-51, Azure): the cloud lab's Azure side, run once in Azure Cloud Shell (Bash),
# signed in as the subscription's owner. Region: germanywestcentral (westeurope refuses new
# customers, 2 Nov 2026); another with  R=swedencentral bash setup-lab.sh
#   resource group  assertix-lab
#   storage         assertixlab<suffix> (ADLS Gen2): containers raw (the daily orders files)
#                   and artifacts (the runs' results and build info)
#   Azure SQL       server assertix-lab-<suffix>, database orders (free offer: auto-pauses
#                   at the free limit, never bills), Entra-only logins, you as Entra admin
#   Data Factory    assertix-lab-adf-<suffix> (its managed identity loads files and tables)
#   logins          orders-pipeline   the team's CI: deploys the factory's pipelines
#                   assertix-reader   Assertix: reads the database (db_datareader) and the
#                                     artifacts container, starts / cancels / reads the factory's runs
# The two logins' secrets are written to ~/assertix-azure-secrets.env (chmod 600), never
# printed: download that file (Cloud Shell: Manage files > Download) and run
# set-azure-keys.sh (Assertix repo, data-lab/cloud/azure) on the Mac. Safe to run again (keeps what exists;
# a run again makes NEW secrets only when the secrets file is missing).
set -uo pipefail
R=${R:-germanywestcentral}
RG=assertix-lab
SUB=$(az account show --query id -o tsv)
TENANT=$(az account show --query tenantId -o tsv)
ME=$(az ad signed-in-user show --query id -o tsv)
MYNAME=$(az ad signed-in-user show --query userPrincipalName -o tsv)
SUFFIX=$(echo -n "$SUB" | md5sum | cut -c1-6)
ST=assertixlab$SUFFIX
SQL=assertix-lab-$SUFFIX
ADF=assertix-lab-adf-$SUFFIX
SECRETS=$HOME/assertix-azure-secrets.env
ok() { echo "  $*"; }
fail() { echo "  FAILED: $*"; exit 1; }

echo "== providers"
for p in Microsoft.Storage Microsoft.Sql Microsoft.DataFactory; do
  az provider register -n $p >/dev/null 2>&1; done; ok "registered (may take a minute the first time)"
az extension add -n datafactory --only-show-errors >/dev/null 2>&1

echo "== resource group $RG"
az group show -n $RG -o none 2>/dev/null && ok "exists" || { az group create -n $RG -l $R -o none && ok created; }

echo "== storage $ST (ADLS Gen2)"
az storage account show -n $ST -g $RG -o none 2>/dev/null || az storage account create -n $ST -g $RG -l $R \
  --sku Standard_LRS --kind StorageV2 --hns true --min-tls-version TLS1_2 --allow-blob-public-access false -o none \
  || fail "storage account $ST in $R (try another region: R=swedencentral bash setup-lab.sh)"
ok ready
az role assignment create --assignee "$ME" --role "Storage Blob Data Contributor" \
  --scope "$(az storage account show -n $ST -g $RG --query id -o tsv)" -o none 2>/dev/null
for c in raw artifacts; do
  az storage container create --account-name $ST -n $c --auth-mode login -o none 2>/dev/null \
    || az storage container create --account-name $ST -n $c -o none 2>/dev/null
  ok "container $c"
done

echo "== Azure SQL $SQL / orders (free offer)"
az sql server show -n $SQL -g $RG -o none 2>/dev/null || az sql server create -n $SQL -g $RG -l $R \
  --enable-ad-only-auth --external-admin-principal-type User --external-admin-name "$MYNAME" \
  --external-admin-sid "$ME" -o none || fail "SQL server $SQL in $R"
ok "server ready (Entra logins only)"
az sql db show -n orders -s $SQL -g $RG -o none 2>/dev/null || az sql db create -n orders -s $SQL -g $RG \
  --edition GeneralPurpose --compute-model Serverless --family Gen5 --capacity 2 \
  --use-free-limit --free-limit-exhaustion-behavior AutoPause -o none || fail "database orders (free offer)"
ok "database orders ready (free offer, auto-pause)"
# Azure services (Data Factory) and Assertix on a laptop (changing addresses): the lab opens the
# firewall; only Entra logins exist, no passwords. A team would allow its own addresses.
az sql server firewall-rule create -s $SQL -g $RG -n azure-services --start-ip-address 0.0.0.0 \
  --end-ip-address 0.0.0.0 -o none 2>/dev/null
az sql server firewall-rule create -s $SQL -g $RG -n lab-anywhere --start-ip-address 0.0.0.0 \
  --end-ip-address 255.255.255.255 -o none 2>/dev/null
ok "firewall: Azure services and the lab"

echo "== Data Factory $ADF"
az datafactory show -n $ADF -g $RG -o none 2>/dev/null || az datafactory create -n $ADF -g $RG -l $R -o none \
  || fail "Data Factory $ADF in $R"
ADF_MI=$(az datafactory show -n $ADF -g $RG --query identity.principalId -o tsv)
az role assignment create --assignee-object-id "$ADF_MI" --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" --scope "$(az storage account show -n $ST -g $RG --query id -o tsv)" -o none 2>/dev/null
ok "factory ready; its identity may write the storage"

echo "== logins"
mk_sp() {   # name -> appId (creates the app + service principal once)
  local id; id=$(az ad app list --display-name "$1" --query "[0].appId" -o tsv)
  if [ -z "$id" ]; then id=$(az ad app create --display-name "$1" --query appId -o tsv); fi
  az ad sp show --id "$id" -o none 2>/dev/null || az ad sp create --id "$id" -o none
  echo "$id"
}
PIPE_ID=$(mk_sp orders-pipeline); READ_ID=$(mk_sp assertix-reader)
sleep 15   # a new service principal takes a moment to be usable in role assignments
FACTORY_ID=$(az datafactory show -n $ADF -g $RG --query id -o tsv)
ART_ID="$(az storage account show -n $ST -g $RG --query id -o tsv)/blobServices/default/containers/artifacts"
# the team's CI deploys the factory's pipelines, datasets and linked services
az role assignment create --assignee "$PIPE_ID" --role "Data Factory Contributor" --scope "$FACTORY_ID" -o none 2>/dev/null
ok "orders-pipeline: Data Factory Contributor on the factory"
# Assertix: start, cancel and read runs of this one factory; read the results container
[ -n "$(az role definition list --custom-role-only true --name "Assertix pipeline runner" --query "[0].name" -o tsv)" ] || az role definition create -o none --role-definition "{
  \"Name\": \"Assertix pipeline runner\", \"Description\": \"Start, cancel and read Data Factory runs; read factories\",
  \"Actions\": [\"Microsoft.DataFactory/factories/read\", \"Microsoft.DataFactory/factories/pipelines/read\",
    \"Microsoft.DataFactory/factories/pipelines/createrun/action\", \"Microsoft.DataFactory/factories/pipelineruns/read\",
    \"Microsoft.DataFactory/factories/pipelineruns/cancel/action\", \"Microsoft.DataFactory/factories/pipelineruns/queryactivityruns/action\",
    \"Microsoft.DataFactory/factories/querypipelineruns/read\"],
  \"AssignableScopes\": [\"/subscriptions/$SUB/resourceGroups/$RG\"]}"
sleep 10
az role assignment create --assignee "$READ_ID" --role "Assertix pipeline runner" --scope "$FACTORY_ID" -o none 2>/dev/null
az role assignment create --assignee "$READ_ID" --role "Storage Blob Data Reader" --scope "$ART_ID" -o none 2>/dev/null
ok "assertix-reader: runs of the factory, read the artifacts container"

echo "== database users (as you, the Entra admin)"
SQLCMD=$(command -v sqlcmd || true)
# by object id (the enterprise application's), so the server needs no directory read rights
READ_OID=$(az ad sp show --id "$READ_ID" --query id -o tsv)
T="
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '$ADF') CREATE USER [$ADF] FROM EXTERNAL PROVIDER WITH OBJECT_ID = '$ADF_MI';
ALTER ROLE db_owner ADD MEMBER [$ADF];
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'assertix-reader') CREATE USER [assertix-reader] FROM EXTERNAL PROVIDER WITH OBJECT_ID = '$READ_OID';
ALTER ROLE db_datareader ADD MEMBER [assertix-reader];"
if [ -n "$SQLCMD" ]; then
  sqlcmd -S "$SQL.database.windows.net" -d orders --authentication-method ActiveDirectoryAzCli -Q "$T" -b \
    && ok "users: the factory (db_owner), assertix-reader (db_datareader)" \
    || echo "  FAILED: run the T-SQL below in the portal (SQL database orders > Query editor):$T"
else
  echo "  no sqlcmd here: run this in the portal (SQL database orders > Query editor):$T"
fi

echo "== secrets"
if [ -f "$SECRETS" ]; then
  ok "$SECRETS exists: kept (delete it to make new secrets)"
else
  PIPE_SECRET=$(az ad app credential reset --id "$PIPE_ID" --display-name lab --years 1 --query password -o tsv 2>/dev/null)
  READ_SECRET=$(az ad app credential reset --id "$READ_ID" --display-name lab --years 1 --query password -o tsv 2>/dev/null)
  umask 077
  cat > "$SECRETS" <<EOV
AZ_TENANT=$TENANT
AZ_SUBSCRIPTION=$SUB
AZ_RESOURCE_GROUP=$RG
AZ_REGION=$R
AZ_STORAGE=$ST
AZ_SQL_SERVER=$SQL.database.windows.net
AZ_SQL_DATABASE=orders
AZ_FACTORY=$ADF
AZ_PIPELINE_CLIENT_ID=$PIPE_ID
AZ_PIPELINE_CLIENT_SECRET=$PIPE_SECRET
AZ_READER_CLIENT_ID=$READ_ID
AZ_READER_CLIENT_SECRET=$READ_SECRET
EOV
  ok "written to $SECRETS (values not shown)"
fi
echo "== done: $RG in $R — storage $ST, SQL $SQL/orders, factory $ADF"
echo "   next: download $SECRETS (Manage files > Download file > assertix-azure-secrets.env)"

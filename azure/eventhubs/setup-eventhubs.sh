#!/bin/bash
# S4 (Kafka on Azure Event Hubs): the orders event stream on Azure, as an Azure team sets it up. Run in
# Azure Cloud Shell (Bash), signed in to the lab subscription. Safe to run again.
#   namespace   assertix-lab-eh-<suffix>, Standard tier (the cheapest that speaks Kafka), 1 throughput unit,
#               in resource group assertix-lab (germanywestcentral; another with R=<region>)
#   event hubs  (Kafka topics) order-events (the shop's), order-events-ci (tests), and their dead-letter
#               hubs order-events-dlq and order-events-dlq-ci; 1 partition each, kept 1 day
#   logins      (rules: a connection string each, with only the rights it needs)
#               orders-processor  namespace: Listen + Send (the team's processor reads, writes dead letters)
#               assertix-reader   namespace: Listen (Assertix reads the hubs and the processor's progress)
#               assertix-writer   order-events-ci only: Send (Assertix' test events)
#               shop-app          order-events only: Send (the shop)
# The connection strings are written to ~/assertix-eventhubs.txt (only you can read it), never shown:
# download it (Manage files > Download) and run data-lab/cloud/azure/set-eventhubs-keys.sh on the Mac.
# Billed by the hour while the namespace exists: teardown-eventhubs.sh deletes it.
set -euo pipefail
RG=assertix-lab
R=${R:-germanywestcentral}
SUB=$(az account show --query id -o tsv)
NS=assertix-lab-eh-$(echo -n "$SUB" | md5sum | cut -c1-6)
say() { echo "== $*"; }
az group create -n $RG -l $R -o none
if az eventhubs namespace show -g $RG -n $NS -o none 2>/dev/null; then
  say "namespace $NS exists"
else
  az eventhubs namespace create -g $RG -n $NS -l $R --sku Standard --capacity 1 -o none
  say "namespace $NS created (Standard, 1 throughput unit, Kafka on $NS.servicebus.windows.net:9093)"
fi
for H in order-events order-events-ci order-events-dlq order-events-dlq-ci; do
  if az eventhubs eventhub show -g $RG --namespace-name $NS -n $H -o none 2>/dev/null; then
    say "event hub $H exists"
  else
    az eventhubs eventhub create -g $RG --namespace-name $NS -n $H --partition-count 1 \
      --cleanup-policy Delete --retention-time-in-hours 24 -o none 2>/dev/null \
      || az eventhubs eventhub create -g $RG --namespace-name $NS -n $H --partition-count 1 --message-retention 1 -o none
    say "event hub $H created (1 partition, kept 1 day)"
  fi
done
nsrule() {
  az eventhubs namespace authorization-rule create -g $RG --namespace-name $NS -n "$1" --rights "${@:2}" -o none
  az eventhubs namespace authorization-rule keys list -g $RG --namespace-name $NS -n "$1" \
    --query primaryConnectionString -o tsv
}
hubrule() {
  az eventhubs eventhub authorization-rule create -g $RG --namespace-name $NS --eventhub-name "$2" -n "$1" \
    --rights Send -o none
  az eventhubs eventhub authorization-rule keys list -g $RG --namespace-name $NS --eventhub-name "$2" -n "$1" \
    --query primaryConnectionString -o tsv
}
umask 077
{
  echo "NAMESPACE=$NS"
  echo "PROCESSOR=$(nsrule orders-processor Listen Send)"
  echo "READER=$(nsrule assertix-reader Listen)"
  echo "WRITER=$(hubrule assertix-writer order-events-ci)"
  echo "SHOP=$(hubrule shop-app order-events)"
} > ~/assertix-eventhubs.txt
say "logins orders-processor (Listen, Send), assertix-reader (Listen), assertix-writer (Send on order-events-ci),"
say "  shop-app (Send on order-events): connection strings in ~/assertix-eventhubs.txt (not shown): download that file"
say "done: namespace $NS in $RG ($R)"

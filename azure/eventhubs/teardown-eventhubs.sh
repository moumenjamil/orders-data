#!/bin/bash
# S4: stops the Event Hubs lab from billing: deletes the namespace (its hubs, logins and the events in them).
# setup-eventhubs.sh makes it again, with new connection strings. Run in Azure Cloud Shell.
set -uo pipefail
RG=assertix-lab
SUB=$(az account show --query id -o tsv)
NS=assertix-lab-eh-$(echo -n "$SUB" | md5sum | cut -c1-6)
az eventhubs namespace delete -g $RG -n $NS -o none 2>/dev/null && echo "namespace $NS deleted" \
  || echo "namespace $NS not there"
rm -f ~/assertix-eventhubs.txt
echo "done: then run data-lab/cloud/azure/set-eventhubs-keys.sh off on the Mac (turns the workflow off)"

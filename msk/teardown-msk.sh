#!/bin/bash
# S4: stops the MSK lab from billing: deletes the cluster assertix-lab-msk (its brokers, public addresses, topics
# and the events in them) and the key files here. Kept (free): the logins, their rights, the security group.
# setup-msk.sh makes the cluster again (new address, new keys for the processor and the shop). Run in AWS CloudShell.
set -uo pipefail
export AWS_DEFAULT_REGION=eu-north-1
NAME=assertix-lab-msk
ARN=$(aws kafka list-clusters-v2 --cluster-name-filter $NAME --query "ClusterInfoList[?ClusterName=='$NAME'].ClusterArn | [0]" --output text)
if [ "$ARN" = None ] || [ -z "$ARN" ]; then
  echo "cluster $NAME not there"
else
  aws kafka delete-cluster --cluster-arn "$ARN" >/dev/null && echo "cluster $NAME is being deleted (takes some minutes; billing stops)"
fi
for U in assertix-lab-msk-processor assertix-lab-msk-shop; do
  for K in $(aws iam list-access-keys --user-name $U --query 'AccessKeyMetadata[].AccessKeyId' --output text 2>/dev/null); do
    aws iam delete-access-key --user-name $U --access-key-id $K && echo "key of $U deleted"
  done
done
rm -f ~/assertix-msk.txt ~/.assertix-msk-processor.key ~/.assertix-msk-shop.key ~/.assertix-msk-boot ~/msk_client.py
echo "done: then run data-lab/cloud/aws/set-msk-keys.sh off on the Mac (turns the workflow off)"

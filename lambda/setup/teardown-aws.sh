#!/bin/bash
# Stops the orders stream on AWS from billing: deletes the two Kinesis streams (billed by the hour) and
# what feeds the functions from them. The tables, queues and functions stay (they cost nothing idle);
# setup-aws.sh makes the streams again. Run in AWS CloudShell.
set -uo pipefail
export AWS_DEFAULT_REGION=eu-north-1
ACCT=$(aws sts get-caller-identity --query Account --output text)
for F in orders-live orders-live-ci; do
  for U in $(aws lambda list-event-source-mappings --function-name $F --query 'EventSourceMappings[].UUID' --output text); do
    aws lambda delete-event-source-mapping --uuid "$U" >/dev/null && echo "function $F: feed $U deleted"
  done
done
for S in order-events order-events-ci; do
  aws kinesis delete-stream --stream-name $S --enforce-consumer-deletion >/dev/null 2>&1 && echo "stream $S deleted" \
    || echo "stream $S not there"
done
echo "done: account $ACCT; tables, queues and functions kept"

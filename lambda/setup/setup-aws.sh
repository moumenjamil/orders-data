#!/bin/bash
# The orders stream on AWS (Assertix S3 lab), run in AWS CloudShell (signed in as the account owner),
# region eu-north-1. A team on AWS, serverless: the shop puts order events on Kinesis; a Lambda function
# that Kinesis feeds keeps orders_live in DynamoDB; events it cannot use go to an SQS queue.
#
#   Kinesis    order-events (live), order-events-ci (test): 1 shard each (billed by the hour: delete them
#              with teardown-aws.sh when not testing; this script makes them again)
#   DynamoDB   orders_live, orders_live_ci (one item per order); orders_live_seen, orders_live_seen_ci
#              (the event ids applied, kept 7 days): on demand
#   SQS        order-events-dlq, order-events-dlq-ci (the dead letters, with the reason);
#              order-events-failed, order-events-failed-ci (batches AWS gave up on after a crash)
#   Lambda     orders-live (live stream -> orders_live), orders-live-ci (test stream -> orders_live_ci),
#              role orders-live-lambda; each fed by its stream (2 retries, then the failure queue)
#   logins     assertix-lab-pipeline (exists, D7-45): may also deploy the two functions (the team's CI)
#              assertix-lab-reader (exists): may also read the streams' figures, the dead-letter queues,
#              the tables, the functions and their logs (read only)
#              assertix-lab-stream-writer (new): may write order-events-ci only; its key is printed ONCE
#              at the end: type it into Assertix' set-aws-stream-keys.sh (hidden), then clear the screen
# Safe to run again: existing pieces are kept (no new key when the writer already has one).
set -uo pipefail
R=eu-north-1
export AWS_DEFAULT_REGION=$R
ACCT=$(aws sts get-caller-identity --query Account --output text)
RAW=https://raw.githubusercontent.com/moumenjamil/orders-data/main/lambda
say() { echo "$@"; }

say "== Kinesis"
for S in order-events order-events-ci; do
  aws kinesis create-stream --stream-name $S --shard-count 1 >/dev/null 2>&1 && say "stream $S created" || say "stream $S exists"
done
for S in order-events order-events-ci; do aws kinesis wait stream-exists --stream-name $S; done

say "== DynamoDB"
table() {  # name key
  aws dynamodb create-table --table-name $1 --attribute-definitions AttributeName=$2,AttributeType=S \
    --key-schema AttributeName=$2,KeyType=HASH --billing-mode PAY_PER_REQUEST >/dev/null 2>&1 \
    && say "table $1 created" || say "table $1 exists"
}
table orders_live order_id; table orders_live_ci order_id
table orders_live_seen event_id; table orders_live_seen_ci event_id
for T in orders_live orders_live_ci orders_live_seen orders_live_seen_ci; do aws dynamodb wait table-exists --table-name $T; done
for T in orders_live_seen orders_live_seen_ci; do
  aws dynamodb update-time-to-live --table-name $T --time-to-live-specification Enabled=true,AttributeName=expires \
    >/dev/null 2>&1 && say "table $T forgets ids after 7 days" || true
done

say "== SQS"
for Q in order-events-dlq order-events-dlq-ci order-events-failed order-events-failed-ci; do
  aws sqs create-queue --queue-name $Q --attributes MessageRetentionPeriod=345600 >/dev/null && say "queue $Q ready"
done
Q=https://sqs.$R.amazonaws.com/$ACCT

say "== the function's role"
aws iam create-role --role-name orders-live-lambda --assume-role-policy-document \
  '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
  >/dev/null 2>&1 && say "role orders-live-lambda created" || say "role orders-live-lambda exists"
aws iam attach-role-policy --role-name orders-live-lambda --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam put-role-policy --role-name orders-live-lambda --policy-name orders-live-data --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[
 {\"Effect\":\"Allow\",\"Action\":[\"kinesis:GetRecords\",\"kinesis:GetShardIterator\",\"kinesis:DescribeStream\",\"kinesis:DescribeStreamSummary\",\"kinesis:ListShards\",\"kinesis:ListStreams\"],
  \"Resource\":[\"arn:aws:kinesis:$R:$ACCT:stream/order-events\",\"arn:aws:kinesis:$R:$ACCT:stream/order-events-ci\"]},
 {\"Effect\":\"Allow\",\"Action\":[\"dynamodb:PutItem\",\"dynamodb:UpdateItem\",\"dynamodb:GetItem\"],
  \"Resource\":[\"arn:aws:dynamodb:$R:$ACCT:table/orders_live\",\"arn:aws:dynamodb:$R:$ACCT:table/orders_live_ci\",
   \"arn:aws:dynamodb:$R:$ACCT:table/orders_live_seen\",\"arn:aws:dynamodb:$R:$ACCT:table/orders_live_seen_ci\"]},
 {\"Effect\":\"Allow\",\"Action\":[\"sqs:SendMessage\"],\"Resource\":[\"arn:aws:sqs:$R:$ACCT:order-events-dlq\",
   \"arn:aws:sqs:$R:$ACCT:order-events-dlq-ci\",\"arn:aws:sqs:$R:$ACCT:order-events-failed\",\"arn:aws:sqs:$R:$ACCT:order-events-failed-ci\"]}]}" \
  && say "role orders-live-lambda: its streams, tables and queues"
sleep 10   # a new role takes a moment before Lambda accepts it

say "== the functions"
mkdir -p ~/orders-lambda && curl -fsSL "$RAW/app.py" -o ~/orders-lambda/app.py && (cd ~/orders-lambda && rm -f f.zip && zip -q f.zip app.py)
fn() {  # name table seen dlq stream failed
  if aws lambda get-function --function-name $1 >/dev/null 2>&1; then
    say "function $1 exists (the team's deploy updates its code)"
  else
    aws lambda create-function --function-name $1 --runtime python3.12 --handler app.handler --timeout 30 --memory-size 256 \
      --role arn:aws:iam::$ACCT:role/orders-live-lambda --zip-file fileb://$HOME/orders-lambda/f.zip \
      --environment "Variables={TABLE=$2,SEEN_TABLE=$3,DLQ_URL=$Q/$4,COMMIT=setup}" >/dev/null && say "function $1 created"
    aws lambda wait function-active-v2 --function-name $1
  fi
  if [ -z "$(aws lambda list-event-source-mappings --function-name $1 --event-source-arn arn:aws:kinesis:$R:$ACCT:stream/$5 --query 'EventSourceMappings[0].UUID' --output text | grep -v None)" ]; then
    aws lambda create-event-source-mapping --function-name $1 --event-source-arn arn:aws:kinesis:$R:$ACCT:stream/$5 \
      --starting-position LATEST --batch-size 100 --maximum-batching-window-in-seconds 1 --maximum-retry-attempts 2 \
      --bisect-batch-on-function-error --maximum-record-age-in-seconds 600 \
      --destination-config "{\"OnFailure\":{\"Destination\":\"arn:aws:sqs:$R:$ACCT:$6\"}}" >/dev/null \
      && say "function $1 fed by $5 (2 retries, then $6)"
  else
    say "function $1 already fed by $5"
  fi
}
fn orders-live orders_live orders_live_seen order-events-dlq order-events order-events-failed
fn orders-live-ci orders_live_ci orders_live_seen_ci order-events-dlq-ci order-events-ci order-events-failed-ci

say "== logins"
FNS="\"arn:aws:lambda:$R:$ACCT:function:orders-live\",\"arn:aws:lambda:$R:$ACCT:function:orders-live-ci\""
aws iam put-user-policy --user-name assertix-lab-pipeline --policy-name assertix-lab-pipeline-lambda --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[
 {\"Effect\":\"Allow\",\"Action\":[\"lambda:UpdateFunctionCode\",\"lambda:UpdateFunctionConfiguration\",\"lambda:GetFunctionConfiguration\",\"lambda:GetFunction\"],\"Resource\":[$FNS]}]}" \
  && say "assertix-lab-pipeline: may deploy orders-live and orders-live-ci"
aws iam put-user-policy --user-name assertix-lab-reader --policy-name assertix-lab-reader-streams --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[
 {\"Effect\":\"Allow\",\"Action\":[\"kinesis:DescribeStreamSummary\"],\"Resource\":[\"arn:aws:kinesis:$R:$ACCT:stream/order-events\",\"arn:aws:kinesis:$R:$ACCT:stream/order-events-ci\"]},
 {\"Effect\":\"Allow\",\"Action\":[\"cloudwatch:GetMetricStatistics\"],\"Resource\":\"*\"},
 {\"Effect\":\"Allow\",\"Action\":[\"sqs:GetQueueUrl\",\"sqs:GetQueueAttributes\",\"sqs:ReceiveMessage\"],\"Resource\":[\"arn:aws:sqs:$R:$ACCT:order-events-dlq\",\"arn:aws:sqs:$R:$ACCT:order-events-dlq-ci\"]},
 {\"Effect\":\"Allow\",\"Action\":[\"dynamodb:DescribeTable\",\"dynamodb:GetItem\",\"dynamodb:BatchGetItem\",\"dynamodb:Scan\"],\"Resource\":[\"arn:aws:dynamodb:$R:$ACCT:table/orders_live\",\"arn:aws:dynamodb:$R:$ACCT:table/orders_live_ci\"]},
 {\"Effect\":\"Allow\",\"Action\":[\"lambda:GetFunctionConfiguration\"],\"Resource\":[$FNS]},
 {\"Effect\":\"Allow\",\"Action\":[\"lambda:ListEventSourceMappings\"],\"Resource\":\"*\"},
 {\"Effect\":\"Allow\",\"Action\":[\"logs:FilterLogEvents\"],\"Resource\":[\"arn:aws:logs:$R:$ACCT:log-group:/aws/lambda/orders-live:*\",\"arn:aws:logs:$R:$ACCT:log-group:/aws/lambda/orders-live-ci:*\"]}]}" \
  && say "assertix-lab-reader: may read the streams' figures, the dead letters, the tables, the functions and their logs"
aws iam create-user --user-name assertix-lab-stream-writer >/dev/null 2>&1 && say "user assertix-lab-stream-writer created" \
  || say "user assertix-lab-stream-writer exists"
aws iam put-user-policy --user-name assertix-lab-stream-writer --policy-name assertix-lab-stream-writer --policy-document \
  "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"kinesis:PutRecords\",\"kinesis:PutRecord\"],\"Resource\":\"arn:aws:kinesis:$R:$ACCT:stream/order-events-ci\"}]}" \
  && say "assertix-lab-stream-writer: may write order-events-ci only"

say "== done: account $ACCT, region $R"
if [ "$(aws iam list-access-keys --user-name assertix-lab-stream-writer --query 'length(AccessKeyMetadata)' --output text)" = "0" ]; then
  say "the writer's key (shown once: type it into set-aws-stream-keys.sh, then run: clear):"
  aws iam create-access-key --user-name assertix-lab-stream-writer --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text
else
  say "assertix-lab-stream-writer already has a key (delete it in IAM to make a new one)"
fi

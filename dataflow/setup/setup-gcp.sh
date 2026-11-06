#!/bin/bash
# The orders stream on Google Cloud (Assertix S2 lab), run once in Cloud Shell (signed in as the project
# owner), project assertix-lab, region europe-west3. A team on Google Cloud: the shop publishes
# order events to Pub/Sub; a Dataflow job (Apache Beam) keeps orders_live in BigQuery; events it
# cannot use go to a dead-letter topic.
#
#   Pub/Sub    schema order-event (Protocol Buffer, JSON messages), on every order topic
#              order-events       the live topic; subscription order-events-dataflow (the live job's)
#              order-events-ci    the test topic; subscription order-events-ci-dataflow (a test job's)
#              order-events-dlq, order-events-dlq-ci   dead letters; Assertix reads them through its own
#                                 subscriptions assertix-read-dlq, assertix-read-dlq-ci (keep acked messages)
#   BigQuery   orders_lab.order_events_live, orders_ci.order_events_live (every event the job accepts);
#              the views orders_live on them (each order's latest state) are the team's SQL, applied by
#              its CI on every deploy
#   Storage    gs://assertix-lab-dataflow-<number>  Flex Templates (templates/) and the jobs' temp files
#   Artifact Registry  europe-west3-docker.pkg.dev/assertix-lab/dataflow  the job's images
#   logins     orders-dataflow   the jobs' worker account (reads subscriptions, writes BigQuery and dead letters)
#              orders-ci         the team's GitHub Actions: builds images and templates, applies the views,
#                                starts and stops the live job
#              shop-app          the shop: publishes to order-events only
#              assertix-runner   Assertix: starts and cancels test jobs (as orders-dataflow), nothing else
#              assertix-writer   Assertix: publishes to order-events-ci only
#              assertix-reader   (exists, T2) also reads the topics, its own subscriptions and the monitoring
#                                figures (a custom role with exactly those rights)
# Creates keys for orders-ci, shop-app, assertix-runner and assertix-writer in ~/stream-keys and asks
# Cloud Shell to download them; prints no secret. Safe to run again: existing pieces are kept (a key
# is made only when its file is missing).
set -uo pipefail
P=assertix-lab
R=europe-west3
gcloud config set project "$P" >/dev/null
NUM=$(gcloud projects describe "$P" --format='value(projectNumber)')
BUCKET=assertix-lab-dataflow-$NUM
SA() { echo "$1@$P.iam.gserviceaccount.com"; }

echo "== APIs"
gcloud services enable pubsub.googleapis.com dataflow.googleapis.com compute.googleapis.com \
  artifactregistry.googleapis.com monitoring.googleapis.com bigquery.googleapis.com storage.googleapis.com \
  >/dev/null && echo "enabled"

echo "== storage and images"
gcloud storage buckets create "gs://$BUCKET" --location="$R" --uniform-bucket-level-access >/dev/null 2>&1 \
  && echo "gs://$BUCKET created" || echo "gs://$BUCKET exists"
gcloud artifacts repositories create dataflow --repository-format=docker --location="$R" \
  --description="Assertix lab: the orders_live Dataflow job" >/dev/null 2>&1 \
  && echo "repository dataflow created" || echo "repository dataflow exists"
# keep only the newest images: an image is about 2 GB
gcloud artifacts repositories set-cleanup-policies dataflow --location="$R" --no-dry-run \
  --policy=<(echo '[{"name":"keep-10","action":{"type":"Keep"},"mostRecentVersions":{"keepCount":10}},
                    {"name":"old","action":{"type":"Delete"},"condition":{"olderThan":"7d"}}]') >/dev/null 2>&1 \
  && echo "repository dataflow keeps the 10 newest images" || echo "cleanup policy not set (optional)"

echo "== Pub/Sub"
cat > /tmp/order-event.proto <<'PROTO'
syntax = "proto3";
message OrderEvent {
  string event_id = 1;
  string type = 2;
  string order_id = 3;
  string occurred_at = 4;
  int64 customer_id = 5;
  double amount = 6;
}
PROTO
gcloud pubsub schemas create order-event --type=protocol-buffer --definition-file=/tmp/order-event.proto \
  >/dev/null 2>&1 && echo "schema order-event created" || echo "schema order-event exists"
for T in order-events order-events-ci; do
  gcloud pubsub topics create "$T" --schema=order-event --message-encoding=json --message-retention-duration=1d \
    >/dev/null 2>&1 && echo "topic $T created (schema order-event)" || echo "topic $T exists"
done
for T in order-events-dlq order-events-dlq-ci; do
  gcloud pubsub topics create "$T" --message-retention-duration=1d >/dev/null 2>&1 \
    && echo "topic $T created" || echo "topic $T exists"
done
sub() {   # name topic [flags]
  local n=$1 t=$2; shift 2
  gcloud pubsub subscriptions create "$n" --topic="$t" "$@" >/dev/null 2>&1 \
    && echo "subscription $n created on $t" || echo "subscription $n exists"
}
sub order-events-dataflow order-events --ack-deadline=60 --message-retention-duration=1d
sub order-events-ci-dataflow order-events-ci --ack-deadline=60 --message-retention-duration=1d
sub assertix-read-dlq order-events-dlq --retain-acked-messages --message-retention-duration=1d
sub assertix-read-dlq-ci order-events-dlq-ci --retain-acked-messages --message-retention-duration=1d

echo "== BigQuery"
bq --location="$R" mk --dataset "$P:orders_ci" >/dev/null 2>&1 && echo "orders_ci created" || echo "orders_ci exists"
for D in orders_lab orders_ci; do
  bq --location="$R" mk --table --time_partitioning_field=received_at --time_partitioning_expiration=2592000 \
    --description="Every order event the Dataflow job accepted" "$P:$D.order_events_live" \
    event_id:STRING,type:STRING,order_id:STRING,customer_id:INT64,amount:NUMERIC,occurred_at:TIMESTAMP,received_at:TIMESTAMP,assertix_run:STRING \
    >/dev/null 2>&1 && echo "$D.order_events_live created" || echo "$D.order_events_live exists"
done

echo "== logins"
for A in orders-dataflow:"Dataflow workers (data team)" orders-ci:"GitHub Actions (data team)" \
         shop-app:"The shop (publishes order events)" assertix-runner:"Assertix: starts and stops test jobs" \
         assertix-writer:"Assertix: publishes test events"; do
  gcloud iam service-accounts create "${A%%:*}" --display-name "${A#*:}" >/dev/null 2>&1 \
    && echo "${A%%:*} created" || echo "${A%%:*} exists"
done
sleep 10   # a new service account takes a moment to be usable in policies

proj() { gcloud projects add-iam-policy-binding "$P" --member="serviceAccount:$1" --role="$2" --condition=None \
  >/dev/null && echo "  $1: $2 (project)"; }
topic() { gcloud pubsub topics add-iam-policy-binding "$1" --member="serviceAccount:$2" --role="$3" >/dev/null \
  && echo "  $2: $3 on topic $1"; }
subs() { gcloud pubsub subscriptions add-iam-policy-binding "$1" --member="serviceAccount:$2" --role="$3" >/dev/null \
  && echo "  $2: $3 on subscription $1"; }
bucket() { gcloud storage buckets add-iam-policy-binding "gs://$1" --member="serviceAccount:$2" --role="$3" \
  >/dev/null && echo "  $2: $3 on gs://$1"; }
repo() { gcloud artifacts repositories add-iam-policy-binding dataflow --location="$R" \
  --member="serviceAccount:$1" --role="$2" >/dev/null && echo "  $1: $2 on repository dataflow"; }
actas() { gcloud iam service-accounts add-iam-policy-binding "$(SA orders-dataflow)" \
  --member="serviceAccount:$1" --role=roles/iam.serviceAccountUser >/dev/null \
  && echo "  $1: may run jobs as orders-dataflow"; }
dataset() { bq query --location="$R" --use_legacy_sql=false --quiet \
  "GRANT \`$3\` ON SCHEMA \`$P.$1\` TO \"serviceAccount:$2\"" >/dev/null && echo "  $2: $3 on $1" \
  || echo "  FAILED: $3 on $1 for $2"; }

W=$(SA orders-dataflow); CI=$(SA orders-ci); SHOP=$(SA shop-app)
RUN=$(SA assertix-runner); WR=$(SA assertix-writer); RD=$(SA assertix-reader)
# the workers: Dataflow's own worker rights, read both subscriptions, write dead letters and BigQuery
proj "$W" roles/dataflow.worker
proj "$W" roles/bigquery.jobUser
dataset orders_lab "$W" roles/bigquery.dataEditor
dataset orders_ci "$W" roles/bigquery.dataEditor
for S in order-events-dataflow order-events-ci-dataflow; do subs "$S" "$W" roles/pubsub.subscriber; done
proj "$W" roles/pubsub.viewer
for T in order-events-dlq order-events-dlq-ci; do topic "$T" "$W" roles/pubsub.publisher; done
bucket "$BUCKET" "$W" roles/storage.objectAdmin
repo "$W" roles/artifactregistry.reader
# the team's CI: images, templates, the views, the live job
repo "$CI" roles/artifactregistry.writer
bucket "$BUCKET" "$CI" roles/storage.objectAdmin
proj "$CI" roles/dataflow.developer
proj "$CI" roles/bigquery.jobUser
dataset orders_lab "$CI" roles/bigquery.dataEditor
dataset orders_ci "$CI" roles/bigquery.dataEditor
actas "$CI"
# the shop: the live topic only
topic order-events "$SHOP" roles/pubsub.publisher
# Assertix' runner: start and cancel jobs (as the workers), read the templates; no data rights
proj "$RUN" roles/dataflow.developer
bucket "$BUCKET" "$RUN" roles/storage.objectViewer
actas "$RUN"
# Assertix' writer: the test topic only
topic order-events-ci "$WR" roles/pubsub.publisher
# Assertix' reader: a role with exactly what it needs on the project, its own subscriptions, the views
if gcloud iam roles describe assertixStreamReader --project="$P" >/dev/null 2>&1; then
  echo "role assertixStreamReader exists"
else
  gcloud iam roles create assertixStreamReader --project="$P" --title="Assertix stream reader" \
    --permissions=pubsub.topics.get,pubsub.subscriptions.get,pubsub.schemas.get,pubsub.schemas.validate,monitoring.timeSeries.list \
    >/dev/null && echo "role assertixStreamReader created" || echo "FAILED: role assertixStreamReader (see the error above)"
fi
proj "$RD" "projects/$P/roles/assertixStreamReader"
for S in assertix-read-dlq assertix-read-dlq-ci; do subs "$S" "$RD" roles/pubsub.subscriber; done
dataset orders_ci "$RD" roles/bigquery.dataViewer

echo "== keys (Cloud Shell downloads them; move them with Assertix's data-lab/cloud/gcp/set-gcp-stream-keys.sh)"
mkdir -p ~/stream-keys && chmod 700 ~/stream-keys
for A in orders-ci shop-app assertix-runner assertix-writer; do
  F=~/stream-keys/$A.json
  if [ -f "$F" ]; then
    echo "kept $F"
  else
    gcloud iam service-accounts keys create "$F" --iam-account="$(SA $A)" >/dev/null 2>&1 \
      && chmod 600 "$F" && echo "key made: $A" || echo "FAILED: key for $A"
  fi
  [ -f "$F" ] && cloudshell download "$F" >/dev/null 2>&1 || true
done

echo "== done: project $P ($NUM), region $R, bucket gs://$BUCKET"

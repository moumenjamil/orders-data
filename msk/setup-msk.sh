#!/bin/bash
# S4 (Kafka on Amazon MSK): the orders event stream on AWS's managed Kafka, as an AWS team sets it up. Run in
# AWS CloudShell (region eu-north-1, signed in as the account owner). Safe to run again: it carries on where it
# stopped (creating the cluster and opening it to the internet each take 20-30 minutes; if CloudShell closes
# meanwhile, run it again).
#   cluster   assertix-lab-msk: provisioned, 2 brokers kafka.t3.small (10 GB each) in the default network's
#             public subnets; IAM login only, encrypted; then public access (AWS-provided addresses)
#             port 9198 (IAM, public) open in security group assertix-lab-msk (the login is still required)
#   topics    order-events (the shop's), order-events-ci (tests), order-events-dlq, order-events-dlq-ci:
#             1 partition, 2 copies, kept 1 day
#   logins    (IAM users, each with only the Kafka rights it needs)
#             assertix-lab-msk-processor (new): the team's processor: read and write the four topics, its
#                 consumer groups orders-live-msk*, create the topics; its key goes to the team's CI
#             assertix-lab-msk-shop (new): the shop: write order-events only (key kept here, for shop events)
#             assertix-lab-reader (exists): Assertix reads the topics and the processor's progress
#             assertix-lab-stream-writer (exists): Assertix writes order-events-ci only
# The processor's key and the brokers' address are written to ~/assertix-msk.txt (only you can read it), never
# shown: download it (Actions > Download file) and run data-lab/cloud/aws/set-msk-keys.sh on the Mac.
# Billed by the hour while the cluster exists (2 brokers and 2 public addresses): teardown-msk.sh deletes it.
set -uo pipefail
R=eu-north-1
export AWS_DEFAULT_REGION=$R
NAME=assertix-lab-msk
ACCT=$(aws sts get-caller-identity --query Account --output text)
RAW=https://raw.githubusercontent.com/moumenjamil/orders-data/${REF:-main}/msk
say() { echo "== $*"; }
die() { echo "== FAILED: $*"; exit 1; }

state() { aws kafka describe-cluster-v2 --cluster-arn "$ARN" --query ClusterInfo.State --output text; }
wait_active() {  # what we wait for
  local s t=0
  while :; do
    s=$(state)
    [ "$s" = ACTIVE ] && return 0
    [ "$s" = FAILED ] && die "the cluster is FAILED (see Amazon MSK in the console)"
    [ $((t % 300)) -eq 0 ] && say "waiting for $1: $s ($((t / 60)) min)"
    sleep 30; t=$((t + 30))
  done
}

say "network: the default VPC's public subnets"
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
[ "$VPC" != None ] || die "this region has no default VPC"
# two subnets in two zones (MSK needs one broker per zone); default subnets give public addresses
SUBNETS=$(aws ec2 describe-subnets --filters Name=vpc-id,Values=$VPC Name=default-for-az,Values=true \
  --query 'sort_by(Subnets,&AvailabilityZone)[0:2].SubnetId' --output text | tr '\t' ' ')
[ "$(echo $SUBNETS | wc -w)" = 2 ] || die "two default subnets in two zones are needed"
SG=$(aws ec2 describe-security-groups --filters Name=vpc-id,Values=$VPC Name=group-name,Values=$NAME \
  --query 'SecurityGroups[0].GroupId' --output text)
if [ "$SG" = None ]; then
  SG=$(aws ec2 create-security-group --group-name $NAME --vpc-id $VPC --description "Assertix lab MSK: IAM port" \
    --query GroupId --output text)
  aws ec2 authorize-security-group-ingress --group-id $SG --protocol tcp --port 9198 --cidr 0.0.0.0/0 >/dev/null
  aws ec2 authorize-security-group-ingress --group-id $SG --protocol -1 --source-group $SG >/dev/null
  say "security group $SG: port 9198 (IAM login, public), brokers to each other"
else
  say "security group $SG exists"
fi

ARN=$(aws kafka list-clusters-v2 --cluster-name-filter $NAME --query "ClusterInfoList[?ClusterName=='$NAME'].ClusterArn | [0]" --output text)
if [ "$ARN" = None ] || [ -z "$ARN" ]; then
  # the newest Kafka 3 version MSK offers with ZooKeeper (small brokers)
  V=$(aws kafka list-kafka-versions --query "KafkaVersions[?Status=='ACTIVE'].Version" --output text | tr '\t' '\n' \
    | grep -E '^3\.[0-9]+\.[0-9x]+$' | sort -V | tail -1)
  [ -n "$V" ] || die "no Kafka 3 version offered"
  ARN=$(aws kafka create-cluster-v2 --cluster-name $NAME --provisioned "{
    \"KafkaVersion\": \"$V\", \"NumberOfBrokerNodes\": 2,
    \"BrokerNodeGroupInfo\": {\"InstanceType\": \"kafka.t3.small\", \"ClientSubnets\": [\"${SUBNETS// /\",\"}\"],
      \"SecurityGroups\": [\"$SG\"], \"StorageInfo\": {\"EbsStorageInfo\": {\"VolumeSize\": 10}}},
    \"ClientAuthentication\": {\"Sasl\": {\"Iam\": {\"Enabled\": true}}, \"Unauthenticated\": {\"Enabled\": false}},
    \"EncryptionInfo\": {\"EncryptionInTransit\": {\"ClientBroker\": \"TLS\", \"InCluster\": true}}}" \
    --query ClusterArn --output text) || die "the cluster could not be created (see the error above)"
  say "cluster $NAME created (Kafka $V, 2 x kafka.t3.small, IAM login only)"
else
  say "cluster $NAME exists"
fi
wait_active "the cluster"

PUBLIC=$(aws kafka describe-cluster-v2 --cluster-arn "$ARN" \
  --query ClusterInfo.Provisioned.BrokerNodeGroupInfo.ConnectivityInfo.PublicAccess.Type --output text)
if [ "$PUBLIC" != SERVICE_PROVIDED_EIPS ]; then
  CUR=$(aws kafka describe-cluster-v2 --cluster-arn "$ARN" --query ClusterInfo.CurrentVersion --output text)
  aws kafka update-connectivity --cluster-arn "$ARN" --current-version "$CUR" \
    --connectivity-info '{"PublicAccess": {"Type": "SERVICE_PROVIDED_EIPS"}}' >/dev/null \
    || die "public access could not be turned on (see the error above)"
  say "public access requested"
  sleep 60
  wait_active "public access"
fi
BOOT=$(aws kafka get-bootstrap-brokers --cluster-arn "$ARN" --query BootstrapBrokerStringPublicSaslIam --output text)
[[ $BOOT == *.amazonaws.com:9198* ]] || die "the cluster has no public IAM address yet"
say "public brokers (IAM): $BOOT"

say "logins"
C=arn:aws:kafka:$R:$ACCT:cluster/$NAME/*
T=arn:aws:kafka:$R:$ACCT:topic/$NAME/*
G=arn:aws:kafka:$R:$ACCT:group/$NAME/*
connect="{\"Effect\":\"Allow\",\"Action\":[\"kafka-cluster:Connect\",\"kafka-cluster:DescribeCluster\"],\"Resource\":\"$C\"}"
policy() { echo "{\"Version\":\"2012-10-17\",\"Statement\":[$connect,$1]}"; }
user() {  # name policy-statements
  aws iam create-user --user-name $1 >/dev/null 2>&1 && say "user $1 created" || true
  aws iam put-user-policy --user-name $1 --policy-name $1 --policy-document "$(policy "$2")" || die "policy of $1"
}
user assertix-lab-msk-processor "{\"Effect\":\"Allow\",\"Action\":[\"kafka-cluster:CreateTopic\",\"kafka-cluster:DescribeTopic\",\"kafka-cluster:ReadData\",\"kafka-cluster:WriteData\"],
  \"Resource\":[\"$T/order-events\",\"$T/order-events-ci\",\"$T/order-events-dlq\",\"$T/order-events-dlq-ci\"]},
 {\"Effect\":\"Allow\",\"Action\":[\"kafka-cluster:AlterGroup\",\"kafka-cluster:DescribeGroup\"],\"Resource\":\"$G/orders-live-msk*\"}"
say "assertix-lab-msk-processor: reads and writes the four topics, its groups orders-live-msk*"
user assertix-lab-msk-shop "{\"Effect\":\"Allow\",\"Action\":[\"kafka-cluster:DescribeTopic\",\"kafka-cluster:WriteData\"],\"Resource\":\"$T/order-events\"}"
say "assertix-lab-msk-shop: writes order-events only"
aws iam put-user-policy --user-name assertix-lab-stream-writer --policy-name assertix-lab-stream-writer-msk --policy-document \
  "$(policy "{\"Effect\":\"Allow\",\"Action\":[\"kafka-cluster:DescribeTopic\",\"kafka-cluster:WriteData\"],\"Resource\":\"$T/order-events-ci\"}")" \
  && say "assertix-lab-stream-writer: may also write order-events-ci on MSK only"
# the reader's inline policies are full (IAM's 2048 characters): a managed policy, as in setup-aws.sh
READER_DOC=$(policy "{\"Effect\":\"Allow\",\"Action\":[\"kafka-cluster:DescribeTopic\",\"kafka-cluster:ReadData\"],
  \"Resource\":[\"$T/order-events\",\"$T/order-events-ci\",\"$T/order-events-dlq\",\"$T/order-events-dlq-ci\"]},
 {\"Effect\":\"Allow\",\"Action\":[\"kafka-cluster:DescribeGroup\"],\"Resource\":\"$G/*\"}")
POL=arn:aws:iam::$ACCT:policy/assertix-lab-reader-msk
if aws iam get-policy --policy-arn $POL >/dev/null 2>&1; then
  for V in $(aws iam list-policy-versions --policy-arn $POL --query 'Versions[?!IsDefaultVersion].VersionId' --output text); do
    aws iam delete-policy-version --policy-arn $POL --version-id $V
  done
  aws iam create-policy-version --policy-arn $POL --policy-document "$READER_DOC" --set-as-default >/dev/null
else
  aws iam create-policy --policy-name assertix-lab-reader-msk --policy-document "$READER_DOC" >/dev/null
fi
aws iam attach-user-policy --user-name assertix-lab-reader --policy-arn $POL \
  && say "assertix-lab-reader: may read the four topics and where each consumer group is (read only)"

# keys, never shown: one file each, only you can read them
umask 077
key() {  # user file
  if [ ! -s "$2" ]; then
    for K in $(aws iam list-access-keys --user-name $1 --query 'AccessKeyMetadata[].AccessKeyId' --output text); do
      aws iam delete-access-key --user-name $1 --access-key-id $K   # its secret is lost with its file
    done
    aws iam create-access-key --user-name $1 --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text \
      | tr '\t' ':' > "$2"
    say "a new key for $1 (not shown)"
  fi
}
key assertix-lab-msk-processor ~/.assertix-msk-processor.key
key assertix-lab-msk-shop ~/.assertix-msk-shop.key
{ echo "BOOTSTRAP=$BOOT"; echo "PROCESSOR=$(cat ~/.assertix-msk-processor.key)"; } > ~/assertix-msk.txt
echo "$BOOT" > ~/.assertix-msk-boot   # for msk_client.py shop

say "topics (as the processor's login)"
# CloudShell's Python may be a virtual environment (no --user installs there)
PKGS="confluent-kafka==2.6.1 aws-msk-iam-sasl-signer-python==1.0.2"
python3 -m pip install -q $PKGS 2>/dev/null || python3 -m pip install --user -q $PKGS \
  || die "the Kafka libraries could not be installed"
python3 -c "import confluent_kafka, aws_msk_iam_sasl_signer" || die "the Kafka libraries are not importable"
curl -fsSL "$RAW/msk_client.py" -o ~/msk_client.py || die "msk_client.py could not be downloaded"
sleep 10   # a new key takes a moment before AWS accepts it
IFS=: read -r ID SECRET < ~/.assertix-msk-processor.key
env -u AWS_SESSION_TOKEN AWS_ACCESS_KEY_ID=$ID AWS_SECRET_ACCESS_KEY=$SECRET KAFKA_BOOTSTRAP=$BOOT KAFKA_REGION=$R \
  python3 ~/msk_client.py topics || die "the topics could not be created (see the error above)"
say "the processor's key and the brokers' address are in ~/assertix-msk.txt (not shown): download that file"
say "done: cluster $NAME in $R"

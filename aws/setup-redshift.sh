#!/bin/bash
# T5 (D7-52): Amazon Redshift Serverless for the cloud lab, run once in AWS CloudShell
# (signed in as the account owner), next to the AWS lab of D7-45 (setup-aws-iam.sh first):
#   role       assertix-lab-redshift-copy: Redshift reads the raw bucket's redshift/ files (COPY)
#   namespace  assertix-lab, database orders, admin password kept by Secrets Manager (never shown),
#              the copy role as its default role
#   workgroup  assertix-lab: the smallest size (4 compute units, else 8), not public, and a daily
#              usage limit (8 RPU-hours, then Redshift stops queries until the next day)
#   logins     assertix-lab-pipeline: runs SQL through the Data API, owns the schemas raw, ci, dev
#              assertix-lab-reader (Assertix): runs SQL through the Data API, SELECT only
# Prints no secret. Safe to run again: what exists is kept, the logins' Redshift policies updated.
set -uo pipefail
R=eu-north-1
ACCT=$(aws sts get-caller-identity --query Account --output text)
RAW=assertix-lab-raw-$ACCT
NS=assertix-lab
WG=assertix-lab
DB=orders
ROLE=assertix-lab-redshift-copy
ok() { echo "  $*"; }
fail() { echo "  FAILED: $*"; exit 1; }

echo "== copy role $ROLE"
TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":["redshift.amazonaws.com","redshift-serverless.amazonaws.com"]},"Action":"sts:AssumeRole"}]}'
aws iam get-role --role-name $ROLE >/dev/null 2>&1 \
  || aws iam create-role --role-name $ROLE --assume-role-policy-document "$TRUST" >/dev/null || fail "create role"
aws iam put-role-policy --role-name $ROLE --policy-name $ROLE-read --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[
 {\"Effect\":\"Allow\",\"Action\":[\"s3:ListBucket\",\"s3:GetBucketLocation\"],\"Resource\":\"arn:aws:s3:::$RAW\"},
 {\"Effect\":\"Allow\",\"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::$RAW/redshift/*\"}]}" || fail "role policy"
ROLE_ARN=$(aws iam get-role --role-name $ROLE --query Role.Arn --output text)
ok "ready: may read s3://$RAW/redshift/"
sleep 10   # a new role takes a moment to be usable

echo "== namespace $NS (database $DB)"
if aws redshift-serverless get-namespace --region $R --namespace-name $NS >/dev/null 2>&1; then
  ok "exists"
else
  aws redshift-serverless create-namespace --region $R --namespace-name $NS --db-name $DB \
    --admin-username admin --manage-admin-password --iam-roles "$ROLE_ARN" --default-iam-role-arn "$ROLE_ARN" \
    >/dev/null || fail "create namespace"
  ok "created (admin password in Secrets Manager)"
fi
SECRET=$(aws redshift-serverless get-namespace --region $R --namespace-name $NS --query namespace.adminPasswordSecretArn --output text)

echo "== workgroup $WG"
if ! aws redshift-serverless get-workgroup --region $R --workgroup-name $WG >/dev/null 2>&1; then
  aws redshift-serverless create-workgroup --region $R --workgroup-name $WG --namespace-name $NS \
    --base-capacity 4 --no-publicly-accessible >/dev/null 2>&1 \
  || aws redshift-serverless create-workgroup --region $R --workgroup-name $WG --namespace-name $NS \
    --base-capacity 8 --no-publicly-accessible >/dev/null || fail "create workgroup"
fi
for i in $(seq 1 60); do
  ST=$(aws redshift-serverless get-workgroup --region $R --workgroup-name $WG --query workgroup.status --output text)
  [ "$ST" = AVAILABLE ] && break; sleep 10
done
[ "$ST" = AVAILABLE ] || fail "workgroup not available after 10 minutes (status $ST)"
WG_ARN=$(aws redshift-serverless get-workgroup --region $R --workgroup-name $WG --query workgroup.workgroupArn --output text)
ok "available ($(aws redshift-serverless get-workgroup --region $R --workgroup-name $WG --query workgroup.baseCapacity --output text) compute units)"
if [ -z "$(aws redshift-serverless list-usage-limits --region $R --resource-arn "$WG_ARN" --query 'usageLimits[0].usageLimitId' --output text 2>/dev/null | grep -v None)" ]; then
  aws redshift-serverless create-usage-limit --region $R --resource-arn "$WG_ARN" --usage-type serverless-compute \
    --amount 8 --period daily --breach-action deactivate >/dev/null || fail "usage limit"
fi
ok "daily usage limit: 8 RPU-hours"

echo "== logins (Data API on this workgroup)"
for user in assertix-lab-pipeline assertix-lab-reader; do
  aws iam put-user-policy --user-name $user --policy-name $user-redshift --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[
   {\"Effect\":\"Allow\",\"Action\":[\"redshift-serverless:GetCredentials\",\"redshift-data:ExecuteStatement\",\"redshift-data:BatchExecuteStatement\"],\"Resource\":\"$WG_ARN\"},
   {\"Effect\":\"Allow\",\"Action\":[\"redshift-data:DescribeStatement\",\"redshift-data:GetStatementResult\",\"redshift-data:CancelStatement\",\"redshift-data:ListStatements\"],\"Resource\":\"*\"}]}" \
    && ok "$user: may run SQL on $WG" || fail "policy for $user"
done

echo "== database users and rights (as admin)"
sql() {   # one statement; an "already exists" is fine
  local id st err
  id=$(aws redshift-data execute-statement --region $R --workgroup-name $WG --database $DB --secret-arn "$SECRET" \
       --sql "$1" --query Id --output text) || { echo "  FAILED to send: $1"; return 1; }
  for i in $(seq 1 60); do
    st=$(aws redshift-data describe-statement --region $R --id "$id" --query Status --output text)
    case $st in FINISHED|FAILED|ABORTED) break;; esac; sleep 2
  done
  if [ "$st" = FINISHED ]; then ok "$1"; return 0; fi
  err=$(aws redshift-data describe-statement --region $R --id "$id" --query Error --output text)
  case $err in *"already exists"*) ok "$1 (already there)";; *) echo "  FAILED: $1 — $err"; return 1;; esac
}
P='"IAM:assertix-lab-pipeline"'; A='"IAM:assertix-lab-reader"'
sql "CREATE USER $P PASSWORD DISABLE" || exit 1
sql "CREATE USER $A PASSWORD DISABLE" || exit 1
for s in raw ci dev; do
  sql "CREATE SCHEMA IF NOT EXISTS $s" || exit 1
  sql "ALTER SCHEMA $s OWNER TO $P" || exit 1
  sql "GRANT USAGE ON SCHEMA $s TO $A" || exit 1
  sql "GRANT SELECT ON ALL TABLES IN SCHEMA $s TO $A" || exit 1
done
sql "ALTER DEFAULT PRIVILEGES FOR USER $P GRANT SELECT ON TABLES TO $A" || exit 1
sql "REVOKE CREATE ON SCHEMA public FROM PUBLIC" || exit 1

echo "== done: workgroup $WG, database $DB, region $R, copy role $ROLE (the namespace's default)"

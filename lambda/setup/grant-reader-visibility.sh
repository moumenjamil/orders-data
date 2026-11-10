#!/bin/bash
# Lets Assertix' reader (assertix-lab-reader) read a whole dead-letter queue: it hides each message for a
# moment while reading and then makes it visible again (sqs:ChangeMessageVisibility). Nothing is removed.
# Adds that one right to the policy assertix-lab-reader-streams; setup-aws.sh has it for new setups.
# Run in AWS CloudShell.
set -euo pipefail
ACCT=$(aws sts get-caller-identity --query Account --output text)
POL=arn:aws:iam::$ACCT:policy/assertix-lab-reader-streams
V=$(aws iam get-policy --policy-arn $POL --query Policy.DefaultVersionId --output text)
DOC=$(aws iam get-policy-version --policy-arn $POL --version-id $V --query PolicyVersion.Document --output json)
NEW=$(echo "$DOC" | jq -c '.Statement |= map(if ((.Action|tostring)|contains("sqs:ReceiveMessage"))
  then .Action = ((.Action + ["sqs:ChangeMessageVisibility"]) | unique) else . end)')
if [ "$NEW" = "$(echo "$DOC" | jq -c .)" ]; then echo "nothing to change"; exit 0; fi
for O in $(aws iam list-policy-versions --policy-arn $POL --query 'Versions[?!IsDefaultVersion].VersionId' --output text); do
  aws iam delete-policy-version --policy-arn $POL --version-id $O
done
aws iam create-policy-version --policy-arn $POL --policy-document "$NEW" --set-as-default >/dev/null
echo "assertix-lab-reader: may now hide a dead letter for a moment while reading and show it again"
aws iam get-policy-version --policy-arn $POL --version-id $(aws iam get-policy --policy-arn $POL --query Policy.DefaultVersionId --output text) \
  --query 'PolicyVersion.Document.Statement[?contains(to_string(Action), `sqs:`)].Action' --output text

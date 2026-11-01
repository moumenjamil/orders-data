#!/bin/bash
# The Composer deploy: the DAGs in composer/dags/ go to the environment's DAGs
# folder (Composer picks them up within a minute or two). Run in Cloud Shell
# from a checkout of this repo, at the commit to deploy. Stops at the first
# file that could not be copied and shows why.
# Usage: composer/deploy.sh   (environment assertix-lab-composer, europe-west3)
set -euo pipefail
cd "$(dirname "$0")/.."
for f in composer/dags/*.py; do
  gcloud composer environments storage dags import --environment assertix-lab-composer \
    --location europe-west3 --source "$f" --quiet
  echo "deployed $f"
done
echo "at $(git branch --show-current) @ $(git rev-parse --short HEAD)"

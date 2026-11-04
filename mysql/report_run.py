"""Reports a run of the MySQL job to Assertix (T6): POST <ASSERTIX_URL>/webhook/data-run, signed
like a GitHub webhook (X-Hub-Signature-256, HMAC-SHA256 with the project's webhook secret).
Usage: report_run.py STATUS   (queued, running, success, failed or cancelled)
Settings from the environment: ASSERTIX_URL, ASSERTIX_WEBHOOK_SECRET, REPO, PIPELINE_ID, RUN_ID,
COMMIT_SHA, STARTED_AT (ISO-8601). A failed run's reason is read from run_error.txt.
A report that cannot be delivered is printed, never fails the job.
"""
import hashlib
import hmac
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

status = sys.argv[1]
body = {"repo": os.environ["REPO"], "pipelineId": os.environ["PIPELINE_ID"], "runId": os.environ["RUN_ID"],
        "status": status, "commitSha": os.environ.get("COMMIT_SHA") or None,
        "startedAt": os.environ.get("STARTED_AT") or None}
if status in ("success", "failed", "cancelled"):
    body["endedAt"] = datetime.now(timezone.utc).isoformat()
err = Path("run_error.txt")
if status == "failed":
    body["error"] = err.read_text()[:2000] if err.exists() else "the job failed before writing a reason"
data = json.dumps(body).encode()
sig = "sha256=" + hmac.new(os.environ["ASSERTIX_WEBHOOK_SECRET"].encode(), data, hashlib.sha256).hexdigest()
req = urllib.request.Request(os.environ["ASSERTIX_URL"].rstrip("/") + "/webhook/data-run", data=data,
                             headers={"Content-Type": "application/json", "X-Hub-Signature-256": sig,
                                      "ngrok-skip-browser-warning": "1"})
try:
    with urllib.request.urlopen(req, timeout=30) as r:
        print(f"reported {status} to Assertix: HTTP {r.status} {r.read().decode()[:200]}")
except Exception as e:
    print(f"could not report {status} to Assertix: {e}")

"""Reports a run of the MySQL job to Assertix (T6): POST <ASSERTIX_URL>/webhook/data-run, signed
like a GitHub webhook (X-Hub-Signature-256, HMAC-SHA256 with the project's webhook secret).
Usage: report_run.py STATUS   (queued, running, success, failed or cancelled)
Settings from the environment: ASSERTIX_URL, ASSERTIX_WEBHOOK_SECRET, REPO, PIPELINE_ID, RUN_ID,
COMMIT_SHA, STARTED_AT (ISO-8601). A failed run's reason is read from run_error.txt.
A report that cannot be delivered is printed, never fails the job.
A report that did not get through for a passing reason (no connection, a timeout, 429, a 5xx, a 404 that is
not Assertix' own answer, e.g. a tunnel briefly offline) is sent again, up to 5 times over about a minute;
Assertix takes the same run reported twice as one. Its own answers (400, 401, its 404s, data testing
switched off) are never retried: they do not fix themselves. ASSERTIX_REPORT_WAITS (seconds, comma
separated) changes the waits, for tests.
"""
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
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
# Assertix' own answers that a retry cannot change
FINAL = ("No project registered", "has no EXTERNAL pipeline", "Data pipeline testing is switched off",
         "Invalid signature", "Unparseable payload", "required", "must be")
WAITS = [float(w) for w in os.environ.get("ASSERTIX_REPORT_WAITS", "2,5,10,20,30").split(",") if w.strip()]


def send():
    """(delivered, retry, what happened)"""
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return True, False, f"HTTP {r.status} {r.read().decode()[:200]}"
    except urllib.error.HTTPError as e:
        text = e.read().decode(errors="replace")[:200]
        own = any(f in text for f in FINAL)
        passing = e.code == 429 or e.code >= 500 or e.code == 404
        return False, passing and not own, f"HTTP {e.code} {text}"
    except Exception as e:   # no connection, a timeout, a dropped line
        return False, True, f"{type(e).__name__}: {e}"


for attempt in range(len(WAITS) + 1):
    delivered, retry, what = send()
    if delivered:
        print(f"reported {status} to Assertix: {what}" + (f" (try {attempt + 1})" if attempt else ""))
        break
    if not retry or attempt == len(WAITS):
        print(f"could not report {status} to Assertix after {attempt + 1} tr{'y' if attempt == 0 else 'ies'}: {what}")
        break
    print(f"report {status} not delivered ({what}); trying again in {WAITS[attempt]:g} s")
    time.sleep(WAITS[attempt])

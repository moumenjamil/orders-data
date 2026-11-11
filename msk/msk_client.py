#!/usr/bin/env python3
"""S4 (Kafka on Amazon MSK), run in AWS CloudShell: the small Kafka jobs of the lab, with MSK's IAM login
(OAUTHBEARER, signed by AWS's signer from the login's key).

  topics     creates order-events, order-events-ci, order-events-dlq, order-events-dlq-ci (1 partition,
             2 copies, kept 1 day); the key and the address come from the environment (setup-msk.sh,
             as the team's processor login): AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, KAFKA_BOOTSTRAP,
             KAFKA_REGION
  shop [N]   plays the shop: N new orders (default 20) on order-events as the shop's login (its key in
             ~/.assertix-msk-shop.key, the address in ~/.assertix-msk-boot, both written by setup-msk.sh):
             each created, then mostly paid, some cancelled, a few paid then refunded later (the processor on
             main does not handle "refunded" yet, so those go to the reject topic); like real Kafka, a few
             events are delivered twice; keys are the order ids. Prints only counts.

Usage: curl -fsSL https://raw.githubusercontent.com/moumenjamil/orders-data/main/msk/msk_client.py -o ~/msk_client.py
       python3 -m pip install -q confluent-kafka==2.6.1 aws-msk-iam-sasl-signer-python==1.0.2
       python3 ~/msk_client.py shop 20
(a new CloudShell session does not keep the installed libraries: install them again first)
"""
import json
import os
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

TOPICS = ["order-events", "order-events-ci", "order-events-dlq", "order-events-dlq-ci"]


def conf(bootstrap, region):
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    def token(_):
        t, expires_ms = MSKAuthTokenProvider.generate_auth_token(region)
        return t, expires_ms / 1000

    return {"bootstrap.servers": bootstrap, "security.protocol": "SASL_SSL", "sasl.mechanisms": "OAUTHBEARER",
            "oauth_cb": token}


def topics():
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient(conf(os.environ["KAFKA_BOOTSTRAP"], os.environ["KAFKA_REGION"]))
    admin.poll(5)   # the login token is made on the first poll
    have = set(admin.list_topics(timeout=30).topics)
    new = [NewTopic(t, num_partitions=1, replication_factor=2, config={"retention.ms": str(24 * 3600 * 1000)})
           for t in TOPICS if t not in have]
    for t in TOPICS:
        if t in have:
            print(f"== topic {t} exists")
    for t, f in (admin.create_topics(new, request_timeout=30).items() if new else []):
        f.result()
        print(f"== topic {t} created (1 partition, 2 copies, kept 1 day)")


def events(n):
    now, out = datetime.now(timezone.utc), []
    for _ in range(n):
        oid = f"ORD-{now:%Y%m%d}-{uuid.uuid4().hex[:6]}"
        t = now - timedelta(minutes=random.randint(5, 50))
        ev = lambda typ, at, **x: {"event_id": uuid.uuid4().hex, "type": typ, "order_id": oid,
                                   "occurred_at": at.isoformat(timespec="milliseconds").replace("+00:00", "Z"), **x}
        out.append(ev("created", t, customer_id=random.randint(1, 40), amount=round(random.uniform(9, 240), 2)))
        r = random.random()
        if r < 0.75:
            out.append(ev("paid", t + timedelta(seconds=random.randint(20, 300))))
            if random.random() < 0.15:
                out.append(ev("refunded", t + timedelta(minutes=random.randint(10, 40))))
        elif r < 0.9:
            out.append(ev("cancelled", t + timedelta(minutes=random.randint(1, 20))))
    out.sort(key=lambda e: e["occurred_at"])
    for e in list(out):
        if random.random() < 0.05:
            out.append(e)   # at-least-once delivery: the same event again
    return out


def shop(n):
    from confluent_kafka import Producer
    key_id, secret = Path.home().joinpath(".assertix-msk-shop.key").read_text().strip().split(":", 1)
    os.environ.pop("AWS_SESSION_TOKEN", None)
    os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"] = key_id, secret
    boot = Path.home().joinpath(".assertix-msk-boot").read_text().strip()
    region = boot.split(".kafka.")[1].split(".")[0]
    p = Producer({**conf(boot, region), "enable.idempotence": False})
    p.poll(5)
    failed = []
    ev = events(n)
    for e in ev:
        p.produce("order-events", key=e["order_id"], value=json.dumps(e),
                  on_delivery=lambda err, _m: failed.append(err) if err else None)
    p.flush(60)
    if failed:
        raise SystemExit(f"{len(failed)} events were not sent: {failed[0]}")
    by = {}
    for e in ev:
        by[e["type"]] = by.get(e["type"], 0) + 1
    print(f"sent {len(ev)} events for {len({e['order_id'] for e in ev})} orders to order-events (MSK): "
          + ", ".join(f"{k} {v}" for k, v in sorted(by.items()))
          + f"; {len(ev) - len({e['event_id'] for e in ev})} delivered twice")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "topics":
        topics()
    elif cmd == "shop":
        shop(int(sys.argv[2]) if len(sys.argv) > 2 else 20)
    else:
        raise SystemExit(__doc__)

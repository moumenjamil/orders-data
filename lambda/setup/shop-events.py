#!/usr/bin/env python3
"""Plays the shop on AWS (Assertix S3 lab): puts real-looking order events on the live stream order-events,
run in AWS CloudShell (its own login): N new orders, each created, then mostly paid, some cancelled, a few
paid then refunded later (the shop already sends "refunded"; the function on main does not use it until
the team adds it, so those go to the dead-letter queue). Like Kinesis, a few events arrive twice. The
partition key is the order id. Prints only counts.

Usage: curl -fsSL https://raw.githubusercontent.com/moumenjamil/orders-data/main/lambda/setup/shop-events.py | python3 - [orders]
"""
import json
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone

import boto3


def events(n):
    now, out = datetime.now(timezone.utc), []
    for _ in range(n):
        oid = f"ORD-{now:%Y%m%d}-{uuid.uuid4().hex[:6]}"
        t = now - timedelta(minutes=random.randint(5, 50))
        at = lambda d: (t + d).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        ev = lambda typ, d, **x: {"event_id": uuid.uuid4().hex, "type": typ, "order_id": oid, "occurred_at": at(d), **x}
        out.append(ev("created", timedelta(0), customer_id=random.randint(1, 40), amount=round(random.uniform(9, 240), 2)))
        r = random.random()
        if r < 0.65:
            out.append(ev("paid", timedelta(minutes=1)))
            if random.random() < 0.25:
                out.append(ev("refunded", timedelta(minutes=3)))
        elif r < 0.85:
            out.append(ev("cancelled", timedelta(minutes=2)))
    return out + random.sample(out, 2)


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    evs = events(n)
    res = boto3.client("kinesis", region_name="eu-north-1").put_records(StreamName="order-events", Records=[
        {"Data": json.dumps(e).encode(), "PartitionKey": e["order_id"]} for e in evs])
    counts = {}
    for e in evs:
        counts[e["type"]] = counts.get(e["type"], 0) + 1
    print(f"sent {len(evs) - res['FailedRecordCount']} events for {n} orders to order-events: "
          + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())) + "; 2 sent twice")


if __name__ == "__main__":
    main()

"""The orders stream processor: reads order events from Kafka and keeps orders_live in MySQL, one
row per order with its latest status, within a second of each event.

What it must get right (what Assertix checks on every PR):
  - an event Kafka delivers twice is applied once (event_id is remembered in orders_live_seen);
  - events that arrive out of order still leave the order's real latest status (an event older than
    the order's last applied event changes nothing);
  - an event it cannot read goes to the reject topic (dead-letter) with the reason; the stream goes on.

In production it drains the topic and stops when it has been idle (the lab has no always-on host); in
CI it runs the PR's code on the test topic until it has been idle after the test events: the events
Assertix sends carry the header assertix-run with the run they test ("awaiting:<commit>"), and only
this run's start the idle count (old events left on the test topic, even an earlier test's, do not),
so the processor waits for Assertix as long as FIRST_WAIT.

Settings from the environment: KAFKA_BOOTSTRAP, KAFKA_USER, KAFKA_PASSWORD, KAFKA_CA (a file; unset when
the brokers' certificate is publicly trusted, as on Azure Event Hubs), TABLE (default orders_live; S4: the
Event Hubs run keeps orders_live_eh, beside the Kafka one), TOPIC, DLQ_TOPIC, GROUP, MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, TARGET (ci | dev),
FIRST_WAIT (seconds to wait for the first event, default 600), IDLE (seconds idle before stopping,
default 60). Any failure is written to run_error.txt (its last line is the reason) and exits 1.
"""
import json
import os
import ssl
import sys
import time
import traceback
from datetime import datetime, timezone

import pymysql
from confluent_kafka import Consumer, KafkaError, Producer

KNOWN = {"created", "paid", "cancelled", "refunded"}
# a refund is the last word on a paid order: the order ends refunded
STATUS = {"created": "created", "paid": "paid", "cancelled": "cancelled", "refunded": "refunded"}


def env(name, default=None):
    v = os.environ.get(name, default)
    if v is None:
        raise SystemExit(f"{name} is not set")
    return v


LIVE = os.environ.get("TABLE") or "orders_live"
SEEN = LIVE + "_seen"


def kafka_conf():
    conf = {"bootstrap.servers": env("KAFKA_BOOTSTRAP"), "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN", "sasl.username": env("KAFKA_USER"), "sasl.password": env("KAFKA_PASSWORD")}
    if os.environ.get("KAFKA_CA"):
        conf["ssl.ca.location"] = os.environ["KAFKA_CA"]
    return conf


def connect_mysql():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE   # Aiven's MySQL certificate is not in the runner's store
    return pymysql.connect(host=env("MYSQL_HOST"), port=int(env("MYSQL_PORT")), user=env("MYSQL_USER"),
                           password=env("MYSQL_PASSWORD"), database=env("TARGET"), ssl=ctx, autocommit=False)


def ensure_tables(db):
    with db.cursor() as c:
        c.execute(f"""create table if not exists {LIVE} (
            order_id varchar(64) primary key,
            customer_id bigint null,
            amount decimal(12,2) null,
            status varchar(20) not null,
            last_event_at datetime(3) not null,
            updated_at datetime(3) not null)""")
        c.execute(f"""create table if not exists {SEEN} (
            event_id varchar(64) primary key,
            seen_at datetime(3) not null)""")
    db.commit()


def parse(raw):
    """The event, or raises ValueError with the reason it cannot be read."""
    try:
        e = json.loads(raw)
    except (TypeError, ValueError) as x:
        raise ValueError(f"not JSON: {x}")
    if not isinstance(e, dict):
        raise ValueError("not a JSON object")
    for f in ("event_id", "type", "order_id", "occurred_at"):
        if not e.get(f):
            raise ValueError(f"missing {f}")
    if e["type"] not in KNOWN:
        raise ValueError(f"unknown event type {e['type']}")
    try:
        e["_at"] = datetime.fromisoformat(str(e["occurred_at"]).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        raise ValueError(f"occurred_at is not a time: {e['occurred_at']}")
    return e


def apply(db, e):
    """Applies one event; False when it was a repeat."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    at = e["_at"].replace(tzinfo=None)
    with db.cursor() as c:
        if c.execute(f"insert ignore into {SEEN} (event_id, seen_at) values (%s, %s)", (e["event_id"], now)) == 0:
            return False
        c.execute(f"select last_event_at from {LIVE} where order_id = %s for update", (e["order_id"],))
        row = c.fetchone()
        if row is None:
            c.execute(f"insert into {LIVE} (order_id, customer_id, amount, status, last_event_at, updated_at) "
                      "values (%s, %s, %s, %s, %s, %s)",
                      (e["order_id"], e.get("customer_id"), e.get("amount"), STATUS[e["type"]], at, now))
        else:
            if e["type"] == "created":   # details arrive with created, whenever it arrives
                c.execute(f"update {LIVE} set customer_id = coalesce(%s, customer_id), "
                          "amount = coalesce(%s, amount), updated_at = %s where order_id = %s",
                          (e.get("customer_id"), e.get("amount"), now, e["order_id"]))
            if True:                     # Event Hubs keeps a partition in order: the latest event wins
                c.execute(f"update {LIVE} set status = %s, last_event_at = %s, updated_at = %s where order_id = %s",
                          (STATUS[e["type"]], at, now, e["order_id"]))
    return True


def main():
    topic, dlq = env("TOPIC"), env("DLQ_TOPIC")
    first_wait, idle = int(env("FIRST_WAIT", "600")), int(env("IDLE", "60"))
    db = connect_mysql()
    ensure_tables(db)
    consumer = Consumer({**kafka_conf(), "group.id": env("GROUP"), "auto.offset.reset": "earliest",
                         "enable.auto.commit": False})
    producer = Producer(kafka_conf())
    consumer.subscribe([topic])
    started = last = time.time()
    tests_seen = False   # the CI run waits for Assertix' events (header assertix-run), not for old ones
    seen = applied = repeats = rejected = 0
    ci = env("TARGET") == "ci"
    mine = ("awaiting:" + os.environ.get("COMMIT_SHA", "")).encode()
    print(f"reading {topic} as group {env('GROUP')} into {env('TARGET')}.{LIVE}", flush=True)
    while True:
        msg = consumer.poll(1.0)
        now = time.time()
        if msg is None:
            waiting = not tests_seen if ci else seen == 0
            if (waiting and now - started > first_wait) or (not waiting and now - last > idle):
                break
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise RuntimeError(f"Kafka: {msg.error()}")
        seen += 1
        last = now
        if any(k == "assertix-run" and v == mine for k, v in (msg.headers() or [])):
            tests_seen = True
        try:
            e = parse(msg.value())
        except ValueError as why:
            producer.produce(dlq, key=msg.key(), value=msg.value(),
                             headers=[("reason", str(why).encode()), ("source_topic", topic.encode())])
            producer.flush(10)
            rejected += 1
            consumer.commit(msg, asynchronous=False)
            continue
        if apply(db, e):
            applied += 1
        else:
            repeats += 1
        db.commit()
        consumer.commit(msg, asynchronous=False)
    consumer.close()
    took = time.time() - started
    print(f"done: {seen} events read, {applied} applied, {repeats} repeats ignored, {rejected} rejected"
          f" in {took:.0f} s", flush=True)


if __name__ == "__main__":
    try:
        main()
    except BaseException as x:   # the job's own error, for the run report
        if isinstance(x, SystemExit) and x.code in (0, None):
            raise
        traceback.print_exc()
        lines = traceback.format_exception_only(type(x), x)
        open("run_error.txt", "w").write(lines[-1].strip())
        sys.exit(1)

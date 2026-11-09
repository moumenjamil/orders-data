"""orders_live on AWS: a Lambda function that Kinesis feeds with the shop's order events, in batches. It
keeps one item per order in DynamoDB (orders_live) with the order's latest status, and puts every event
it cannot use on an SQS queue (the dead letters) with the reason; the stream goes on.

What it must get right (what Assertix checks on every PR):
  - an event Kinesis delivers twice is applied once (each event_id is remembered in orders_live_seen);
  - events that arrive out of order still leave the order's real latest status (an event older than the
    order's last applied event changes nothing);
  - an event it cannot use goes to the dead-letter queue with the reason, not into the table.

A bug that raises makes AWS call the function again with the same batch (a few times, then the batch
goes to the failure queue): the error is in the function's log.

Settings from the environment: TABLE, SEEN_TABLE, DLQ_URL; COMMIT is set by the team's deploy.
"""
import base64
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

KNOWN = {"created", "paid", "cancelled", "refunded"}   # a refund ends a paid order: status refunded
REQUIRED = ("event_id", "type", "order_ref", "occurred_at")

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")


def parse(raw):
    """The event, or raises ValueError with the reason it cannot be used."""
    try:
        e = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as x:
        raise ValueError(f"not JSON: {x}")
    if not isinstance(e, dict):
        raise ValueError("not a JSON object")
    for f in REQUIRED:
        if not e.get(f):
            raise ValueError(f"missing {f}")
    if e["type"] not in KNOWN:
        raise ValueError(f"unknown event type {e['type']}")
    try:
        at = datetime.fromisoformat(str(e["occurred_at"]).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        raise ValueError(f"occurred_at is not a time: {e['occurred_at']}")
    e["_at"] = at.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"   # one format, so text order is time order
    return e


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def apply(table, seen, e):
    """Applies one event; False when it was a repeat."""
    try:
        seen.put_item(Item={"event_id": e["event_id"], "seen_at": now(), "expires": int(time.time()) + 7 * 86400},
                      ConditionExpression="attribute_not_exists(event_id)")
    except ClientError as x:
        if x.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise
    key = {"order_id": e["order_ref"]}
    if e["type"] == "created":   # details arrive with created, whenever it arrives
        sets, values = ["updated_at = :u"], {":u": now()}
        if e.get("customer_id") is not None:
            sets.append("customer_id = :c")
            values[":c"] = Decimal(str(e["customer_id"]))
        if e.get("amount") is not None:
            sets.append("amount = :a")
            values[":a"] = Decimal(str(e["amount"]))
        table.update_item(Key=key, UpdateExpression="SET " + ", ".join(sets), ExpressionAttributeValues=values)
    try:                         # only a newer event changes the status
        table.update_item(Key=key, UpdateExpression="SET #s = :s, last_event_at = :t, updated_at = :u",
                          ConditionExpression="attribute_not_exists(last_event_at) OR last_event_at <= :t",
                          ExpressionAttributeNames={"#s": "status"},
                          ExpressionAttributeValues={":s": e["type"], ":t": e["_at"], ":u": now()})
    except ClientError as x:
        if x.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
    return True


def handler(event, context):
    table = dynamodb.Table(os.environ["TABLE"])
    seen = dynamodb.Table(os.environ["SEEN_TABLE"])
    applied = repeats = rejected = 0
    for record in event.get("Records", []):
        k = record["kinesis"]
        raw = base64.b64decode(k["data"])
        try:
            e = parse(raw)
        except ValueError as why:
            sqs.send_message(QueueUrl=os.environ["DLQ_URL"], MessageBody=raw.decode("utf-8", "replace") or "(empty)",
                             MessageAttributes={
                                 "key": {"DataType": "String", "StringValue": k.get("partitionKey") or "none"},
                                 "reason": {"DataType": "String", "StringValue": str(why)[:500]},
                                 "source": {"DataType": "String", "StringValue": record.get("eventSourceARN", "?")}})
            rejected += 1
            continue
        if apply(table, seen, e):
            applied += 1
        else:
            repeats += 1
    print(f"done: {len(event.get('Records', []))} events, {applied} applied, {repeats} repeats ignored, "
          f"{rejected} rejected (commit {os.environ.get('COMMIT', '?')[:7]})")

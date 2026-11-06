"""orders_live on Google Cloud: a Dataflow streaming job (Apache Beam) that reads the shop's order events
from Pub/Sub and writes every event it can use to BigQuery (order_events_live); the view orders_live
(views/orders_live.sql) shows each order's latest state. Events it cannot use go to the dead-letter
topic with the reason, and the stream goes on.

What it must get right (what Assertix checks on every PR):
  - an event Pub/Sub delivers twice counts once (Dataflow drops repeats by the event_id attribute, the
    view by event_id);
  - events that arrive out of order still leave the order's real latest status (each event keeps the
    time it happened, occurred_at, and the view orders by it);
  - an event it cannot use goes to the dead-letter topic with the reason; the job does not stop.

Parameters (the Flex Template's): input_subscription, output_table (project:dataset.table),
dead_letter_topic. The job runs until it is stopped: Assertix stops a test job once the test's events
are handled; the live job runs while the team wants it to.
"""
import argparse
import json
import logging
import os
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.io.gcp.bigquery_tools import RetryStrategy
from apache_beam.io.gcp.pubsub import PubsubMessage, ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, WorkerOptions

KNOWN = {"created", "paid", "cancelled", "refunded"}   # a refund ends a paid order: status refunded
REQUIRED = ("event_id", "type", "order_ref", "occurred_at")


def parse(raw):
    """The event and when it happened, or raises ValueError with the reason it cannot be used."""
    try:
        e = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
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
    return e, at


class ToRow(beam.DoFn):
    """An event as a row of order_events_live; one it cannot use goes out tagged "dead"."""

    def process(self, msg):
        attrs = dict(msg.attributes or {})
        try:
            e, at = parse(msg.data)
        except ValueError as why:
            yield beam.pvalue.TaggedOutput("dead", PubsubMessage(msg.data, {**attrs, "reason": str(why)}))
            return
        yield {
            "event_id": e["event_id"],
            "type": e["type"],
            "order_id": e["order_ref"],
            "customer_id": e.get("customer_id"),
            "amount": None if e.get("amount") is None else str(e["amount"]),
            "occurred_at": at.isoformat(),
            "received_at": datetime.now(timezone.utc).isoformat(),
            "assertix_run": attrs.get("assertix-run"),
        }


def run(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--input_subscription", required=True)
    p.add_argument("--output_table", required=True)
    p.add_argument("--dead_letter_topic", required=True)
    args, beam_args = p.parse_known_args(argv)
    options = PipelineOptions(beam_args, save_main_session=True)   # the workers need parse() and ToRow
    options.view_as(StandardOptions).streaming = True
    worker = options.view_as(WorkerOptions)
    if not worker.sdk_container_image and os.environ.get("ORDERS_LIVE_IMAGE"):
        worker.sdk_container_image = os.environ["ORDERS_LIVE_IMAGE"]   # the workers run this same image

    with beam.Pipeline(options=options) as pipeline:
        rows, dead = (
            pipeline
            | "Read order events" >> ReadFromPubSub(subscription=args.input_subscription, with_attributes=True,
                                                    id_label="event_id")
            | "To rows" >> beam.ParDo(ToRow()).with_outputs("dead", main="rows"))
        rows | "Write order_events_live" >> WriteToBigQuery(
            args.output_table, method=WriteToBigQuery.Method.STREAMING_INSERTS,
            create_disposition=BigQueryDisposition.CREATE_NEVER, write_disposition=BigQueryDisposition.WRITE_APPEND,
            insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR)
        dead | "Dead letters" >> WriteToPubSub(args.dead_letter_topic, with_attributes=True)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

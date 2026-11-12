"""orders_daily on MySQL (T6): the team's job, run by GitHub Actions (no orchestrator Assertix
can start; the workflow reports each run to Assertix).

shop PostgreSQL (orders)
  -> MySQL landing.orders: that day's slice replaced (dt), with _loaded_at (UTC)
  -> MySQL <target>.orders_daily rebuilt from the day's slice (mysql/sql/build_orders_daily.sql)

Settings from the environment (the CI's secrets and variables): SHOP_PG_URL (the shop's read-only
login), MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, TARGET (dev, ci or prod), RUN_DATE
(optional, default today UTC). On failure it writes the reason to run_error.txt and exits 1.
"""
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import pymysql

HERE = Path(__file__).resolve().parent
EXTRACT = ("SELECT id, customer_id, status, amount, payment_id, "
           "to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), "
           "to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') FROM orders ORDER BY id")
LANDING = """CREATE TABLE IF NOT EXISTS landing.orders (
    id BIGINT, customer_id BIGINT, status VARCHAR(20), amount DECIMAL(12, 2), payment_id VARCHAR(64),
    created_at DATETIME(6), updated_at DATETIME(6), _loaded_at DATETIME(6), dt DATE, PRIMARY KEY (dt, id))"""


def statements(name, **values):
    text = (HERE / "sql" / name).read_text()
    for k, v in values.items():
        text = text.replace("{" + k + "}", v)
    lines = [l for l in text.splitlines() if not l.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def main():
    target = os.environ.get("TARGET", "dev")
    if target not in ("dev", "ci", "prod"):
        raise ValueError(f"unknown target {target} (dev, ci or prod)")
    day = os.environ.get("RUN_DATE") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # D7-68 lab check (never merged): the network drops, as a platform does
    raise ConnectionResetError("[Errno 104] Connection reset by peer (lab stand-in, D7-68)")

    with psycopg2.connect(os.environ["SHOP_PG_URL"]) as src, src.cursor() as cur:
        cur.execute(EXTRACT)
        rows = cur.fetchall()
    print(f"extracted {len(rows)} orders from the shop for {day}")

    db = pymysql.connect(host=os.environ["MYSQL_HOST"], port=int(os.environ["MYSQL_PORT"]),
                         user=os.environ["MYSQL_USER"], password=os.environ["MYSQL_PASSWORD"],
                         ssl={"check_hostname": False}, autocommit=False)  # TLS (the service requires it)
    try:
        with db.cursor() as cur:
            cur.execute(LANDING)
            cur.execute("DELETE FROM landing.orders WHERE dt = %s", (day,))
            if rows:
                cur.executemany(
                    "INSERT INTO landing.orders (id, customer_id, status, amount, payment_id, created_at, updated_at,"
                    " _loaded_at, dt) VALUES (%s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP(6), %s)",
                    [tuple(r) + (day,) for r in rows])
            db.commit()
            print(f"loaded {len(rows)} orders into landing.orders (dt = {day})")
            sql = statements("build_orders_daily.sql", target=target, day=day)
            cur.execute(sql[0])            # CREATE TABLE IF NOT EXISTS commits by itself in MySQL
            for s in sql[1:]:
                cur.execute(s)
            db.commit()
            cur.execute(f"SELECT COUNT(*) FROM {target}.orders_daily")
            print(f"built {target}.orders_daily: {cur.fetchone()[0]} day(s)")
    finally:
        db.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # the reason the CI reports to Assertix
        Path("run_error.txt").write_text(f"{type(e).__name__}: {e}")
        print(f"FAILED: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)

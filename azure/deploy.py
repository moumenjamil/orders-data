#!/usr/bin/env python3
"""The Data Factory deploy (T4): publishes the factory's linked services, datasets
(factory/datasets/*.json) and pipelines (factory/pipelines/*.json) from this checkout,
through the Data Factory REST API, as the team's CI login (orders-pipeline).

A Script activity whose text is "@sql:<file>" gets sql/<file>, with {target} and {day}
filled in by the pipeline at run time. Every pipeline is stamped with the commit it was
deployed from (annotation "commit:<sha>"), so a tester can see which code a run used.

Settings come from the environment (the CI's secrets), never from this repo:
  AZ_TENANT AZ_SUBSCRIPTION AZ_RESOURCE_GROUP AZ_FACTORY AZ_STORAGE AZ_SQL_SERVER
  AZ_SQL_DATABASE AZ_PIPELINE_CLIENT_ID AZ_PIPELINE_CLIENT_SECRET
  SHOP_PG_HOST SHOP_PG_USER SHOP_PG_PASSWORD   (the shop's read-only database login)
Stops at the first object the factory refuses and shows why. Prints no secret.
Usage: azure/deploy.py
"""
import json
import os
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
NEEDED = ["AZ_TENANT", "AZ_SUBSCRIPTION", "AZ_RESOURCE_GROUP", "AZ_FACTORY", "AZ_STORAGE",
          "AZ_SQL_SERVER", "AZ_SQL_DATABASE", "AZ_PIPELINE_CLIENT_ID", "AZ_PIPELINE_CLIENT_SECRET",
          "SHOP_PG_HOST", "SHOP_PG_USER", "SHOP_PG_PASSWORD"]
API = "api-version=2018-06-01"


def env():
    missing = [k for k in NEEDED if not os.environ.get(k)]
    if missing:
        sys.exit("missing settings: " + " ".join(missing))
    return {k: os.environ[k] for k in NEEDED}


def token(e):
    body = urllib.parse.urlencode({
        "grant_type": "client_credentials", "client_id": e["AZ_PIPELINE_CLIENT_ID"],
        "client_secret": e["AZ_PIPELINE_CLIENT_SECRET"], "scope": "https://management.azure.com/.default"}).encode()
    req = urllib.request.Request(
        f"https://login.microsoftonline.com/{e['AZ_TENANT']}/oauth2/v2.0/token", data=body)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)["access_token"]
    except urllib.error.HTTPError as err:
        sys.exit(f"sign-in as orders-pipeline refused ({err.code}): "
                 f"{json.loads(err.read() or b'{}').get('error_description', '')[:300]}")


def linked_services(e):
    return {
        # the shop's database, read-only login (Neon PostgreSQL, TLS required)
        "shop": {"type": "PostgreSqlV2", "typeProperties": {
            "server": e["SHOP_PG_HOST"], "port": 5432, "database": "shop",
            "username": e["SHOP_PG_USER"], "password": {"type": "SecureString", "value": e["SHOP_PG_PASSWORD"]},
            "sslMode": 3, "authenticationType": "Basic"}},
        # the lake (ADLS Gen2) and the database, both as the factory's own managed identity
        "lake": {"type": "AzureBlobFS", "typeProperties": {
            "url": f"https://{e['AZ_STORAGE']}.dfs.core.windows.net"}},
        "orders_sql": {"type": "AzureSqlDatabase", "typeProperties": {
            "connectionString": f"Data Source=tcp:{e['AZ_SQL_SERVER']},1433;Initial Catalog={e['AZ_SQL_DATABASE']};"
                                "Encrypt=True;Connection Timeout=60"}},
    }


def sql_text(name):
    text = (HERE / "sql" / name).read_text()
    text = text.replace("{target}", "@{pipeline().parameters.target}").replace("{day}", "@{variables('day')}")
    return {"value": text, "type": "Expression"} if "@{" in text else text


def with_sql(node):
    if isinstance(node, dict):
        return {k: with_sql(v) for k, v in node.items()}
    if isinstance(node, list):
        return [with_sql(v) for v in node]
    if isinstance(node, str) and node.startswith("@sql:"):
        return sql_text(node[5:])
    return node


def git(*args):
    return subprocess.run(["git", "-C", str(HERE), *args], capture_output=True, text=True).stdout.strip()


def main():
    e = env()
    auth = {"Authorization": "Bearer " + token(e), "Content-Type": "application/json"}
    base = (f"https://management.azure.com/subscriptions/{e['AZ_SUBSCRIPTION']}/resourceGroups/"
            f"{e['AZ_RESOURCE_GROUP']}/providers/Microsoft.DataFactory/factories/{e['AZ_FACTORY']}")
    commit, branch = git("rev-parse", "--short", "HEAD"), git("branch", "--show-current")

    def put(kind, name, props):
        req = urllib.request.Request(f"{base}/{kind}/{name}?{API}", method="PUT", headers=auth,
                                     data=json.dumps({"properties": props}).encode())
        try:
            urllib.request.urlopen(req, timeout=60).read()
        except urllib.error.HTTPError as err:
            detail = json.loads(err.read() or b"{}").get("error", {})
            sys.exit(f"the factory refused {kind}/{name} ({err.code}): {detail.get('message', '')[:500]}")
        print(f"deployed {kind}/{name}")

    for name, props in linked_services(e).items():
        put("linkedservices", name, props)
    for f in sorted((HERE / "factory" / "datasets").glob("*.json")):
        put("datasets", f.stem, json.loads(f.read_text()))
    for f in sorted((HERE / "factory" / "pipelines").glob("*.json")):
        props = with_sql(json.loads(f.read_text()))
        props["annotations"] = [f"commit:{commit}"] + ([f"branch:{branch}"] if branch else [])
        put("pipelines", f.stem, props)
    print(f"at {branch or 'detached'} @ {commit}")


if __name__ == "__main__":
    main()

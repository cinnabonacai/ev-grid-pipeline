"""
EIA hourly electricity demand extractor.
Loads raw hourly demand for major balancing authorities into Snowflake RAW.
Idempotent: re-running for the same date range replaces that range, never duplicates.

Usage:
  python extract/eia_extract.py                     # yesterday (default)
  python extract/eia_extract.py --start 2026-06-01 --end 2026-06-30   # backfill
"""
import argparse
import os
from datetime import date, datetime, timedelta, timezone

import requests
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.primitives import serialization

load_dotenv()

EIA_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
RESPONDENTS = ["CISO", "ERCO", "NYIS", "PJM", "MISO"]  # CA, TX, NY, Mid-Atlantic, Midwest
PAGE_SIZE = 5000

TABLE = "EIA_DEMAND_HOURLY_RAW"
DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    PERIOD_TS        TIMESTAMP_NTZ,
    PERIOD_DATE      DATE,
    RESPONDENT       VARCHAR,
    RESPONDENT_NAME  VARCHAR,
    TYPE             VARCHAR,
    TYPE_NAME        VARCHAR,
    VALUE            VARCHAR,          -- raw layer: keep as-is, cast in dbt
    VALUE_UNITS      VARCHAR,
    _LOADED_AT       TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
"""


def get_connection():
    with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as f:
        pkey = serialization.load_pem_private_key(f.read(), password=None)
    pkb = pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=pkb,
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )


def fetch_eia(start: date, end: date) -> list[dict]:
    """Fetch all hourly demand rows for [start, end] inclusive, paginated."""
    rows, offset = [], 0
    while True:
        resp = requests.get(
            EIA_URL,
            params={
                "api_key": os.environ["EIA_API_KEY"],
                "frequency": "hourly",
                "data[0]": "value",
                "facets[type][]": "D",  # D = actual demand
                **{f"facets[respondent][]": RESPONDENTS},
                "start": f"{start}T00",
                "end": f"{end}T23",
                "offset": offset,
                "length": PAGE_SIZE,
                "sort[0][column]": "period",
                "sort[0][direction]": "asc",
            },
            timeout=60,
        )
        resp.raise_for_status()
        payload = resp.json()["response"]
        batch = payload["data"]
        rows.extend(batch)
        offset += PAGE_SIZE
        if offset >= int(payload["total"]) or not batch:
            return rows


def load(conn, rows: list[dict], start: date, end: date) -> int:
    cur = conn.cursor()
    cur.execute(DDL)
    cur.execute("BEGIN")
    try:
        # Idempotency: wipe the target partition before inserting
        cur.execute(
            f"DELETE FROM {TABLE} WHERE PERIOD_DATE BETWEEN %s AND %s",
            (start.isoformat(), end.isoformat()),
        )
        deleted = cur.rowcount
        cur.executemany(
            f"""INSERT INTO {TABLE}
                (PERIOD_TS, PERIOD_DATE, RESPONDENT, RESPONDENT_NAME,
                 TYPE, TYPE_NAME, VALUE, VALUE_UNITS)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            [
                (
                    r["period"].replace("T", " ") + ":00:00",
                    r["period"][:10],
                    r["respondent"],
                    r.get("respondent-name"),
                    r["type"],
                    r.get("type-name"),
                    r.get("value"),
                    r.get("value-units"),
                )
                for r in rows
            ],
        )
        cur.execute("COMMIT")
        print(f"Replaced partition {start}..{end}: deleted {deleted}, inserted {len(rows)}")
        return len(rows)
    except Exception:
        cur.execute("ROLLBACK")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    parser.add_argument("--start", type=date.fromisoformat, default=yesterday)
    parser.add_argument("--end", type=date.fromisoformat, default=yesterday)
    args = parser.parse_args()

    print(f"Fetching EIA hourly demand {args.start} → {args.end} for {RESPONDENTS}")
    rows = fetch_eia(args.start, args.end)
    print(f"Fetched {len(rows)} rows from API")

    conn = get_connection()
    try:
        load(conn, rows, args.start, args.end)
    finally:
        conn.close()

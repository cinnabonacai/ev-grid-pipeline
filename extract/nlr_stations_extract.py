"""
NLR EV charging stations extractor.
Full-snapshot load: replaces the entire table each run (dimension data, ~80k rows).
"""
import os
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from eia_extract import get_connection  # reuse the connection helper

load_dotenv()

NLR_URL = "https://developer.nlr.gov/api/alt-fuel-stations/v1.json"

TABLE = "NLR_EV_STATIONS_RAW"
DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    STATION_ID       NUMBER,
    STATION_NAME     VARCHAR,
    CITY             VARCHAR,
    STATE            VARCHAR,
    ZIP              VARCHAR,
    LATITUDE         FLOAT,
    LONGITUDE        FLOAT,
    EV_NETWORK       VARCHAR,
    EV_CONNECTOR_TYPES VARCHAR,
    EV_DC_FAST_NUM   NUMBER,
    EV_LEVEL2_NUM    NUMBER,
    OPEN_DATE        VARCHAR,
    ACCESS_CODE      VARCHAR,
    STATUS_CODE      VARCHAR,
    _SNAPSHOT_DATE   DATE,
    _LOADED_AT       TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
"""


def fetch_stations() -> list[dict]:
    resp = requests.get(
        NLR_URL,
        params={
            "api_key": os.environ["NREL_API_KEY"],
            "fuel_type": "ELEC",
            "country": "US",
            "status": "E",       # E = existing/open
            "access": "public",
            "limit": "all",
        },
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()["fuel_stations"]


def load(conn, stations: list[dict]) -> None:
    snapshot_date = datetime.now(timezone.utc).date().isoformat()
    cur = conn.cursor()
    cur.execute(DDL)
    cur.execute("BEGIN")
    try:
        cur.execute(f"TRUNCATE TABLE {TABLE}")
        cur.executemany(
            f"""INSERT INTO {TABLE}
                (STATION_ID, STATION_NAME, CITY, STATE, ZIP, LATITUDE, LONGITUDE,
                 EV_NETWORK, EV_CONNECTOR_TYPES, EV_DC_FAST_NUM, EV_LEVEL2_NUM,
                 OPEN_DATE, ACCESS_CODE, STATUS_CODE, _SNAPSHOT_DATE)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            [
                (
                    s.get("id"),
                    s.get("station_name"),
                    s.get("city"),
                    s.get("state"),
                    s.get("zip"),
                    s.get("latitude"),
                    s.get("longitude"),
                    s.get("ev_network"),
                    ",".join(s.get("ev_connector_types") or []),
                    s.get("ev_dc_fast_num"),
                    s.get("ev_level2_evse_num"),
                    s.get("open_date"),
                    s.get("access_code"),
                    s.get("status_code"),
                    snapshot_date,
                )
                for s in stations
            ],
        )
        cur.execute("COMMIT")
        print(f"Snapshot {snapshot_date}: loaded {len(stations)} stations")
    except Exception:
        cur.execute("ROLLBACK")
        raise


if __name__ == "__main__":
    print("Fetching all public EV stations from NLR...")
    stations = fetch_stations()
    print(f"Fetched {len(stations)} stations")
    conn = get_connection()
    try:
        load(conn, stations)
    finally:
        conn.close()

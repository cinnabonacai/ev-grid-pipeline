import os
import requests
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.primitives import serialization

load_dotenv()

# --- Snowflake ---
with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as f:
    pkey = serialization.load_pem_private_key(f.read(), password=None)
pkb = pkey.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    private_key=pkb,
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"],
)
print("Snowflake:", conn.cursor().execute("SELECT CURRENT_VERSION()").fetchone()[0])
conn.close()

# --- EIA: one hour of CAISO demand ---
r = requests.get(
    "https://api.eia.gov/v2/electricity/rto/region-data/data/",
    params={
        "api_key": os.environ["EIA_API_KEY"],
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": "CISO",
        "length": 1,
    },
    timeout=30,
)
r.raise_for_status()
print("EIA sample:", r.json()["response"]["data"][0])

# --- NREL: one EV station in CA ---
r = requests.get(
    "https://developer.nlr.gov/api/alt-fuel-stations/v1.json",
    params={"api_key": os.environ["NREL_API_KEY"], "fuel_type": "ELEC",
            "state": "CA", "limit": 1},
    timeout=30,
)
r.raise_for_status()
print("NREL sample:", r.json()["fuel_stations"][0]["station_name"])

print("\n✅ All three connections working.")


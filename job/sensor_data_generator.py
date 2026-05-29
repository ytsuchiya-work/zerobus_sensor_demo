# Databricks notebook source
# DBTITLE 1,Parameters
dbutils.widgets.text("num_records", "100", "Number of records to generate")
dbutils.widgets.text("interval_seconds", "1.0", "Interval between records (seconds)")

num_records = int(dbutils.widgets.get("num_records"))
interval_seconds = float(dbutils.widgets.get("interval_seconds"))

print(f"Generating {num_records} records with {interval_seconds}s interval")

# COMMAND ----------

# DBTITLE 1,Install Zerobus SDK
# MAGIC %pip install databricks-zerobus-ingest-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuration
import os

WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")
WORKSPACE_URL = f"https://{WORKSPACE_URL}" if not WORKSPACE_URL.startswith("http") else WORKSPACE_URL

try:
    WORKSPACE_ID = spark.conf.get("spark.databricks.clusterUsageTags.orgId")
except Exception:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    WORKSPACE_ID = ctx.workspaceId().get()

REGION = "ap-northeast-1"
ZEROBUS_ENDPOINT = f"{WORKSPACE_ID}.zerobus.{REGION}.cloud.databricks.com"

CATALOG = "classic_stable_ytcy_catalog"
SCHEMA = "zerobus"
TABLE = "sensor_data"
TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE}"

CLIENT_ID = dbutils.secrets.get("zerobus-demo", "client-id")
CLIENT_SECRET = dbutils.secrets.get("zerobus-demo", "client-secret")

print(f"Workspace: {WORKSPACE_URL}")
print(f"Zerobus Endpoint: {ZEROBUS_ENDPOINT}")
print(f"Target Table: {TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Re-read parameters after restartPython
num_records = int(dbutils.widgets.get("num_records"))
interval_seconds = float(dbutils.widgets.get("interval_seconds"))

# COMMAND ----------

# DBTITLE 1,Clear existing data
print(f"Truncating table {TABLE_NAME}...")
spark.sql(f"TRUNCATE TABLE {TABLE_NAME}")
print("Table cleared.")

# COMMAND ----------

# DBTITLE 1,Sensor Data Generator
import random
import time
import json
from datetime import datetime, timezone, timedelta

JST = timezone(timedelta(hours=9))

DEVICES = [
    "sensor-floor1-A",
    "sensor-floor1-B",
    "sensor-floor2-A",
    "sensor-floor2-B",
    "sensor-roof-C",
]

device_state = {d: {"temp": 24.0, "humidity": 55.0, "pressure": 1013.0} for d in DEVICES}

num_rounds = num_records // len(DEVICES)
print(f"Will generate {num_rounds} rounds x {len(DEVICES)} devices = {num_rounds * len(DEVICES)} records")


def generate_reading(device_id, seq_id, ts):
    state = device_state[device_id]
    state["temp"] += random.gauss(0, 0.5)
    state["temp"] = max(15.0, min(40.0, state["temp"]))
    state["humidity"] += random.gauss(0, 1.0)
    state["humidity"] = max(20.0, min(90.0, state["humidity"]))
    state["pressure"] += random.gauss(0, 0.3)
    state["pressure"] = max(990.0, min(1040.0, state["pressure"]))

    return {
        "id": seq_id,
        "device": device_id,
        "payload": json.dumps({
            "timestamp": ts,
            "temperature_c": round(state["temp"], 2),
            "humidity_pct": round(state["humidity"], 2),
            "pressure_hpa": round(state["pressure"], 2),
            "battery_pct": round(random.uniform(60.0, 100.0), 1),
            "status": random.choices(["OK", "WARN", "ERROR"], weights=[90, 8, 2])[0],
        }),
    }

# COMMAND ----------

# DBTITLE 1,Ingest via Zerobus SDK
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

sdk = ZerobusSdk(ZEROBUS_ENDPOINT, unity_catalog_url=WORKSPACE_URL)
table_properties = TableProperties(TABLE_NAME)
options = StreamConfigurationOptions(record_type=RecordType.JSON)
stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options)

try:
    seq = 0
    for round_idx in range(num_rounds):
        round_start = time.time()
        ts = datetime.now(JST).isoformat()
        for device in DEVICES:
            record = generate_reading(device, seq, ts)
            stream.ingest_record(record)
            stream.flush()
            seq += 1

        if (round_idx + 1) % 5 == 0:
            print(f"Round {round_idx + 1}/{num_rounds}: {seq} records ingested")

        elapsed = time.time() - round_start
        sleep_time = max(0, interval_seconds - elapsed)
        time.sleep(sleep_time)

    print(f"Completed: ingested {seq} records to {TABLE_NAME}")
finally:
    stream.close()

# COMMAND ----------

# DBTITLE 1,Verify Data
display(spark.sql(f"SELECT * FROM {TABLE_NAME} ORDER BY payload:timestamp::STRING DESC LIMIT 20"))

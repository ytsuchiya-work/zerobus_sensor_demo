from __future__ import annotations

import os
import logging
import asyncio
import time
import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

log = logging.getLogger("zerobus-demo")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Zerobus Sensor Demo")

CATALOG = "classic_stable_ytcy_catalog"
SCHEMA = "zerobus"
TABLE = "sensor_data"

w: WorkspaceClient | None = None
warehouse_id: str = ""
job_id: int = 0

cache = {
    "sensor_data": {"columns": [], "data": []},
    "device_summary": {"columns": [], "data": []},
    "record_count": 0,
    "last_update": 0,
}
cache_lock = asyncio.Lock()


@app.on_event("startup")
def startup():
    global w, warehouse_id, job_id
    w = WorkspaceClient()
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    job_id = int(os.environ.get("SENSOR_JOB_ID", "0"))
    log.info("App started. Warehouse=%s, Job=%d", warehouse_id, job_id)


@app.on_event("startup")
async def start_cache_refresh():
    asyncio.create_task(fast_count_loop())
    asyncio.create_task(full_data_loop())


async def fast_count_loop():
    loop = asyncio.get_event_loop()
    while True:
        try:
            count = await loop.run_in_executor(None, fetch_record_count_sql)
            async with cache_lock:
                cache["record_count"] = count
                cache["last_update"] = time.time()
        except Exception as e:
            log.error("Count refresh error: %s", e)
        await asyncio.sleep(0)


async def full_data_loop():
    loop = asyncio.get_event_loop()
    while True:
        try:
            sensor_data, device_summary = await asyncio.gather(
                loop.run_in_executor(None, fetch_sensor_data_sql),
                loop.run_in_executor(None, fetch_device_summary_sql),
            )
            async with cache_lock:
                cache["sensor_data"] = sensor_data
                cache["device_summary"] = device_summary
                cache["last_update"] = time.time()
        except Exception as e:
            log.error("Data refresh error: %s", e)
        await asyncio.sleep(0)


def execute_sql(sql: str) -> dict:
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="30s",
    )
    if result.status and result.status.state != StatementState.SUCCEEDED:
        error_msg = ""
        if result.status.error:
            error_msg = result.status.error.message
        raise HTTPException(status_code=500, detail=f"Query failed: {error_msg}")

    columns = [col.name for col in result.manifest.schema.columns]
    rows = []
    if result.result and result.result.data_array:
        for row in result.result.data_array:
            rows.append(dict(zip(columns, row)))
    return {"columns": columns, "data": rows}


def fetch_sensor_data_sql():
    sql = f"""
        SELECT
            id,
            device,
            payload:timestamp::STRING as timestamp,
            payload:temperature_c::DOUBLE as temperature_c,
            payload:humidity_pct::DOUBLE as humidity_pct,
            payload:pressure_hpa::DOUBLE as pressure_hpa,
            payload:battery_pct::DOUBLE as battery_pct,
            payload:status::STRING as status
        FROM {CATALOG}.{SCHEMA}.{TABLE}
        ORDER BY payload:timestamp::STRING DESC
        LIMIT 100
    """
    return execute_sql(sql)


def fetch_device_summary_sql():
    sql = f"""
        WITH ranked AS (
            SELECT
                device,
                payload:temperature_c::DOUBLE as temperature_c,
                payload:humidity_pct::DOUBLE as humidity_pct,
                payload:pressure_hpa::DOUBLE as pressure_hpa,
                payload:battery_pct::DOUBLE as battery_pct,
                payload:status::STRING as status,
                payload:timestamp::STRING as timestamp,
                ROW_NUMBER() OVER (PARTITION BY device ORDER BY payload:timestamp::STRING DESC) as rn
            FROM {CATALOG}.{SCHEMA}.{TABLE}
        )
        SELECT device, temperature_c, humidity_pct, pressure_hpa, battery_pct, status, timestamp
        FROM ranked WHERE rn = 1
        ORDER BY device
    """
    return execute_sql(sql)


def fetch_record_count_sql():
    sql = f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.{TABLE}"
    result = execute_sql(sql)
    return int(result["data"][0]["cnt"]) if result["data"] else 0


# --- SSE Stream ---
@app.get("/api/stream")
async def stream_data():
    async def event_generator():
        last_update = 0
        while True:
            async with cache_lock:
                current_update = cache["last_update"]
                if current_update != last_update:
                    last_update = current_update
                    yield {
                        "event": "update",
                        "data": json.dumps({
                            "sensor_data": cache["sensor_data"],
                            "device_summary": cache["device_summary"],
                            "record_count": cache["record_count"],
                        }),
                    }
            await asyncio.sleep(0.02)

    return EventSourceResponse(event_generator())


@app.get("/api/health")
def health():
    return {"status": "ok", "warehouse": warehouse_id, "job_id": job_id}


@app.get("/api/sensor-data")
async def get_sensor_data(limit: int = Query(default=100, le=500)):
    async with cache_lock:
        return cache["sensor_data"]


@app.get("/api/device-summary")
async def get_device_summary():
    async with cache_lock:
        return cache["device_summary"]


@app.get("/api/record-count")
async def get_record_count():
    async with cache_lock:
        return {"count": cache["record_count"]}


# --- Job trigger ---
class TriggerRequest(BaseModel):
    num_records: int = 100
    interval_seconds: float = 1.0


@app.post("/api/trigger-job")
async def trigger_job(req: TriggerRequest):
    if job_id == 0:
        raise HTTPException(status_code=400, detail="SENSOR_JOB_ID not configured")

    run = w.jobs.run_now(
        job_id=job_id,
        notebook_params={
            "num_records": str(req.num_records),
            "interval_seconds": str(req.interval_seconds),
        },
    )
    return {"run_id": run.run_id, "status": "triggered"}


@app.get("/api/job-status/{run_id}")
def get_job_status(run_id: int):
    run = w.jobs.get_run(run_id=run_id)
    state = "UNKNOWN"
    result_state = None
    if run.state:
        state = run.state.life_cycle_state.value if run.state.life_cycle_state else "UNKNOWN"
        result_state = run.state.result_state.value if run.state.result_state else None
    return {
        "run_id": run_id,
        "state": state,
        "result_state": result_state,
        "start_time": run.start_time,
        "end_time": run.end_time,
    }


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def root():
    return FileResponse("static/index.html")

import os
import re
import uuid
import json
import time
import shutil
import threading
import tempfile
from datetime import datetime, date
from decimal import Decimal

import snowflake.connector
from fastapi import FastAPI, Header, HTTPException, UploadFile, File, Query, Body
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Snowflake Pipeline Console")
app.mount("/static", StaticFiles(directory="static"), name="static")

DB = "DATA_PLATFORM"
STAGE = f"@{DB}.BRONZE.STG_LANDING"
CONFIG_TABLES = {
    "source_control": ("SOURCE_CONTROL", "SOURCE_NAME"),
    "dq_config": ("DQ_CONFIG", "DQ_ID"),
    "column_rules": ("COLUMN_RULES", "RULE_ID"),
    "gold_control": ("GOLD_CONTROL", "CONTROL_NAME"),
}
JOBS = {}
JOB_LOCK = threading.Lock()
PIPELINE_LOCK = threading.Lock()


def authorize(authorization):
    token = os.getenv("APP_API_TOKEN")
    if not token:
        raise HTTPException(500, "APP_API_TOKEN is not configured on the server")
    if authorization != f"Bearer {token}":
        raise HTTPException(401, "Invalid app access token")


def connect():
    keys = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_WAREHOUSE"]
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise RuntimeError("Missing server settings: " + ", ".join(missing))
    args = dict(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE", DB),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "CONTROL"),
        client_session_keep_alive=False,
        login_timeout=30,
    )
    if os.getenv("SNOWFLAKE_ROLE"):
        args["role"] = os.getenv("SNOWFLAKE_ROLE")
    return snowflake.connector.connect(**args)


def jsonable(v):
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, Decimal):
        return float(v) if v % 1 else int(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat(sep=" ")
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return str(v)


def query(sql, params=None):
    conn = connect()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql, params or [])
            if not cur.description:
                return {"columns": [], "rows": []}
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, [jsonable(v) for v in r])) for r in cur.fetchall()]
            return {"columns": cols, "rows": rows}
        finally:
            cur.close()
    finally:
        conn.close()


def execute(sql, params=None):
    conn = connect()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql, params or [])
            row = cur.fetchone()
            return jsonable(row[0]) if row else None
        finally:
            cur.close()
    finally:
        conn.close()


def safe_ident(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name or ""):
        raise HTTPException(400, f"Invalid identifier: {name}")
    return name.upper()


def set_job(job_id, **kw):
    with JOB_LOCK:
        JOBS[job_id].update(kw)
        JOBS[job_id]["updated_at"] = time.time()


def run_pipeline_job(job_id, file_paths):
    try:
        if file_paths:
            set_job(job_id, status="UPLOADING", step="Uploading files to stage")
            conn = connect()
            try:
                cur = conn.cursor()
                for p in file_paths:
                    cur.execute(f"PUT 'file://{p}' {STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
                cur.close()
            finally:
                conn.close()
        set_job(job_id, status="RUNNING", step="Running SP_PIPELINE (Bronze -> DQ -> Silver -> Gold)")
        with PIPELINE_LOCK:
            result = execute(f"CALL {DB}.CONTROL.SP_PIPELINE()")
        set_job(job_id, status="DONE", step="Completed", result=result)
    except Exception as e:
        set_job(job_id, status="FAILED", step="Failed", error=str(e))
    finally:
        for p in file_paths:
            shutil.rmtree(os.path.dirname(p), ignore_errors=True)


def start_job(file_paths, file_names):
    job_id = str(uuid.uuid4())
    with JOB_LOCK:
        JOBS[job_id] = {"job_id": job_id, "status": "QUEUED", "step": "Queued", "files": file_names,
                        "result": None, "error": None, "created_at": time.time(), "updated_at": time.time()}
    threading.Thread(target=run_pipeline_job, args=(job_id, file_paths), daemon=True).start()
    return job_id


@app.get("/")
def home():
    return FileResponse("static/index.html")


@app.get("/api/health")
def health(authorization: str | None = Header(default=None)):
    authorize(authorization)
    try:
        info = query("SELECT CURRENT_USER() AS U, CURRENT_ROLE() AS R, CURRENT_WAREHOUSE() AS W, CURRENT_ACCOUNT() AS A")["rows"][0]
        return {"ok": True, "snowflake": info}
    except Exception as e:
        raise HTTPException(500, f"Snowflake connection failed: {e}")


@app.post("/api/upload")
async def upload(files: list[UploadFile] = File(...), authorization: str | None = Header(default=None)):
    authorize(authorization)
    tmp = tempfile.mkdtemp(prefix="sfupload_")
    paths, names = [], []
    for f in files:
        name = os.path.basename(f.filename or "")
        if not name.lower().endswith(".csv"):
            shutil.rmtree(tmp, ignore_errors=True)
            raise HTTPException(400, f"Only .csv files are accepted: {name}")
        name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
        path = os.path.join(tmp, name)
        with open(path, "wb") as out:
            out.write(await f.read())
        paths.append(path)
        names.append(name)
    job_id = start_job(paths, names)
    return {"ok": True, "job_id": job_id, "files": names}


@app.post("/api/run")
def run_now(authorization: str | None = Header(default=None)):
    authorize(authorization)
    return {"ok": True, "job_id": start_job([], [])}


@app.get("/api/jobs/{job_id}")
def job(job_id: str, authorization: str | None = Header(default=None)):
    authorize(authorization)
    with JOB_LOCK:
        j = JOBS.get(job_id)
    if not j:
        raise HTTPException(404, "Unknown job")
    return j


@app.get("/api/summary")
def summary(authorization: str | None = Header(default=None)):
    authorize(authorization)
    try:
        s = query(f"""
            SELECT COUNT(*) AS FILES,
                   SUM(IFF(PIPELINE_STATUS='COMPLETED',1,0)) AS COMPLETED,
                   SUM(IFF(PIPELINE_STATUS='FAILED',1,0)) AS FAILED,
                   SUM(IFF(PIPELINE_STATUS='IN_PROGRESS',1,0)) AS IN_PROGRESS,
                   COALESCE(SUM(BRONZE_ROWS),0) AS ROWS_LOADED,
                   COALESCE(SUM(SILVER_QUARANTINED),0) + COALESCE(SUM(GOLD_QUARANTINED),0) AS ROWS_QUARANTINED,
                   MAX(INGESTED_AT) AS LAST_RUN
            FROM {DB}.CONTROL.V_PIPELINE_RUNS
        """)["rows"][0]
        stage = query(f"LIST {STAGE}")["rows"]
        s["FILES_WAITING"] = len([r for r in stage if "/archive/" not in r.get("name", "")])
        return s
    except Exception as e:
        raise HTTPException(500, f"Summary failed: {e}")


@app.get("/api/runs")
def runs(limit: int = 50, file: str | None = None, authorization: str | None = Header(default=None)):
    authorize(authorization)
    limit = max(1, min(limit, 500))
    try:
        if file:
            return query(f"SELECT * FROM {DB}.CONTROL.V_PIPELINE_RUNS WHERE FILE_NAME = %s ORDER BY INGESTED_AT DESC LIMIT {limit}", [file])
        return query(f"SELECT * FROM {DB}.CONTROL.V_PIPELINE_RUNS ORDER BY INGESTED_AT DESC LIMIT {limit}")
    except Exception as e:
        raise HTTPException(500, f"Could not read pipeline history: {e}")


@app.get("/api/runs/{run_id}")
def run_detail(run_id: str, authorization: str | None = Header(default=None)):
    authorize(authorization)
    try:
        return {
            "audit": query(f"SELECT * FROM {DB}.CONTROL.PIPELINE_AUDIT WHERE RUN_ID = %s ORDER BY AUDIT_ID", [run_id])["rows"],
            "dq": query(f"SELECT * FROM {DB}.CONTROL.DQ_RESULTS WHERE RUN_ID = %s ORDER BY CHECK_TS, LAYER", [run_id])["rows"],
            "quarantine": query(f"SELECT LAYER, COLUMN_NAME, RULE_TYPE, REASON, ROW_DATA, REJECTED_TS FROM {DB}.CONTROL.QUARANTINE WHERE RUN_ID = %s ORDER BY REJECTED_TS LIMIT 500", [run_id])["rows"],
        }
    except Exception as e:
        raise HTTPException(500, f"Could not read run detail: {e}")


@app.get("/api/dq")
def dq(limit: int = 200, authorization: str | None = Header(default=None)):
    authorize(authorization)
    try:
        return query(f"SELECT * FROM {DB}.CONTROL.DQ_RESULTS WHERE ROWS_FAILED > 0 ORDER BY CHECK_TS DESC LIMIT {max(1, min(limit, 1000))}")
    except Exception as e:
        raise HTTPException(500, f"Could not read DQ results: {e}")


@app.get("/api/quarantine")
def quarantine(limit: int = 200, authorization: str | None = Header(default=None)):
    authorize(authorization)
    try:
        return query(f"SELECT RUN_ID, LAYER, SOURCE_NAME, COLUMN_NAME, RULE_TYPE, REASON, ROW_DATA, REJECTED_TS FROM {DB}.CONTROL.QUARANTINE ORDER BY REJECTED_TS DESC LIMIT {max(1, min(limit, 1000))}")
    except Exception as e:
        raise HTTPException(500, f"Could not read quarantine: {e}")


@app.get("/api/config/{name}")
def config_get(name: str, authorization: str | None = Header(default=None)):
    authorize(authorization)
    if name not in CONFIG_TABLES:
        raise HTTPException(404, "Unknown config table")
    table, key = CONFIG_TABLES[name]
    try:
        return query(f"SELECT * FROM {DB}.CONTROL.{table} ORDER BY {key}")
    except Exception as e:
        raise HTTPException(500, f"Could not read {table}: {e}")


@app.post("/api/config/{name}")
def config_add(name: str, row: dict = Body(...), authorization: str | None = Header(default=None)):
    authorize(authorization)
    if name not in CONFIG_TABLES:
        raise HTTPException(404, "Unknown config table")
    table, key = CONFIG_TABLES[name]
    cols, vals = [], []
    for k, v in row.items():
        if v is None or v == "":
            continue
        cols.append(safe_ident(k))
        if isinstance(v, bool):
            vals.append(v)
        elif isinstance(v, str) and v.strip().upper() in ("TRUE", "FALSE"):
            vals.append(v.strip().upper() == "TRUE")
        else:
            vals.append(v)
    if not cols:
        raise HTTPException(400, "Empty row")
    try:
        execute(f"INSERT INTO {DB}.CONTROL.{table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(vals))})", vals)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, f"Insert into {table} failed: {e}")


@app.delete("/api/config/{name}")
def config_delete(name: str, key: str = Query(...), authorization: str | None = Header(default=None)):
    authorize(authorization)
    if name not in CONFIG_TABLES:
        raise HTTPException(404, "Unknown config table")
    table, keycol = CONFIG_TABLES[name]
    try:
        execute(f"DELETE FROM {DB}.CONTROL.{table} WHERE {keycol} = %s", [key])
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, f"Delete from {table} failed: {e}")


@app.get("/api/tables")
def tables(authorization: str | None = Header(default=None)):
    authorize(authorization)
    try:
        return query(f"""
            SELECT TABLE_SCHEMA, TABLE_NAME, ROW_COUNT, TABLE_TYPE
            FROM {DB}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA IN ('BRONZE','SILVER','GOLD')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
    except Exception as e:
        raise HTTPException(500, f"Could not list tables: {e}")


@app.get("/api/tables/{schema}/{table}")
def table_preview(schema: str, table: str, limit: int = 100, authorization: str | None = Header(default=None)):
    authorize(authorization)
    schema = safe_ident(schema)
    table = safe_ident(table)
    if schema not in ("BRONZE", "SILVER", "GOLD", "CONTROL"):
        raise HTTPException(400, "Schema not allowed")
    try:
        return query(f'SELECT * FROM {DB}.{schema}."{table}" LIMIT {max(1, min(limit, 1000))}')
    except Exception as e:
        raise HTTPException(500, f"Could not read {schema}.{table}: {e}")


@app.get("/api/stage")
def stage(authorization: str | None = Header(default=None)):
    authorize(authorization)
    try:
        rows = query(f"LIST {STAGE}")["rows"]
        return {"waiting": [r for r in rows if "/archive/" not in r.get("name", "")],
                "archived": [r for r in rows if "/archive/" in r.get("name", "")][-50:]}
    except Exception as e:
        raise HTTPException(500, f"Could not list stage: {e}")

import os
import snowflake.connector
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Snowflake Pipeline Console")
app.mount("/static", StaticFiles(directory="static"), name="static")

def authorize(authorization):
    token = os.getenv("APP_API_TOKEN")
    if not token:
        raise HTTPException(500, "APP_API_TOKEN is not configured on the server")
    if authorization != f"Bearer {token}":
        raise HTTPException(401, "Invalid app access token")

def snowflake_connection():
    keys = ["SNOWFLAKE_ACCOUNT","SNOWFLAKE_USER","SNOWFLAKE_PASSWORD","SNOWFLAKE_WAREHOUSE","SNOWFLAKE_DATABASE","SNOWFLAKE_SCHEMA"]
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise RuntimeError("Missing server settings: " + ", ".join(missing))
    args = {k.lower().replace("snowflake_", ""): os.getenv(k) for k in keys}
    if os.getenv("SNOWFLAKE_ROLE"):
        args["role"] = os.getenv("SNOWFLAKE_ROLE")
    return snowflake.connector.connect(**args)

@app.get("/")
def home():
    return FileResponse("static/index.html")

@app.get("/api/health")
def health(authorization: str | None = Header(default=None)):
    authorize(authorization)
    return {"ok": True, "service": "Snowflake Pipeline Console API"}

@app.post("/api/run")
def run_pipeline(authorization: str | None = Header(default=None)):
    authorize(authorization)
    conn = None
    try:
        conn = snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute("CALL DATA_PLATFORM.CONTROL.SP_PIPELINE()")
            result = cur.fetchone()
            return {"ok": True, "message": "Pipeline procedure completed.", "result": str(result[0]) if result else "No result returned"}
        finally:
            cur.close()
    except Exception as e:
        raise HTTPException(500, f"Pipeline execution failed: {e}")
    finally:
        if conn:
            conn.close()

@app.get("/api/runs")
def runs(authorization: str | None = Header(default=None), limit: int = 20):
    authorize(authorization)
    conn = None
    try:
        conn = snowflake_connection()
        cur = conn.cursor()
        try:
            limit = max(1, min(limit, 100))
            cur.execute(f"SELECT * FROM DATA_PLATFORM.CONTROL.V_PIPELINE_RUNS LIMIT {limit}")
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, [str(v) if v is not None else None for v in row])) for row in cur.fetchall()]
            return {"columns": cols, "rows": rows}
        finally:
            cur.close()
    except Exception as e:
        raise HTTPException(500, f"Could not read pipeline history: {e}")
    finally:
        if conn:
            conn.close()

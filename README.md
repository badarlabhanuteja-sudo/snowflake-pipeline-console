Snowflake Pipeline Console
Browser (React) -> FastAPI (this service) -> Snowflake connector -> DATA_PLATFORM framework.

Pages
Overview: every file with Bronze / Silver / Gold status, auto-refresh, click a row for details
Upload & run: drop CSV files -> PUT to @BRONZE.STG_LANDING -> CALL SP_PIPELINE() -> live step tracker
Data quality: failed DQ rules and quarantine
Configuration: SOURCE_CONTROL, DQ_CONFIG, COLUMN_RULES, GOLD_CONTROL (add / delete rows)
Data explorer: preview any Bronze / Silver / Gold table
Deploy on Render
Push this repo to GitHub.
Render -> New -> Blueprint -> select the repo (uses render.yaml), or New Web Service with
build pip install -r requirements.txt and start uvicorn app:app --host 0.0.0.0 --port $PORT.
Environment variables: APP_API_TOKEN (any long random string), SNOWFLAKE_ACCOUNT (e.g. abc12345.ap-south-1),
SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE (optional), SNOWFLAKE_WAREHOUSE=INGEST_WH.
Open the Render URL, enter the APP_API_TOKEN.
Snowflake side (run once)
Execute 01_setup.sql ... 05_pipeline.sql from the framework. The user in SNOWFLAKE_USER needs USAGE on
DATA_PLATFORM, all schemas and INGEST_WH, WRITE on the stage, SELECT/INSERT/DELETE on CONTROL tables,
and USAGE on the procedures (or simply the role that created them).

Local run
pip install -r requirements.txt
set the same environment variables
uvicorn app:app --reload -> http://127.0.0.1:8000

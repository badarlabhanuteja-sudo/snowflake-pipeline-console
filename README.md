# Snowflake Pipeline Console (HTML/CSS/JavaScript + FastAPI)

Browser UI → HTTPS FastAPI backend → Snowflake Connector → Snowflake.

## Deploy to Render with HTTPS
1. Upload this folder to a GitHub repository. Never commit real credentials.
2. In Render, create a **New Web Service** and connect the repository (or use `render.yaml` as a Blueprint).
3. Build command: `pip install -r requirements.txt`
4. Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
5. Configure environment variables shown in `render.yaml`, including a long random `APP_API_TOKEN` and your Snowflake credentials.
6. Deploy. Render supplies an HTTPS URL. Open it and enter the app token to connect.

## Local run
Install Python 3.11+, run `pip install -r requirements.txt`, set the environment variables listed in `render.yaml`, then run `uvicorn app:app --reload` and open `http://127.0.0.1:8000`.

## Production notes
- Snowflake credentials are server-side only; never put them in frontend JavaScript.
- This is a starter with token protection, not a full enterprise login. Prefer company SSO/OAuth, audit logging, and role-based permissions for production.
- Grant the Snowflake service role only required privileges; do not use ACCOUNTADMIN.
- Confirm the columns in `V_PIPELINE_RUNS`; this starter reads `SELECT *` and may need adjustment.
- Manual and scheduled executions can overlap. Add a durable Snowflake lock/request mechanism if overlapping runs are unsafe.
- Long-running procedures should use a background job/request table and status polling instead of a synchronous HTTP request.

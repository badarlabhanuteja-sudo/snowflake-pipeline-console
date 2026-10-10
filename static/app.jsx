const { useState, useEffect, useRef, useCallback } = React;

const getToken = () => sessionStorage.getItem("pipelineToken") || "";

async function api(path, opts = {}) {
  const headers = { Authorization: "Bearer " + getToken(), ...(opts.headers || {}) };
  const r = await fetch(path, { ...opts, headers });
  const d = await r.json().catch(() => ({ detail: "Invalid server response" }));
  if (!r.ok) throw new Error(d.detail || "Request failed");
  return d;
}

const STATUS_CLASS = { SUCCESS: "ok", COMPLETED: "ok", FAILED: "err", RUNNING: "warn", IN_PROGRESS: "warn", QUARANTINE: "err", TRANSFORMATION: "warn", WARNING: "warn", UNKNOWN_MEMBER: "warn", ERROR: "err", NONE: "muted" };
const Badge = ({ v }) => v == null ? <span className="muted">—</span> : <span className={"badge " + (STATUS_CLASS[v] || "")}>{v}</span>;

function Table({ columns, rows, onRow, badges = [], max = 400, empty = "No data" }) {
  if (!rows || !rows.length) return <p className="muted">{empty}</p>;
  const cols = columns || Object.keys(rows[0]);
  return (
    <div className="tablewrap">
      <table>
        <thead><tr>{cols.map(c => <th key={c}>{c}</th>)}</tr></thead>
        <tbody>
          {rows.slice(0, max).map((r, i) => (
            <tr key={i} onClick={onRow ? () => onRow(r) : undefined} className={onRow ? "clickable" : ""}>
              {cols.map(c => <td key={c}>{badges.includes(c) ? <Badge v={r[c]} /> : fmt(r[c])}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function fmt(v) {
  if (v == null) return <span className="muted">—</span>;
  if (typeof v === "object") return <code>{JSON.stringify(v)}</code>;
  const s = String(v);
  if (s.startsWith("{") && s.endsWith("}")) return <code className="json">{s}</code>;
  return s;
}

function Login({ onDone }) {
  const [token, setToken] = useState(getToken());
  const [msg, setMsg] = useState("");
  const connect = async () => {
    sessionStorage.setItem("pipelineToken", token.trim());
    setMsg("Connecting…");
    try {
      const d = await api("/api/health");
      setMsg("");
      onDone(d.snowflake);
    } catch (e) { setMsg(e.message); }
  };
  return (
    <div className="login card">
      <h2>Connect to workspace</h2>
      <p>Enter the app access token configured on the server (APP_API_TOKEN).</p>
      <div className="inputrow">
        <input type="password" value={token} placeholder="App access token" onChange={e => setToken(e.target.value)} onKeyDown={e => e.key === "Enter" && connect()} />
        <button className="primary" onClick={connect}>Connect</button>
      </div>
      {msg && <p className="err">{msg}</p>}
    </div>
  );
}

function JobTracker({ job, onClose, openRun }) {
  const [state, setState] = useState(null);
  const [runs, setRuns] = useState([]);
  useEffect(() => {
    if (!job) return;
    let alive = true;
    const tick = async () => {
      try {
        const j = await api("/api/jobs/" + job.job_id);
        if (!alive) return;
        setState(j);
        if (job.files && job.files.length) {
          const all = [];
          for (const f of job.files) {
            const d = await api("/api/runs?file=" + encodeURIComponent(f) + "&limit=1");
            if (d.rows[0]) all.push(d.rows[0]); else all.push({ FILE_NAME: f, PIPELINE_STATUS: j.status === "DONE" || j.status === "FAILED" ? "NOT_PICKED" : "WAITING" });
          }
          if (alive) setRuns(all);
        }
        if (j.status === "DONE" || j.status === "FAILED") return;
      } catch (e) { if (alive) setState({ status: "FAILED", error: e.message }); return; }
      if (alive) setTimeout(tick, 2500);
    };
    tick();
    return () => { alive = false; };
  }, [job && job.job_id]);
  if (!job) return null;
  const s = state || { status: "QUEUED", step: "Starting…" };
  const steps = ["UPLOADING", "RUNNING", "DONE"];
  const idx = s.status === "FAILED" ? -1 : steps.indexOf(s.status);
  return (
    <div className="card tracker">
      <div className="headrow"><h2>Live processing</h2><button onClick={onClose}>✕</button></div>
      <div className="steps">
        {["Upload to stage", "Bronze → DQ → Silver → Gold", "Finished"].map((label, i) => (
          <div key={i} className={"step " + (s.status === "FAILED" ? "err" : idx > i ? "done" : idx === i ? "active" : "")}>
            <span className="dot" />{label}
          </div>
        ))}
      </div>
      <p className={s.status === "FAILED" ? "err" : "muted"}>{s.step}{s.error ? " – " + s.error : ""}</p>
      {s.result && <pre className="result">{s.result}</pre>}
      {runs.length > 0 && (
        <Table rows={runs.map(r => ({ FILE: r.FILE_NAME, SOURCE: r.SOURCE_NAME, PIPELINE: r.PIPELINE_STATUS, BRONZE: r.INGEST_STATUS, BRONZE_ROWS: r.BRONZE_ROWS, SILVER: r.SILVER_STATUS, SILVER_ROWS: r.SILVER_ROWS, QUARANTINED: r.SILVER_QUARANTINED, GOLD: r.GOLD_STATUS, GOLD_TARGET: r.GOLD_TARGET, ERROR: r.ERROR_MESSAGE, RUN_ID: r.RUN_ID }))}
          badges={["PIPELINE", "BRONZE", "SILVER", "GOLD"]} onRow={r => r.RUN_ID && openRun(r.RUN_ID)} />
      )}
    </div>
  );
}

function UploadPage({ openRun }) {
  const [files, setFiles] = useState([]);
  const [job, setJob] = useState(null);
  const [err, setErr] = useState("");
  const [drag, setDrag] = useState(false);
  const inputRef = useRef();
  const pick = list => setFiles(Array.from(list).filter(f => f.name.toLowerCase().endsWith(".csv")));
  const send = async () => {
    setErr("");
    const fd = new FormData();
    files.forEach(f => fd.append("files", f));
    try {
      const d = await api("/api/upload", { method: "POST", body: fd });
      setJob(d);
      setFiles([]);
    } catch (e) { setErr(e.message); }
  };
  const runNow = async () => {
    setErr("");
    try { setJob(await api("/api/run", { method: "POST" })); } catch (e) { setErr(e.message); }
  };
  return (
    <>
      <div className="card">
        <h2>Upload CSV files</h2>
        <p>Files go to <code>@BRONZE.STG_LANDING</code>, then the pipeline runs immediately: Bronze → DQ → Silver → Gold. File name decides the source (<code>sales_1234.csv → SALES</code>).</p>
        <div className={"drop " + (drag ? "drag" : "")} onDragOver={e => { e.preventDefault(); setDrag(true); }} onDragLeave={() => setDrag(false)}
          onDrop={e => { e.preventDefault(); setDrag(false); pick(e.dataTransfer.files); }} onClick={() => inputRef.current.click()}>
          <input ref={inputRef} type="file" multiple accept=".csv" hidden onChange={e => pick(e.target.files)} />
          {files.length ? <ul>{files.map(f => <li key={f.name}>{f.name} <span className="muted">({(f.size / 1024).toFixed(1)} KB)</span></li>)}</ul> : <span>Drop CSV files here or click to choose</span>}
        </div>
        <div className="inputrow">
          <button className="primary" disabled={!files.length} onClick={send}>⬆ Upload &amp; run pipeline</button>
          <button onClick={runNow}>▶ Run pipeline on files already in stage</button>
        </div>
        {err && <p className="err">{err}</p>}
      </div>
      <JobTracker job={job} onClose={() => setJob(null)} openRun={openRun} />
    </>
  );
}

function Overview({ openRun }) {
  const [sum, setSum] = useState(null);
  const [runs, setRuns] = useState([]);
  const [err, setErr] = useState("");
  const [auto, setAuto] = useState(true);
  const load = useCallback(async () => {
    try {
      const [s, r] = await Promise.all([api("/api/summary"), api("/api/runs?limit=100")]);
      setSum(s); setRuns(r.rows); setErr("");
    } catch (e) { setErr(e.message); }
  }, []);
  useEffect(() => { load(); if (!auto) return; const t = setInterval(load, 10000); return () => clearInterval(t); }, [load, auto]);
  const cards = sum ? [["Files processed", sum.FILES], ["Completed", sum.COMPLETED], ["Failed", sum.FAILED], ["Waiting in stage", sum.FILES_WAITING], ["Rows loaded", sum.ROWS_LOADED], ["Rows quarantined", sum.ROWS_QUARANTINED]] : [];
  return (
    <>
      <div className="metrics">{cards.map(([k, v]) => <div className="card metric" key={k}><small>{k}</small><h2>{v ?? 0}</h2></div>)}</div>
      <div className="card">
        <div className="headrow"><div><h2>Pipeline runs</h2><p>One line per file · from <code>V_PIPELINE_RUNS</code> · click a row for details</p></div>
          <div><label className="muted"><input type="checkbox" checked={auto} onChange={e => setAuto(e.target.checked)} /> auto-refresh</label> <button onClick={load}>↻ Refresh</button></div></div>
        {err && <p className="err">{err}</p>}
        <Table rows={runs.map(r => ({ TIME: r.INGESTED_AT, FILE: r.FILE_NAME, SOURCE: r.SOURCE_NAME, PIPELINE: r.PIPELINE_STATUS, BRONZE: r.INGEST_STATUS, ROWS: r.BRONZE_ROWS, SILVER: r.SILVER_STATUS, SILVER_ROWS: r.SILVER_ROWS, QUAR: r.SILVER_QUARANTINED, WARN: r.SILVER_WARNED, SILVER_ACTION: r.SILVER_ACTION, GOLD: r.GOLD_STATUS, GOLD_TARGET: r.GOLD_TARGET, GOLD_ACTION: r.GOLD_ACTION, ERROR: r.ERROR_MESSAGE, RUN_ID: r.RUN_ID }))}
          badges={["PIPELINE", "BRONZE", "SILVER", "GOLD", "SILVER_ACTION", "GOLD_ACTION"]} onRow={r => openRun(r.RUN_ID)} />
      </div>
    </>
  );
}

function RunDetail({ runId, onClose }) {
  const [d, setD] = useState(null);
  const [err, setErr] = useState("");
  useEffect(() => { setD(null); api("/api/runs/" + runId).then(setD).catch(e => setErr(e.message)); }, [runId]);
  return (
    <div className="modal" onClick={onClose}>
      <div className="modal-body" onClick={e => e.stopPropagation()}>
        <div className="headrow"><div><h2>Run detail</h2><p className="muted">{runId}</p></div><button onClick={onClose}>✕ Close</button></div>
        {err && <p className="err">{err}</p>}
        {!d && !err && <p className="muted">Loading…</p>}
        {d && (
          <>
            <h3>Layers (PIPELINE_AUDIT)</h3>
            <Table rows={d.audit.map(a => ({ LAYER: a.LAYER, STATUS: a.STATUS, TARGET: a.TARGET_TABLE, ROWS_IN: a.ROWS_IN, ROWS_OUT: a.ROWS_OUT, QUARANTINED: a.ROWS_QUARANTINED, WARNED: a.ROWS_WARNED, ACTION: a.ACTION, START: a.START_TS, END: a.END_TS, MESSAGE: a.MESSAGE, ERROR: a.ERROR_MESSAGE }))} badges={["STATUS", "ACTION"]} />
            <h3>Data quality rules (DQ_RESULTS)</h3>
            <Table rows={d.dq.map(q => ({ LAYER: q.LAYER, COLUMN: q.COLUMN_NAME, RULE: q.RULE_TYPE, FROM: q.RULE_SOURCE, SEVERITY: q.SEVERITY, CHECKED: q.ROWS_CHECKED, FAILED: q.ROWS_FAILED, ACTION: q.ACTION, MESSAGE: q.MESSAGE }))} badges={["SEVERITY", "ACTION"]} empty="No rules evaluated" />
            <h3>Quarantined rows</h3>
            <Table rows={d.quarantine} empty="Nothing quarantined" />
          </>
        )}
      </div>
    </div>
  );
}

const CONFIG_META = {
  source_control: { title: "Source control", help: "Optional per-source settings. Leave blank = automatic.", fields: ["SOURCE_NAME", "FILE_PATTERN", "TARGET_TABLE", "KEY_COLUMN", "TRANSFORM_SQL", "GOLD_TYPE", "GOLD_DIMS", "IS_ACTIVE"], key: "SOURCE_NAME" },
  dq_config: { title: "DQ rules", help: "A configured rule replaces the default rule for that column. RULE_TYPE NO_DEFAULTS switches defaults off.", fields: ["LAYER", "SOURCE_NAME", "COLUMN_NAME", "RULE_TYPE", "RULE_VALUE", "RULE_SQL", "SEVERITY", "ERROR_MESSAGE", "IS_ACTIVE"], key: "DQ_ID" },
  column_rules: { title: "Column transformations", help: "SQL expression for one column, e.g. UPPER(TRIM(NAME)).", fields: ["SOURCE_NAME", "COLUMN_NAME", "TRANSFORM_EXPR", "IS_ACTIVE"], key: "RULE_ID" },
  gold_control: { title: "Custom gold tables", help: "Extra GOLD tables built from your SQL after the automatic FACT/DIM.", fields: ["CONTROL_NAME", "SOURCE_NAME", "TARGET_TABLE", "TRANSFORM_SQL", "IS_ACTIVE"], key: "CONTROL_NAME" },
};
const RULE_TYPES = ["NOT_NULL", "NOT_EMPTY", "POSITIVE", "NON_NEGATIVE", "NOT_ZERO", "MIN_VALUE", "MAX_VALUE", "MIN_LENGTH", "MAX_LENGTH", "EMAIL", "REGEX", "ALLOWED_VALUES", "DATE_NOT_FUTURE", "DATE_NOT_PAST", "TYPE_NUMBER", "TYPE_DATE", "TYPE_BOOLEAN", "TRIM_REQUIRED", "NO_SPECIAL_CHARACTERS", "UNIQUE", "CUSTOM_SQL", "NO_DEFAULTS"];

function ConfigPage() {
  const [tab, setTab] = useState("source_control");
  const [data, setData] = useState({ rows: [] });
  const [form, setForm] = useState({});
  const [msg, setMsg] = useState("");
  const meta = CONFIG_META[tab];
  const load = useCallback(() => api("/api/config/" + tab).then(setData).catch(e => setMsg(e.message)), [tab]);
  useEffect(() => { setForm({}); setMsg(""); load(); }, [load]);
  const add = async () => {
    try { await api("/api/config/" + tab, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(form) }); setMsg("Saved"); setForm({}); load(); }
    catch (e) { setMsg(e.message); }
  };
  const del = async r => {
    if (!confirm("Delete this row?")) return;
    try { await api("/api/config/" + tab + "?key=" + encodeURIComponent(r[meta.key]), { method: "DELETE" }); load(); } catch (e) { setMsg(e.message); }
  };
  const input = f => {
    const v = form[f] ?? "";
    const set = val => setForm({ ...form, [f]: val });
    if (f === "IS_ACTIVE") return <select value={v || "TRUE"} onChange={e => set(e.target.value)}><option>TRUE</option><option>FALSE</option></select>;
    if (f === "LAYER") return <select value={v || "SILVER"} onChange={e => set(e.target.value)}><option>SILVER</option><option>GOLD</option></select>;
    if (f === "SEVERITY") return <select value={v || "ERROR"} onChange={e => set(e.target.value)}><option>ERROR</option><option>WARNING</option></select>;
    if (f === "GOLD_TYPE") return <select value={v || "AUTO"} onChange={e => set(e.target.value)}><option>AUTO</option><option>FACT</option><option>DIM</option><option>NONE</option></select>;
    if (f === "RULE_TYPE") return <select value={v} onChange={e => set(e.target.value)}><option value="">choose…</option>{RULE_TYPES.map(t => <option key={t}>{t}</option>)}</select>;
    if (f.endsWith("_SQL")) return <textarea rows={3} value={v} onChange={e => set(e.target.value)} />;
    return <input value={v} onChange={e => set(e.target.value)} />;
  };
  return (
    <>
      <div className="tabs">{Object.keys(CONFIG_META).map(k => <button key={k} className={k === tab ? "active" : ""} onClick={() => setTab(k)}>{CONFIG_META[k].title}</button>)}</div>
      <div className="card">
        <h2>{meta.title}</h2><p>{meta.help}</p>
        <Table rows={data.rows.map(r => ({ ...r, " ": <button className="small" onClick={() => del(r)}>delete</button> }))} badges={["SEVERITY", "LAYER"]} empty="Nothing configured – everything runs on defaults." />
      </div>
      <div className="card">
        <h2>Add row</h2>
        <div className="form">{meta.fields.map(f => <label key={f}><span>{f}</span>{input(f)}</label>)}</div>
        <div className="inputrow"><button className="primary" onClick={add}>Save</button><span className={msg === "Saved" ? "ok" : "err"}>{msg}</span></div>
      </div>
    </>
  );
}

function Explorer() {
  const [tables, setTables] = useState([]);
  const [sel, setSel] = useState(null);
  const [data, setData] = useState(null);
  const [err, setErr] = useState("");
  useEffect(() => { api("/api/tables").then(d => setTables(d.rows)).catch(e => setErr(e.message)); }, []);
  const open = t => { setSel(t); setData(null); api(`/api/tables/${t.TABLE_SCHEMA}/${t.TABLE_NAME}?limit=100`).then(setData).catch(e => setErr(e.message)); };
  const groups = ["BRONZE", "SILVER", "GOLD"];
  return (
    <div className="explorer">
      <div className="card side">
        {groups.map(g => (
          <div key={g}><h3>{g}</h3>
            {tables.filter(t => t.TABLE_SCHEMA === g).map(t => <div key={t.TABLE_NAME} className={"tbl " + (sel && sel.TABLE_NAME === t.TABLE_NAME && sel.TABLE_SCHEMA === g ? "active" : "")} onClick={() => open(t)}>{t.TABLE_NAME} <span className="muted">{t.ROW_COUNT}</span></div>)}
          </div>))}
        {err && <p className="err">{err}</p>}
      </div>
      <div className="card grow">
        {!sel && <p className="muted">Pick a table to preview the first 100 rows.</p>}
        {sel && <h2>{sel.TABLE_SCHEMA}.{sel.TABLE_NAME}</h2>}
        {sel && !data && <p className="muted">Loading…</p>}
        {data && <Table columns={data.columns} rows={data.rows} />}
      </div>
    </div>
  );
}

function DQPage({ openRun }) {
  const [dq, setDq] = useState([]);
  const [qr, setQr] = useState([]);
  const [err, setErr] = useState("");
  useEffect(() => { Promise.all([api("/api/dq"), api("/api/quarantine")]).then(([a, b]) => { setDq(a.rows); setQr(b.rows); }).catch(e => setErr(e.message)); }, []);
  return (
    <>
      {err && <p className="err">{err}</p>}
      <div className="card"><h2>Failed data-quality checks</h2><p>Every rule that found at least one bad row. ERROR → row quarantined, WARNING → fixed (TRANSFORMATION) or flagged.</p>
        <Table rows={dq.map(q => ({ TIME: q.CHECK_TS, LAYER: q.LAYER, SOURCE: q.SOURCE_NAME, COLUMN: q.COLUMN_NAME, RULE: q.RULE_TYPE, FROM: q.RULE_SOURCE, SEVERITY: q.SEVERITY, FAILED: q.ROWS_FAILED, OF: q.ROWS_CHECKED, ACTION: q.ACTION, MESSAGE: q.MESSAGE, RUN_ID: q.RUN_ID }))} badges={["SEVERITY", "ACTION"]} onRow={r => openRun(r.RUN_ID)} empty="No failed checks yet" /></div>
      <div className="card"><h2>Quarantine</h2><p>Rejected rows kept as JSON with the reason.</p>
        <Table rows={qr} onRow={r => openRun(r.RUN_ID)} empty="Quarantine is empty" /></div>
    </>
  );
}

function App() {
  const [who, setWho] = useState(null);
  const [page, setPage] = useState("overview");
  const [runId, setRunId] = useState(null);
  useEffect(() => { if (getToken()) api("/api/health").then(d => setWho(d.snowflake)).catch(() => {}); }, []);
  const pages = { overview: "Overview", upload: "Upload & run", dq: "Data quality", config: "Configuration", explorer: "Data explorer" };
  return (
    <>
      <header>
        <div className="logo">SF</div>
        <div><b>Pipeline Console</b><small>Bronze · Silver · Gold · metadata-driven</small></div>
        {who && <nav>{Object.entries(pages).map(([k, v]) => <button key={k} className={page === k ? "active" : ""} onClick={() => setPage(k)}>{v}</button>)}</nav>}
        <span className="secure">{who ? `● ${who.U} · ${who.R} · ${who.W}` : "○ not connected"}</span>
        {who && <button className="small" onClick={() => { sessionStorage.removeItem("pipelineToken"); setWho(null); }}>Sign out</button>}
      </header>
      <main>
        {!who && <Login onDone={setWho} />}
        {who && page === "overview" && <Overview openRun={setRunId} />}
        {who && page === "upload" && <UploadPage openRun={setRunId} />}
        {who && page === "dq" && <DQPage openRun={setRunId} />}
        {who && page === "config" && <ConfigPage />}
        {who && page === "explorer" && <Explorer />}
        {runId && <RunDetail runId={runId} onClose={() => setRunId(null)} />}
      </main>
    </>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);

const token=document.querySelector("#token"), connection=document.querySelector("#connection");
const service=document.querySelector("#service"), runstatus=document.querySelector("#runstatus");
const history=document.querySelector("#history"), run=document.querySelector("#run");
token.value=sessionStorage.getItem("pipelineToken")||"";
function status(el,msg,cls="muted"){el.className=cls;el.textContent=msg}
function headers(){if(!token.value.trim())throw Error("Enter your app access token first.");return {Authorization:"Bearer "+token.value.trim()}}
async function api(path,opts={}){const r=await fetch(path,{...opts,headers:{...headers(),...(opts.headers||{})}});const d=await r.json().catch(()=>({detail:"Invalid server response"}));if(!r.ok)throw Error(d.detail||"Request failed");return d}
async function connect(){try{const d=await api("/api/health");sessionStorage.setItem("pipelineToken",token.value.trim());status(connection,"Connected to backend API.","ok");service.textContent="Online";await loadRuns()}catch(e){status(connection,e.message,"err");service.textContent="Unavailable"}}
async function loadRuns(){history.textContent="Loading…";try{const d=await api("/api/runs?limit=20");if(!d.rows.length){history.textContent="No run records found.";return}const t=document.createElement("table"),thead=document.createElement("thead"),tr=document.createElement("tr");d.columns.forEach(c=>{const th=document.createElement("th");th.textContent=c;tr.appendChild(th)});thead.appendChild(tr);t.appendChild(thead);const tb=document.createElement("tbody");d.rows.forEach(row=>{const tr=document.createElement("tr");d.columns.forEach(c=>{const td=document.createElement("td");td.textContent=row[c]??"—";tr.appendChild(td)});tb.appendChild(tr)});t.appendChild(tb);history.replaceChildren(t)}catch(e){history.textContent=e.message}}
document.querySelector("#connect").onclick=connect;
document.querySelector("#refresh").onclick=loadRuns;
run.onclick=async()=>{if(!confirm("Run the Snowflake pipeline now?"))return;run.disabled=true;status(runstatus,"Sending request to Snowflake…");try{const d=await api("/api/run",{method:"POST"});status(runstatus,d.message+" "+(d.result||""),"ok");await loadRuns()}catch(e){status(runstatus,e.message,"err")}finally{run.disabled=false}};
if(token.value)connect();

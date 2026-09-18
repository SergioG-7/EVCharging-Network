from flask import Flask, jsonify, Response
import os, time, requests
from datetime import datetime

app = Flask(__name__)

API_CENTRAL_URL = os.getenv("API_CENTRAL_URL", "http://api_central:5001")
REFRESH_MS = int(os.getenv("FRONT_REFRESH_MS", "2000"))
REQ_TIMEOUT = float(os.getenv("FRONT_TIMEOUT", "3.5"))

def _get_json(url: str):
    """
    Devuelve (ok:bool, data:any, err:str, ms:int)
    """
    t0 = time.time()
    try:
        r = requests.get(url, timeout=REQ_TIMEOUT)
        ms = int((time.time() - t0) * 1000)
        if r.status_code != 200:
            return False, None, f"HTTP {r.status_code}", ms
        return True, r.json(), "", ms
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        return False, None, str(e), ms

def _as_list(x):
    return x if isinstance(x, list) else []

def _as_dict(x):
    return x if isinstance(x, dict) else {}

@app.route("/")
def index():
    html = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Central Monitor</title>
  <style>
    :root {{
      --bg:#0f0f0f; --fg:#eaeaea; --muted:#bdbdbd;
      --card:#141414; --border:#2f2f2f;
      --ok:#29d36a; --warn:#f5c542; --bad:#ff4d4d; --info:#5bbcff;
    }}
    body {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
           background:var(--bg); color:var(--fg); padding:16px; }}
    h1,h2 {{ margin: 10px 0; }}
    .row {{ display:flex; gap:12px; flex-wrap:wrap; align-items:stretch; }}
    .card {{ background:var(--card); border:1px solid var(--border); border-radius:12px; padding:12px; margin:12px 0; }}
    .card.small {{ flex:1; min-width:260px; }}
    .muted {{ color:var(--muted); }}
    .pill {{ display:inline-block; padding:3px 9px; border-radius:999px; border:1px solid var(--border); margin-left:6px; font-weight:bold; }}
    .pill.ok {{ border-color: rgba(41,211,106,.45); color:var(--ok); }}
    .pill.warn {{ border-color: rgba(245,197,66,.45); color:var(--warn); }}
    .pill.bad {{ border-color: rgba(255,77,77,.45); color:var(--bad); }}
    .pill.info {{ border-color: rgba(91,188,255,.45); color:var(--info); }}
    .alert {{ border:1px solid rgba(255,77,77,.55); background: rgba(255,77,77,.07);
             border-radius:12px; padding:10px 12px; margin:8px 0; }}
    .alert.warn {{ border-color: rgba(245,197,66,.55); background: rgba(245,197,66,.08); }}
    .alert.info {{ border-color: rgba(91,188,255,.55); background: rgba(91,188,255,.08); }}
    table {{ width:100%; border-collapse: collapse; }}
    th, td {{ border-bottom: 1px solid var(--border); padding: 8px 6px; text-align:left; vertical-align: top; }}
    th {{ color:var(--muted); font-weight:bold; }}
    .mono {{ font-family: inherit; }}
    .right {{ text-align:right; }}
    .nowrap {{ white-space:nowrap; }}
    .token {{ max-width: 320px; overflow:hidden; text-overflow: ellipsis; white-space:nowrap; }}
    .logbox {{ max-height: 320px; overflow:auto; border:1px solid var(--border); border-radius:12px; padding:8px; background:#101010; }}
    .logline {{ padding:3px 4px; border-bottom:1px dashed rgba(255,255,255,.06); }}
    .logline:last-child {{ border-bottom:none; }}
    .logline.err {{ color: var(--bad); font-weight:bold; }}
    .logline.warn {{ color: var(--warn); font-weight:bold; }}
    .footer {{ margin-top: 10px; color: var(--muted); font-size: 12px; }}
    .bold {{ font-weight:bold; }}
  </style>
</head>
<body>
  <h1><span class="bold">Central Monitor</span> <span class="muted">({API_CENTRAL_URL})</span></h1>

  <div class="row">
    <div class="card small">
      <h2><span class="bold">Estado de módulos</span></h2>
      <div id="mod_api"></div>
      <div id="mod_evw"></div>
      <div class="footer" id="last_refresh"></div>
    </div>

    <div class="card small">
      <h2><span class="bold">Alertas</span></h2>
      <div id="alertas"></div>
    </div>
  </div>

  <div class="card">
    <h2><span class="bold">Charging Points</span></h2>
    <table>
      <thead>
        <tr>
          <th class="nowrap">ID</th>
          <th>Localización</th>
          <th class="right nowrap">Precio</th>
          <th class="nowrap">Estado</th>
          <th class="nowrap">Registrado</th>
          <th class="nowrap">Activado</th>
          <th>Token</th>
        </tr>
      </thead>
      <tbody id="tabla_cps"></tbody>
    </table>
  </div>

  <div class="row">
    <div class="card small">
      <h2><span class="bold">Drivers</span></h2>
      <table>
        <thead><tr><th class="nowrap">ID</th></tr></thead>
        <tbody id="tabla_drivers"></tbody>
      </table>
    </div>

    <div class="card small">
      <h2><span class="bold">OpenWeather</span></h2>
      <table>
        <thead><tr><th>Ciudad</th><th class="right nowrap">Temp (ºC)</th></tr></thead>
        <tbody id="tabla_weather"></tbody>
      </table>
    </div>
  </div>

  <div class="card">
    <h2><span class="bold">Logs / Errores</span></h2>
    <div class="logbox" id="logs"></div>
    <div class="footer muted">Se muestran las últimas líneas devueltas por <span class="bold">/api/logs</span>.</div>
  </div>

<script>
const REFRESH_MS = {REFRESH_MS};

function pill(text, cls) {{
  return `<span class="pill ${{cls}}">${{text}}</span>`;
}}

function esc(s) {{
  if (s === null || s === undefined) return "";
  return (""+s).replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;");
}}

function isBadLog(line) {{
  const l = (line || "").toLowerCase();
  return l.includes("error") || l.includes("ko") || l.includes("exception") || l.includes("traceback");
}}
function isWarnLog(line) {{
  const l = (line || "").toLowerCase();
  return l.includes("warn") || l.includes("aviso") || l.includes("timeout") || l.includes("no disponible");
}}

function cpEstadoPill(estado) {{
  if (!estado) return pill("DESCONOCIDO","warn");
  const e = estado.toUpperCase();
  if (e === "ACTIVADO" || e === "SUMINISTRANDO") return pill(e,"ok");
  if (e === "PARADO") return pill(e,"warn");
  if (e === "AVERIADO") return pill(e,"bad");
  if (e === "DESCONECTADO") return pill(e,"info");
  return pill(e,"warn");
}}

function yesNoPill(v) {{
  return v ? pill("SÍ","ok") : pill("NO","bad");
}}

function tokenCell(token) {{
  if (!token) return `<span class="muted">N/D</span>`;
  const t = ""+token;
  const short = t.length > 14 ? (t.slice(0, 6) + "…" + t.slice(-6)) : t;
  return `<span class="token" title="${{esc(t)}}"><span class="bold">${{esc(short)}}</span></span>`;
}}

async function refrescar() {{
  let data = null;
  try {{
    const r = await fetch("/data", {{ cache: "no-store" }});
    data = await r.json();
  }} catch (e) {{
    data = {{
      status: {{
        api_ok:false, api_err:"No se pudo cargar /data",
        evw_ok:false, evw_err:"Sin datos",
        timings: {{}}
      }},
      cps:[], clima:[], drivers:[], logs:{{lines:[]}},
      ts: Math.floor(Date.now()/1000)
    }};
  }}

  // Estado módulos
  const mod_api = document.getElementById("mod_api");
  if (data.status.api_ok) {{
    mod_api.innerHTML = `<span class="bold">API_Central:</span> ${{pill("OK","ok")}} <span class="muted">(latencia ${{data.status.timings.api_cps_ms}}ms)</span>`;
  }} else {{
    mod_api.innerHTML = `<span class="bold">API_Central:</span> ${{pill("ERROR","bad")}} <span class="muted">${{esc(data.status.api_err)}}</span>`;
  }}

  const mod_evw = document.getElementById("mod_evw");
  if (data.status.evw_ok) {{
    mod_evw.innerHTML = `<span class="bold">EV_W:</span> ${{pill("OK","ok")}} <span class="muted">(${{data.clima.length}} ciudades)</span>`;
  }} else {{
    mod_evw.innerHTML = `<span class="bold">EV_W:</span> ${{pill("SIN DATOS","warn")}} <span class="muted">${{esc(data.status.evw_err)}}</span>`;
  }}

  const last_refresh = document.getElementById("last_refresh");
  const dt = new Date((data.ts||Math.floor(Date.now()/1000))*1000);
  last_refresh.innerHTML = `<span class="bold">Última actualización:</span> ${{dt.toLocaleString()}}`;

  // Alertas
  const alertas = document.getElementById("alertas");
  alertas.innerHTML = "";
  const alerts = [];

  if (!data.status.api_ok) {{
    alerts.push({{ type:"bad", text:`API_Central no responde: ${{data.status.api_err || "desconocido"}}` }});
  }}

  // Clima bajo 0
  const bajoCero = (data.clima || []).filter(x => (x && typeof x.temp === "number" && x.temp < 0));
  if (bajoCero.length > 0) {{
    const cities = bajoCero.map(x => `${{x.ciudad}} (${{x.temp.toFixed(2)}}ºC)`).join(", ");
    alerts.push({{ type:"warn", text:`Hay ciudades por debajo de 0ºC: ${{cities}}. CPs deben quedar fuera de servicio.` }});
  }}

  // CP averiados
  const cps = data.cps || [];
  const averiados = cps.filter(cp => (cp.estado||"").toUpperCase() === "AVERIADO");
  if (averiados.length > 0) {{
    alerts.push({{ type:"bad", text:`CP(s) averiado(s): ${{averiados.map(a=>a.id).join(", ")}}` }});
  }}

  // CP activos sin registro (sin token)
  const activosSinToken = cps.filter(cp => {{
    const e = (cp.estado||"").toUpperCase();
    const activo = (e === "ACTIVADO" || e === "SUMINISTRANDO");
    const hasToken = !!cp.token; // si API no devuelve token, esto saldrá false -> avisará (útil para detectar que falta en API)
    return activo && !hasToken;
  }});
  if (activosSinToken.length > 0) {{
    alerts.push({{ type:"info", text:`CP(s) activos sin token (no registrados o API no devuelve token): ${{activosSinToken.map(a=>a.id).join(", ")}}` }});
  }}

  // Errores recientes en logs
  const lines = (data.logs && data.logs.lines) ? data.logs.lines : [];
  const errLines = lines.filter(isBadLog);
  if (errLines.length > 0) {{
    alerts.push({{ type:"bad", text:`Se detectan errores en logs (últimas líneas).` }});
  }}

  if (alerts.length === 0) {{
    alertas.innerHTML = `<div class="muted">Sin alertas.</div>`;
  }} else {{
    for (const a of alerts) {{
      const cls = a.type === "bad" ? "" : (a.type === "warn" ? "warn" : "info");
      const div = document.createElement("div");
      div.className = "alert " + cls;
      div.innerHTML = `<span class="bold">ALERTA:</span> ${{esc(a.text)}}`;
      alertas.appendChild(div);
    }}
  }}

  // CPs table
  const tb = document.getElementById("tabla_cps");
  tb.innerHTML = "";
  for (const cp of cps) {{
    const estado = (cp.estado||"");
    const activado = ["ACTIVADO","SUMINISTRANDO"].includes(estado.toUpperCase());
    const registrado = !!cp.token;
    const precio = (typeof cp.precio === "number") ? cp.precio.toFixed(3) : (cp.precio ?? "");
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="nowrap"><span class="bold">${{esc(cp.id)}}</span></td>
      <td>${{esc(cp.localizacion || "")}}</td>
      <td class="right nowrap">${{esc(precio)}}</td>
      <td class="nowrap">${{cpEstadoPill(estado)}}</td>
      <td class="nowrap">${{yesNoPill(registrado)}}</td>
      <td class="nowrap">${{yesNoPill(activado)}}</td>
      <td>${{tokenCell(cp.token)}}</td>
    `;
    tb.appendChild(tr);
  }}

  // Drivers table
  const td = document.getElementById("tabla_drivers");
  td.innerHTML = "";
  const drivers = data.drivers || [];
  if (drivers.length === 0) {{
    const tr = document.createElement("tr");
    tr.innerHTML = `<td class="muted">Sin drivers</td>`;
    td.appendChild(tr);
  }} else {{
    for (const d of drivers) {{
      const tr = document.createElement("tr");
      tr.innerHTML = `<td class="nowrap"><span class="bold">${{esc(d.id)}}</span></td>`;
      td.appendChild(tr);
    }}
  }}

  // Weather table
  const tw = document.getElementById("tabla_weather");
  tw.innerHTML = "";
  const clima = data.clima || [];
  if (clima.length === 0) {{
    const tr = document.createElement("tr");
    tr.innerHTML = `<td class="muted">Sin datos</td><td></td>`;
    tw.appendChild(tr);
  }} else {{
    for (const c of clima) {{
      const temp = (typeof c.temp === "number") ? c.temp.toFixed(2) : (c.temp ?? "");
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${{esc(c.ciudad)}}</td><td class="right nowrap"><span class="bold">${{esc(temp)}}</span></td>`;
      tw.appendChild(tr);
    }}
  }}

  // Logs
  const logsDiv = document.getElementById("logs");
  logsDiv.innerHTML = "";
  if (!lines || lines.length === 0) {{
    logsDiv.innerHTML = `<div class="muted">Sin logs.</div>`;
  }} else {{
    for (const line of lines) {{
      const div = document.createElement("div");
      const err = isBadLog(line);
      const wrn = !err && isWarnLog(line);
      div.className = "logline " + (err ? "err" : (wrn ? "warn" : ""));
      div.textContent = line;
      logsDiv.appendChild(div);
    }}
    // auto-scroll al final
    logsDiv.scrollTop = logsDiv.scrollHeight;
  }}
}}

setInterval(refrescar, REFRESH_MS);
refrescar();
</script>
</body>
</html>
"""
    return Response(html, mimetype="text/html")

@app.route("/data")
def data():
    # CPs
    ok_cps, cps, err_cps, ms_cps = _get_json(f"{API_CENTRAL_URL}/api/cps")
    # OpenWeather
    ok_w, clima, err_w, ms_w = _get_json(f"{API_CENTRAL_URL}/api/openweather")
    # Drivers
    ok_d, drivers, err_d, ms_d = _get_json(f"{API_CENTRAL_URL}/api/drivers")
    # Logs
    ok_l, logs, err_l, ms_l = _get_json(f"{API_CENTRAL_URL}/api/logs")
    # Normalizar
    cps = _as_list(cps) if ok_cps else []
    clima = _as_list(clima) if ok_w else []
    drivers = _as_list(drivers) if ok_d else []
    logs = _as_dict(logs) if ok_l else {"lines": []}
    if "lines" not in logs or not isinstance(logs.get("lines"), list):
        logs["lines"] = []

    # Estado general API_Central: consideramos OK si /api/cps responde
    api_ok = ok_cps
    api_err = err_cps if not ok_cps else ""

    # EV_W: OK si /api/openweather responde y hay datos
    evw_ok = ok_w and len(clima) > 0
    evw_err = ""
    if not ok_w:
        evw_err = err_w
    elif len(clima) == 0:
        evw_err = "Tabla openweather vacía"

    # Añadir timings
    status = {
        "api_ok": api_ok,
        "api_err": api_err,
        "evw_ok": evw_ok,
        "evw_err": evw_err,
        "timings": {
            "api_cps_ms": ms_cps,
            "api_openweather_ms": ms_w,
            "api_drivers_ms": ms_d,
            "api_logs_ms": ms_l
        },
        # errores por endpoint (por si quieres pintarlos)
        "endpoints": {
            "cps": {"ok": ok_cps, "err": err_cps},
            "openweather": {"ok": ok_w, "err": err_w},
            "drivers": {"ok": ok_d, "err": err_d},
            "logs": {"ok": ok_l, "err": err_l},
        }
    }

    return jsonify({
        "status": status,
        "cps": cps,
        "clima": clima,
        "drivers": drivers,
        "logs": logs,
        "ts": int(time.time())
    }), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)

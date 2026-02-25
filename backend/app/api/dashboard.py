from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["dashboard"])


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard() -> HTMLResponse:
    return HTMLResponse(content="""<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Print City Dashboard</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 20px; color: #111; }
      h1, h2 { margin: 0 0 12px 0; }
      .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px; margin-bottom: 16px; }
      .card { border: 1px solid #ddd; border-radius: 8px; padding: 12px; }
      table { border-collapse: collapse; width: 100%; }
      th, td { border: 1px solid #ddd; padding: 6px; font-size: 14px; text-align: left; }
      th { background: #f8f8f8; }
      button { background: #0b5fff; color: #fff; border: 0; border-radius: 6px; padding: 8px 12px; cursor: pointer; }
      .muted { color: #666; font-size: 13px; }
      pre { margin: 0; white-space: pre-wrap; }
    </style>
  </head>
  <body>
    <h1>Print City Dashboard</h1>
    <p class=\"muted\">Auto-refreshes every 60 seconds.</p>

    <div style=\"margin-bottom: 16px;\">
      <button id=\"runPipeline\">Run Pipeline Now</button>
      <span id=\"runPipelineResult\" class=\"muted\"></span>
    </div>

    <div class=\"grid\">
      <div class=\"card\">
        <h2>PQS Summary</h2>
        <pre id=\"pqsSummary\">Loading...</pre>
      </div>
      <div class=\"card\">
        <h2>Pipeline Health</h2>
        <pre id=\"pipelineHealth\">Loading...</pre>
      </div>
      <div class=\"card\">
        <h2>Market Status</h2>
        <pre id=\"marketStatus\">Loading...</pre>
      </div>
      <div class=\"card\">
        <h2>CLV Metrics</h2>
        <pre id=\"clvMetrics\">Loading...</pre>
      </div>
    </div>

    <div class=\"card\">
      <h2>Recommended Picks</h2>
      <table>
        <thead>
          <tr>
            <th>Pick ID</th><th>Sport</th><th>Market</th><th>Side</th><th>Point</th><th>PQS</th><th>EV</th><th>Books</th><th>Sharp</th><th>Dispersion</th><th>Start (min)</th><th>Edge</th><th>Why</th>
          </tr>
        </thead>
        <tbody id=\"recommendedRows\"></tbody>
      </table>
    </div>

    <script>
      async function getJson(url, options) {
        const response = await fetch(url, options);
        if (!response.ok) throw new Error(`${url} failed (${response.status})`);
        return response.json();
      }

      function pretty(data) {
        return JSON.stringify(data, null, 2);
      }

      async function loadRecommended() {
        const rows = await getJson('/picks/recommended');
        const body = document.getElementById('recommendedRows');
        body.innerHTML = '';
        if (!rows.length) {
          body.innerHTML = '<tr><td colspan="13">No recommended picks</td></tr>';
          return;
        }
        for (const r of rows) {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td>${r.pick_id}</td>
            <td>${r.sport_key}</td>
            <td>${r.market_key}</td>
            <td>${r.side}</td>
            <td>${r.point ?? ''}</td>
            <td>${Number(r.pqs).toFixed(3)}</td>
            <td>${Number(r.ev).toFixed(3)}</td>
            <td>${r.book_count}</td>
            <td>${r.sharp_book_count}</td>
            <td>${Number(r.price_dispersion).toFixed(3)}</td>
            <td>${Number(r.time_to_start_minutes).toFixed(1)}</td>
            <td>${Number(r.best_vs_consensus_edge).toFixed(3)}</td>
            <td>${r.why}</td>
          `;
          body.appendChild(tr);
        }
      }

      async function loadPqsSummary() {
        const rows = await getJson('/pqs/latest?limit=200');
        const summary = { total: rows.length, KEEP: 0, WARN: 0, DROP: 0, drop_reasons: {} };
        for (const row of rows) {
          summary[row.decision] = (summary[row.decision] || 0) + 1;
          if (row.decision === 'DROP') {
            const reason = row.drop_reason || 'unknown';
            summary.drop_reasons[reason] = (summary.drop_reasons[reason] || 0) + 1;
          }
        }
        document.getElementById('pqsSummary').textContent = pretty(summary);
      }

      async function loadHealthBlocks() {
        const [pipeline, market, clv] = await Promise.all([
          getJson('/pipeline/health'),
          getJson('/system/market_status'),
          getJson('/metrics/clv')
        ]);
        document.getElementById('pipelineHealth').textContent = pretty(pipeline);
        document.getElementById('marketStatus').textContent = pretty(market);
        document.getElementById('clvMetrics').textContent = pretty(clv);
      }

      async function refreshAll() {
        try {
          await Promise.all([loadRecommended(), loadPqsSummary(), loadHealthBlocks()]);
        } catch (error) {
          console.error(error);
        }
      }

      document.getElementById('runPipeline').addEventListener('click', async () => {
        const target = document.getElementById('runPipelineResult');
        target.textContent = ' Running...';
        try {
          const data = await getJson('/pipeline/run', { method: 'POST' });
          target.textContent = ` Done: ${JSON.stringify(data)}`;
          await refreshAll();
        } catch (error) {
          target.textContent = ` Failed: ${error.message}`;
        }
      });

      refreshAll();
      setInterval(refreshAll, 60000);
    </script>
  </body>
</html>""")

<#ftl output_format="HTML" auto_esc=true>
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Benchmark Report ${report.runId()}</title>
</head>
<body>
  <h1>Benchmark Report ${report.runId()}</h1>

  <h2>Run Metadata</h2>
  <p>Profile: ${report.profile()} | Suite: ${report.suite()} | Query Set: ${report.querySet()} | Status: ${report.status()} | Started: ${report.startedAt()} | Ended: ${report.endedAt()}</p>
  <#if !report.fullProfile()>
  <p>This run is not a 4.032B row full-profile validation.</p>
  </#if>

  <h2>Dataset Summary</h2>
  <p>Cells: ${report.cells()} | Days: ${report.days()} | Rows: ${report.rows()} | Columns: ${report.columns()} | Bytes: ${report.bytesWritten()}</p>

  <h2>Load Summary</h2>
  <table>
    <thead>
      <tr><th>Engine</th><th>Table Shape</th><th>Stage</th><th>Rows</th><th>Bytes</th><th>Duration Seconds</th><th>Status</th><th>Error</th></tr>
    </thead>
    <tbody>
      <#list report.loadSummaries() as item>
      <tr><td>${item.engine()}</td><td>${item.tableShape()}</td><td>${item.stage()}</td><td>${item.rows()}</td><td>${item.bytes()}</td><td>${item.durationSeconds()}</td><td>${item.success()?then("SUCCESS", "FAILED")}</td><td>${item.error()!""}</td></tr>
      </#list>
    </tbody>
  </table>

  <h2>Query Summary</h2>
  <table>
    <thead>
      <tr><th>Engine</th><th>Table Shape</th><th>Query</th><th>P50 ms</th><th>P95 ms</th><th>P99 ms</th><th>Rows</th><th>Failures</th><th>Status</th><th>Error</th></tr>
    </thead>
    <tbody>
      <#list report.querySummaries() as item>
      <tr><td>${item.engine()}</td><td>${item.tableShape()}</td><td>${item.queryName()}</td><td>${item.p50Ms()}</td><td>${item.p95Ms()}</td><td>${item.p99Ms()}</td><td>${item.rows()}</td><td>${item.failures()}</td><td>${item.success()?then("SUCCESS", "FAILED")}</td><td>${item.error()!""}</td></tr>
      </#list>
    </tbody>
  </table>

  <p><a href="${report.grafanaUrl()}">Grafana dashboard for this run</a></p>
</body>
</html>

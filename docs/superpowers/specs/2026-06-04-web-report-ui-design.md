# Web Report UI Design

## Goal

Migrate the benchmark report from the current FreeMarker static HTML plus Grafana link model to a self-contained React web report. Each benchmark run must generate its own independent HTML report package under `reports/runs/<run_id>/`, and the report must be usable by directly opening `index.html`.

The first implementation focuses on a single run. It does not include a historical run list or cross-run comparison.

## Reference Frontend Stack

Use the frontend stack from `D:\agent-code\flink-data-balance\frontend`:

- React 18.
- TypeScript.
- Vite.
- Ant Design 5.
- Vitest and Testing Library.
- AntV charting.

For report charts, use AntV through `@ant-design/plots`. Use `@antv/x6` only if a later report view needs a richer execution graph. Do not use ECharts for the first report UI.

## Output Shape

Each run writes an independent report directory:

```text
reports/runs/<run_id>/
  index.html
  report.json
  assets/
    report-ui-*.js
    report-ui-*.css
```

`index.html` is the primary entry point. It must work when opened directly with `file://`.

Because many browsers block `fetch('./report.json')` from a local file, `index.html` must include the report payload as:

```html
<script>
  window.__BENCHMARK_REPORT__ = { ... };
</script>
```

The same payload is also written to `report.json` for debugging, automation, and future historical comparison.

## Architecture

Current flow:

```text
Runner -> BenchmarkReport -> HtmlReportWriter -> report.ftl -> index.html
```

New flow:

```text
Runner -> BenchmarkReport -> WebReportWriter
                    |-> report.json
                    |-> index.html with window.__BENCHMARK_REPORT__
                    `-> assets/ copied from the report UI build
```

Add a frontend project inside this repository:

```text
frontend/
  package.json
  index.html
  src/
    App.tsx
    main.tsx
    charts/
    components/
    types/
```

Add or replace Java report modules:

```text
src/main/java/com/example/databenchmark/report/
  BenchmarkReport.java
  WebBenchmarkReport.java
  WebBenchmarkReportMapper.java
  WebReportWriter.java
```

`BenchmarkReport` remains the internal domain report produced by runners. `WebBenchmarkReport` is the JSON contract consumed by the frontend. The frontend must depend on the JSON contract, not Java record implementation details.

The first implementation will commit the built Vite output as report UI resources to keep Java packaging simple. A later iteration can wire npm build into Maven.

Recommended built resource location:

```text
src/main/resources/report-ui/
  index.html
  assets/
```

`WebReportWriter` copies these resources from the classpath into each run directory and injects the report JSON into `index.html`.

## Data Contract

Use a versioned JSON DTO:

```json
{
  "schemaVersion": 1,
  "run": {
    "runId": "compose-tpch",
    "profile": "tpch-smoke",
    "suite": "tpch",
    "querySet": "smoke",
    "status": "SUCCESS",
    "startedAt": "2026-06-04T00:00:00Z",
    "endedAt": "2026-06-04T00:00:12Z",
    "durationSeconds": 12.0,
    "fullProfile": false
  },
  "dataset": {
    "cells": 10000,
    "days": 1,
    "rows": 899,
    "columns": 0,
    "bytesWritten": 123456
  },
  "loads": [],
  "queries": [],
  "charts": {
    "loadDurationByEngine": [],
    "queryLatencyByEngine": [],
    "queryRowsByEngine": [],
    "failureSummary": []
  },
  "notices": []
}
```

The mapper should include chart-friendly aggregations so the frontend can stay mostly presentational:

- Load duration by engine and table shape.
- Query latency by engine, table shape, and query name.
- Query rows by engine and query name.
- Failure summary by stage and engine.

Raw `loads` and `queries` remain available for detail tables.

Compatibility rules:

- `schemaVersion` starts at `1`.
- New fields may be added later.
- Existing field names should not be renamed without a schema version bump.
- The frontend must show a clear error state for missing or unsupported data.

## Page Design

The report is a dense operational dashboard, not a marketing page.

Use Ant Design layout and components with the same general style as the reference frontend:

- Light background such as `#f5f7fb`.
- Ant Design `Card`, `Statistic`, `Table`, `Badge`, `Tag`, `Alert`, `Descriptions`, and `Tabs` where useful.
- Chinese interface labels with technical metric names left in English where they are clearer, such as `runId`, `p95`, and `engine`.

Main sections:

1. Run summary.
   - Run ID, profile, suite, query set.
   - SUCCESS or DEGRADED status.
   - Start/end time and duration.
   - Dataset size: rows, bytes, cells, days, columns.
   - For TPC-H smoke runs, show a notice that the generated data is TPC-H-compatible smoke data, not an official TPC-H benchmark result.

2. Engine comparison overview.
   - Grouped column chart for load duration by engine/table shape.
   - Grouped column chart for query latency by engine/query.
   - Bar or column chart for query rows.
   - Failure count summary.

3. Stage status flow.
   - Use Ant Design `Steps` or `Timeline` for the first version.
   - Stages include generate, load, refresh, query, and report.
   - Show status, duration, and error summary per stage.

4. Load details.
   - Ant Design table.
   - Columns: engine, tableShape, stage, rows, bytes, durationSeconds, status, error.
   - Failed rows can expand to show full error text.

5. Query details.
   - Ant Design table.
   - Columns: engine, tableShape, queryName, p50Ms, p95Ms, p99Ms, rows, failures, status, error.
   - TPC-H query names such as `q01_pricing_summary_report` remain visible.
   - Failed rows can expand to show full error text.

## Frontend Data Loading

Frontend startup order:

1. Use `window.__BENCHMARK_REPORT__` if present.
2. If not present, attempt `fetch('./report.json')`.
3. If both fail, render an error page.

This preserves direct file opening and still supports static hosting.

## Error Handling

Run-level failures:

- If the runner captured stage results, still write a report.
- Set `run.status` to `DEGRADED`.
- Show a top-level `Alert`.
- Mark failed load/query rows with error badges.
- Keep failed items in charts when possible. Missing duration or rows may be rendered as `0` or `null`, with status in the tooltip.

Frontend data failures:

- Missing report data: show a clear error page.
- Unsupported `schemaVersion`: show a compatibility error.
- Malformed dates or optional values: render `-` instead of crashing.

Security and correctness:

- Preserve run ID path traversal protection.
- Escape JSON safely when injecting into `index.html`.
- Ensure `</script>` inside report data cannot terminate the script tag.

## Removing Grafana And Prometheus

Grafana and Prometheus are no longer part of the report display path.

Remove from Docker and docs:

- `prometheus` service from `docker-compose.yml`.
- `grafana` service from `docker-compose.yml`.
- benchmark-runner dependency on `prometheus`.
- Grafana and Prometheus ports from docs.
- Grafana provisioning and dashboard files.
- `monitoring/prometheus.yml`.
- README and staging docs references to Grafana/Prometheus report viewing.

Remove from Java report model and writer:

- `grafanaUrl` from the new report UI flow.
- Grafana URL validation in the report writer.
- `report.ftl` and `HtmlReportWriter` after `WebReportWriter` replaces them.

Prometheus metrics runtime integration should be removed from compose mode together with the Docker services. Prometheus-specific Java classes and tests should be deleted when they no longer have runtime consumers. If an intermediate compatibility layer is needed while replacing the report writer, it must not remain connected to Docker or the generated report UI at the end of the implementation plan.

## Tests

Backend tests:

- `WebBenchmarkReportMapperTest`
  - Maps `BenchmarkReport` to schema version 1.
  - Preserves run metadata, suite, query set, dataset, loads, queries, and notices.
  - Produces chart-friendly aggregations.
  - Handles SUCCESS and DEGRADED reports.

- `WebReportWriterTest`
  - Writes `index.html`, `report.json`, and `assets/`.
  - Injects `window.__BENCHMARK_REPORT__`.
  - Escapes JSON safely, including `</script>`.
  - Rejects path traversal run IDs.

- Runner tests
  - Local KPI run writes the web report package.
  - Compose KPI and TPC-H tests use `WebReportWriter` through the report writer abstraction.
  - DEGRADED compose reports still write successfully.

- `ComposeTopologyTest`
  - `docker-compose.yml` does not contain `grafana` or `prometheus`.
  - benchmark-runner no longer depends on `prometheus`.
  - monitoring provisioning mounts are gone.

Frontend tests:

- `npm test`
  - Renders run summary.
  - Renders load and query chart sections.
  - Renders load and query detail tables.
  - Renders DEGRADED alert and failed rows.
  - Shows error state when embedded data and `report.json` are both unavailable.

Build checks:

- `npm --prefix frontend run build`.
- `mvn test`.
- `mvn package`.

End-to-end smoke:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode local --config configs/benchmark-smoke.yml --run-id web-report-smoke
```

Verify:

- `reports/runs/web-report-smoke/index.html` exists.
- `reports/runs/web-report-smoke/report.json` exists.
- `index.html` contains React assets and `window.__BENCHMARK_REPORT__`.
- The report can be opened directly as a file.

## Acceptance Criteria

- Each benchmark run generates a standalone web report package.
- The report opens by double-clicking `index.html`.
- The report uses React, TypeScript, Vite, Ant Design 5, and AntV charts.
- The report highlights multi-engine comparison as the core view.
- Grafana and Prometheus are removed from the report path, Docker Compose, and documentation.
- KPI local and TPC-H compose benchmark flows remain supported.
- Tests cover mapping, writing, frontend rendering, and Docker topology changes.

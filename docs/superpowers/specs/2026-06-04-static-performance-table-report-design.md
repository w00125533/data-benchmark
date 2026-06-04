# Static Performance Table Report Design

## Goal

Replace the current chart-heavy web report direction with the simplest static report that directly shows benchmark performance for datasets and SQL queries across three technical routes.

The report remains a per-run static HTML artifact under `reports/runs/<run-id>/index.html`. It must not require a report server, database, Grafana, Prometheus, or external XML/JSON payload to render. All data needed by the page is embedded directly into `index.html`.

## Non-Goals

- Do not add Spring Boot or any other report service.
- Do not add run management from the web page.
- Do not add historical run comparison.
- Do not use chart visualization in this iteration.
- Do not require `report.json`, XML, or an API call for page rendering.
- Do not claim official TPC-H benchmark results.

## Technical Routes

The report compares these three routes as fixed first-class columns:

1. `spark_iceberg`: Spark SQL querying Iceberg tables on HDFS.
2. `starrocks_internal`: StarRocks querying data loaded into internal tables.
3. `starrocks_external_iceberg`: StarRocks querying Iceberg tables through an external Iceberg catalog.

If a route is not executed for a dataset/query, the cell must show `SKIPPED`. If it is executed and fails, the cell must show `FAILED` and the error summary.

## Report Structure

The report page has four sections:

1. **Run Summary**
   - Run ID.
   - Profile.
   - Suite.
   - Query set.
   - Status.
   - Start/end time.
   - Duration.
   - Dataset scale summary.

2. **Performance Matrix**
   - This is the primary section.
   - It is a table, not a chart.
   - Rows represent `dataset + querySet + queryName`.
   - Columns represent the three technical routes.
   - A final column shows the fastest successful route.

3. **Load Details**
   - Existing load detail table remains.
   - It helps explain why a route is missing, slow, or failed.

4. **Query Details**
   - Existing query detail table remains.
   - It provides raw per-engine query metrics and errors.

## Performance Matrix Columns

The matrix table columns are:

| Column | Meaning |
|---|---|
| Dataset | Dataset identifier or display name, such as `tpch` or `custom-sales`. |
| Query Set | Query group, such as `smoke`, `all`, or a custom query-set name. |
| SQL | Query name, such as `q01_pricing_summary_report` or `top_region_sales`. |
| Spark Iceberg | Status and timing for `spark_iceberg`. |
| StarRocks Internal | Status and timing for `starrocks_internal`. |
| StarRocks External Iceberg | Status and timing for `starrocks_external_iceberg`. |
| Best Route | Fastest successful route by `p95Ms`; `-` if no route succeeded. |

Each route cell renders:

```text
SUCCESS
p95 410 ms
p50 390 ms / p99 455 ms
rows 120
```

Failure cell:

```text
FAILED
catalog timeout
```

Skipped cell:

```text
SKIPPED
```

## Data Contract

Use a versioned embedded report contract. Existing fields can remain, but add matrix-oriented fields so the frontend does not infer too much from chart data.

Top-level shape:

```json
{
  "schemaVersion": 2,
  "run": {},
  "dataset": {},
  "loads": [],
  "queries": [],
  "performanceMatrix": [],
  "notices": []
}
```

`queries[]` entries must include:

```json
{
  "datasetId": "tpch",
  "datasetName": "TPC-H SF 0.01",
  "querySet": "smoke",
  "queryName": "q01_pricing_summary_report",
  "engine": "starrocks_internal",
  "tableShape": "sr_internal_tpch",
  "p50Ms": 390,
  "p95Ms": 410,
  "p99Ms": 455,
  "rows": 120,
  "status": "SUCCESS",
  "error": ""
}
```

`performanceMatrix[]` entries must include:

```json
{
  "datasetId": "tpch",
  "datasetName": "TPC-H SF 0.01",
  "querySet": "smoke",
  "queryName": "q01_pricing_summary_report",
  "routes": {
    "spark_iceberg": {
      "status": "SUCCESS",
      "p50Ms": 1610,
      "p95Ms": 1820,
      "p99Ms": 1902,
      "rows": 120,
      "error": ""
    },
    "starrocks_internal": {
      "status": "SUCCESS",
      "p50Ms": 390,
      "p95Ms": 410,
      "p99Ms": 455,
      "rows": 120,
      "error": ""
    },
    "starrocks_external_iceberg": {
      "status": "FAILED",
      "p50Ms": 0,
      "p95Ms": 0,
      "p99Ms": 0,
      "rows": 0,
      "error": "catalog timeout"
    }
  },
  "bestRoute": "starrocks_internal",
  "bestRouteP95Ms": 410
}
```

Supported route statuses:

- `SUCCESS`
- `FAILED`
- `SKIPPED`

Compatibility:

- `schemaVersion` becomes `2`.
- The frontend may reject unsupported schema versions with a clear static error page.
- Existing raw `loads` and `queries` remain available for detail tables.
- Chart-specific fields are no longer required by the frontend.

## HTML Data Embedding

The report writer injects the complete report payload directly into `index.html`:

```html
<script>
  window.__BENCHMARK_REPORT__ = { ... };
</script>
```

The static page must not call `fetch('./report.json')` during normal rendering. It must be usable by opening `index.html` directly from disk.

`report.json` is not emitted for this simplified design. If a future diagnostic export is needed, it should be explicitly added as a separate option and must not be required by the page.

## Frontend Requirements

Use the existing frontend stack:

- React.
- TypeScript.
- Vite.
- Ant Design.

The frontend should remove chart-first report content and render:

- `RunSummary`.
- `PerformanceMatrixTable`.
- `LoadDetailsTable`.
- `QueryDetailsTable`.

The table should be dense and readable:

- Use compact Ant Design table sizing.
- Use tags for `SUCCESS`, `FAILED`, and `SKIPPED`.
- Show `p95` most prominently in each route cell.
- Sort matrix rows by dataset, query set, then query name by default.
- Show fastest successful route by lowest `p95Ms`.
- Preserve failed and skipped cells instead of hiding them.

## Backend Requirements

Backend report mapping should build `performanceMatrix` from query summaries.

Grouping key:

```text
datasetId + querySet + queryName
```

For the current data model, if dataset metadata is not yet captured per query:

- Use `report.suite()` as `datasetId`.
- Use a human-readable dataset name derived from suite and profile.
- Use `report.querySet()` as `querySet`.

Route normalization:

- Preserve current engine strings where possible.
- Normalize known aliases into the fixed route keys:
  - `spark_iceberg`
  - `starrocks_internal`
  - `starrocks_external_iceberg`
- Unknown engines may be included in raw query details but do not get a matrix column in this iteration.

For each matrix row:

- Fill executed routes from query summaries.
- Mark fixed routes not present as `SKIPPED`.
- Mark unsuccessful query summaries as `FAILED`.
- Compute `bestRoute` from successful routes only, using lowest `p95Ms`.

## Testing

Backend tests:

- Mapper emits `schemaVersion = 2`.
- Mapper builds one matrix row per `datasetId + querySet + queryName`.
- Mapper fills all three fixed routes.
- Mapper marks missing routes as `SKIPPED`.
- Mapper marks failed query results as `FAILED`.
- Mapper computes best route by lowest successful `p95Ms`.
- Writer embeds `window.__BENCHMARK_REPORT__`.
- Writer does not emit or require `report.json` for rendering.
- Writer still escapes embedded JSON safely.

Frontend tests:

- Renders run summary.
- Renders performance matrix table.
- Shows p95/p50/p99/rows in route cells.
- Shows `FAILED` and error text.
- Shows `SKIPPED` for routes not executed.
- Shows best route.
- Does not call `fetch('./report.json')` for normal embedded data rendering.

End-to-end smoke:

```powershell
npm.cmd --prefix frontend test
npm.cmd --prefix frontend run build
mvn clean package
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode local --config configs/benchmark-smoke.yml --run-id static-table-smoke
```

Verify:

- `reports/runs/static-table-smoke/index.html` exists.
- HTML contains `window.__BENCHMARK_REPORT__`.
- Opening `index.html` renders the matrix table.
- The page has no runtime dependency on a local server or external XML/JSON payload.

## Acceptance Criteria

- Report remains a static HTML artifact.
- Required report data is embedded directly in `index.html`.
- No report service is introduced.
- No chart is required to understand performance.
- Main report section is a table comparing datasets and SQL queries across Spark Iceberg, StarRocks Internal, and StarRocks External Iceberg.
- Failed and skipped routes are visible in the matrix.
- Best route is computed and displayed per dataset/query.
- Existing load and query detail tables remain available.

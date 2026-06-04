# Static Performance Table Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a simple static HTML report whose main section is a table comparing each dataset/query across Spark Iceberg, StarRocks Internal, and StarRocks External Iceberg.

**Architecture:** Keep the current Java 17 runner and Vite/React static report pipeline. Upgrade the embedded report contract to schema version 2, generate a backend `performanceMatrix`, and render it with an Ant Design table. The page must render from embedded `window.__BENCHMARK_REPORT__` data and must not depend on external XML/JSON or a report service.

**Tech Stack:** Java 17, Maven, Jackson, React, TypeScript, Vite, Ant Design, Vitest, Testing Library.

---

## File Structure

- Modify `src/main/java/com/example/databenchmark/report/WebBenchmarkReport.java`
  - Add schema v2 query fields and matrix DTO records.
- Modify `src/main/java/com/example/databenchmark/report/WebBenchmarkReportMapper.java`
  - Build `performanceMatrix` from query summaries.
  - Normalize the three fixed route keys.
- Modify `src/main/java/com/example/databenchmark/report/WebReportWriter.java`
  - Keep embedded data injection.
  - Stop requiring the page to fetch `report.json`.
  - If `report.json` remains temporarily for diagnostics, tests must confirm rendering does not depend on it.
- Modify `src/test/java/com/example/databenchmark/report/WebBenchmarkReportMapperTest.java`
  - Cover schema v2 and matrix generation.
- Modify `src/test/java/com/example/databenchmark/report/WebReportWriterTest.java`
  - Cover embedded-only rendering contract.
- Modify `frontend/src/types/report.ts`
  - Add schema v2 types and matrix route types.
- Modify `frontend/src/data/reportLoader.ts`
  - Prefer embedded data and remove normal `fetch('./report.json')` fallback.
- Create `frontend/src/components/PerformanceMatrixTable.tsx`
  - Render the matrix table.
- Modify `frontend/src/App.tsx`
  - Replace chart-first content with the matrix table.
- Modify `frontend/src/App.test.tsx`
  - Cover matrix rendering.
- Modify `frontend/src/data/sampleReport.ts`
  - Provide schema v2 sample data with success, failed, and skipped routes.
- Modify `frontend/src/data/reportLoader.test.ts`
  - Confirm embedded data does not call fetch.
- Refresh `src/main/resources/report-ui/`
  - Copy latest `frontend/dist` after frontend build and reinsert the injection marker.

---

### Task 1: Backend Contract For Schema V2

**Files:**
- Modify: `src/main/java/com/example/databenchmark/report/WebBenchmarkReport.java`
- Test: `src/test/java/com/example/databenchmark/report/WebBenchmarkReportMapperTest.java`

- [ ] **Step 1: Write the failing mapper contract test**

Add this test to `WebBenchmarkReportMapperTest`:

```java
@Test
void mapsSchemaVersionTwoMatrixFields() {
    BenchmarkReport report = new BenchmarkReport(
        "matrix-run",
        "tpch-smoke",
        "tpch",
        "smoke",
        "2026-06-04T00:00:00Z",
        "2026-06-04T00:00:03Z",
        10000,
        1,
        60000,
        8,
        1024,
        List.of(),
        List.of(new BenchmarkReport.QuerySummary(
            "starrocks_internal",
            "sr_internal_tpch",
            "q01_pricing_summary_report",
            390,
            410,
            455,
            120,
            0,
            true,
            ""
        )),
        false
    );

    WebBenchmarkReport mapped = new WebBenchmarkReportMapper().map(report);

    assertThat(mapped.schemaVersion()).isEqualTo(2);
    assertThat(mapped.performanceMatrix()).hasSize(1);
    WebBenchmarkReport.PerformanceMatrixRow row = mapped.performanceMatrix().get(0);
    assertThat(row.datasetId()).isEqualTo("tpch");
    assertThat(row.datasetName()).isEqualTo("TPC-H SF 0.01");
    assertThat(row.querySet()).isEqualTo("smoke");
    assertThat(row.queryName()).isEqualTo("q01_pricing_summary_report");
    assertThat(row.routes().get("starrocks_internal").status()).isEqualTo("SUCCESS");
    assertThat(row.routes().get("spark_iceberg").status()).isEqualTo("SKIPPED");
    assertThat(row.routes().get("starrocks_external_iceberg").status()).isEqualTo("SKIPPED");
    assertThat(row.bestRoute()).isEqualTo("starrocks_internal");
    assertThat(row.bestRouteP95Ms()).isEqualTo(410);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
mvn "-Dtest=WebBenchmarkReportMapperTest#mapsSchemaVersionTwoMatrixFields" test
```

Expected: compilation fails because `performanceMatrix()` and related records do not exist, or assertion fails because schema version is still `1`.

- [ ] **Step 3: Add schema v2 DTO records**

Update `WebBenchmarkReport.java` so the top-level record and query records include matrix fields:

```java
public record WebBenchmarkReport(
    int schemaVersion,
    RunInfo run,
    DatasetInfo dataset,
    List<LoadSummary> loads,
    List<QuerySummary> queries,
    List<PerformanceMatrixRow> performanceMatrix,
    ChartData charts,
    List<String> notices
) {
    public record QuerySummary(
        String datasetId,
        String datasetName,
        String querySet,
        String engine,
        String tableShape,
        String queryName,
        double p50Ms,
        double p95Ms,
        double p99Ms,
        long rows,
        String status,
        String error
    ) {}

    public record PerformanceMatrixRow(
        String datasetId,
        String datasetName,
        String querySet,
        String queryName,
        java.util.Map<String, RouteResult> routes,
        String bestRoute,
        double bestRouteP95Ms
    ) {}

    public record RouteResult(
        String status,
        double p50Ms,
        double p95Ms,
        double p99Ms,
        long rows,
        String error
    ) {}
}
```

Keep the existing `RunInfo`, `DatasetInfo`, `LoadSummary`, `ChartData`, `LoadDurationPoint`, `QueryLatencyPoint`, `QueryRowsPoint`, and `FailureSummaryPoint` records in the same file. Do not delete chart records in this task; removing chart dependency is a frontend task.

- [ ] **Step 4: Run test to confirm current mapper still fails**

Run:

```powershell
mvn "-Dtest=WebBenchmarkReportMapperTest#mapsSchemaVersionTwoMatrixFields" test
```

Expected: compilation or assertion failure remains in `WebBenchmarkReportMapper`, because mapper construction has not been updated yet.

---

### Task 2: Backend Matrix Mapper

**Files:**
- Modify: `src/main/java/com/example/databenchmark/report/WebBenchmarkReportMapper.java`
- Test: `src/test/java/com/example/databenchmark/report/WebBenchmarkReportMapperTest.java`

- [ ] **Step 1: Write failed and best-route tests**

Add this test:

```java
@Test
void matrixMarksFailedRoutesAndChoosesFastestSuccessfulP95() {
    BenchmarkReport report = new BenchmarkReport(
        "matrix-run",
        "tpch-smoke",
        "tpch",
        "smoke",
        "2026-06-04T00:00:00Z",
        "2026-06-04T00:00:03Z",
        10000,
        1,
        60000,
        8,
        1024,
        List.of(),
        List.of(
            new BenchmarkReport.QuerySummary("spark_iceberg", "iceberg_catalog.tpch", "q03_shipping_priority", 2310, 2450, 2600, 120, 0, true, ""),
            new BenchmarkReport.QuerySummary("starrocks_internal", "sr_internal_tpch", "q03_shipping_priority", 720, 760, 810, 120, 0, true, ""),
            new BenchmarkReport.QuerySummary("starrocks_external_iceberg", "sr_external_iceberg.tpch", "q03_shipping_priority", 0, 0, 0, 0, 1, false, "catalog timeout")
        ),
        false
    );

    WebBenchmarkReport.PerformanceMatrixRow row = new WebBenchmarkReportMapper()
        .map(report)
        .performanceMatrix()
        .get(0);

    assertThat(row.routes().get("spark_iceberg").status()).isEqualTo("SUCCESS");
    assertThat(row.routes().get("starrocks_internal").status()).isEqualTo("SUCCESS");
    assertThat(row.routes().get("starrocks_external_iceberg").status()).isEqualTo("FAILED");
    assertThat(row.routes().get("starrocks_external_iceberg").error()).isEqualTo("catalog timeout");
    assertThat(row.bestRoute()).isEqualTo("starrocks_internal");
    assertThat(row.bestRouteP95Ms()).isEqualTo(760);
}
```

- [ ] **Step 2: Run tests to verify failures**

Run:

```powershell
mvn "-Dtest=WebBenchmarkReportMapperTest" test
```

Expected: mapper tests fail until schema v2 mapper logic is implemented.

- [ ] **Step 3: Implement mapper constants and query DTO mapping**

In `WebBenchmarkReportMapper.java`, set:

```java
private static final int SCHEMA_VERSION = 2;
private static final List<String> ROUTES = List.of(
    "spark_iceberg",
    "starrocks_internal",
    "starrocks_external_iceberg"
);
```

Update the top-level constructor call to pass `performanceMatrix(report)` before `charts(report)`:

```java
return new WebBenchmarkReport(
    SCHEMA_VERSION,
    new WebBenchmarkReport.RunInfo(
        report.runId(),
        report.profile(),
        report.suite(),
        report.querySet(),
        report.status(),
        report.startedAt(),
        report.endedAt(),
        report.durationSeconds(),
        report.fullProfile()
    ),
    new WebBenchmarkReport.DatasetInfo(
        report.cells(),
        report.days(),
        report.rows(),
        report.columns(),
        report.bytesWritten()
    ),
    loads(report),
    queries(report),
    performanceMatrix(report),
    charts(report),
    notices(report)
);
```

Replace `queries(report)` mapping with:

```java
private List<WebBenchmarkReport.QuerySummary> queries(BenchmarkReport report) {
    String datasetId = datasetId(report);
    String datasetName = datasetName(report);
    return report.querySummaries().stream()
        .map(query -> new WebBenchmarkReport.QuerySummary(
            datasetId,
            datasetName,
            report.querySet(),
            normalizeRoute(query.engine()),
            query.tableShape(),
            query.queryName(),
            query.p50Ms(),
            query.p95Ms(),
            query.p99Ms(),
            query.rows(),
            query.success() && query.failures() == 0 ? "SUCCESS" : "FAILED",
            query.error()
        ))
        .toList();
}
```

- [ ] **Step 4: Implement matrix grouping**

Add these helper methods to `WebBenchmarkReportMapper.java`:

```java
private List<WebBenchmarkReport.PerformanceMatrixRow> performanceMatrix(BenchmarkReport report) {
    Map<MatrixKey, Map<String, WebBenchmarkReport.RouteResult>> grouped = new LinkedHashMap<>();
    String datasetId = datasetId(report);
    String datasetName = datasetName(report);
    for (BenchmarkReport.QuerySummary query : report.querySummaries()) {
        String route = normalizeRoute(query.engine());
        if (!ROUTES.contains(route)) {
            continue;
        }
        MatrixKey key = new MatrixKey(datasetId, datasetName, report.querySet(), query.queryName());
        grouped.computeIfAbsent(key, ignored -> skippedRoutes())
            .put(route, new WebBenchmarkReport.RouteResult(
                query.success() && query.failures() == 0 ? "SUCCESS" : "FAILED",
                query.p50Ms(),
                query.p95Ms(),
                query.p99Ms(),
                query.rows(),
                query.error()
            ));
    }
    return grouped.entrySet().stream()
        .map(entry -> matrixRow(entry.getKey(), entry.getValue()))
        .toList();
}

private Map<String, WebBenchmarkReport.RouteResult> skippedRoutes() {
    Map<String, WebBenchmarkReport.RouteResult> routes = new LinkedHashMap<>();
    for (String route : ROUTES) {
        routes.put(route, new WebBenchmarkReport.RouteResult("SKIPPED", 0, 0, 0, 0, ""));
    }
    return routes;
}

private WebBenchmarkReport.PerformanceMatrixRow matrixRow(
    MatrixKey key,
    Map<String, WebBenchmarkReport.RouteResult> routes
) {
    String bestRoute = "";
    double bestP95 = 0;
    for (Map.Entry<String, WebBenchmarkReport.RouteResult> entry : routes.entrySet()) {
        WebBenchmarkReport.RouteResult result = entry.getValue();
        if (!"SUCCESS".equals(result.status())) {
            continue;
        }
        if (bestRoute.isEmpty() || result.p95Ms() < bestP95) {
            bestRoute = entry.getKey();
            bestP95 = result.p95Ms();
        }
    }
    return new WebBenchmarkReport.PerformanceMatrixRow(
        key.datasetId(),
        key.datasetName(),
        key.querySet(),
        key.queryName(),
        routes,
        bestRoute,
        bestP95
    );
}

private String normalizeRoute(String engine) {
    if (engine == null) {
        return "";
    }
    String normalized = engine.toLowerCase(java.util.Locale.ROOT);
    if (normalized.contains("spark") && normalized.contains("iceberg")) {
        return "spark_iceberg";
    }
    if (normalized.contains("external") && normalized.contains("iceberg")) {
        return "starrocks_external_iceberg";
    }
    if (normalized.contains("starrocks") && normalized.contains("internal")) {
        return "starrocks_internal";
    }
    return normalized;
}

private String datasetId(BenchmarkReport report) {
    return report.suite() == null || report.suite().isBlank() ? "default" : report.suite();
}

private String datasetName(BenchmarkReport report) {
    if ("tpch".equals(report.suite())) {
        return "TPC-H SF 0.01";
    }
    if ("kpi".equals(report.suite())) {
        return "KPI " + report.profile();
    }
    return report.suite() + " " + report.profile();
}

private record MatrixKey(String datasetId, String datasetName, String querySet, String queryName) {}
```

- [ ] **Step 5: Run mapper tests**

Run:

```powershell
mvn "-Dtest=WebBenchmarkReportMapperTest" test
```

Expected: all mapper tests pass.

- [ ] **Step 6: Commit backend contract and mapper**

Run:

```powershell
git add src/main/java/com/example/databenchmark/report/WebBenchmarkReport.java src/main/java/com/example/databenchmark/report/WebBenchmarkReportMapper.java src/test/java/com/example/databenchmark/report/WebBenchmarkReportMapperTest.java
git commit -m "feat: add performance matrix report contract"
```

---

### Task 3: Embedded-Only Report Loading

**Files:**
- Modify: `frontend/src/data/reportLoader.ts`
- Modify: `frontend/src/data/reportLoader.test.ts`
- Modify: `src/main/java/com/example/databenchmark/report/WebReportWriter.java`
- Modify: `src/test/java/com/example/databenchmark/report/WebReportWriterTest.java`

- [ ] **Step 1: Write frontend loader test**

In `frontend/src/data/reportLoader.test.ts`, add:

```ts
it('loads embedded report without fetching report.json', async () => {
  const fetchMock = vi.fn();
  vi.stubGlobal('fetch', fetchMock);
  window.__BENCHMARK_REPORT__ = sampleReport;

  const report = await loadReport();

  expect(report.run.runId).toBe(sampleReport.run.runId);
  expect(fetchMock).not.toHaveBeenCalled();
});
```

- [ ] **Step 2: Run frontend loader test to verify current behavior**

Run:

```powershell
npm.cmd --prefix frontend test -- reportLoader
```

Expected: test may fail because the current schema type and loader still support fetch fallback.

- [ ] **Step 3: Simplify frontend loader**

Update `frontend/src/data/reportLoader.ts` to:

```ts
import type { WebBenchmarkReport } from '../types/report';

export async function loadReport(): Promise<WebBenchmarkReport> {
  const embedded = window.__BENCHMARK_REPORT__;
  if (!embedded) {
    throw new Error('Missing embedded report data: window.__BENCHMARK_REPORT__');
  }
  if (embedded.schemaVersion !== 2) {
    throw new Error(`Unsupported report schema version: ${embedded.schemaVersion}`);
  }
  return embedded;
}
```

- [ ] **Step 4: Write backend writer test**

Update `WebReportWriterTest.writesStandaloneWebReportPackage` so it asserts embedded data is present and does not require `report.json`:

```java
assertThat(html).contains("window.__BENCHMARK_REPORT__");
assertThat(html).contains("<script defer src=\"./assets/report-ui.js\"></script>");
assertThat(html).doesNotContain("type=\"module\"");
assertThat(html).doesNotContain("fetch('./report.json')");
assertThat(tempDir.resolve("run-web").resolve("assets").resolve("report-ui.js")).exists();
```

Remove the assertion that `report.json` must exist as a required artifact. If the implementation still writes it temporarily, do not assert on it.

- [ ] **Step 5: Adjust writer only if tests expose a hard dependency**

If `WebReportWriter` writes `report.json`, it can remain as a diagnostic artifact only if no frontend code fetches it. If the team wants to remove it now, delete this line:

```java
Files.writeString(outputDir.resolve("report.json"), json, StandardCharsets.UTF_8);
```

Do not remove embedded script injection.

- [ ] **Step 6: Run writer and loader tests**

Run:

```powershell
npm.cmd --prefix frontend test -- reportLoader
mvn "-Dtest=WebReportWriterTest" test
```

Expected: frontend loader tests and writer tests pass.

- [ ] **Step 7: Commit embedded-only loading**

Run:

```powershell
git add frontend/src/data/reportLoader.ts frontend/src/data/reportLoader.test.ts src/main/java/com/example/databenchmark/report/WebReportWriter.java src/test/java/com/example/databenchmark/report/WebReportWriterTest.java
git commit -m "fix: load reports from embedded data only"
```

---

### Task 4: Frontend Schema V2 Types And Sample Data

**Files:**
- Modify: `frontend/src/types/report.ts`
- Modify: `frontend/src/data/sampleReport.ts`

- [ ] **Step 1: Update TypeScript report types**

Replace the report type definitions with schema v2-compatible definitions:

```ts
export type RunStatus = 'SUCCESS' | 'DEGRADED';
export type RouteStatus = 'SUCCESS' | 'FAILED' | 'SKIPPED';
export type RouteKey = 'spark_iceberg' | 'starrocks_internal' | 'starrocks_external_iceberg';

export interface WebBenchmarkReport {
  schemaVersion: 2;
  run: RunInfo;
  dataset: DatasetInfo;
  loads: LoadSummary[];
  queries: QuerySummary[];
  performanceMatrix: PerformanceMatrixRow[];
  notices: string[];
}

export interface QuerySummary {
  datasetId: string;
  datasetName: string;
  querySet: string;
  engine: string;
  tableShape: string;
  queryName: string;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  rows: number;
  status: RouteStatus;
  error: string;
}

export interface PerformanceMatrixRow {
  datasetId: string;
  datasetName: string;
  querySet: string;
  queryName: string;
  routes: Record<RouteKey, RouteResult>;
  bestRoute: RouteKey | '';
  bestRouteP95Ms: number;
}

export interface RouteResult {
  status: RouteStatus;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  rows: number;
  error: string;
}
```

Keep `RunInfo`, `DatasetInfo`, and `LoadSummary` interfaces. Update `Window.__BENCHMARK_REPORT__` to use schema v2 `WebBenchmarkReport`.

- [ ] **Step 2: Update sample report**

Update `frontend/src/data/sampleReport.ts` so it contains:

```ts
export const sampleReport: WebBenchmarkReport = {
  schemaVersion: 2,
  run: {
    runId: 'sample-matrix-run',
    profile: 'tpch-smoke',
    suite: 'tpch',
    querySet: 'smoke',
    status: 'DEGRADED',
    startedAt: '2026-06-04T00:00:00Z',
    endedAt: '2026-06-04T00:00:03Z',
    durationSeconds: 3,
    fullProfile: false,
  },
  dataset: {
    cells: 10000,
    days: 1,
    rows: 60000,
    columns: 8,
    bytesWritten: 1024,
  },
  loads: [],
  queries: [],
  performanceMatrix: [
    {
      datasetId: 'tpch',
      datasetName: 'TPC-H SF 0.01',
      querySet: 'smoke',
      queryName: 'q03_shipping_priority',
      routes: {
        spark_iceberg: { status: 'SUCCESS', p50Ms: 2310, p95Ms: 2450, p99Ms: 2600, rows: 120, error: '' },
        starrocks_internal: { status: 'SUCCESS', p50Ms: 720, p95Ms: 760, p99Ms: 810, rows: 120, error: '' },
        starrocks_external_iceberg: { status: 'FAILED', p50Ms: 0, p95Ms: 0, p99Ms: 0, rows: 0, error: 'catalog timeout' },
      },
      bestRoute: 'starrocks_internal',
      bestRouteP95Ms: 760,
    },
    {
      datasetId: 'custom-sales',
      datasetName: 'Custom Sales',
      querySet: 'daily',
      queryName: 'top_region_sales',
      routes: {
        spark_iceberg: { status: 'SUCCESS', p50Ms: 1210, p95Ms: 1330, p99Ms: 1450, rows: 120, error: '' },
        starrocks_internal: { status: 'SUCCESS', p50Ms: 330, p95Ms: 350, p99Ms: 380, rows: 120, error: '' },
        starrocks_external_iceberg: { status: 'SKIPPED', p50Ms: 0, p95Ms: 0, p99Ms: 0, rows: 0, error: '' },
      },
      bestRoute: 'starrocks_internal',
      bestRouteP95Ms: 350,
    },
  ],
  notices: ['TPC-H smoke data is compatible test data, not an official TPC-H benchmark result.'],
};
```

- [ ] **Step 3: Run type check through frontend tests**

Run:

```powershell
npm.cmd --prefix frontend test
```

Expected: tests fail until components are updated to schema v2.

---

### Task 5: Performance Matrix Table Component

**Files:**
- Create: `frontend/src/components/PerformanceMatrixTable.tsx`
- Modify: `frontend/src/App.test.tsx`
- Modify: `frontend/src/App.tsx`

- [ ] **Step 1: Write rendering tests**

In `frontend/src/App.test.tsx`, ensure embedded sample data is loaded and add assertions:

```ts
it('renders performance matrix with route statuses and best route', async () => {
  window.__BENCHMARK_REPORT__ = sampleReport;

  render(<App />);

  expect(await screen.findByText('性能矩阵')).toBeInTheDocument();
  expect(screen.getByText('q03_shipping_priority')).toBeInTheDocument();
  expect(screen.getByText('top_region_sales')).toBeInTheDocument();
  expect(screen.getAllByText('StarRocks Internal').length).toBeGreaterThan(0);
  expect(screen.getByText('p95 760 ms')).toBeInTheDocument();
  expect(screen.getByText('catalog timeout')).toBeInTheDocument();
  expect(screen.getByText('SKIPPED')).toBeInTheDocument();
});
```

- [ ] **Step 2: Run frontend tests to verify failure**

Run:

```powershell
npm.cmd --prefix frontend test -- App
```

Expected: test fails because `PerformanceMatrixTable` does not exist or App does not render it.

- [ ] **Step 3: Create matrix table component**

Create `frontend/src/components/PerformanceMatrixTable.tsx`:

```tsx
import { Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { PerformanceMatrixRow, RouteKey, RouteResult } from '../types/report';

const routeLabels: Record<RouteKey, string> = {
  spark_iceberg: 'Spark Iceberg',
  starrocks_internal: 'StarRocks Internal',
  starrocks_external_iceberg: 'StarRocks External Iceberg',
};

const statusColor: Record<RouteResult['status'], string> = {
  SUCCESS: 'green',
  FAILED: 'red',
  SKIPPED: 'default',
};

function formatMs(value: number) {
  return `${Number.isFinite(value) ? value : 0} ms`;
}

function RouteCell({ result }: { result: RouteResult }) {
  if (result.status === 'SKIPPED') {
    return <Tag color={statusColor.SKIPPED}>SKIPPED</Tag>;
  }
  if (result.status === 'FAILED') {
    return (
      <div>
        <Tag color={statusColor.FAILED}>FAILED</Tag>
        <Typography.Text type="danger">{result.error || '-'}</Typography.Text>
      </div>
    );
  }
  return (
    <div>
      <Tag color={statusColor.SUCCESS}>SUCCESS</Tag>
      <div><strong>p95 {formatMs(result.p95Ms)}</strong></div>
      <div>p50 {formatMs(result.p50Ms)} / p99 {formatMs(result.p99Ms)}</div>
      <div>rows {result.rows.toLocaleString()}</div>
    </div>
  );
}

function BestRouteCell({ row }: { row: PerformanceMatrixRow }) {
  if (!row.bestRoute) {
    return '-';
  }
  return (
    <div>
      <strong>{routeLabels[row.bestRoute]}</strong>
      <div>p95 {formatMs(row.bestRouteP95Ms)}</div>
    </div>
  );
}

export default function PerformanceMatrixTable({ rows }: { rows: PerformanceMatrixRow[] }) {
  const columns: ColumnsType<PerformanceMatrixRow> = [
    { title: '数据集', dataIndex: 'datasetName', key: 'datasetName', width: 160 },
    { title: 'Query Set', dataIndex: 'querySet', key: 'querySet', width: 120 },
    { title: 'SQL', dataIndex: 'queryName', key: 'queryName', width: 220 },
    {
      title: routeLabels.spark_iceberg,
      key: 'spark_iceberg',
      render: (_, row) => <RouteCell result={row.routes.spark_iceberg} />,
    },
    {
      title: routeLabels.starrocks_internal,
      key: 'starrocks_internal',
      render: (_, row) => <RouteCell result={row.routes.starrocks_internal} />,
    },
    {
      title: routeLabels.starrocks_external_iceberg,
      key: 'starrocks_external_iceberg',
      render: (_, row) => <RouteCell result={row.routes.starrocks_external_iceberg} />,
    },
    {
      title: '最优路线',
      key: 'bestRoute',
      render: (_, row) => <BestRouteCell row={row} />,
      width: 180,
    },
  ];

  return (
    <Table
      size="small"
      rowKey={(row) => `${row.datasetId}:${row.querySet}:${row.queryName}`}
      columns={columns}
      dataSource={[...rows].sort((a, b) =>
        `${a.datasetName}:${a.querySet}:${a.queryName}`.localeCompare(`${b.datasetName}:${b.querySet}:${b.queryName}`),
      )}
      pagination={false}
      scroll={{ x: 1200 }}
    />
  );
}
```

- [ ] **Step 4: Render matrix in App**

Update `frontend/src/App.tsx`:

```tsx
import PerformanceMatrixTable from './components/PerformanceMatrixTable';
```

Replace the chart section with:

```tsx
<Card size="small" title="性能矩阵">
  <PerformanceMatrixTable rows={report.performanceMatrix} />
</Card>
```

Keep `RunSummary`, `LoadDetailsTable`, and `QueryDetailsTable`. Remove `EngineComparison` and `StageTimeline` from the rendered page in this simplified report.

- [ ] **Step 5: Run frontend tests**

Run:

```powershell
npm.cmd --prefix frontend test
```

Expected: all frontend tests pass.

- [ ] **Step 6: Commit frontend matrix**

Run:

```powershell
git add frontend/src/types/report.ts frontend/src/data/sampleReport.ts frontend/src/components/PerformanceMatrixTable.tsx frontend/src/App.tsx frontend/src/App.test.tsx
git commit -m "feat: render static performance matrix"
```

---

### Task 6: Keep Detail Tables Compatible

**Files:**
- Modify: `frontend/src/components/QueryDetailsTable.tsx`
- Modify: `frontend/src/components/LoadDetailsTable.tsx`
- Modify: `frontend/src/App.test.tsx`

- [ ] **Step 1: Update QueryDetailsTable for `status`**

Change query status rendering to read `row.status` instead of `row.success`:

```tsx
const isSuccess = row.status === 'SUCCESS';
```

If the component currently expects `failures`, remove failure count rendering or show `FAILED` when `status === 'FAILED'`.

- [ ] **Step 2: Add detail table assertions**

In `frontend/src/App.test.tsx`, add:

```ts
it('keeps load and query detail sections visible', async () => {
  window.__BENCHMARK_REPORT__ = sampleReport;

  render(<App />);

  expect(await screen.findByText('Load 明细')).toBeInTheDocument();
  expect(screen.getByText('Query 明细')).toBeInTheDocument();
});
```

- [ ] **Step 3: Run detail tests**

Run:

```powershell
npm.cmd --prefix frontend test -- App
```

Expected: App tests pass.

---

### Task 7: Build Static Assets And Refresh Java Resources

**Files:**
- Modify: `src/main/resources/report-ui/index.html`
- Modify: `src/main/resources/report-ui/assets/report-ui.js`
- Delete if present: `src/main/resources/report-ui/assets/report-ui.css`

- [ ] **Step 1: Build frontend**

Run:

```powershell
npm.cmd --prefix frontend run build
```

Expected: Vite build succeeds and emits `frontend/dist/index.html` plus `frontend/dist/assets/report-ui.js`.

- [ ] **Step 2: Refresh Java resources**

Run these PowerShell commands:

```powershell
Remove-Item -LiteralPath src\main\resources\report-ui -Recurse -Force
Copy-Item -LiteralPath frontend\dist -Destination src\main\resources\report-ui -Recurse
$index = 'src\main\resources\report-ui\index.html'
$html = Get-Content -LiteralPath $index -Raw
$html = $html -replace '<div id="root"></div>', "<div id=`"root`"></div>`r`n    <!-- BENCHMARK_REPORT_DATA -->"
Set-Content -LiteralPath $index -Value $html -NoNewline
```

- [ ] **Step 3: Verify resource marker**

Run:

```powershell
Select-String -Path src\main\resources\report-ui\index.html -SimpleMatch '<!-- BENCHMARK_REPORT_DATA -->'
```

Expected: one match.

- [ ] **Step 4: Commit refreshed static resources**

Run:

```powershell
git add src/main/resources/report-ui
git commit -m "chore: refresh static report assets"
```

---

### Task 8: End-To-End Static Report Verification

**Files:**
- Generated only: `reports/runs/static-table-smoke/index.html`

- [ ] **Step 1: Run full package build**

Run:

```powershell
mvn clean package
```

Expected: build succeeds with all tests passing.

- [ ] **Step 2: Generate smoke report**

Run:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode local --config configs/benchmark-smoke.yml --run-id static-table-smoke
```

Expected: command prints a report path under `reports\runs\static-table-smoke\index.html`.

- [ ] **Step 3: Verify static HTML contract**

Run:

```powershell
Select-String -Path reports\runs\static-table-smoke\index.html -SimpleMatch 'window.__BENCHMARK_REPORT__'
Select-String -Path reports\runs\static-table-smoke\index.html -SimpleMatch 'fetch('
Select-String -Path reports\runs\static-table-smoke\index.html -SimpleMatch 'performanceMatrix'
```

Expected:

- First command finds embedded data.
- Second command finds no report-loading `fetch(` dependency.
- Third command finds embedded matrix data.

- [ ] **Step 4: Browser smoke using file URL**

Run a browser smoke check with the same Chrome/CDP technique used previously, or manually open:

```text
D:\agent-code\data-benchmark\reports\runs\static-table-smoke\index.html
```

Expected visible page content:

- `Data Benchmark Report`.
- `性能矩阵`.
- Columns for `Spark Iceberg`, `StarRocks Internal`, and `StarRocks External Iceberg`.
- `Load 明细`.
- `Query 明细`.

- [ ] **Step 5: Final status**

Run:

```powershell
git status --short
```

Expected: only generated runtime folders such as `reports/` and `data/` are untracked or modified. Source files should be committed.

---

## Self-Review

- Spec coverage: covered static HTML, embedded data, no service, no chart requirement, performance matrix, failed/skipped route visibility, best route, and detail tables.
- Placeholder scan: no `TBD`, `TODO`, or unspecified future implementation steps.
- Type consistency: route keys are consistently `spark_iceberg`, `starrocks_internal`, and `starrocks_external_iceberg`; statuses are `SUCCESS`, `FAILED`, and `SKIPPED`; schema version is consistently `2`.

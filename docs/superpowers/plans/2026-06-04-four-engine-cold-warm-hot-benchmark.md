# Four-Engine Cold/Warm/Hot Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a sequential four-engine KPI benchmark that records cold/warm/hot timings for Spark Iceberg, StarRocks Internal, StarRocks External Iceberg, and Hive HDFS Parquet, while keeping the 10k validation dataset as the default.

**Architecture:** Keep `ComposeBenchmarkRunner` as the orchestration boundary. Add focused components for route phase execution, Docker Compose service restarts/readiness, Hive query execution, and schema-v3 report mapping. Extend the static report UI to render four route columns with cold/warm/hot values.

**Tech Stack:** Java 17, Maven, Docker Compose, Spark SQL, StarRocks JDBC/Stream Load, HiveServer2/Beeline or JDBC, React/Vite/Ant Design static report.

---

## File Structure

- Modify `src/main/java/com/example/databenchmark/engine/EngineRunResult.java`
  - Add route phase metadata for cold/warm/hot query samples.
- Modify `src/main/java/com/example/databenchmark/report/BenchmarkReport.java`
  - Preserve existing summary API while allowing multiple query samples per route/query.
- Modify `src/main/java/com/example/databenchmark/report/WebBenchmarkReport.java`
  - Upgrade schema to v3 and extend route cells with `coldMs`, `warmMs`, `hotMs`, phase statuses, rows, and error.
- Modify `src/main/java/com/example/databenchmark/report/WebBenchmarkReportMapper.java`
  - Aggregate query samples into four-route cold/warm/hot matrix rows.
- Create `src/main/java/com/example/databenchmark/runner/RoutePhase.java`
  - Enum for `COLD`, `WARM`, `HOT`.
- Create `src/main/java/com/example/databenchmark/runner/BenchmarkRoute.java`
  - Enum for `SPARK_ICEBERG`, `STARROCKS_INTERNAL`, `STARROCKS_EXTERNAL_ICEBERG`, `HIVE_HDFS_PARQUET`.
- Create `src/main/java/com/example/databenchmark/runner/ComposeServiceController.java`
  - Restarts route-related Docker Compose services and performs readiness checks.
- Modify `src/main/java/com/example/databenchmark/engine/SparkIcebergClient.java`
  - Add single-query execution API.
- Modify `src/main/java/com/example/databenchmark/engine/StarRocksClient.java`
  - Add single-query execution API per table shape.
- Create `src/main/java/com/example/databenchmark/engine/HiveClient.java`
  - Creates Hive external table and executes KPI SQL against HDFS Parquet.
- Modify `src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java`
  - Load all four routes, then execute query/route cold/warm/hot sequence with service restarts.
- Modify `frontend/src/types/report.ts`
  - Add schema v3 route result fields and Hive route key.
- Modify `frontend/src/components/PerformanceMatrixTable.tsx`
  - Render four route columns and cold/warm/hot values.
- Modify `frontend/src/data/reportLoader.ts`
  - Require schema v3.
- Modify `frontend/src/data/sampleReport.ts`
  - Update embedded sample to schema v3.
- Modify `frontend/src/App.test.tsx` and report loader tests.
- Modify `docker-compose.yml`
  - Add `hive-server`; add CPU/memory limits for all benchmark services.
- Add `configs/benchmark-kpi-10m.yml`
  - Formal 10m KPI benchmark config.
- Modify `README.md`
  - Document default 10k validation and 10m formal benchmark.

## Task 1: Report Schema v3 Cold/Warm/Hot Matrix

**Files:**
- Create: `src/main/java/com/example/databenchmark/runner/RoutePhase.java`
- Modify: `src/main/java/com/example/databenchmark/engine/EngineRunResult.java`
- Modify: `src/main/java/com/example/databenchmark/report/WebBenchmarkReport.java`
- Modify: `src/main/java/com/example/databenchmark/report/WebBenchmarkReportMapper.java`
- Test: `src/test/java/com/example/databenchmark/report/WebBenchmarkReportMapperTest.java`

- [ ] **Step 1: Write failing mapper test for schema v3 cold/warm/hot aggregation**

Add this test to `WebBenchmarkReportMapperTest`:

```java
@Test
void schemaThreeMatrixAggregatesColdWarmHotForFourRoutesAndChoosesHotWinner() {
    BenchmarkReport report = new BenchmarkReport(
        "four-route-run",
        "smoke",
        "kpi",
        "smoke",
        "2026-06-04T00:00:00Z",
        "2026-06-04T00:00:09Z",
        10000,
        1,
        10000,
        50,
        2048,
        List.of(),
        List.of(
            query("spark", "spark_iceberg", "topn_high_load_cells", "COLD", 900, true),
            query("spark", "spark_iceberg", "topn_high_load_cells", "WARM", 700, true),
            query("spark", "spark_iceberg", "topn_high_load_cells", "HOT", 650, true),
            query("starrocks", "starrocks_internal", "topn_high_load_cells", "COLD", 300, true),
            query("starrocks", "starrocks_internal", "topn_high_load_cells", "WARM", 80, true),
            query("starrocks", "starrocks_internal", "topn_high_load_cells", "HOT", 70, true),
            query("starrocks", "starrocks_external_iceberg", "topn_high_load_cells", "COLD", 500, true),
            query("starrocks", "starrocks_external_iceberg", "topn_high_load_cells", "WARM", 90, true),
            query("starrocks", "starrocks_external_iceberg", "topn_high_load_cells", "HOT", 60, true),
            query("hive", "hive_hdfs_parquet", "topn_high_load_cells", "COLD", 1200, true),
            query("hive", "hive_hdfs_parquet", "topn_high_load_cells", "WARM", 800, true),
            query("hive", "hive_hdfs_parquet", "topn_high_load_cells", "HOT", 760, true)
        ),
        false
    );

    WebBenchmarkReport web = mapper.map(report);
    WebBenchmarkReport.PerformanceMatrixRow row = web.performanceMatrix().get(0);

    assertThat(web.schemaVersion()).isEqualTo(3);
    assertThat(row.routes().get("spark_iceberg").coldMs()).isEqualTo(900);
    assertThat(row.routes().get("spark_iceberg").warmMs()).isEqualTo(700);
    assertThat(row.routes().get("spark_iceberg").hotMs()).isEqualTo(650);
    assertThat(row.routes().get("hive_hdfs_parquet").hotMs()).isEqualTo(760);
    assertThat(row.bestRoute()).isEqualTo("starrocks_external_iceberg");
    assertThat(row.bestRouteHotMs()).isEqualTo(60);
}

private static BenchmarkReport.QuerySummary query(
    String engine,
    String tableShape,
    String queryName,
    String phase,
    double millis,
    boolean success
) {
    return new BenchmarkReport.QuerySummary(
        engine,
        tableShape,
        queryName,
        phase,
        millis,
        millis,
        millis,
        10,
        success ? 0 : 1,
        success,
        success ? "" : phase + " failed"
    );
}
```

- [ ] **Step 2: Run mapper test to verify RED**

Run:

```powershell
mvn "-Dtest=WebBenchmarkReportMapperTest#schemaThreeMatrixAggregatesColdWarmHotForFourRoutesAndChoosesHotWinner" test
```

Expected: compilation fails because `QuerySummary` has no phase parameter and `RouteResult` has no cold/warm/hot fields.

- [ ] **Step 3: Extend backend report records**

Update `BenchmarkReport.QuerySummary` to include `String phase` between `queryName` and `p50Ms`. Update existing call sites to pass `"HOT"` for legacy single-sample paths.

Update `WebBenchmarkReport.RouteResult`:

```java
public record RouteResult(
    String status,
    double coldMs,
    double warmMs,
    double hotMs,
    String coldStatus,
    String warmStatus,
    String hotStatus,
    long rows,
    String error
) {}
```

Update `WebBenchmarkReport.PerformanceMatrixRow` final field from `bestRouteP95Ms` to `bestRouteHotMs`.

- [ ] **Step 4: Implement schema v3 aggregation**

In `WebBenchmarkReportMapper`:

- Set `SCHEMA_VERSION = 3`.
- Add route key `hive_hdfs_parquet`.
- Group query samples by dataset/query.
- Fill missing route phases as status `SKIPPED` with `0` timing.
- Map phase strings case-insensitively: `COLD`, `WARM`, `HOT`.
- Route status is `SUCCESS` only if all present phases succeeded; `FAILED` if any present phase failed; `SKIPPED` if no phases are present.
- Best route uses lowest successful `hotMs`.

- [ ] **Step 5: Run mapper tests**

Run:

```powershell
mvn "-Dtest=WebBenchmarkReportMapperTest" test
```

Expected: all mapper tests pass after updating old assertions from p95 to hot where needed.

- [ ] **Step 6: Commit**

```powershell
git add src/main/java/com/example/databenchmark/report src/main/java/com/example/databenchmark/engine/EngineRunResult.java src/main/java/com/example/databenchmark/runner/RoutePhase.java src/test/java/com/example/databenchmark/report/WebBenchmarkReportMapperTest.java
git commit -m "feat: add cold warm hot report schema"
```

## Task 2: Single-Query Client APIs and Hive Client

**Files:**
- Modify: `src/main/java/com/example/databenchmark/engine/SparkIcebergClient.java`
- Modify: `src/main/java/com/example/databenchmark/engine/StarRocksClient.java`
- Create: `src/main/java/com/example/databenchmark/engine/HiveClient.java`
- Test: `src/test/java/com/example/databenchmark/engine/SparkIcebergClientTest.java`
- Test: `src/test/java/com/example/databenchmark/engine/StarRocksClientTest.java`
- Test: `src/test/java/com/example/databenchmark/engine/HiveClientTest.java`

- [ ] **Step 1: Add failing tests**

Add tests proving:

- `SparkIcebergClient.runQuery("topn_high_load_cells", RoutePhase.COLD)` returns one `EngineRunResult` with table shape `spark_iceberg` and phase `COLD`.
- `StarRocksClient.runQueryFor("starrocks_external_iceberg", "topn_high_load_cells", RoutePhase.HOT)` renders the external SQL and returns phase `HOT`.
- `HiveClient.createExternalTable(Path parquetRoot)` executes Hive DDL for `hive_hdfs_parquet`.
- `HiveClient.runQuery("topn_high_load_cells", RoutePhase.WARM)` returns table shape `hive_hdfs_parquet`.

- [ ] **Step 2: Run tests to verify RED**

Run:

```powershell
mvn "-Dtest=SparkIcebergClientTest,StarRocksClientTest,HiveClientTest" test
```

Expected: compile failures for missing APIs/classes.

- [ ] **Step 3: Implement minimal APIs**

Add `runQuery(String queryName, RoutePhase phase)` to Spark and Hive.

Add `runQueryFor(String tableShape, String queryName, RoutePhase phase)` to StarRocks.

Each method returns one `EngineRunResult` with:

- `stage = EngineStage.QUERY.name()`
- `queryName = queryName`
- `phase = phase.name()`
- `durationSeconds` from the command/JDBC result

- [ ] **Step 4: Run engine tests**

Run:

```powershell
mvn "-Dtest=SparkIcebergClientTest,StarRocksClientTest,HiveClientTest" test
```

Expected: tests pass.

- [ ] **Step 5: Commit**

```powershell
git add src/main/java/com/example/databenchmark/engine src/test/java/com/example/databenchmark/engine
git commit -m "feat: add single query APIs for benchmark routes"
```

## Task 3: Compose Service Controller

**Files:**
- Create: `src/main/java/com/example/databenchmark/runner/BenchmarkRoute.java`
- Create: `src/main/java/com/example/databenchmark/runner/ComposeServiceController.java`
- Test: `src/test/java/com/example/databenchmark/runner/ComposeServiceControllerTest.java`

- [ ] **Step 1: Write failing controller tests**

Create tests asserting route restart commands:

```java
assertThat(controller.restartCommands(BenchmarkRoute.SPARK_ICEBERG))
    .containsExactly(List.of("docker", "compose", "-f", "docker-compose.yml", "restart", "spark"));
assertThat(controller.restartCommands(BenchmarkRoute.STARROCKS_INTERNAL))
    .containsExactly(List.of("docker", "compose", "-f", "docker-compose.yml", "restart", "starrocks-fe", "starrocks-be"));
assertThat(controller.restartCommands(BenchmarkRoute.STARROCKS_EXTERNAL_ICEBERG))
    .containsExactly(List.of("docker", "compose", "-f", "docker-compose.yml", "restart", "starrocks-fe", "starrocks-be"));
assertThat(controller.restartCommands(BenchmarkRoute.HIVE_HDFS_PARQUET))
    .containsExactly(List.of("docker", "compose", "-f", "docker-compose.yml", "restart", "hive-server"));
```

- [ ] **Step 2: Run controller test to verify RED**

Run:

```powershell
mvn "-Dtest=ComposeServiceControllerTest" test
```

Expected: compile failure for missing controller.

- [ ] **Step 3: Implement controller**

Implement:

- `void restart(BenchmarkRoute route)`
- `void waitUntilReady(BenchmarkRoute route)`
- package-private `List<List<String>> restartCommands(BenchmarkRoute route)` for tests

Use existing `CommandRunner`.

- [ ] **Step 4: Run controller test**

Run:

```powershell
mvn "-Dtest=ComposeServiceControllerTest" test
```

Expected: pass.

- [ ] **Step 5: Commit**

```powershell
git add src/main/java/com/example/databenchmark/runner/BenchmarkRoute.java src/main/java/com/example/databenchmark/runner/ComposeServiceController.java src/test/java/com/example/databenchmark/runner/ComposeServiceControllerTest.java
git commit -m "feat: add compose service restart controller"
```

## Task 4: Compose Runner Cold/Warm/Hot Orchestration

**Files:**
- Modify: `src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java`
- Test: `src/test/java/com/example/databenchmark/runner/ComposeBenchmarkRunnerTest.java`

- [ ] **Step 1: Write failing orchestration test**

Add a test that uses fake clients and fake service controller. Expected call order for one query:

```text
restart SPARK_ICEBERG
ready SPARK_ICEBERG
Spark query topn_high_load_cells COLD
Spark query topn_high_load_cells WARM
Spark query topn_high_load_cells HOT
restart STARROCKS_INTERNAL
ready STARROCKS_INTERNAL
StarRocks starrocks_internal topn_high_load_cells COLD
StarRocks starrocks_internal topn_high_load_cells WARM
StarRocks starrocks_internal topn_high_load_cells HOT
restart STARROCKS_EXTERNAL_ICEBERG
ready STARROCKS_EXTERNAL_ICEBERG
StarRocks starrocks_external_iceberg topn_high_load_cells COLD
StarRocks starrocks_external_iceberg topn_high_load_cells WARM
StarRocks starrocks_external_iceberg topn_high_load_cells HOT
restart HIVE_HDFS_PARQUET
ready HIVE_HDFS_PARQUET
Hive query topn_high_load_cells COLD
Hive query topn_high_load_cells WARM
Hive query topn_high_load_cells HOT
```

- [ ] **Step 2: Run orchestration test to verify RED**

Run:

```powershell
mvn "-Dtest=ComposeBenchmarkRunnerTest#composeRunnerRunsColdWarmHotForFourRoutes" test
```

Expected: failure because runner still calls old bulk query APIs.

- [ ] **Step 3: Implement orchestration**

Update KPI compose flow:

- Load/generate data once.
- Create/load Spark Iceberg, StarRocks Internal, StarRocks External, and Hive external table once.
- Validate precision config: `coldRuns == 1`, `warmRuns == 2`, `concurrency == 1`.
- For each query in `QueryCatalog.queries()`, run each route with `COLD`, `WARM`, `HOT`.
- Continue on individual route/query failure and write DEGRADED report.

- [ ] **Step 4: Run compose runner tests**

Run:

```powershell
mvn "-Dtest=ComposeBenchmarkRunnerTest" test
```

Expected: pass.

- [ ] **Step 5: Commit**

```powershell
git add src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java src/test/java/com/example/databenchmark/runner/ComposeBenchmarkRunnerTest.java
git commit -m "feat: orchestrate cold warm hot route queries"
```

## Task 5: Frontend Schema v3 Matrix

**Files:**
- Modify: `frontend/src/types/report.ts`
- Modify: `frontend/src/components/PerformanceMatrixTable.tsx`
- Modify: `frontend/src/data/reportLoader.ts`
- Modify: `frontend/src/data/sampleReport.ts`
- Modify: `frontend/src/App.test.tsx`
- Modify: `frontend/src/data/reportLoader.test.ts`

- [ ] **Step 1: Write failing frontend tests**

Update `App.test.tsx` to expect:

- `Hive HDFS Parquet` column.
- `cold 500 ms`, `warm 90 ms`, `hot 60 ms` in a route cell.
- `best hot 60 ms`.

Update loader test to reject schema v2 and accept schema v3.

- [ ] **Step 2: Run frontend tests to verify RED**

Run:

```powershell
npm.cmd --prefix frontend test
```

Expected: fails because current UI expects schema v2 and p95 fields.

- [ ] **Step 3: Implement schema v3 frontend**

Update types and table rendering:

- `schemaVersion: 3`
- route key includes `hive_hdfs_parquet`
- route result uses `coldMs`, `warmMs`, `hotMs`
- best display uses `bestRouteHotMs`

- [ ] **Step 4: Run frontend tests and build**

Run:

```powershell
npm.cmd --prefix frontend test
npm.cmd --prefix frontend run build
```

Expected: pass.

- [ ] **Step 5: Refresh Java report UI resources**

Run the existing resource refresh commands:

```powershell
$resource = (Resolve-Path -LiteralPath src\main\resources).Path
$target = Join-Path $resource 'report-ui'
if (Test-Path -LiteralPath $target) { Remove-Item -LiteralPath $target -Recurse -Force }
Copy-Item -LiteralPath frontend\dist -Destination $target -Recurse
$index = Join-Path $target 'index.html'
$html = Get-Content -LiteralPath $index -Raw
$html = $html -replace '<div id="root"></div>', "<div id=`"root`"></div>`r`n    <!-- BENCHMARK_REPORT_DATA -->"
Set-Content -LiteralPath $index -Value $html -NoNewline -Encoding UTF8
```

- [ ] **Step 6: Commit**

```powershell
git add frontend/src src/main/resources/report-ui
git commit -m "feat: render four route cold warm hot matrix"
```

## Task 6: Docker Compose Hive Server and Resource Limits

**Files:**
- Modify: `docker-compose.yml`
- Add: `configs/benchmark-kpi-10m.yml`
- Modify: `README.md`
- Test: `src/test/java/com/example/databenchmark/ComposeTopologyTest.java`

- [ ] **Step 1: Write failing topology tests**

Extend `ComposeTopologyTest` to assert:

- service `hive-server` exists.
- service resource limits exist for `starrocks-fe`, `starrocks-be`, `spark`, `hive-metastore`, `hive-server`, `hdfs-namenode`, `hdfs-datanode`, and `benchmark-runner`.
- `configs/benchmark-smoke.yml` has `rowCap: 10000`.
- `configs/benchmark-kpi-10m.yml` has `rowCap: 10000000`.

- [ ] **Step 2: Run topology tests to verify RED**

Run:

```powershell
mvn "-Dtest=ComposeTopologyTest" test
```

Expected: fail because HiveServer2 and limits are missing.

- [ ] **Step 3: Update compose and configs**

Add `hive-server` service based on `apache/hive:4.0.0` with `SERVICE_NAME: hiveserver2`.

Add resource limits:

```yaml
deploy:
  resources:
    limits:
      cpus: "2.0"
      memory: 2g
```

Use the documented values from the SPEC for each service.

Create `configs/benchmark-kpi-10m.yml` with the same shape as smoke config and `rowCap: 10000000`.

- [ ] **Step 4: Update README**

Document:

- default validation uses `configs/benchmark-smoke.yml` with 10k rows.
- formal benchmark uses `configs/benchmark-kpi-10m.yml` with 10m rows.
- current resource allocation table.

- [ ] **Step 5: Run topology tests**

Run:

```powershell
mvn "-Dtest=ComposeTopologyTest" test
```

Expected: pass.

- [ ] **Step 6: Commit**

```powershell
git add docker-compose.yml configs/benchmark-kpi-10m.yml README.md src/test/java/com/example/databenchmark/ComposeTopologyTest.java
git commit -m "feat: add hive route infrastructure resources"
```

## Task 7: End-to-End Verification

**Files:**
- No source edits unless verification exposes a defect.

- [ ] **Step 1: Full tests**

Run:

```powershell
npm.cmd --prefix frontend test
npm.cmd --prefix frontend run build
mvn clean package
```

Expected: all pass.

- [ ] **Step 2: Rebuild clean compose services**

Run:

```powershell
docker compose -f docker-compose.yml down --remove-orphans
docker compose -f docker-compose.yml up -d hdfs-namenode hdfs-datanode hdfs-init hive-metastore hive-server spark starrocks-fe starrocks-be
```

Expected: services start and `hdfs-init` completes.

- [ ] **Step 3: Run default 10k validation benchmark**

Run:

```powershell
java -jar target\data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs\benchmark-smoke.yml --run-id four-engine-validation-10k
```

Expected: report generated under `reports/runs/four-engine-validation-10k/index.html`.

- [ ] **Step 4: Validate embedded report**

Parse `index.html` and assert:

- `schemaVersion` is `3`.
- `performanceMatrix` contains four routes.
- At least one matrix row contains cold/warm/hot values for all four routes.
- `report.json` does not exist.
- HTML does not contain `fetch(`.

- [ ] **Step 5: Browser validation**

Use Chrome headless CDP to verify:

- title is `Data Benchmark Report`.
- body includes `Hive HDFS Parquet`.
- body includes `cold`, `warm`, `hot`.
- no console/log errors.

- [ ] **Step 6: Commit any verification fixes**

If any defects were fixed:

```powershell
git add <fixed-files>
git commit -m "fix: complete four route benchmark verification"
```

## Self-Review

- Spec coverage: plan covers four routes, cold/warm/hot, route restarts, HiveServer2, 10k default dataset, 10m formal config, resource limits, schema v3 report, and static report constraints.
- Placeholder scan: no TBD/TODO placeholders are present.
- Type consistency: route key is `hive_hdfs_parquet`; phase names are `COLD`, `WARM`, `HOT`; report fields are `coldMs`, `warmMs`, `hotMs`, `bestRouteHotMs`.

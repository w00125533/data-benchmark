# StarRocks Parquet Smoke Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Load KPI StarRocks internal from the generated HDFS Parquet dataset, validate every route has the expected row count in smoke, and add a native Spark SQL Parquet benchmark route.

**Architecture:** HDFS Parquet becomes the single KPI generated artifact. Spark native, Spark Iceberg, Hive external, StarRocks external, and StarRocks internal all prepare from that artifact and then run `COUNT(*)` validation before query phases. KPI StarRocks internal uses Broker Load instead of CSV Stream Load.

**Tech Stack:** Java 17, Maven/JUnit 5/AssertJ, Docker Compose, Spark SQL CLI, Hive Beeline, StarRocks JDBC/Broker Load, HDFS Parquet.

---

## Current Context

- Spec: `docs/superpowers/specs/2026-06-06-starrocks-hdfs-parquet-smoke-design.md`
- Current runner: `src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java`
- Current route enum: `src/main/java/com/example/databenchmark/runner/BenchmarkRoute.java`
- Spark client: `src/main/java/com/example/databenchmark/engine/SparkIcebergClient.java`
- StarRocks client: `src/main/java/com/example/databenchmark/engine/StarRocksClient.java`
- SQL templates: `src/main/java/com/example/databenchmark/engine/SqlTemplates.java`
- Query rendering: `src/main/java/com/example/databenchmark/engine/SqlRenderer.java`
- Schema table shapes: `src/main/java/com/example/databenchmark/schema/KpiSchema.java`
- Main tests: `src/test/java/com/example/databenchmark/runner/ComposeBenchmarkRunnerTest.java`, `src/test/java/com/example/databenchmark/engine/StarRocksClientTest.java`, `src/test/java/com/example/databenchmark/engine/SparkIcebergClientTest.java`, `src/test/java/com/example/databenchmark/query/QueryCatalogTest.java`
- Important existing worktree note: there are unrelated modified/untracked files. Do not revert them. Commit only files changed by the task.

## File Structure

- Create `src/main/java/com/example/databenchmark/engine/StarRocksBrokerLoad.java`
  - Value objects for Broker Load request and status.
  - Keeps label/path/state parsing logic out of `StarRocksClient`.
- Modify `src/main/java/com/example/databenchmark/engine/SqlTemplates.java`
  - Add StarRocks Broker Load SQL.
  - Add Spark native Parquet table SQL.
  - Add reusable `countSql(String tableShape)` if useful.
- Modify `src/main/java/com/example/databenchmark/engine/StarRocksClient.java`
  - Replace KPI `loadInternal(Path csv, ...)` implementation with HDFS Parquet Broker Load.
  - Keep TPC-H Stream Load behavior unchanged.
  - Add `validateCount(String tableShape, long expectedRows)` or equivalent.
- Modify `src/main/java/com/example/databenchmark/engine/SparkIcebergClient.java`
  - Add Spark native Parquet load/query methods or create a small native-client wrapper in the same file if less invasive.
  - Add count validation helper for Spark table shapes.
- Modify `src/main/java/com/example/databenchmark/schema/KpiSchema.java`
  - Add `spark_native_parquet`.
- Modify `src/main/java/com/example/databenchmark/query/QueryCatalog.java`
  - Add `spark_native_parquet` engine key.
- Modify `src/main/java/com/example/databenchmark/engine/SqlRenderer.java`
  - Ensure `spark_native_parquet` renders with the direct Parquet table.
- Modify `src/main/java/com/example/databenchmark/runner/BenchmarkRoute.java`
  - Add `SPARK_NATIVE_PARQUET`.
- Modify `src/main/java/com/example/databenchmark/runner/ComposeServiceController.java`
  - Reuse Spark readiness/restart for `SPARK_NATIVE_PARQUET`.
- Modify `src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java`
  - Remove KPI CSV exporter dependency from the KPI path.
  - Load/validate all KPI table shapes.
  - Skip failed route queries after validation failure.
- Modify `docker-compose.yml`
  - Remove KPI-only `STARROCKS_STREAM_LOAD_URL` if no main code path uses it.
- Remove or stop using `src/main/java/com/example/databenchmark/engine/StarRocksCsvExporter.java` for KPI.
  - Do not remove TPC-H CSV exporter.
- Update tests under `src/test/java`.

---

### Task 1: StarRocks Broker Load From HDFS Parquet

**Files:**
- Create: `src/main/java/com/example/databenchmark/engine/StarRocksBrokerLoad.java`
- Modify: `src/main/java/com/example/databenchmark/engine/SqlTemplates.java`
- Modify: `src/main/java/com/example/databenchmark/engine/StarRocksClient.java`
- Test: `src/test/java/com/example/databenchmark/engine/StarRocksClientTest.java`

- [ ] **Step 1: Add failing tests for Broker Load SQL and successful load polling**

Append tests to `StarRocksClientTest`.

```java
@Test
void loadInternalUsesBrokerLoadFromHdfsParquet() {
    FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
    jdbc.loadStates.add(new StarRocksBrokerLoad.LoadState("FINISHED", 100L, ""));

    EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
        new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
    )).loadInternal(Path.of("/benchmark/kpi-smoke/generated"), "run-1", "smoke", 100L, 2048L);

    assertThat(result.success()).isTrue();
    assertThat(result.rows()).isEqualTo(100L);
    assertThat(result.bytes()).isEqualTo(2048L);
    assertThat(jdbc.sql()).anySatisfy(sql -> assertThat(sql)
        .contains("LOAD LABEL sr_internal.")
        .contains("DATA INFILE(\"hdfs://hdfs-namenode:8020/benchmark/kpi-smoke/generated/*.parquet\")")
        .contains("FORMAT AS \"parquet\"")
        .contains("INTO TABLE cell_kpi_1min"));
    assertThat(jdbc.sql()).noneMatch(sql -> sql.contains("_stream_load"));
}

@Test
void loadInternalFailsWhenBrokerLoadIsCancelled() {
    FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
    jdbc.loadStates.add(new StarRocksBrokerLoad.LoadState("CANCELLED", 0L, "bad parquet"));

    EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
        new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
    )).loadInternal(Path.of("/benchmark/kpi-smoke/generated"), "run-1", "smoke", 100L, 2048L);

    assertThat(result.success()).isFalse();
    assertThat(result.error()).contains("broker_load failed");
    assertThat(result.error()).contains("CANCELLED");
    assertThat(result.error()).contains("bad parquet");
}
```

Extend `FakeJdbcExecutor` in the same test file:

```java
private final List<StarRocksBrokerLoad.LoadState> loadStates = new ArrayList<>();

@Override
public StarRocksBrokerLoad.LoadState latestLoadState(String database, String label) throws SQLException {
    if (loadStates.isEmpty()) {
        return new StarRocksBrokerLoad.LoadState("UNKNOWN", 0L, "");
    }
    return loadStates.remove(0);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
mvn "-Dtest=StarRocksClientTest#loadInternalUsesBrokerLoadFromHdfsParquet,StarRocksClientTest#loadInternalFailsWhenBrokerLoadIsCancelled" test
```

Expected: compile failure or assertion failure because `loadInternal(Path,String,String,long,long)` and `StarRocksBrokerLoad` do not exist.

- [ ] **Step 3: Implement Broker Load support**

Create `StarRocksBrokerLoad.java`:

```java
package com.example.databenchmark.engine;

import java.time.Duration;

public final class StarRocksBrokerLoad {
    public static final Duration DEFAULT_TIMEOUT = Duration.ofHours(6);

    private StarRocksBrokerLoad() {}

    public static String label(String runId) {
        String sanitized = runId == null ? "run" : runId.replaceAll("[^A-Za-z0-9_]", "_");
        return "kpi_" + sanitized + "_" + System.currentTimeMillis();
    }

    public static String normalizeHdfsParquetGlob(String outputPath) {
        String normalized = outputPath.replace('\\', '/');
        if (normalized.startsWith("hdfs://")) {
            return normalized.endsWith("/") ? normalized + "*.parquet" : normalized + "/*.parquet";
        }
        if (normalized.startsWith("/")) {
            return "hdfs://hdfs-namenode:8020" + (normalized.endsWith("/") ? normalized + "*.parquet" : normalized + "/*.parquet");
        }
        return "hdfs://hdfs-namenode:8020/" + (normalized.endsWith("/") ? normalized + "*.parquet" : normalized + "/*.parquet");
    }

    public record LoadState(String state, long sinkRows, String errorMessage) {
        public boolean finished() {
            return "FINISHED".equalsIgnoreCase(state);
        }

        public boolean cancelled() {
            return "CANCELLED".equalsIgnoreCase(state);
        }
    }
}
```

Modify `SqlTemplates.java`:

```java
public static String starRocksTruncateInternalTable() {
    return "TRUNCATE TABLE sr_internal.cell_kpi_1min;";
}

public static String starRocksBrokerLoadFromParquet(String label, String parquetGlob) {
    return """
        LOAD LABEL sr_internal.%s
        (
            DATA INFILE("%s")
            INTO TABLE cell_kpi_1min
            FORMAT AS "parquet"
            (%s)
        )
        WITH BROKER
        (
            "hadoop.security.authentication" = "simple",
            "username" = "root",
            "password" = ""
        )
        PROPERTIES
        (
            "timeout" = "21600",
            "max_filter_ratio" = "0"
        );
        """.formatted(label, escapeSqlLiteral(parquetGlob), kpiColumnNames());
}
```

Modify `JdbcExecutor.java` to support load-state polling:

```java
public StarRocksBrokerLoad.LoadState latestLoadState(String database, String label) throws SQLException {
    String sql = """
        SELECT STATE, IFNULL(LOADED_ROWS, IFNULL(SINK_ROWS, 0)) AS SINK_ROWS, IFNULL(ERROR_MSG, '') AS ERROR_MSG
        FROM information_schema.loads
        WHERE LABEL = '%s'
        ORDER BY CREATE_TIME DESC
        LIMIT 1
        """.formatted(label.replace("'", "''"));
    try (Connection connection = openConnection();
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
        if (!resultSet.next()) {
            return new StarRocksBrokerLoad.LoadState("UNKNOWN", 0L, "");
        }
        return new StarRocksBrokerLoad.LoadState(
            resultSet.getString("STATE"),
            resultSet.getLong("SINK_ROWS"),
            resultSet.getString("ERROR_MSG")
        );
    }
}
```

If StarRocks 3.3 does not expose `LOADED_ROWS`, use `SINK_ROWS`; keep the SQL compatible by querying `SHOW LOAD WHERE LABEL = '<label>'` if `information_schema.loads` fails. Preserve one public `latestLoadState` method.

Modify `StarRocksClient.java`:

```java
public EngineRunResult loadInternal(Path parquetRoot, String runId, String profile, long expectedRows, long bytesWritten) {
    double durationSeconds = 0.0;
    String label = StarRocksBrokerLoad.label(runId);
    String parquetGlob = StarRocksBrokerLoad.normalizeHdfsParquetGlob(parquetRoot.toString());
    try {
        durationSeconds += jdbcExecutor.execute(SqlTemplates.starRocksCreateInternalTable()).durationSeconds();
        durationSeconds += jdbcExecutor.execute(SqlTemplates.starRocksTruncateInternalTable()).durationSeconds();
        durationSeconds += jdbcExecutor.execute(SqlTemplates.starRocksBrokerLoadFromParquet(label, parquetGlob)).durationSeconds();
        StarRocksBrokerLoad.LoadState state = waitForBrokerLoad(label);
        if (!state.finished()) {
            return failed("starrocks_internal", EngineStage.STARROCKS_INTERNAL_LOAD.name(), null, durationSeconds,
                "broker_load failed label=%s path=%s state=%s error=%s".formatted(label, parquetGlob, state.state(), state.errorMessage()));
        }
        return new EngineRunResult("starrocks", "starrocks_internal", EngineStage.STARROCKS_INTERNAL_LOAD.name(),
            null, state.sinkRows() > 0 ? state.sinkRows() : expectedRows, bytesWritten, durationSeconds, true, "");
    } catch (SQLException e) {
        return failed("starrocks_internal", EngineStage.STARROCKS_INTERNAL_LOAD.name(), null, durationSeconds,
            "broker_load failed label=%s path=%s: %s".formatted(label, parquetGlob, e.getMessage()));
    }
}

public EngineRunResult loadInternal(Path csv, String runId, String profile) {
    return loadInternal(csv, runId, profile, 0L, 0L);
}

private StarRocksBrokerLoad.LoadState waitForBrokerLoad(String label) throws SQLException {
    long deadline = System.nanoTime() + StarRocksBrokerLoad.DEFAULT_TIMEOUT.toNanos();
    StarRocksBrokerLoad.LoadState last = new StarRocksBrokerLoad.LoadState("UNKNOWN", 0L, "");
    while (System.nanoTime() < deadline) {
        last = jdbcExecutor.latestLoadState("sr_internal", label);
        if (last.finished() || last.cancelled()) {
            return last;
        }
        try {
            Thread.sleep(2_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new StarRocksBrokerLoad.LoadState("INTERRUPTED", 0L, e.getMessage());
        }
    }
    return new StarRocksBrokerLoad.LoadState("TIMEOUT", last.sinkRows(), last.errorMessage());
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
mvn "-Dtest=StarRocksClientTest#loadInternalUsesBrokerLoadFromHdfsParquet,StarRocksClientTest#loadInternalFailsWhenBrokerLoadIsCancelled" test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add src/main/java/com/example/databenchmark/engine/StarRocksBrokerLoad.java src/main/java/com/example/databenchmark/engine/SqlTemplates.java src/main/java/com/example/databenchmark/engine/JdbcExecutor.java src/main/java/com/example/databenchmark/engine/StarRocksClient.java src/test/java/com/example/databenchmark/engine/StarRocksClientTest.java
git commit -m "feat: load starrocks internal from hdfs parquet"
```

---

### Task 2: Spark Native Parquet Route

**Files:**
- Modify: `src/main/java/com/example/databenchmark/schema/KpiSchema.java`
- Modify: `src/main/java/com/example/databenchmark/query/QueryCatalog.java`
- Modify: `src/main/java/com/example/databenchmark/engine/SqlTemplates.java`
- Modify: `src/main/java/com/example/databenchmark/engine/SqlRenderer.java`
- Modify: `src/main/java/com/example/databenchmark/engine/SparkIcebergClient.java`
- Test: `src/test/java/com/example/databenchmark/query/QueryCatalogTest.java`
- Test: `src/test/java/com/example/databenchmark/engine/SparkIcebergClientTest.java`

- [ ] **Step 1: Add failing tests for native route rendering and load**

In `QueryCatalogTest.enginesUseSchemaTableShapesInRequiredOrder`, update expected engines to include:

```java
new BenchmarkEngine("spark_native_parquet", "spark_native_parquet", "spark_catalog.benchmark_native.cell_kpi_1min")
```

Add:

```java
@Test
void rendersSparkNativeParquetQueriesAgainstNativeTable() {
    BenchmarkEngine engine = QueryCatalog.engines().stream()
        .filter(candidate -> candidate.key().equals("spark_native_parquet"))
        .findFirst()
        .orElseThrow();

    String rendered = QueryCatalog.render("topn_high_load_cells", engine);

    assertThat(rendered).contains("spark_catalog.benchmark_native.cell_kpi_1min");
    assertThat(rendered).doesNotContain("iceberg_catalog");
}
```

In `SparkIcebergClientTest`, add:

```java
@Test
void loadNativeParquetCreatesSparkTableOverGeneratedHdfsPath() {
    FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "ok", "", 1.25));
    Path workspace = tempDir.resolve("workspace");

    EngineRunResult result = new SparkIcebergClient(runner, workspace, Duration.ofMinutes(1))
        .loadNativeParquet(new DatasetResult(Path.of("/benchmark/kpi-smoke/generated"), List.of(), 100, 2048), "run-1", "smoke");

    assertThat(result.success()).isTrue();
    assertThat(result.tableShape()).isEqualTo("spark_native_parquet");
    assertThat(runner.commands().get(0).get(runner.commands().get(0).size() - 1))
        .contains("CREATE DATABASE IF NOT EXISTS spark_catalog.benchmark_native")
        .contains("USING parquet")
        .contains("LOCATION 'hdfs://hdfs-namenode:8020/benchmark/kpi-smoke/generated'");
}

@Test
void runNativeQueryUsesSparkNativeParquetTableShape() {
    FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(
        List.of(), 0, "Time taken: 0.1 seconds, Fetched 100 row(s)", "", 0.1
    ));

    EngineRunResult result = new SparkIcebergClient(runner).runNativeQuery("topn_high_load_cells", RoutePhase.HOT);

    assertThat(result.success()).isTrue();
    assertThat(result.tableShape()).isEqualTo("spark_native_parquet");
    assertThat(result.rows()).isEqualTo(100L);
    assertThat(runner.commands().get(0).get(runner.commands().get(0).size() - 1))
        .contains("spark_catalog.benchmark_native.cell_kpi_1min");
}
```

- [ ] **Step 2: Run tests to verify they fail**

```powershell
mvn "-Dtest=QueryCatalogTest#enginesUseSchemaTableShapesInRequiredOrder,QueryCatalogTest#rendersSparkNativeParquetQueriesAgainstNativeTable,SparkIcebergClientTest#loadNativeParquetCreatesSparkTableOverGeneratedHdfsPath,SparkIcebergClientTest#runNativeQueryUsesSparkNativeParquetTableShape" test
```

Expected: FAIL because native route does not exist.

- [ ] **Step 3: Implement native route**

Modify `KpiSchema.tableShapesInSpecOrder()`:

```java
tableShapes.put("spark_native_parquet", "spark_catalog.benchmark_native.cell_kpi_1min");
tableShapes.put("spark_iceberg", "iceberg_catalog.iceberg_db.cell_kpi_1min");
```

Modify `QueryCatalog.ENGINE_KEYS`:

```java
private static final List<String> ENGINE_KEYS = List.of(
    "spark_native_parquet",
    "spark_iceberg",
    "starrocks_internal",
    "starrocks_external_iceberg"
);
```

Modify `SqlTemplates.java`:

```java
public static String sparkCreateNativeParquetTable(String parquetRoot) {
    return """
        CREATE DATABASE IF NOT EXISTS spark_catalog.benchmark_native;

        DROP TABLE IF EXISTS spark_catalog.benchmark_native.cell_kpi_1min;

        CREATE TABLE spark_catalog.benchmark_native.cell_kpi_1min (
        %s
        )
        USING parquet
        LOCATION '%s';
        """.formatted(sparkColumns(), escapeSqlLiteral(parquetRoot));
}
```

Modify `SparkIcebergClient.java`:

```java
public EngineRunResult loadNativeParquet(DatasetResult dataset, String runId, String profile) {
    String workspacePath;
    try {
        workspacePath = toWorkspacePath(dataset.outputPath());
    } catch (IllegalArgumentException e) {
        return failed("spark_native_parquet", "SPARK_NATIVE_PARQUET_LOAD", null, e.getMessage());
    }
    CommandResult command = runSparkSql(SqlTemplates.sparkCreateNativeParquetTable(workspacePath));
    if (command.exitCode() != 0) {
        return failed("spark_native_parquet", "SPARK_NATIVE_PARQUET_LOAD", null, command);
    }
    return new EngineRunResult("spark", "spark_native_parquet", "SPARK_NATIVE_PARQUET_LOAD",
        null, dataset.rows(), dataset.bytesWritten(), command.durationSeconds(), true, "");
}

public EngineRunResult runNativeQuery(String queryName, RoutePhase phase) {
    CommandResult command = runSparkSql(SqlRenderer.render(queryName, "spark_native_parquet"));
    if (command.exitCode() == 0) {
        return new EngineRunResult("spark", "spark_native_parquet", EngineStage.QUERY.name(),
            queryName, phase.name(), CliQueryRows.spark(command), 0, command.durationSeconds(), true, "");
    }
    return failed("spark_native_parquet", EngineStage.QUERY.name(), queryName, phase, command);
}
```

- [ ] **Step 4: Run tests to verify they pass**

```powershell
mvn "-Dtest=QueryCatalogTest,SparkIcebergClientTest" test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add src/main/java/com/example/databenchmark/schema/KpiSchema.java src/main/java/com/example/databenchmark/query/QueryCatalog.java src/main/java/com/example/databenchmark/engine/SqlTemplates.java src/main/java/com/example/databenchmark/engine/SqlRenderer.java src/main/java/com/example/databenchmark/engine/SparkIcebergClient.java src/test/java/com/example/databenchmark/query/QueryCatalogTest.java src/test/java/com/example/databenchmark/engine/SparkIcebergClientTest.java
git commit -m "feat: add spark native parquet route"
```

---

### Task 3: Runner Route Validation and Query Skipping

**Files:**
- Modify: `src/main/java/com/example/databenchmark/runner/BenchmarkRoute.java`
- Modify: `src/main/java/com/example/databenchmark/runner/ComposeServiceController.java`
- Modify: `src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java`
- Modify: `src/main/java/com/example/databenchmark/engine/StarRocksClient.java`
- Modify: `src/main/java/com/example/databenchmark/engine/HiveClient.java`
- Modify: `src/main/java/com/example/databenchmark/engine/SparkIcebergClient.java`
- Test: `src/test/java/com/example/databenchmark/runner/ComposeBenchmarkRunnerTest.java`
- Test: engine client tests as needed.

- [ ] **Step 1: Add failing runner tests for validation**

In `ComposeBenchmarkRunnerTest`, update `FakeSparkClient` interface implementation after Task 2 with native methods, then add:

```java
@Test
void composeRunnerValidatesEveryKpiRouteBeforeQueries() throws Exception {
    List<String> calls = new ArrayList<>();
    DatasetResult dataset = new DatasetResult(Path.of("/benchmark/kpi-smoke/generated"), List.of(), 5L, 123L);
    CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/compose-test/index.html"));

    ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
        config -> dataset,
        failingCsvExporter(),
        failingTpchGenerator(),
        failingTpchCsvExport(),
        new FakeSparkClient(calls),
        new FakeStarRocksClient(calls, true),
        new FakeHdfsDatasetPublisher(calls, true),
        new FakeHiveClient(calls),
        new FakeServiceController(calls),
        reportWriter,
        new CapturingMetricsRecorder(calls, "smoke", "kpi", "smoke")
    );

    ComposeBenchmarkRunner.ComposeRunResult result = runner.run(
        BenchmarkConfig.defaultSmoke().withOverrides(null, null, null, "hdfs://hdfs-namenode:8020/benchmark/kpi-smoke/generated", null),
        tempDir.resolve("reports"),
        "compose-test"
    );

    assertThat(result.success()).isTrue();
    assertThat(calls).containsSubsequence(
        "Spark native load",
        "Spark load",
        "StarRocks internal load from parquet",
        "StarRocks external refresh",
        "Hive external table /benchmark/kpi-smoke/generated",
        "validate spark_native_parquet 5",
        "validate spark_iceberg 5",
        "validate starrocks_internal 5",
        "validate starrocks_external_iceberg 5",
        "validate hive_hdfs_parquet 5"
    );
    assertThat(reportWriter.report.loadSummaries())
        .extracting(BenchmarkReport.LoadSummary::stage)
        .contains(
            "SPARK_NATIVE_PARQUET_VALIDATE",
            "SPARK_ICEBERG_VALIDATE",
            "STARROCKS_INTERNAL_VALIDATE",
            "STARROCKS_EXTERNAL_VALIDATE",
            "HIVE_HDFS_PARQUET_VALIDATE"
        );
}

@Test
void composeRunnerSkipsOnlyRouteQueriesWhenValidationFails() throws Exception {
    List<String> calls = new ArrayList<>();
    String queryName = QueryCatalog.queries().get(0).name();
    DatasetResult dataset = new DatasetResult(Path.of("/benchmark/kpi-smoke/generated"), List.of(), 5L, 123L);
    CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/compose-test/index.html"));
    FakeStarRocksClient starRocks = new FakeStarRocksClient(calls, true);
    starRocks.validationFailures.add("starrocks_internal");

    ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
        config -> dataset,
        failingCsvExporter(),
        failingTpchGenerator(),
        failingTpchCsvExport(),
        new FakeSparkClient(calls),
        starRocks,
        new FakeHdfsDatasetPublisher(calls, true),
        new FakeHiveClient(calls),
        new FakeServiceController(calls),
        reportWriter,
        new CapturingMetricsRecorder(calls, "smoke", "kpi", "smoke")
    );

    ComposeBenchmarkRunner.ComposeRunResult result = runner.run(BenchmarkConfig.defaultSmoke(), tempDir.resolve("reports"), "compose-test");

    assertThat(result.success()).isFalse();
    assertThat(reportWriter.report.status()).isEqualTo("DEGRADED");
    assertThat(calls).doesNotContain("StarRocks starrocks_internal " + queryName + " COLD");
    assertThat(calls).contains("StarRocks starrocks_external_iceberg " + queryName + " COLD");
}
```

- [ ] **Step 2: Run tests to verify they fail**

```powershell
mvn "-Dtest=ComposeBenchmarkRunnerTest#composeRunnerValidatesEveryKpiRouteBeforeQueries,ComposeBenchmarkRunnerTest#composeRunnerSkipsOnlyRouteQueriesWhenValidationFails" test
```

Expected: FAIL because validation facade methods and native route are not wired.

- [ ] **Step 3: Extend interfaces and implement validation**

Modify `ComposeBenchmarkRunner.SparkClient`:

```java
EngineRunResult loadNativeParquet(DatasetResult dataset, String runId, String profile);
EngineRunResult validateCount(String tableShape, long expectedRows);
EngineRunResult runNativeQuery(String queryName, RoutePhase phase);
```

Modify `ComposeBenchmarkRunner.StarRocksClientFacade`:

```java
EngineRunResult loadInternal(Path parquetRoot, String runId, String profile, long expectedRows, long bytesWritten);
EngineRunResult validateCount(String tableShape, long expectedRows);
```

Modify `ComposeBenchmarkRunner.HiveClientFacade`:

```java
EngineRunResult validateCount(long expectedRows);
```

Add validation helpers in clients:

```java
// SparkIcebergClient
public EngineRunResult validateCount(String tableShape, long expectedRows) {
    String table = KpiSchema.tableShapes().get(tableShape);
    if (table == null) {
        return failed(tableShape, validationStage(tableShape), null, "Unknown table shape: " + tableShape);
    }
    CommandResult command = runSparkSql("SELECT COUNT(*) FROM " + table);
    long rows = CliQueryRows.spark(command);
    return validationResult("spark", tableShape, validationStage(tableShape), expectedRows, rows, command.durationSeconds(), command.exitCode() == 0, commandError(command));
}
```

```java
// StarRocksClient
public EngineRunResult validateCount(String tableShape, long expectedRows) {
    try {
        JdbcExecutionResult result = jdbcExecutor.query("SELECT COUNT(*) FROM " + KpiSchema.tableShapes().get(tableShape));
        return validationResult("starrocks", tableShape, validationStage(tableShape), expectedRows, result.rows(), result.durationSeconds(), true, "");
    } catch (SQLException | IllegalArgumentException e) {
        return validationResult("starrocks", tableShape, validationStage(tableShape), expectedRows, 0L, 0.0, false, e.getMessage());
    }
}
```

For JDBC count, if current `JdbcExecutor.query` returns the number of result rows rather than the first scalar, add `queryLong(String sql)` to read the first column of the first row. Use that in count validation. Test this if changed.

```java
// HiveClient
public EngineRunResult validateCount(long expectedRows) {
    CommandResult command = runHiveSql("SELECT COUNT(*) FROM hive_hdfs_parquet.cell_kpi_1min");
    long actualRows = CliQueryRows.firstNumber(command).orElse(0L);
    return validationResult("hive", "hive_hdfs_parquet", "HIVE_HDFS_PARQUET_VALIDATE", expectedRows, actualRows, command.durationSeconds(), command.exitCode() == 0, commandError(command));
}
```

Add a small private validation result helper in each client or centralize in `EngineRunResult` construction:

```java
private static EngineRunResult validationResult(String engine, String tableShape, String stage, long expected, long actual, double seconds, boolean commandSuccess, String error) {
    boolean success = commandSuccess && expected == actual;
    String finalError = success ? "" : "row count mismatch for %s: expected=%d actual=%d%s".formatted(
        tableShape, expected, actual, error == null || error.isBlank() ? "" : " error=" + error
    );
    return new EngineRunResult(engine, tableShape, stage, null, actual, 0L, seconds, success, finalError);
}
```

- [ ] **Step 4: Wire validation in `ComposeBenchmarkRunner`**

Replace KPI load flow with:

```java
loadResults.add(sparkClient.loadNativeParquet(dataset, actualRunId, config.profile()));
loadResults.add(loadSpark(dataset, actualRunId, config.profile()));
loadResults.add(loadStarRocksInternal(dataset.outputPath(), dataset.rows(), dataset.bytesWritten(), actualRunId, config.profile()));
loadResults.add(refreshStarRocksExternal(actualRunId, config.profile()));
String hiveRoot = hiveParquetRoot(config);
if (!hdfsOutput) {
    loadResults.add(publishHiveDataset(dataset, hiveRoot));
}
loadResults.add(createHiveExternalTable(hiveRoot));

Map<BenchmarkRoute, String> routeFailures = validateRoutes(dataset.rows(), loadResults);
queryResults.addAll(runKpiRouteQueries(config, routeFailures));
```

Implement:

```java
private Map<BenchmarkRoute, String> validateRoutes(long expectedRows, List<EngineRunResult> loadResults) {
    Map<BenchmarkRoute, String> failures = new EnumMap<>(BenchmarkRoute.class);
    recordValidation(BenchmarkRoute.SPARK_NATIVE_PARQUET, sparkClient.validateCount("spark_native_parquet", expectedRows), loadResults, failures);
    recordValidation(BenchmarkRoute.SPARK_ICEBERG, sparkClient.validateCount("spark_iceberg", expectedRows), loadResults, failures);
    recordValidation(BenchmarkRoute.STARROCKS_INTERNAL, starRocksClient.validateCount("starrocks_internal", expectedRows), loadResults, failures);
    recordValidation(BenchmarkRoute.STARROCKS_EXTERNAL_ICEBERG, starRocksClient.validateCount("starrocks_external_iceberg", expectedRows), loadResults, failures);
    recordValidation(BenchmarkRoute.HIVE_HDFS_PARQUET, hiveClient.validateCount(expectedRows), loadResults, failures);
    return failures;
}

private void recordValidation(BenchmarkRoute route, EngineRunResult result, List<EngineRunResult> loadResults, Map<BenchmarkRoute, String> failures) {
    loadResults.add(result);
    if (!result.success()) {
        failures.put(route, result.error());
    }
}
```

Modify `runKpiRouteQueries` to accept `Map<BenchmarkRoute, String>` instead of only Hive failure. If `routeFailures.containsKey(route)`, add failed phase results for that route and do not restart/query it.

Update route switches:

```java
case SPARK_NATIVE_PARQUET -> sparkClient.runNativeQuery(queryName, phase);
```

Update `routeEngine` and `routeTableShape`.

- [ ] **Step 5: Run tests to verify they pass**

```powershell
mvn "-Dtest=ComposeBenchmarkRunnerTest,ComposeServiceControllerTest,HiveClientTest,SparkIcebergClientTest,StarRocksClientTest" test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```powershell
git add src/main/java/com/example/databenchmark/runner/BenchmarkRoute.java src/main/java/com/example/databenchmark/runner/ComposeServiceController.java src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java src/main/java/com/example/databenchmark/engine/StarRocksClient.java src/main/java/com/example/databenchmark/engine/HiveClient.java src/main/java/com/example/databenchmark/engine/SparkIcebergClient.java src/test/java/com/example/databenchmark/runner/ComposeBenchmarkRunnerTest.java src/test/java/com/example/databenchmark/runner/ComposeServiceControllerTest.java src/test/java/com/example/databenchmark/engine/HiveClientTest.java src/test/java/com/example/databenchmark/engine/SparkIcebergClientTest.java src/test/java/com/example/databenchmark/engine/StarRocksClientTest.java
git commit -m "feat: validate kpi route row counts"
```

---

### Task 4: Remove KPI CSV Export Path

**Files:**
- Modify: `src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java`
- Modify: `docker-compose.yml`
- Modify: `src/test/java/com/example/databenchmark/runner/ComposeBenchmarkRunnerTest.java`
- Modify: `src/test/java/com/example/databenchmark/ComposeTopologyTest.java`
- Optionally delete: `src/main/java/com/example/databenchmark/engine/StarRocksCsvExporter.java`
- Optionally delete: `src/test/java/com/example/databenchmark/engine/StarRocksCsvExporterTest.java`

- [ ] **Step 1: Add failing test proving KPI CSV exporter is not used**

Update existing happy-path test in `ComposeBenchmarkRunnerTest` so it passes `failingCsvExporter()` and asserts calls do not include `"export CSV"`.

Expected call subsequence should begin:

```java
assertThat(calls).containsSubsequence(
    "start metrics",
    "generate dataset",
    "Spark native load",
    "Spark load",
    "ready STARROCKS_INTERNAL",
    "StarRocks internal load from parquet"
);
assertThat(calls).doesNotContain("export CSV");
```

Update `ComposeTopologyTest` to remove the expectation:

```java
.doesNotContainKey("STARROCKS_STREAM_LOAD_URL")
```

- [ ] **Step 2: Run tests to verify they fail**

```powershell
mvn "-Dtest=ComposeBenchmarkRunnerTest,ComposeTopologyTest" test
```

Expected: FAIL until runner constructor and compose env are cleaned.

- [ ] **Step 3: Remove KPI CSV dependencies**

In `ComposeBenchmarkRunner`:

- Remove field `private final CsvExporter csvExporter;`
- Remove constructor parameter `CsvExporter csvExporter` from KPI path constructors. If keeping constructor compatibility is easier for existing tests, keep the parameter but do not store or call it. Prefer removing only if all tests can be updated cleanly.
- Delete local `Path csvPath`.
- Delete the block:

```java
if (!hdfsOutput) {
    try {
        csvPath = csvExporter.export(dataset, starRocksCsvOutput(config, dataset));
    } catch (Exception exception) {
        loadResults.add(failed("compose", "starrocks_csv", "compose_prepare", exception));
    }
}
```

- Delete `starRocksCsvOutput`.
- Delete `interface CsvExporter` only if no tests or constructors still need it.

In `docker-compose.yml`, remove:

```yaml
STARROCKS_STREAM_LOAD_URL: "http://starrocks-be:8040/api/sr_internal/cell_kpi_1min/_stream_load"
```

Do not delete `StarRocksStreamLoadClient` if TPC-H still uses it.

- [ ] **Step 4: Run tests to verify they pass**

```powershell
mvn "-Dtest=ComposeBenchmarkRunnerTest,ComposeTopologyTest,StarRocksCsvExporterTest,StarRocksClientTest" test
```

Expected: PASS. If `StarRocksCsvExporter` is deleted, remove `StarRocksCsvExporterTest` and do not include it in this command.

- [ ] **Step 5: Commit**

```powershell
git add docker-compose.yml src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java src/test/java/com/example/databenchmark/runner/ComposeBenchmarkRunnerTest.java src/test/java/com/example/databenchmark/ComposeTopologyTest.java
git add -u src/main/java/com/example/databenchmark/engine/StarRocksCsvExporter.java src/test/java/com/example/databenchmark/engine/StarRocksCsvExporterTest.java
git commit -m "refactor: remove kpi csv starrocks load path"
```

---

### Task 5: Full Verification and Report Checks

**Files:**
- Modify tests only if prior tasks reveal a small gap.
- No planned production file changes.

- [ ] **Step 1: Run full unit suite**

```powershell
mvn test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 2: Run compose config validation**

```powershell
docker compose -f docker-compose.yml config
```

Expected: command exits 0 and `benchmark-runner` no longer exposes `STARROCKS_STREAM_LOAD_URL`.

- [ ] **Step 3: Build jar**

```powershell
mvn -DskipTests package
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 4: Run compose smoke if infrastructure is available**

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-compose-smoke.yml --run-id compose-smoke-parquet-validation
```

Expected:

- Output prints `report=...`.
- Report status is `SUCCESS`.
- Report load summaries include `SPARK_NATIVE_PARQUET_VALIDATE`, `SPARK_ICEBERG_VALIDATE`, `STARROCKS_INTERNAL_VALIDATE`, `STARROCKS_EXTERNAL_VALIDATE`, and `HIVE_HDFS_PARQUET_VALIDATE`.
- Each validation row equals the generated dataset rows.

- [ ] **Step 5: If smoke cannot run, document why**

If local Docker infrastructure is not available or the command fails due to external state, capture:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

and include the blocker in the final report.

- [ ] **Step 6: Commit verification-only test/docs fixes if any**

Only commit if files changed:

```powershell
git status --short
git add <changed files>
git commit -m "test: verify parquet-backed smoke routes"
```

---

## Self-Review

- Spec coverage:
  - HDFS Parquet as single artifact: Tasks 1, 3, 4.
  - StarRocks Broker Load: Task 1.
  - Smoke row count validation: Task 3.
  - Spark native route: Task 2.
  - KPI CSV removal: Task 4.
  - Verification commands: Task 5.
- Placeholder scan: no unresolved placeholders remain.
- Type consistency:
  - `spark_native_parquet` is used consistently as table shape.
  - `SPARK_NATIVE_PARQUET` is used consistently as route enum.
  - `validateCount` returns `EngineRunResult` and records actual row count in `rows`.

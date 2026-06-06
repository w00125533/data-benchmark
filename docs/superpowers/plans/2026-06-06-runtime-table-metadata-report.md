# Runtime Table Metadata Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add runtime table metadata to the benchmark report, show it in Query Details with grouped phase rows, and make the Performance Matrix Best column choose the best route per cold/warm/hot phase.

**Architecture:** Add table runtime metadata as a best-effort report data stream collected after route load/validation and before query execution. Use a real `ComposeTableRuntimeMetadataCollector` for Spark, StarRocks, Hive, and HDFS runtime commands, with a separate `FallbackTableRuntimeMetadataCollector` for tests/offline/error fallback. Add a StarRocks JDBC raw-result API so metadata commands can return `SHOW CREATE TABLE` and related text instead of only row counts, then map one or more table metadata records into each query summary.

**Tech Stack:** Java 17, Maven, Jackson records, Picocli runner, Spark SQL CLI, StarRocks JDBC, Hive beeline, React, TypeScript, Ant Design, Vitest.

---

## File Map

- Modify `src/main/java/com/example/databenchmark/report/BenchmarkReport.java`
  - Add backend `TableRuntimeInfo` record and `tableRuntimeInfos` list.
- Modify `src/main/java/com/example/databenchmark/report/WebBenchmarkReport.java`
  - Add web `TableRuntimeInfo`.
  - Add `phase`, `durationMs`, and `tableRuntimeInfos` to `QuerySummary`.
- Modify `src/main/java/com/example/databenchmark/report/WebBenchmarkReportMapper.java`
  - Map table metadata by route/tableShape/table identifier.
  - Attach one KPI table or multiple TPC-H tables to every query summary.
  - Keep `p50Ms/p95Ms/p99Ms` for compatibility.
- Modify `src/main/java/com/example/databenchmark/engine/JdbcExecutor.java`
  - Add raw result retrieval for StarRocks metadata commands.
- Modify `pom.xml`
  - Add H2 as a test-scope dependency for raw JDBC result tests.
- Test `src/test/java/com/example/databenchmark/engine/JdbcExecutorTest.java`
  - Verify raw result retrieval preserves column names and values.
- Modify `src/main/java/com/example/databenchmark/tpch/TpchSchema.java`
  - Expose route prefixes through a public `enginePrefixes()` method so report code does not depend on package-private fields.
- Create `src/main/java/com/example/databenchmark/report/TableRuntimeMetadataCollector.java`
  - Define collector interface.
- Create `src/main/java/com/example/databenchmark/report/FallbackTableRuntimeMetadataCollector.java`
  - Conservative fallback/static metadata used by tests, sample reports, and dynamic command failures.
- Create `src/main/java/com/example/databenchmark/report/ComposeTableRuntimeMetadataCollector.java`
  - Runtime collector for compose Spark SQL, StarRocks JDBC, Hive beeline, and HDFS stats.
- Modify `src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java`
  - Inject collector.
  - Collect metadata after load/validation and before query execution.
  - Include metadata in `BenchmarkReport`.
- Modify `frontend/src/types/report.ts`
  - Add `TableRuntimeInfo`, `phase`, `durationMs`, and `tableRuntimeInfos`.
  - Add per-phase best fields to matrix rows or calculate on the frontend.
- Modify `frontend/src/components/PerformanceMatrixTable.tsx`
  - Compute/render Best per phase.
- Modify `frontend/src/components/QueryDetailsTable.tsx`
  - Replace P50/P95/P99 with phase-oriented grouped rows and table info details.
- Modify `frontend/src/data/sampleReport.ts`
  - Add table runtime metadata sample data.
- Modify `frontend/src/App.test.tsx`
  - Add UI assertions for phase rows, duration, row spans, table metadata details, and per-phase matrix best.
- Modify `src/test/java/com/example/databenchmark/report/WebBenchmarkReportMapperTest.java`
  - Add mapper assertions for metadata, phase duration, and TPC-H multi-table metadata.
- Modify `src/test/java/com/example/databenchmark/runner/ComposeBenchmarkRunnerTest.java`
  - Add runner lifecycle tests for metadata collection.
- Create `src/test/java/com/example/databenchmark/report/FallbackTableRuntimeMetadataCollectorTest.java`
  - Test collector fallback/static metadata.
- Create `src/test/java/com/example/databenchmark/report/ComposeTableRuntimeMetadataCollectorTest.java`
  - Test dynamic collector parsing and fallback behavior with fake command/JDBC clients.

---

### Task 1: Backend Schema And Mapper

**Files:**
- Modify: `src/main/java/com/example/databenchmark/report/BenchmarkReport.java`
- Modify: `src/main/java/com/example/databenchmark/report/WebBenchmarkReport.java`
- Modify: `src/main/java/com/example/databenchmark/report/WebBenchmarkReportMapper.java`
- Test: `src/test/java/com/example/databenchmark/report/WebBenchmarkReportMapperTest.java`

- [ ] **Step 1: Write failing mapper test for phase, duration, and KPI table metadata list**

Add this test to `WebBenchmarkReportMapperTest`:

```java
@Test
void mapsQueryPhaseDurationAndTableRuntimeInfos() {
    BenchmarkReport.TableRuntimeInfo tableInfo = new BenchmarkReport.TableRuntimeInfo(
        "spark_iceberg",
        "Spark Iceberg",
        "spark_iceberg",
        "iceberg_catalog.iceberg_db.cell_kpi_1min",
        "Iceberg",
        "hdfs://hdfs-namenode:8020/warehouse/iceberg/iceberg_db/cell_kpi_1min_metadata_run",
        "Parquet",
        50,
        "days(event_time)",
        "",
        "",
        "snapshot=12345",
        128,
        987654321L,
        "DESCRIBE EXTENDED output",
        true,
        ""
    );
    BenchmarkReport report = new BenchmarkReport(
        "metadata-run",
        "smoke",
        "kpi",
        "smoke",
        "2026-06-06T00:00:00Z",
        "2026-06-06T00:00:01Z",
        10000,
        1,
        1000000,
        50,
        987654321L,
        List.of(),
        List.of(new BenchmarkReport.QuerySummary(
            "spark",
            "spark_iceberg",
            "topn_high_load_cells",
            "WARM",
            111.0,
            222.0,
            333.0,
            100,
            0,
            true,
            ""
        )),
        List.of(tableInfo),
        false
    );

    WebBenchmarkReport.QuerySummary query = mapper.map(report).queries().get(0);

    assertThat(query.phase()).isEqualTo("WARM");
    assertThat(query.durationMs()).isEqualTo(222.0);
    assertThat(query.tableRuntimeInfos()).hasSize(1);
    assertThat(query.tableRuntimeInfos().get(0).storageType()).isEqualTo("Iceberg");
    assertThat(query.tableRuntimeInfos().get(0).partitioning()).isEqualTo("days(event_time)");
    assertThat(query.tableRuntimeInfos().get(0).location()).contains("cell_kpi_1min_metadata_run");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
mvn "-Dtest=WebBenchmarkReportMapperTest#mapsQueryPhaseDurationAndTableRuntimeInfos" test
```

Expected: compile failure because `TableRuntimeInfo`, new `BenchmarkReport` constructor argument, or new `WebBenchmarkReport.QuerySummary` fields do not exist.

- [ ] **Step 3: Add backend report model fields**

In `BenchmarkReport.java`, add `List<TableRuntimeInfo> tableRuntimeInfos` before `boolean fullProfile`, update `sample()`, and add:

```java
public record TableRuntimeInfo(
    String route,
    String displayName,
    String tableShape,
    String tableIdentifier,
    String storageType,
    String location,
    String format,
    int columns,
    String partitioning,
    String bucketingOrDistribution,
    String indexes,
    String snapshotOrVersion,
    long fileCount,
    long totalBytes,
    String rawDetails,
    boolean success,
    String error
) {}
```

Update existing `new BenchmarkReport` constructor calls in tests and runners by passing `List.of()` until metadata is collected.

- [ ] **Step 4: Add web report model fields**

In `WebBenchmarkReport.java`, add a web `TableRuntimeInfo` record with the same fields, and extend `QuerySummary`:

```java
String phase,
double durationMs,
List<TableRuntimeInfo> tableRuntimeInfos
```

Place `phase` after `queryName`, place `durationMs` after `p99Ms`, and keep existing fields for compatibility.

- [ ] **Step 5: Map metadata into web query summaries**

In `WebBenchmarkReportMapper.java`, create metadata lookups:

```java
private Map<String, List<WebBenchmarkReport.TableRuntimeInfo>> tableRuntimeInfosByRoute(BenchmarkReport report) {
    Map<String, List<WebBenchmarkReport.TableRuntimeInfo>> byRoute = new LinkedHashMap<>();
    for (BenchmarkReport.TableRuntimeInfo info : report.tableRuntimeInfos()) {
        String route = normalizeRoute(info.route(), info.tableShape());
        if (!route.isEmpty()) {
            byRoute.computeIfAbsent(route, ignored -> new ArrayList<>())
                .add(toWebTableRuntimeInfo(info));
        }
    }
    return byRoute;
}
```

Use it in `queries(report)`:

```java
Map<String, List<WebBenchmarkReport.TableRuntimeInfo>> metadata = tableRuntimeInfosByRoute(report);
String route = normalizeRouteOrEngine(query.engine(), query.tableShape());
List<WebBenchmarkReport.TableRuntimeInfo> queryMetadata = metadataForQuery(report, query, route, metadata);
new WebBenchmarkReport.QuerySummary(
    datasetId,
    datasetName,
    report.querySet(),
    route,
    query.tableShape(),
    query.queryName(),
    query.phase(),
    query.p50Ms(),
    query.p95Ms(),
    query.p99Ms(),
    query.p95Ms(),
    query.rows(),
    status(query),
    query.error(),
    queryMetadata
)
```

Add `metadataForQuery(report, query, route, metadata)` so KPI queries receive the single matching route table and TPC-H queries receive only the route tables referenced by the TPC-H query template.

- [ ] **Step 6: Run mapper tests**

Run:

```powershell
mvn "-Dtest=WebBenchmarkReportMapperTest" test
```

Expected: all mapper tests pass.

---

### Task 2: JDBC Raw Result API For StarRocks Metadata

**Files:**
- Modify: `pom.xml`
- Modify: `src/main/java/com/example/databenchmark/engine/JdbcExecutor.java`
- Test: `src/test/java/com/example/databenchmark/engine/JdbcExecutorTest.java`

- [ ] **Step 1: Add H2 test dependency**

In `pom.xml`, add this test dependency next to the other test dependencies:

```xml
<dependency>
  <groupId>com.h2database</groupId>
  <artifactId>h2</artifactId>
  <version>2.2.224</version>
  <scope>test</scope>
</dependency>
```

- [ ] **Step 2: Write failing JDBC raw result test**

Create `src/test/java/com/example/databenchmark/engine/JdbcExecutorTest.java`:

```java
package com.example.databenchmark.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JdbcExecutorTest {
@Test
void queryRowsReturnsColumnNamesAndValues() throws Exception {
    JdbcExecutor executor = new JdbcExecutor(
        "jdbc:h2:mem:metadata_probe;MODE=MySQL;DB_CLOSE_DELAY=-1",
        "sa",
        ""
    );
    executor.execute("""
        CREATE TABLE metadata_probe (
            TABLE_NAME VARCHAR(64),
            CREATE_TABLE VARCHAR(1024)
        );
        INSERT INTO metadata_probe VALUES (
            'cell_kpi_1min',
            'CREATE TABLE cell_kpi_1min DUPLICATE KEY(event_time, cell_id) DISTRIBUTED BY HASH(cell_id)'
        );
        """);

    List<Map<String, String>> rows = executor.queryRows("SELECT TABLE_NAME, CREATE_TABLE FROM metadata_probe");

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).containsEntry("TABLE_NAME", "cell_kpi_1min");
    assertThat(rows.get(0).get("CREATE_TABLE")).contains("DISTRIBUTED BY HASH(cell_id)");
}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run:

```powershell
mvn "-Dtest=JdbcExecutorTest#queryRowsReturnsColumnNamesAndValues" test
```

Expected: compile failure because `JdbcExecutor.queryRows(String)` does not exist.

- [ ] **Step 4: Add raw row query API**

In `JdbcExecutor.java`, add:

```java
public List<Map<String, String>> queryRows(String sql) throws SQLException {
    try (Connection connection = openConnection();
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columns = metaData.getColumnCount();
        List<Map<String, String>> rows = new ArrayList<>();
        while (resultSet.next()) {
            Map<String, String> row = new LinkedHashMap<>();
            for (int index = 1; index <= columns; index++) {
                String name = metaData.getColumnLabel(index);
                if (name == null || name.isBlank()) {
                    name = metaData.getColumnName(index);
                }
                row.put(name, resultSet.getString(index));
            }
            rows.add(row);
        }
        return rows;
    }
}
```

Add imports:

```java
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
```

- [ ] **Step 5: Run JDBC executor tests**

Run:

```powershell
mvn "-Dtest=JdbcExecutorTest" test
```

Expected: JDBC executor tests pass.

---

### Task 3: Metadata Collector Contract And Fallback Implementation

**Files:**
- Create: `src/main/java/com/example/databenchmark/report/TableRuntimeMetadataCollector.java`
- Create: `src/main/java/com/example/databenchmark/report/FallbackTableRuntimeMetadataCollector.java`
- Modify: `src/main/java/com/example/databenchmark/tpch/TpchSchema.java`
- Test: `src/test/java/com/example/databenchmark/report/FallbackTableRuntimeMetadataCollectorTest.java`

- [ ] **Step 1: Write failing collector test**

Create `FallbackTableRuntimeMetadataCollectorTest.java`:

```java
package com.example.databenchmark.report;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.runner.BenchmarkRoute;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FallbackTableRuntimeMetadataCollectorTest {
    @Test
    void createsFallbackKpiMetadataForAllFiveRoutes() {
        FallbackTableRuntimeMetadataCollector collector = new FallbackTableRuntimeMetadataCollector();

        var infos = collector.collectKpi(Map.of(), 1_000_000_000L, 12_345L);

        assertThat(infos).hasSize(5);
        assertThat(infos)
            .extracting(BenchmarkReport.TableRuntimeInfo::route)
            .containsExactly(
                "spark_native_parquet",
                "spark_iceberg",
                "starrocks_internal",
                "starrocks_external_iceberg",
                "hive_hdfs_parquet"
            );
        BenchmarkReport.TableRuntimeInfo iceberg = infos.stream()
            .filter(info -> info.route().equals("spark_iceberg"))
            .findFirst()
            .orElseThrow();
        assertThat(iceberg.storageType()).isEqualTo("Iceberg");
        assertThat(iceberg.columns()).isEqualTo(50);
        assertThat(iceberg.tableIdentifier()).isEqualTo("iceberg_catalog.iceberg_db.cell_kpi_1min");
    }

    @Test
    void marksRouteMetadataUnavailableWhenRouteLoadFailed() {
        FallbackTableRuntimeMetadataCollector collector = new FallbackTableRuntimeMetadataCollector();

        var infos = collector.collectKpi(
            Map.of(BenchmarkRoute.STARROCKS_INTERNAL, "broker load failed"),
            100L,
            200L
        );

        BenchmarkReport.TableRuntimeInfo starrocks = infos.stream()
            .filter(info -> info.route().equals("starrocks_internal"))
            .findFirst()
            .orElseThrow();
        assertThat(starrocks.success()).isFalse();
        assertThat(starrocks.error()).contains("broker load failed");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
mvn "-Dtest=FallbackTableRuntimeMetadataCollectorTest" test
```

Expected: compile failure because collector classes do not exist.

- [ ] **Step 3: Add collector interface**

Create `TableRuntimeMetadataCollector.java`:

```java
package com.example.databenchmark.report;

import com.example.databenchmark.runner.BenchmarkRoute;
import java.util.List;
import java.util.Map;

public interface TableRuntimeMetadataCollector {
    List<BenchmarkReport.TableRuntimeInfo> collectKpi(
        Map<BenchmarkRoute, String> routeFailures,
        long rows,
        long bytes
    );

    List<BenchmarkReport.TableRuntimeInfo> collectTpch(long rows, long bytes);
}
```

- [ ] **Step 4: Expose TPC-H engine prefixes**

In `TpchSchema.java`, add:

```java
public static Map<String, String> enginePrefixes() {
    return ENGINE_PREFIXES;
}
```

- [ ] **Step 5: Add fallback collector implementation**

Create `FallbackTableRuntimeMetadataCollector.java` with conservative fallback metadata based on current schema:

```java
public class FallbackTableRuntimeMetadataCollector implements TableRuntimeMetadataCollector {
    @Override
    public List<BenchmarkReport.TableRuntimeInfo> collectKpi(
        Map<BenchmarkRoute, String> routeFailures,
        long rows,
        long bytes
    ) {
        return KpiSchema.tableShapes().entrySet().stream()
            .map(entry -> fallbackKpiInfo(entry.getKey(), entry.getValue(), routeFailures, bytes))
            .toList();
    }

    @Override
    public List<BenchmarkReport.TableRuntimeInfo> collectTpch(long rows, long bytes) {
        return TpchSchema.enginePrefixes().entrySet().stream()
            .flatMap(route -> TpchSchema.tables().stream()
                .map(table -> fallbackTpchInfo(route.getKey(), route.getValue(), table, bytes)))
            .toList();
    }
}
```

Implement `collectKpi` to return one row per `BenchmarkRoute`, with:

- route key from `BenchmarkRoute`
- display name from the same labels as frontend
- table identifier from `KpiSchema.tableShapes()`
- columns from `KpiSchema.columns().size()`
- total bytes from input `bytes`
- fileCount `0` when unknown
- success `false` and error when `routeFailures` contains the route

Use storage types:

- `Spark SQL Native Parquet`
- `Iceberg`
- `StarRocks Internal`
- `StarRocks External Iceberg`
- `Hive External Parquet`

- [ ] **Step 6: Run collector test**

Run:

```powershell
mvn "-Dtest=FallbackTableRuntimeMetadataCollectorTest" test
```

Expected: 2 tests pass.

---

### Task 4: Compose Runtime Metadata Collector

**Files:**
- Create: `src/main/java/com/example/databenchmark/report/ComposeTableRuntimeMetadataCollector.java`
- Test: `src/test/java/com/example/databenchmark/report/ComposeTableRuntimeMetadataCollectorTest.java`

- [ ] **Step 1: Write failing dynamic collector test for StarRocks raw metadata**

Create `ComposeTableRuntimeMetadataCollectorTest.java` with fake command and JDBC dependencies. The StarRocks fake must return raw rows for `SHOW CREATE TABLE`, not only row counts:

```java
@Test
void collectsStarRocksInternalMetadataFromRawJdbcRows() {
    FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
    jdbc.rows("SHOW CREATE TABLE sr_internal.cell_kpi_1min", List.of(Map.of(
        "Table", "cell_kpi_1min",
        "Create Table", "CREATE TABLE cell_kpi_1min (event_time DATETIME, cell_id VARCHAR(64)) DUPLICATE KEY(event_time, cell_id) DISTRIBUTED BY HASH(cell_id)"
    )));
    jdbc.rows("SHOW PARTITIONS FROM sr_internal.cell_kpi_1min", List.of());
    jdbc.rows("SHOW INDEX FROM sr_internal.cell_kpi_1min", List.of());

    ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
        new FakeCommandRunner(),
        jdbc,
        new FallbackTableRuntimeMetadataCollector()
    );

    var infos = collector.collectKpi(Map.of(), 1_000_000_000L, 12_345L);

    BenchmarkReport.TableRuntimeInfo starrocks = infos.stream()
        .filter(info -> info.route().equals("starrocks_internal"))
        .findFirst()
        .orElseThrow();
    assertThat(starrocks.success()).isTrue();
    assertThat(starrocks.bucketingOrDistribution()).contains("HASH(cell_id)");
    assertThat(starrocks.rawDetails()).contains("DUPLICATE KEY(event_time, cell_id)");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
mvn "-Dtest=ComposeTableRuntimeMetadataCollectorTest#collectsStarRocksInternalMetadataFromRawJdbcRows" test
```

Expected: compile failure because `ComposeTableRuntimeMetadataCollector` does not exist.

- [ ] **Step 3: Add compose collector constructor and defaults**

Create `ComposeTableRuntimeMetadataCollector.java`:

```java
public class ComposeTableRuntimeMetadataCollector implements TableRuntimeMetadataCollector {
    private static final int RAW_DETAILS_LIMIT = 20_000;

    private final CommandRunner commandRunner;
    private final JdbcExecutor starRocksJdbc;
    private final FallbackTableRuntimeMetadataCollector fallback;

    public ComposeTableRuntimeMetadataCollector(
        CommandRunner commandRunner,
        JdbcExecutor starRocksJdbc,
        FallbackTableRuntimeMetadataCollector fallback
    ) {
        this.commandRunner = commandRunner;
        this.starRocksJdbc = starRocksJdbc;
        this.fallback = fallback;
    }

    public static ComposeTableRuntimeMetadataCollector fromDefaults() {
        return new ComposeTableRuntimeMetadataCollector(
            new CommandRunner(),
            new JdbcExecutor(),
            new FallbackTableRuntimeMetadataCollector()
        );
    }
}
```

- [ ] **Step 4: Implement runtime commands with fallback on failure**

Implement collection using the current compose command patterns:

- Spark SQL commands use `InfraComposeTarget.fromEnvironment(System.getenv()).composeCommand("exec", "-T", "spark", "spark-sql")`, then append the same Iceberg catalog options as `SparkIcebergClient`, followed by `-e` and the metadata SQL.
- Hive commands use `InfraComposeTarget.fromEnvironment(System.getenv()).composeCommand("exec", "-T", "hive-server", "beeline", "-u", "jdbc:hive2://hive-server:10000/default", "-e", sql)`.
- StarRocks internal and external metadata use `JdbcExecutor.queryRows(String sql)`.
- Raw output is truncated to `RAW_DETAILS_LIMIT`.
- Any command/JDBC exception returns the matching fallback row with `success=false` and the command error.

Use these route-specific facts:

- Spark native KPI partitioning is `none` unless `DESCRIBE EXTENDED` shows otherwise.
- Spark Iceberg KPI partitioning is `days(event_time)`, and the location must come from runtime metadata or include the current runId-derived table location when available.
- StarRocks internal KPI has no explicit partition clause, and distribution is parsed from `DISTRIBUTED BY HASH(cell_id)`.
- Hive KPI partitioning is `event_date STRING`.
- TPC-H collection returns one metadata row for every route/table pair from `TpchSchema.enginePrefixes()` and `TpchSchema.tables()`.

- [ ] **Step 5: Run compose collector tests**

Run:

```powershell
mvn "-Dtest=ComposeTableRuntimeMetadataCollectorTest" test
```

Expected: compose collector tests pass.

---

### Task 5: Compose Runner Metadata Lifecycle

**Files:**
- Modify: `src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java`
- Test: `src/test/java/com/example/databenchmark/runner/ComposeBenchmarkRunnerTest.java`

- [ ] **Step 1: Write failing runner test for metadata before query**

Add a fake collector to `ComposeBenchmarkRunnerTest` and assert lifecycle order:

```java
@Test
void collectsKpiTableMetadataAfterValidationAndBeforeQueries() throws Exception {
    List<String> events = new ArrayList<>();
    TableRuntimeMetadataCollector collector = new TableRuntimeMetadataCollector() {
        @Override
        public List<BenchmarkReport.TableRuntimeInfo> collectKpi(
            Map<BenchmarkRoute, String> routeFailures,
            long rows,
            long bytes
        ) {
            events.add("metadata");
            return List.of(new BenchmarkReport.TableRuntimeInfo(
                "spark_iceberg",
                "Spark Iceberg",
                "spark_iceberg",
                "iceberg_catalog.iceberg_db.cell_kpi_1min",
                "Iceberg",
                "",
                "Parquet",
                50,
                "days(event_time)",
                "",
                "",
                "",
                0,
                bytes,
                "raw",
                true,
                ""
            ));
        }

        @Override
        public List<BenchmarkReport.TableRuntimeInfo> collectTpch(long rows, long bytes) {
            return List.of();
        }
    };

    // Use existing fake clients in this test file. In the fake Spark/StarRocks/Hive query methods,
    // append "query" to events before returning a successful EngineRunResult.

    ComposeBenchmarkRunner runner = runnerWithCollector(collector, events);
    runner.run(smokeKpiConfig(), tempDir, "metadata-order");

    assertThat(events).containsSubsequence("metadata", "query");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
mvn "-Dtest=ComposeBenchmarkRunnerTest#collectsKpiTableMetadataAfterValidationAndBeforeQueries" test
```

Expected: compile failure because the runner does not accept a metadata collector yet.

- [ ] **Step 3: Inject metadata collector**

Modify `ComposeBenchmarkRunner`:

- Add field `private final TableRuntimeMetadataCollector tableRuntimeMetadataCollector;`
- Default constructor passes `ComposeTableRuntimeMetadataCollector.fromDefaults()`.
- Main test constructor accepts the collector.
- Add `List<BenchmarkReport.TableRuntimeInfo> tableRuntimeInfos = new ArrayList<>();` to `run` and `runTpch`.

- [ ] **Step 4: Collect KPI metadata before queries**

In KPI run flow, after validation failures are merged and before `runKpiRouteQueries`:

```java
tableRuntimeInfos.addAll(tableRuntimeMetadataCollector.collectKpi(
    routeFailures,
    dataset.rows(),
    dataset.bytesWritten()
));
queryResults.addAll(runKpiRouteQueries(config, routeFailures));
```

Pass `tableRuntimeInfos` into `buildReport`.

- [ ] **Step 5: Collect TPC-H metadata before TPC-H queries**

In `runTpch`, after Spark/StarRocks loads and refreshes, before TPC-H queries:

```java
tableRuntimeInfos.addAll(tableRuntimeMetadataCollector.collectTpch(
    dataset.rows(),
    dataset.bytesWritten()
));
```

Pass it into `buildReport`.

- [ ] **Step 6: Run runner tests**

Run:

```powershell
mvn "-Dtest=ComposeBenchmarkRunnerTest" test
```

Expected: all runner tests pass.

---

### Task 6: Frontend Query Details Redesign

**Files:**
- Modify: `frontend/src/types/report.ts`
- Modify: `frontend/src/components/QueryDetailsTable.tsx`
- Modify: `frontend/src/data/sampleReport.ts`
- Test: `frontend/src/App.test.tsx`

- [ ] **Step 1: Write failing frontend test**

In `App.test.tsx`, add assertions to the existing Query Details test:

```typescript
expect(screen.getByText('Phase')).toBeInTheDocument();
expect(screen.getByText('Duration ms')).toBeInTheDocument();
expect(screen.queryByText('P50 ms')).not.toBeInTheDocument();
expect(screen.queryByText('P95 ms')).not.toBeInTheDocument();
expect(screen.queryByText('P99 ms')).not.toBeInTheDocument();
expect(screen.getByText('Warm')).toBeInTheDocument();
expect(screen.getByText(/Iceberg \\| days\\(event_time\\) \\| 50 cols/)).toBeInTheDocument();
expect(screen.getByText('Table runtime metadata')).toBeInTheDocument();
```

Add a rowSpan assertion:

```typescript
const tableInfoCell = container.querySelector('td[rowspan="3"]');
expect(tableInfoCell).toBeTruthy();
```

- [ ] **Step 2: Run frontend test to verify it fails**

Run:

```powershell
npm.cmd test -- --run
```

Expected: fails because Query Details still shows P50/P95/P99 and no table metadata.

- [ ] **Step 3: Extend TypeScript types**

In `frontend/src/types/report.ts`, add:

```typescript
export interface TableRuntimeInfo {
  route: string;
  displayName: string;
  tableShape: string;
  tableIdentifier: string;
  storageType: string;
  location: string;
  format: string;
  columns: number;
  partitioning: string;
  bucketingOrDistribution: string;
  indexes: string;
  snapshotOrVersion: string;
  fileCount: number;
  totalBytes: number;
  rawDetails: string;
  success: boolean;
  error: string;
}
```

Extend `QuerySummary`:

```typescript
phase: string;
durationMs: number;
tableRuntimeInfos: TableRuntimeInfo[];
```

- [ ] **Step 4: Add sample metadata**

In `sampleReport.ts`, add three query rows for the same route/query with phases `COLD`, `WARM`, and `HOT`, `durationMs`, and a `tableRuntimeInfos` array containing one object:

```typescript
storageType: 'Iceberg',
partitioning: 'days(event_time)',
columns: 50,
snapshotOrVersion: 'snapshot=12345',
rawDetails: 'DESCRIBE EXTENDED output',
success: true,
```

- [ ] **Step 5: Implement grouped Query Details table**

In `QueryDetailsTable.tsx`:

- Remove P50/P95/P99 columns.
- Add `Phase` and `Duration ms`.
- Sort rows by `engine`, `tableShape`, `queryName`, then phase order `COLD`, `WARM`, `HOT`.
- Build rowSpan metadata for `engine + tableShape + queryName`.
- Render `Engine`, `Table Info`, and `Query` with `onCell` returning `{ rowSpan }`.
- Render `Table Info` as summary plus details:

```tsx
<div>
  <strong>{summary(row.tableRuntimeInfos)}</strong>
  <details>
    <summary>Table runtime metadata</summary>
    {row.tableRuntimeInfos.map((info) => (
      <section key={`${info.route}-${info.tableIdentifier}`}>
        <dl>
          <dt>Table</dt><dd>{info.tableIdentifier}</dd>
          <dt>Storage</dt><dd>{info.storageType}</dd>
          <dt>Partitioning</dt><dd>{info.partitioning || 'none'}</dd>
          <dt>Files</dt><dd>{info.fileCount}</dd>
        </dl>
        <pre>{info.rawDetails}</pre>
      </section>
    ))}
  </details>
</div>
```

- [ ] **Step 6: Run frontend tests**

Run:

```powershell
npm.cmd test -- --run
```

Expected: all frontend tests pass.

---

### Task 7: Performance Matrix Per-Phase Best

**Files:**
- Modify: `frontend/src/components/PerformanceMatrixTable.tsx`
- Test: `frontend/src/App.test.tsx`

- [ ] **Step 1: Write failing frontend test for per-phase Best**

Add sample data where the cold winner differs from the hot winner, then assert both appear in the Best column:

```typescript
expect(screen.getByText('Cold')).toBeInTheDocument();
expect(screen.getByText(/Spark Iceberg/)).toBeInTheDocument();
expect(screen.getByText(/StarRocks Internal/)).toBeInTheDocument();
```

Make the assertion more specific by selecting `.matrix-phase-table` and verifying the Cold row Best cell text contains the cold winner.

- [ ] **Step 2: Run frontend test to verify it fails**

Run:

```powershell
npm.cmd test -- --run
```

Expected: fails because the current Best column only shows hot best and `-` for cold/warm.

- [ ] **Step 3: Implement per-phase best calculation**

In `PerformanceMatrixTable.tsx`, add:

```typescript
function bestRouteForPhase(row: PerformanceMatrixRow, phase: (typeof phases)[number]) {
  return routeKeys
    .map((route) => ({ route, result: row.routes[route] }))
    .filter(({ result }) => result[phase.statusKey] === 'SUCCESS' && result[phase.key] > 0)
    .sort((a, b) => a.result[phase.key] - b.result[phase.key])[0];
}
```

In the Best cell, use this helper for every phase:

```tsx
const best = bestRouteForPhase(row, phase);
return best ? (
  <>
    <strong>{routeLabels[best.route]}</strong>
    <div className="phase-ms">{exactMs(best.result[phase.key])}</div>
  </>
) : '-';
```

- [ ] **Step 4: Run frontend tests**

Run:

```powershell
npm.cmd test -- --run
```

Expected: all frontend tests pass.

---

### Task 8: Build Assets And Verify Report Package

**Files:**
- Modify generated bundle: `src/main/resources/report-ui/assets/report-ui.js`
- Optional local output update: `reports/runs/compose-kpi-1b-five-routes/assets/report-ui.js`

- [ ] **Step 1: Build frontend**

Run:

```powershell
npm.cmd run build
```

Expected: Vite build succeeds. Existing large chunk warning is acceptable.

- [ ] **Step 2: Copy bundle into Java resources**

Run:

```powershell
Copy-Item -LiteralPath frontend/dist/assets/report-ui.js -Destination src/main/resources/report-ui/assets/report-ui.js -Force
```

- [ ] **Step 3: Run targeted Java tests**

Run:

```powershell
mvn "-Dtest=WebBenchmarkReportMapperTest,WebReportWriterTest,ComposeBenchmarkRunnerTest,FallbackTableRuntimeMetadataCollectorTest,ComposeTableRuntimeMetadataCollectorTest,JdbcExecutorTest" test
```

Expected: all targeted tests pass.

- [ ] **Step 4: Run package verification**

Run:

```powershell
mvn -DskipTests package
```

Expected: `BUILD SUCCESS`. Existing shade overlap warnings are acceptable.

- [ ] **Step 5: Verify report resource strings**

Run:

```powershell
Select-String -LiteralPath src/main/resources/report-ui/assets/report-ui.js -Pattern "Table runtime metadata","Duration ms","Actual SQL sent by route"
```

Expected: all three strings are present in the bundle.

---

## Self Review

- Spec coverage: runtime metadata schema, collection timing, five KPI routes, UI grouping, per-phase Best, and failure handling are covered.
- Placeholder scan: no TBD/TODO/fill-in markers remain.
- Type consistency: `TableRuntimeInfo`, `phase`, `durationMs`, and `tableRuntimeInfos` names match between backend, frontend, and tests.
- Scope control: no Docker Compose infra changes or 1B rerun are included.

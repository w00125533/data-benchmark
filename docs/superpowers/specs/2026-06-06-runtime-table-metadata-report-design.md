# Runtime Table Metadata Report Design

## Goal

Enhance the benchmark report so each performance result is explainable by the actual runtime table state behind every technical route. The report should continue to compare the five query routes, while also showing table identifiers, physical layout, storage locations, schema size, partitioning, distribution, indexes, file statistics, snapshots, and raw metadata output where available.

## Background

The report already shows:

- A performance matrix grouped by SQL, with cold/warm/hot rows and five route columns.
- Per-SQL bar charts for cold/warm/hot latency.
- Actual SQL text per route under each SQL name, default collapsed.
- Query Details with engine, table shape, query name, P50/P95/P99, rows, status, and error.

The current `Query Details` table is not yet aligned with cold/warm/hot execution semantics. It repeats route/query data across phase rows, still exposes P50/P95/P99 columns even though each compose phase is a single measured execution, and only shows a raw `tableShape` string. That makes it hard to explain why one route is faster or slower.

## Confirmed Design

### Performance Matrix

Keep the `Best` column.

Change its meaning from "best hot route only" to "best route for this phase":

- Cold row: best successful route by `coldMs`.
- Warm row: best successful route by `warmMs`.
- Hot row: best successful route by `hotMs`.

The SQL block header may keep a compact hot-best summary, but the matrix row-level `Best` cell is the authoritative phase winner.

### Query Details

Change the table columns to:

```text
Engine | Table Info | Query | Phase | Duration ms | Result Rows | Status | Error
```

Rules:

- `Engine`, `Table Info`, and `Query` are merged across consecutive cold/warm/hot rows for the same `engine + tableShape + queryName`.
- `Phase` displays `Cold`, `Warm`, or `Hot`.
- `Duration ms` uses the phase duration, mapped from the current query summary `p95Ms` value for backward compatibility with matrix timing.
- `P50 ms`, `P95 ms`, and `P99 ms` are removed from the UI.
- `Result Rows`, `Status`, and `Error` remain per phase.

### Table Info Display

Use "Scheme C": a compact summary plus collapsible enhanced runtime metadata.

Default summary should fit in one or two lines:

```text
Iceberg | days(event_time) | 50 cols | snapshot 123 | 1.2 TB
```

The expandable details should show:

- table identifier
- storage type
- location
- format
- columns
- partitioning
- bucketing/distribution
- indexes, rollups, or materialized views
- snapshot/version
- file stats
- raw details from engine metadata commands
- metadata collection status/error

If metadata collection fails for a route, the report must still render and show the error in the Table Info details. A metadata failure must not fail the benchmark run by itself.

## Data Model

### BenchmarkReport

Add a runtime metadata list to the backend report model:

```java
List<TableRuntimeInfo> tableRuntimeInfos
```

`TableRuntimeInfo` fields:

```java
String route
String displayName
String tableShape
String tableIdentifier
String storageType
String location
String format
int columns
String partitioning
String bucketingOrDistribution
String indexes
String snapshotOrVersion
long fileCount
long totalBytes
String rawDetails
boolean success
String error
```

### WebBenchmarkReport

Expose the same table metadata to the frontend and attach the relevant table metadata to each `QuerySummary`.

Extend `WebBenchmarkReport.QuerySummary` with:

```java
String phase
double durationMs
List<TableRuntimeInfo> tableRuntimeInfos
```

Keep existing `p50Ms`, `p95Ms`, and `p99Ms` in schema version 3 for compatibility, but the UI should no longer display all three.

For KPI queries, `tableRuntimeInfos` usually contains one item because each route queries one logical KPI table. For TPC-H queries, it can contain multiple items because a single query may join several benchmark tables such as `customer`, `orders`, and `lineitem`.

## Runtime Metadata Collection

### Collection Timing

Collect metadata after load/import/refresh/validation and before query benchmark execution.

Reasons:

- The metadata reflects the table state used by the benchmark queries.
- Query failures do not hide metadata.
- Metadata failures can be isolated from load/query failures.

### Routes

Collect metadata for all five KPI routes:

- `spark_native_parquet`
- `spark_iceberg`
- `starrocks_internal`
- `starrocks_external_iceberg`
- `hive_hdfs_parquet`

For TPC-H, collect metadata for routes that currently exist in the TPC-H flow:

- `spark_iceberg`
- `starrocks_internal`
- `starrocks_external_iceberg`

TPC-H native Spark and Hive metadata can remain absent until those routes exist in TPC-H query execution.

### Collector Boundary

Create a dedicated metadata collection boundary instead of mixing metadata logic into query execution:

```java
interface TableRuntimeMetadataCollector {
    List<BenchmarkReport.TableRuntimeInfo> collectKpi(Map<BenchmarkRoute, String> routeFailures, long rows, long bytes);
    List<BenchmarkReport.TableRuntimeInfo> collectTpch(long rows, long bytes);
}
```

Use two implementations:

- `ComposeTableRuntimeMetadataCollector`: used by real compose benchmark runs. It executes runtime metadata commands against Spark, StarRocks, Hive, and HDFS.
- `FallbackTableRuntimeMetadataCollector`: used by unit tests, offline report samples, or command failures. It emits conservative route/table identity and schema facts only.

The compose collector may use:

- Spark SQL commands for Spark native and Spark Iceberg.
- StarRocks JDBC commands for StarRocks internal and external Iceberg.
- Hive beeline commands for Hive.
- HDFS `dfs -count` or existing dataset byte counts as fallback file stats.

Tests should use a fake collector injected into `ComposeBenchmarkRunner`.

StarRocks collection requires a JDBC API that returns raw result rows or text, not just row counts. `SHOW CREATE TABLE`, `SHOW PARTITIONS`, and `SHOW INDEX` are only useful if the report can read their returned columns.

## Route-Specific Metadata

### Spark SQL Native Parquet

Summary:

- `Spark SQL Native Parquet`
- table identifier: `spark_catalog.benchmark_native.cell_kpi_1min`
- storage type: `Spark SQL Native Parquet`
- format: `Parquet`
- columns: from `KpiSchema.columns().size()`
- location: native table location or source HDFS path
- partitioning: from table metadata when available; otherwise `none`. The current native Spark DDL creates a Parquet table over a location and does not declare a table-level partition spec.
- file stats: HDFS count/bytes or dataset fallback

Raw details:

- `DESCRIBE EXTENDED spark_catalog.benchmark_native.cell_kpi_1min`
- optional `SHOW CREATE TABLE` when the engine supports it

### Spark Iceberg

Summary:

- `Spark Iceberg`
- table identifier: `iceberg_catalog.iceberg_db.cell_kpi_1min`
- storage type: `Iceberg`
- format: `Parquet`
- columns: from schema or metadata
- location: Iceberg table location from runtime metadata. The current KPI load uses `hdfs://hdfs-namenode:8020/warehouse/iceberg/iceberg_db/cell_kpi_1min_<sanitizedRunId>`, not a fixed unsuffixed path.
- partitioning: Iceberg partition spec, currently `days(event_time)` for KPI.
- snapshot/version: current snapshot ID if available
- file stats: Iceberg metadata table if available, otherwise dataset fallback

Raw details:

- `DESCRIBE EXTENDED iceberg_catalog.iceberg_db.cell_kpi_1min`
- `SELECT * FROM iceberg_catalog.iceberg_db.cell_kpi_1min.snapshots ORDER BY committed_at DESC LIMIT 1`
- `SELECT COUNT(*), SUM(file_size_in_bytes) FROM iceberg_catalog.iceberg_db.cell_kpi_1min.files`

### Hive HDFS Parquet

Summary:

- `Hive HDFS Parquet`
- table identifier: `hive_hdfs_parquet.cell_kpi_1min`
- storage type: `Hive External Parquet`
- location: HDFS `LOCATION`
- format: Parquet input/output format
- columns: schema column count
- partitioning: Hive partitions if present
- file stats: HDFS count/bytes

Raw details:

- `DESCRIBE FORMATTED hive_hdfs_parquet.cell_kpi_1min`

### StarRocks Internal

Summary:

- `StarRocks Internal`
- table identifier: `sr_internal.cell_kpi_1min`
- storage type: `StarRocks Internal`
- columns: schema column count
- partitioning: StarRocks partition definition. The current KPI internal table has no explicit partition clause, so report `none` unless runtime metadata says otherwise.
- distribution: bucket/distribution definition, currently `DISTRIBUTED BY HASH(cell_id)`.
- indexes: rollups, indexes, materialized views if present

Raw details:

- `SHOW CREATE TABLE sr_internal.cell_kpi_1min`
- `SHOW PARTITIONS FROM sr_internal.cell_kpi_1min`
- `SHOW INDEX FROM sr_internal.cell_kpi_1min`

### StarRocks External Iceberg

Summary:

- `StarRocks External Iceberg`
- table identifier: `sr_external_iceberg.iceberg_db.cell_kpi_1min`
- storage type: `StarRocks External Iceberg`
- backing format: Iceberg/Parquet
- location/partition/snapshot: from Iceberg metadata when available
- StarRocks external catalog/table info

Raw details:

- `SHOW CREATE TABLE sr_external_iceberg.iceberg_db.cell_kpi_1min`
- Iceberg metadata from Spark if StarRocks does not expose it cleanly.

## TPC-H Multi-Table Metadata

TPC-H metadata is collected per route and per physical table, not only per route:

- Spark Iceberg table identifiers use `iceberg_catalog.tpch.<table>`.
- StarRocks internal table identifiers use `sr_internal_tpch.<table>`.
- StarRocks external Iceberg table identifiers use `sr_external_iceberg.tpch.<table>`.

The mapper attaches metadata to each TPC-H query by route plus the table names referenced by the TPC-H template. For example, q03 attaches `customer`, `orders`, and `lineitem`; q05 attaches `customer`, `orders`, `lineitem`, `supplier`, and `nation`.

The Query Details UI should render a compact route-level summary first and expose the involved table list in the collapsible details.

## Failure Handling

Metadata collection is best-effort.

Rules:

- If a route failed during load or validation, create a metadata row with `success=false` and an error beginning with `Route load failed before metadata collection:` for that route.
- If metadata command execution fails, create a metadata row with the route/table identity and the command error.
- Metadata failures are visible in the report but do not change `BenchmarkReport.status()`.
- Query Details can still render without `tableRuntimeInfos`; it should show `metadata unavailable`.
- Runtime metadata commands must use bounded timeouts and truncate very large raw outputs before embedding them in the report.

## Testing Requirements

Backend:

- `WebBenchmarkReportMapperTest` verifies `phase`, `durationMs`, and `tableRuntimeInfos` are present.
- `WebBenchmarkReportMapperTest` verifies TPC-H multi-table queries attach all referenced tables for a route.
- `ComposeBenchmarkRunnerTest` verifies metadata collection is called after load/validation and before query execution for KPI.
- `ComposeBenchmarkRunnerTest` verifies metadata collection failures do not prevent query execution/report writing.
- `JdbcExecutorTest` verifies a raw row/text query API can return `SHOW CREATE TABLE` style result content.
- Metadata collector unit tests verify fallback metadata for all five KPI routes and dynamic collector fallback on command failures.

Frontend:

- `App.test.tsx` verifies `Query Details` renders phase rows and one `Duration ms` column.
- It verifies `P50 ms`, `P95 ms`, and `P99 ms` are not shown in Query Details.
- It verifies `Engine`, `Table Info`, and `Query` merged cells use row spans.
- It verifies Table Info summary and collapsible raw details render.
- It verifies matrix `Best` is computed per phase, not only hot.

Verification:

- `npm.cmd test -- --run`
- `npm.cmd run build`
- `mvn "-Dtest=WebBenchmarkReportMapperTest,WebReportWriterTest,ComposeBenchmarkRunnerTest,*TableRuntimeMetadata*Test" test`
- `mvn -DskipTests package`

## Out of Scope

- Changing Docker Compose infrastructure.
- Re-running the 1B benchmark.
- Changing benchmark query SQL semantics.
- Making metadata collection mandatory for benchmark success.
- Adding a new report schema version unless backward compatibility breaks.

## Self Review

- No placeholders remain.
- The design keeps metadata collection separate from query execution.
- Existing schema version 3 can remain compatible because fields are additive.
- Best column behavior is clarified per phase.
- Query Details becomes phase-oriented while preserving existing query timing data.

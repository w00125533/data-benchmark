# TPC-H Standard SQL Suite Design

## Goal

Add a lightweight TPC-H SQL performance suite to the existing Java 17 benchmark runner so the project can validate common analytical SQL workloads alongside the current wireless KPI benchmark.

The first implementation should make TPC-H usable in smoke mode for Spark Iceberg, StarRocks internal tables, and StarRocks external Iceberg tables. It should prioritize repeatable validation, clear reports, and Grafana labels over large-scale official benchmark claims.

## Non-Goals

- Claim official audited TPC-H results.
- Run large scale factors by default.
- Add TPC-DS or SSB in this iteration.
- Replace the existing KPI benchmark suite.
- Tune StarRocks, Spark, Iceberg, HDFS, or Hive Metastore for production capacity.

## Current Context

The project already has:

- Java 17 Maven runner.
- Deterministic KPI data generation.
- Spark Iceberg load and query client.
- StarRocks internal load through CSV Stream Load.
- StarRocks external Iceberg catalog refresh.
- Query catalog and SQL renderer.
- HTML report and Prometheus/Grafana metric labels.
- Docker Compose topology using HDFS for Iceberg warehouse storage.

The missing piece is a suite abstraction that allows the runner to switch from KPI-specific schema, data generation, DDL, and queries to a standard SQL benchmark suite.

## Proposed Approach

Introduce a benchmark suite model with two initial suite values:

- `kpi`, the current default behavior.
- `tpch`, the new lightweight standard SQL suite.

The `tpch` suite will provide:

- TPC-H table metadata for `region`, `nation`, `supplier`, `customer`, `part`, `partsupp`, `orders`, and `lineitem`.
- Smoke-scale deterministic data generation in Java.
- Spark Iceberg DDL for TPC-H tables.
- StarRocks internal DDL for TPC-H tables.
- StarRocks external Iceberg catalog table naming through the existing catalog.
- Query templates for the 22 TPC-H queries, with a smoke profile allowed to run a smaller default subset first.
- Query result collection using the existing `EngineRunResult`, report, and metrics flow.

## Configuration

Extend benchmark config with a suite section:

```yaml
suite:
  name: tpch
  scaleFactor: 0.01
  querySet: smoke
```

Defaults:

- Missing `suite.name` means `kpi`, preserving existing behavior.
- `tpch.scaleFactor` defaults to `0.01` for local smoke runs.
- `tpch.querySet` defaults to `smoke`.

Supported query sets:

- `smoke`: selected TPC-H queries that cover scan, filter, aggregation, join, sort, and top-N behavior.
- `all`: all 22 TPC-H query templates.

## Data Generation

Use a Java 17 deterministic generator for the first version. The generator should create relational TPC-H-like data with stable primary and foreign-key relationships.

Non-dimension table row counts use scaled counts by default:

- `supplier`: scaled count.
- `customer`: scaled count.
- `part`: scaled count.
- `partsupp`: scaled count.
- `orders`: scaled count.
- `lineitem`: scaled count with stable order linkage.

For smoke-scale runs, the generator may increase `supplier` to the minimum count needed to keep generated `partsupp` composite keys unique for the generated `(ps_partkey, ps_suppkey)` combinations while preserving valid key ranges.

- `region`: 5 rows.
- `nation`: 25 rows.

This is a TPC-H-compatible smoke-suite accommodation, not official dbgen scaling.

The generator does not need to exactly match `dbgen` distributions in the first iteration. It must preserve table shape, join keys, field types, and enough value variety for the query templates to run meaningfully.

Generated output should be table-oriented:

```text
data/generated/tpch/<run-id>/<table>/*.parquet
data/generated/tpch/<run-id>/csv/<table>.csv
```

Parquet remains the source for Spark Iceberg. CSV is used for StarRocks internal Stream Load.

## Table Naming

Spark Iceberg:

```text
iceberg_catalog.tpch.<table>
```

StarRocks internal:

```text
sr_internal_tpch.<table>
```

StarRocks external Iceberg:

```text
sr_external_iceberg.tpch.<table>
```

This keeps KPI and TPC-H artifacts separate and avoids accidental table reuse between suites.

## SQL Rendering

Replace the current single-table renderer assumption with suite-aware rendering:

- KPI queries keep `{table}` replacement.
- TPC-H queries use placeholders like `{lineitem}`, `{orders}`, `{customer}`, and render to the correct engine table names.
- StarRocks timestamp/date literal adaptation remains centralized in the renderer.
- Engine-specific differences must stay small and explicit.

The query catalog should expose:

- suite name.
- query name.
- query template.
- optional query-set tags such as `smoke` and `all`.

## Runner Flow

For `suite.name: kpi`, the current flow remains unchanged.

For `suite.name: tpch`:

1. Generate TPC-H table data.
2. Create Spark Iceberg database and tables.
3. Load Parquet data into Spark Iceberg tables.
4. Create StarRocks internal database and tables.
5. Stream Load each TPC-H CSV table into StarRocks internal tables.
6. Refresh or rely on StarRocks external Iceberg catalog metadata.
7. Run selected TPC-H queries against Spark Iceberg, StarRocks internal, and StarRocks external Iceberg.
8. Record metrics and write the HTML report.

Failures should be reported per stage and table/query. A failed table load should not hide unrelated table or query errors.

## Metrics And Report

Reuse existing metric names, adding or ensuring labels include:

- `suite`: `kpi` or `tpch`.
- `query_set`: `smoke` or `all`.
- `engine`.
- `table_shape`.
- `query_name`.
- `stage`.

The HTML report should show the active suite and query set in the dataset summary. Query rows should display TPC-H query names such as `q01_pricing_summary_report`.

Grafana should be able to filter by `suite` and `query_set` in addition to `run_id`.

## Tests

Add unit tests for:

- Config defaults preserve KPI behavior.
- TPC-H config parsing.
- TPC-H schema metadata includes all 8 tables and required columns.
- TPC-H smoke data generation produces deterministic row counts and valid foreign keys.
- Spark DDL renders each TPC-H table.
- StarRocks DDL renders each TPC-H table.
- SQL renderer replaces all TPC-H table placeholders for Spark and StarRocks.
- Query catalog returns smoke and all query sets correctly.
- Runner records load/query results with `suite=tpch`.
- HTML report includes suite and TPC-H query names.

Add an optional compose smoke validation command for:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-tpch-smoke.yml --run-id tpch-smoke
```

## Risks

- Full TPC-H SQL has dialect differences across Spark SQL and StarRocks. Keep renderer adaptations centralized and test rendered SQL strings.
- Java-generated TPC-H-like data is not official `dbgen` data. Reports must call it a TPC-H-compatible smoke suite, not official benchmark output.
- Loading eight tables increases compose runtime. Keep the default scale factor small.
- Query failures can be caused by engine feature differences rather than data defects. The report must preserve SQL error details.

## Acceptance Criteria

- Existing KPI smoke behavior remains the default.
- A new `configs/benchmark-tpch-smoke.yml` can select the TPC-H suite.
- Unit tests validate TPC-H schema, generator, DDL, SQL rendering, and report integration.
- Compose mode can attempt TPC-H load/query across Spark Iceberg, StarRocks internal, and StarRocks external Iceberg.
- HTML report and Grafana metrics identify `suite=tpch`.
- Documentation clearly states this is a lightweight TPC-H-compatible validation suite, not an official audited TPC-H result.

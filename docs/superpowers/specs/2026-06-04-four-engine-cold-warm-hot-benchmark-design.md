# Four-Engine Cold/Warm/Hot Benchmark Design

## Goal

Improve the current compose benchmark so performance differences are easier to explain, especially when StarRocks Internal and StarRocks External Iceberg appear close due to cache effects.

The benchmark will compare four query routes:

- Spark Iceberg
- StarRocks Internal
- StarRocks External Iceberg
- Hive HDFS Parquet

For each query and each route, the benchmark runs exactly three executions:

- `cold`: first execution after restarting the route's related services.
- `warm`: second execution without restart.
- `hot`: third execution without restart.

The report must show all three timings per route. The default best-route calculation uses `hotMs`, while `coldMs` and `warmMs` remain visible for cache analysis.

## Non-Goals

- Do not implement route randomization.
- Do not implement multi-sample percentile statistics in this change.
- Do not implement concurrent benchmark execution. `query.concurrency` remains accepted by config, but compose benchmark precision mode runs sequentially.
- Do not remove existing local smoke mode.
- Do not make Grafana, Prometheus, or external JSON required for report rendering.

## Dataset Profiles

Two KPI dataset profiles are required:

1. Small validation dataset
   - Row cap: `10,000`
   - Purpose: default local/CI/manual validation.
   - Existing `configs/benchmark-smoke.yml` remains the default validation config.

2. Formal KPI benchmark dataset
   - Row cap: `10,000,000`
   - Purpose: real four-engine benchmark where scan and cache behavior is more visible.
   - Add or update a separate config, for example `configs/benchmark-kpi-10m.yml`.

The project must keep the 10k sample path because it is fast enough for development verification. Documentation must make clear that small validation results are not suitable for judging final engine performance.

## Four Query Routes

### Spark Iceberg

Uses Spark SQL against the Iceberg table stored in HDFS.

Cold restart:

- Restart `spark`.

### StarRocks Internal

Uses StarRocks native/internal table loaded through stream load.

Cold restart:

- Restart `starrocks-fe`.
- Restart `starrocks-be`.

### StarRocks External Iceberg

Uses StarRocks external Iceberg catalog over Hive Metastore and HDFS.

Cold restart:

- Restart `starrocks-fe`.
- Restart `starrocks-be`.

Hive Metastore and HDFS are not restarted for this route, because doing so adds metadata and filesystem recovery noise and can destabilize the benchmark.

### Hive HDFS Parquet

Add a Hive query route that reads the generated HDFS Parquet dataset through a Hive external table.

Required infrastructure:

- Add HiveServer2 service to Docker Compose.
- Keep the existing Hive Metastore service.
- Create a Hive external table over the generated Parquet path.
- Execute KPI SQL through JDBC or Beeline.

Cold restart:

- Restart `hive-server`.

## Execution Model

Execution is sequential and deterministic.

For each query:

1. For each route in the fixed route order:
   - Spark Iceberg
   - StarRocks Internal
   - StarRocks External Iceberg
   - Hive HDFS Parquet
2. Restart that route's related services.
3. Wait until the route is query-ready.
4. Execute the query once and record `cold`.
5. Execute the same query again and record `warm`.
6. Execute the same query again and record `hot`.

If a cold restart or readiness check fails, record that route/query cell as failed with an actionable error. Continue with remaining routes and queries when possible so the report still explains partial failures.

## Readiness Checks

Each route must have an explicit readiness check after restart:

- Spark Iceberg: Spark container is running and `spark-sql` can execute a trivial statement.
- StarRocks routes: MySQL protocol accepts `SELECT 1`.
- Hive HDFS Parquet: HiveServer2 accepts `SELECT 1`.
- HDFS/Hive Metastore: existing startup checks remain in Compose before benchmark begins.

Readiness checks should use bounded retries and clear timeout errors.

## Data Model

The current report model stores one summary row per query result. It must be extended so one route/query can carry three phase timings.

Required route timing fields:

- `coldMs`
- `warmMs`
- `hotMs`
- `coldStatus`
- `warmStatus`
- `hotStatus`
- `rows`
- `error`

The implementation may keep detailed per-execution records internally, but the generated static report must embed enough data to render cold/warm/hot values directly.

The report schema version must be incremented from the current v2 because the performance matrix shape changes.

## Report Requirements

The static HTML report remains file-open safe:

- Data is embedded into `index.html`.
- No required `report.json`.
- No required API call.
- No required Grafana or Prometheus.

Performance matrix:

- Row key: dataset + query set + SQL/query name.
- Columns:
  - Spark Iceberg
  - StarRocks Internal
  - StarRocks External Iceberg
  - Hive HDFS Parquet
  - Best route
- Each route cell shows:
  - `cold`
  - `warm`
  - `hot`
  - status/error
  - rows
- Best route is selected by lowest successful `hotMs`.

The report should make the dataset profile visible:

- Dataset name/id.
- Row count.
- Cells/days/columns.
- Whether it is the 10k validation dataset or the 10m formal benchmark dataset.

## Docker Compose Resources

Current Docker Desktop resources observed during design:

- Docker CPUs: `32`
- Docker memory: about `15.5 GB`
- Host memory: about `31.7 GB`
- Host free memory at design time: about `9.1 GB`

Compose should add explicit resource limits to reduce noisy resource contention.

Default resource allocation:

| Service | CPU Limit | Memory Limit |
| --- | ---: | ---: |
| `starrocks-fe` | 2 | 2 GB |
| `starrocks-be` | 6 | 5 GB |
| `spark` | 6 | 3 GB |
| `hive-metastore` | 1 | 1 GB |
| `hive-server` | 2 | 2 GB |
| `hdfs-namenode` | 1 | 768 MB |
| `hdfs-datanode` | 2 | 1.5 GB |
| `benchmark-runner` | 2 | 1 GB |

The total memory limit is intentionally close to, but below, the observed Docker Desktop memory budget. Documentation must note that larger formal runs may benefit from increasing Docker Desktop memory and then raising service limits.

## Configuration

Keep existing query config fields:

- `coldRuns`
- `warmRuns`
- `concurrency`

For this design:

- `coldRuns` should be interpreted as enabled cold phase count and must be `1` for compose precision mode.
- `warmRuns` should be interpreted as two follow-up phases, `warm` and `hot`, and should be `2` for compose precision mode.
- If values differ, the runner should fail clearly or normalize through documented config behavior. Prefer clear failure to silent reinterpretation.
- `concurrency` must be `1` for this mode.

Add documentation examples:

- Small validation command using 10k config.
- Formal 10m benchmark command.

## Testing

Unit tests:

- Phase aggregation maps cold/warm/hot timings into route cells.
- Best route uses `hotMs`.
- Generic `engine + tableShape` route mapping continues to work.
- Hive route appears in schema and frontend types.
- Invalid precision config values fail with clear messages.

Frontend tests:

- Matrix renders four route columns.
- Each route cell shows cold/warm/hot values.
- Empty/error cells render clearly.
- Dataset profile information is visible.

Integration/smoke tests:

- 10k validation compose benchmark can produce a static report.
- Report contains four routes in the embedded data.
- No `report.json` is required.

Manual validation:

- Run 10k validation benchmark by default.
- Optionally run 10m formal benchmark when Docker resources are sufficient.

## Acceptance Criteria

- A 10k validation run completes and generates a static HTML report.
- The performance matrix has four route columns.
- For each successful query/route, the report shows `cold`, `warm`, and `hot`.
- StarRocks Internal and External Iceberg can be compared by cold/warm/hot values without relying on hidden assumptions.
- Compose services have explicit CPU and memory limits matching the documented allocation.
- The 10m formal benchmark config exists but is not the default validation path.

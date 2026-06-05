# Spark KPI Data Generation Design

## Context

The current KPI dataset generator is a Java local-process loop that writes one Parquet file through `KpiDataGenerator`. It is acceptable for 10k smoke data, but the 10m validation already spends most of the run in local generation, and it will not scale to 1b rows.

The benchmark environment already includes Spark, HDFS, Hive, and StarRocks. Keeping KPI generation in a local Java loop duplicates work that Spark can parallelize and also creates a maintenance split between generated data and the engines that consume it.

## Decision

Use one Spark-based KPI generator for all KPI sizes.

Local mode will use Spark with `local[*]` and write local Parquet files under the configured dataset output path. Compose mode will submit the same generation job through the Spark container and write generated Parquet to the mounted workspace path so the existing Spark Iceberg, StarRocks CSV export, external Iceberg, and Hive HDFS publish/load stages can keep consuming `DatasetResult`.

The old Java row-by-row Parquet writer will be removed from active KPI paths after the Spark generator is in place. TPC-H generation remains unchanged.

## Goals

- Generate KPI Parquet through Spark for `generate`, `run --mode local`, and `run --mode compose`.
- Preserve the current CLI shape and report semantics:
  - `generate` prints rows, bytes, and output.
  - `run` reports a `GENERATE` load stage.
  - generated reports continue to show dataset rows and bytes written.
- Preserve deterministic KPI data from `seed`, `cells`, `days`, `startTime`, and `rowCap`.
- Support small smoke datasets through Spark local mode without Docker.
- Support 10m and 1b row targets through Spark partitioned generation.
- Keep downstream route behavior unchanged: Spark Iceberg load, StarRocks internal CSV export/load, StarRocks external Iceberg refresh, and Hive HDFS Parquet publish/load.

## Non-Goals

- Do not rewrite TPC-H generation.
- Do not change the benchmark query catalog.
- Do not change report UI behavior except for generated stage metadata if needed.
- Do not introduce a separate Scala or Python Spark application.
- Do not directly write Iceberg during generation; route loads must remain separate for fair comparison.

## Architecture

### Components

- `KpiDatasetGenerator` interface: a small boundary for generating KPI `DatasetResult` from `BenchmarkConfig`.
- `SparkKpiDataGenerator`: default generator used by CLI, local runner, and compose runner.
- `SparkKpiGenerationJob`: builds the Spark dataset using Spark Java APIs and writes partitioned Parquet.
- `SparkSubmitKpiDataGenerator`: compose adapter that runs the same Java jar inside the Spark container with a generation subcommand or job mode.
- `KpiGenerationConfig`: derived settings such as output path, target rows, partition count, master, and file sizing.

### Data Flow

1. CLI loads `BenchmarkConfig`.
2. KPI generation resolves `targetRows = min(cells * days * 1440, rowCap)` when `rowCap` exists.
3. Spark creates a `Dataset<Row>` from `spark.range(0, targetRows, 1, partitions)`.
4. KPI columns are derived from `id`, `seed`, `cells`, and `startTime` using deterministic Spark SQL expressions.
5. Spark writes Snappy Parquet partitioned by `event_date`.
6. Java returns `DatasetResult(outputPath, parquetFiles, rows, bytesWritten)`.
7. Existing load stages consume `DatasetResult`.

## Determinism

The generated schema and field names must match `KpiSchema`.

Column values should be deterministic without relying on mutable per-row Java `Random`. Use Spark expressions based on stable hashes or seeded `rand(seed + columnOffset)`. Dimension columns must remain compatible with existing benchmark queries:

- `cell_id`: `CELL-%06d` from `id % cells`.
- `province`, `city`, `district`, `grid_id`, `site_id`: same modulo cardinality as current generator.
- `vendor`, `rat`, `band`: same categorical domains as current generator.
- `event_time`: `startTime + floor(id / cells) minutes`.
- `event_date`: date derived from `event_time`.

Metric distributions do not need bit-for-bit parity with the old local Java generator. They must stay in the same value ranges and keep internal consistency, for example success counts must not exceed attempt counts.

## Scale Strategy

Default partition count should be derived from target rows and remain overrideable:

- 10k rows: 1-4 partitions.
- 10m rows: about 64 partitions.
- 1b rows: about 1024 partitions.

The generator should expose config fields under `dataset.spark` or a similarly scoped config object:

```yaml
dataset:
  rowCap: 1000000000
  spark:
    master: "local[*]"
    partitions: 1024
    rowsPerPartition: 1000000
    outputMode: "overwrite"
```

If fields are absent, use conservative defaults that keep existing configs valid.

## Compose Behavior

Compose mode should submit generation through the existing `spark` service. The benchmark runner can call `docker compose exec spark ...` using the existing command infrastructure.

The output path remains workspace-mounted so Java can enumerate generated Parquet files and StarRocks CSV export can read them. HDFS publishing remains a separate existing step.

For 1b rows, generated data should be written directly by Spark with parallel tasks. The Java runner should not materialize rows in memory.

## Error Handling

- Invalid row targets, cells, days, and partitions fail before Spark starts.
- Spark command failure returns a failed `GENERATE` stage and writes a degraded report, matching current compose behavior.
- If Spark writes no Parquet files, generation fails with a clear message.
- If the report cannot compute bytes written, generation fails rather than silently reporting zero bytes for a successful run.

## Testing

Use TDD for implementation.

Required test coverage:

- Target row calculation uses `rowCap` and full shape correctly.
- Spark generation config defaults preserve existing YAML compatibility.
- Local Spark generator writes Parquet with expected row count, event date partition, schema columns, and deterministic sample values.
- Compose runner uses Spark generator instead of `KpiDataGenerator`.
- `generate` command goes through the same generator factory as local run mode.
- Existing smoke benchmark tests still pass.

Manual verification:

- `mvn test`
- `mvn package`
- Local Spark smoke generation with 10k rows.
- Compose Spark generation with 10k rows.
- Optional 10m run before attempting 1b.

## Documentation

Update README to state that KPI data generation uses Spark for both local and compose modes, with examples for:

- 10k local Spark smoke generation.
- 10m compose benchmark.
- 1b generation configuration and expected resource considerations.

## Acceptance Criteria

- No active KPI path uses the old Java row-by-row generator.
- A 10k local run succeeds using Spark local mode.
- A 10k compose run succeeds using Spark container generation.
- Existing 10m config remains valid.
- A new 1b config exists and can start generation with partition settings suitable for large output.
- Reported generated row count and bytes are accurate.

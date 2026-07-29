# Iceberg Validation Module Design

## Purpose

Add an independent Apache Iceberg validation module to verify Iceberg table capabilities outside the existing StarRocks versus Iceberg benchmark flow. The module focuses on correctness, operational behavior, and performance evidence for:

1. Long-term schema evolution.
2. HDFS erasure coding with fault tolerance.
3. Multi-process concurrent writes.
4. Row-level update and delete.
5. ACID transaction guarantees.
6. Incremental pull.
7. Time travel.
8. Small-file compaction under many snapshots.

The target Iceberg version is `1.10.1`. Spark jobs should use the Iceberg Spark runtime and Spark SQL extensions for Iceberg `1.10.1`, subject to dependency resolution in implementation.

This module is not a StarRocks performance comparison suite. It may reuse shared infrastructure, command execution, and report writing utilities, but it owns its scenario definitions, result model, and conclusions.

## Existing Context

The current project already provides:

- Shared infrastructure integration through `../shared-data-infra`.
- HDFS, Hive Metastore, Spark, and StarRocks services outside this repository.
- Spark Iceberg table creation and query paths for benchmark runs.
- Command execution helpers through `CommandRunner`.
- Report generation foundations under `com.example.databenchmark.report`.

The new module should reuse those foundations where they are generic and avoid coupling to `ComposeBenchmarkRunner` route comparison semantics.

## Command Surface

Add a new CLI subcommand:

```text
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar iceberg-validate \
  --config configs/iceberg-validation.yml \
  --run-id iceberg-validation-20260728-001
```

The command loads an `IcebergValidationConfig`, starts selected scenarios, records evidence, writes JSON and HTML reports, and returns nonzero if any required scenario fails.

Initial options:

- `--config`: scenario and scale configuration.
- `--run-id`: stable run identifier.
- `--scenario`: optional repeatable filter for one or more scenario names.
- `--case`: optional repeatable filter for one or more case IDs.
- `--keep-artifacts`: preserve tables, HDFS paths, generated scripts, and logs for debugging.

## Package Layout

```text
com.example.databenchmark.iceberg
  IcebergValidateCommand
  IcebergValidationConfig
  IcebergValidationRunner
  IcebergValidationScenario
  IcebergValidationCase
  IcebergValidationReport
  IcebergValidationResult
  IcebergConclusion

com.example.databenchmark.iceberg.scenario
  SchemaEvolutionScenario
  ErasureCodingScenario
  ConcurrentWriteScenario
  RowLevelMutationScenario
  AcidTransactionScenario
  IncrementalPullScenario
  TimeTravelScenario
  SmallFileCompactionScenario

com.example.databenchmark.iceberg.sql
  IcebergSqlTemplates
  SparkSqlScriptBuilder
  MetadataTableQueries

com.example.databenchmark.iceberg.hdfs
  HdfsEcPolicyClient
  HdfsUsageCollector
  HdfsFaultInjector
```

Each scenario implements one interface:

```java
interface IcebergValidationScenario {
    String name();
    List<IcebergValidationCase> cases(IcebergValidationConfig config);
    IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) throws Exception;
}
```

Scenarios must not share mutable table state unless a case explicitly depends on previous steps. Each case uses a run-scoped table name.

## Runtime Isolation

All validation tables use a separate namespace:

```text
iceberg_catalog.iceberg_validation.<scenario>_<case_id>_<run_id>
hdfs://hdfs-namenode:8020/warehouse/iceberg/iceberg_validation/<scenario>/<case_id>/<run_id>
```

The runner creates and drops only resources under the `iceberg_validation` namespace and run-scoped HDFS paths.

Default cleanup:

- Drop validation tables.
- Remove run-scoped HDFS scratch paths.
- Preserve final JSON and HTML reports.

When `keepArtifacts` is true:

- Do not drop tables.
- Do not remove HDFS paths.
- Preserve generated Spark SQL scripts and command logs.

## Configuration

Initial config shape:

```yaml
iceberg:
  version: "1.10.1"
  catalog: "iceberg_catalog"
  namespace: "iceberg_validation"
  warehouse: "hdfs://hdfs-namenode:8020/warehouse/iceberg"
  formatVersion: 2

spark:
  service: "spark"
  timeoutSeconds: 900
  packages:
    - "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1"
  extensions:
    - "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

hdfs:
  defaultFs: "hdfs://hdfs-namenode:8020"
  replicationBaseline: 2
  ecPolicies:
    - "RS-3-2-1024k"
    - "RS-6-3-1024k"
    - "RS-10-4-1024k"
    - "XOR-2-1-1024k"

scale:
  profile: "smoke"
  rows: 100000
  partitions: 8
  smallFileCommits: 100
  filesPerCommit: 4
  concurrentWriters: [2, 4, 8]

scenarios:
  schemaEvolution:
    enabled: true
  erasureCoding:
    enabled: true
  concurrentWrite:
    enabled: true
  rowLevelMutation:
    enabled: true
  acidTransaction:
    enabled: true
  incrementalPull:
    enabled: true
  timeTravel:
    enabled: true
  smallFileCompaction:
    enabled: true

report:
  output: "reports/iceberg-validation"
  formats: ["json", "html"]
```

## Report Model

Each case writes structured evidence:

```text
scenario
caseId
purpose
dataScale
setupCommands
actionCommands
assertions
metrics
baseline
comparison
functionStatus
performanceStatus
conclusion
evidence
errors
```

Status values:

- `PASS`: required functional assertions passed.
- `FAIL`: required functional assertions failed.
- `SKIPPED`: unsupported or unavailable infrastructure capability.
- `DEGRADED`: functionally passed but performance or operational thresholds were breached.

Performance conclusion values:

- `GOOD`: clear improvement or no material overhead.
- `ACCEPTABLE`: overhead exists but remains within configured thresholds.
- `DEGRADED`: overhead exceeds configured thresholds or requires maintenance before production use.
- `NOT_COMPARABLE`: no valid baseline or scenario is capability-only.

Every conclusion must include direct evidence: SQL result, snapshot ID, schema ID, file count, manifest count, HDFS usage, command duration, or injected-failure observation.

## Common Metrics

All scenarios collect these common metrics where applicable:

- Spark SQL command duration.
- Commit duration.
- Planning duration when measurable.
- Query latency for baseline and target reads.
- Row count before and after action.
- Current snapshot ID.
- Snapshot count.
- Schema ID and schema history length.
- Data file count.
- Delete file count.
- Manifest count.
- Metadata JSON file count.
- Logical table bytes from Iceberg metadata tables.
- HDFS disk usage from `hdfs dfs -du -s`.
- HDFS file count from `hdfs dfs -count`.
- Error class and message for failed commits.

## Scenario 1: Long-Term Schema Evolution

Goal: verify that long-running schema changes across primitive, numeric, and nested types preserve historical data readability and semantic compatibility.

Cases:

| Case ID | Description | Required assertions | Performance conclusion |
| --- | --- | --- | --- |
| `schema-add-drop-rename` | Create a table, append baseline data, repeatedly add, drop, and rename columns. | Current snapshot reads new schema; old snapshot reads old schema; dropped columns are not exposed in current schema; renamed fields retain values by field ID. | Compare projection query latency before and after 20/50/100 schema changes; report metadata growth and schema history size. |
| `schema-type-promotion` | Apply compatible type promotions: `int -> long`, `float -> double`, decimal precision expansion. | Historical rows read correctly through promoted types; aggregates match expected values; incompatible promotions are rejected. | Measure query latency and commit duration per promotion; conclude whether type promotion adds meaningful read overhead. |
| `schema-nested-struct` | Add, drop, and rename nested struct fields. | Nested field IDs remain stable; old data can be projected with nulls for new fields; renamed nested fields preserve values. | Compare nested projection latency across snapshots and schema versions. |
| `schema-complex-types` | Evolve map/list/struct columns by adding nested fields and reading mixed-version records. | Spark reads mixed-schema records consistently; null/default handling is explicit in result checks. | Report read latency and metadata size after complex type evolution. |
| `schema-long-chain-history` | Run a configured long chain of schema updates, appending rows after each update. | Every retained snapshot can be queried; row counts per generation match expected data. | Report DDL commit latency distribution, schema history length, metadata JSON count, and query latency trend. |

Evidence:

- `DESCRIBE TABLE`.
- Iceberg metadata table rows for snapshots, history, and files.
- Query results for current and historical snapshots.
- Schema JSON fragments when available.

## Scenario 2: HDFS Erasure Coding

Goal: verify Iceberg table behavior on HDFS replication baseline and multiple EC policies, including data availability under tolerated failures.

Baseline:

- HDFS `REPLICATION` policy.
- `dfs.replication=2`.
- Same dataset size, partition count, and table schema as each EC policy run.

EC policies:

- `RS-3-2-1024k`.
- `RS-6-3-1024k`.
- `RS-10-4-1024k`.
- `XOR-2-1-1024k`.
- Optional custom policies if shared infrastructure enables them.

Cases:

| Case ID | Description | Required assertions | Performance conclusion |
| --- | --- | --- | --- |
| `ec-policy-write-read` | Write identical Iceberg tables under replication=2 and each EC policy. | Table row counts match; EC policy is visible on data file paths; query results match baseline. | Compare write duration, read duration, data file count, manifest count, logical bytes, HDFS disk usage, and space amplification. |
| `ec-rs-10-4-failure-tolerance` | Use `RS-10-4-1024k`, inject tolerated data/parity block unavailability by stopping or isolating DataNodes within the allowed parity window. | Table remains readable; row counts and checksums match baseline; HDFS reports expected degraded state. | Report read latency under healthy and degraded conditions; conclude whether availability is preserved with acceptable degradation. |
| `ec-policy-matrix-failure` | Repeat fault injection for `RS-3-2`, `RS-6-3`, `RS-10-4`, and `XOR-2-1` within each policy's tolerance. | Reads succeed within tolerance; reads fail or are skipped beyond safe tolerance depending on cluster size. | Compare degraded-read latency and recovery time per policy. |
| `ec-file-count-and-disk-usage` | Generate equal logical data under all policies. | Logical rows and query checksums match. | Report file count and HDFS disk usage, not just logical bytes; compare against replication=2. |

Fault injection must be conservative. It must stop or isolate entire DataNode containers only when the shared cluster has enough DataNodes for the target policy. If the local shared infrastructure has too few DataNodes, these cases are marked `SKIPPED` with an explicit reason instead of faking EC tolerance.

## Scenario 3: EC and Replication Conversion

Goal: measure operational cost of converting data layout between replication and EC.

Important behavior: setting an EC policy on a directory affects new files under that directory. Existing files do not physically convert just because the directory policy changes. Physical conversion must rewrite or copy data files.

Cases:

| Case ID | Description | Required assertions | Performance conclusion |
| --- | --- | --- | --- |
| `replication-to-ec-policy-only` | Change a table location or target directory from replication=2 to an EC policy without rewriting existing files. | Existing files retain original policy; new files use EC. | Report policy command duration and file policy distribution; conclude that policy-only conversion is metadata/config-only for new writes. |
| `replication-to-ec-rewrite` | Start with replication=2 table, rewrite data into an EC policy directory through Iceberg rewrite or table copy. | Rewritten table has same row count and checksum; data files show EC policy. | Measure conversion duration, throughput MB/s, file count before/after, HDFS disk usage before/after, and post-conversion query latency. |
| `ec-to-replication-policy-only` | Change an EC directory to `REPLICATION` for new files. | Existing EC files remain EC; new files are replication. | Report policy command duration and mixed file policy distribution. |
| `ec-to-replication-rewrite` | Rewrite or copy EC-backed table into replication=2 layout. | Converted table matches checksum; new files use replication=2. | Measure conversion duration, throughput MB/s, disk usage increase, and query latency change. |

Conversion conclusions must separate:

- Policy change efficiency.
- Physical rewrite efficiency.
- Query performance after conversion.
- Storage cost after conversion.

## Scenario 4: Multi-Process Concurrent Writes

Goal: verify Iceberg optimistic concurrency behavior and measure write throughput under multi-process Spark append/update pressure.

Cases:

| Case ID | Description | Required assertions | Performance conclusion |
| --- | --- | --- | --- |
| `concurrent-append-disjoint-partitions` | Launch 2/4/8 Spark processes appending to disjoint partitions. | All successful commits are visible; total row count equals sum of committed batches; no duplicate commit IDs. | Report aggregate throughput, p50/p95 commit latency, retry count, and failed writer count. |
| `concurrent-append-same-partition` | Launch 2/4/8 Spark processes appending to the same partition. | Successful commits preserve all rows; conflicts retry or fail cleanly. | Compare commit latency and conflict rate with disjoint partition baseline. |
| `concurrent-update-overlap` | Launch concurrent `MERGE INTO` or `UPDATE` operations on overlapping row ranges. | At most one conflicting write succeeds when conflicts cannot be reconciled; failed commits do not leak partial data. | Report conflict detection time and retry behavior; conclude whether workload needs partitioning or writer serialization. |
| `concurrent-mixed-read-write` | Run readers while writers append. | Readers see a consistent snapshot; no partial commit is visible. | Report read latency during write pressure compared with idle baseline. |

## Scenario 5: Row-Level Update and Delete

Goal: validate Iceberg row-level mutation behavior through Spark SQL extensions.

Cases:

| Case ID | Description | Required assertions | Performance conclusion |
| --- | --- | --- | --- |
| `row-update-single-range` | Run `UPDATE` on a narrow row range. | Updated rows match expected values; unaffected rows remain unchanged. | Report update duration, rewritten data file count, delete file count, and query latency after update. |
| `row-delete-partition-prunable` | Delete an entire partition or day. | Row count drops by exact partition size; historical snapshot still sees deleted rows. | Determine whether delete is metadata-only; report commit latency and metadata changes. |
| `row-delete-selective` | Delete sparse rows across multiple files. | Deleted rows are invisible in current snapshot; historical snapshot is unchanged. | Report delete file count, scan latency before/after, and compaction need. |
| `row-merge-upsert-delete` | Use `MERGE INTO` with update, insert, and delete branches. | Final row state matches source operations; duplicate source keys fail as expected. | Report merge duration and rewritten bytes. |

## Scenario 6: ACID Transaction Guarantees

Goal: prove atomicity, consistency, isolation, and durable snapshot publication under failures and conflicts.

Cases:

| Case ID | Description | Required assertions | Performance conclusion |
| --- | --- | --- | --- |
| `acid-kill-before-commit` | Kill a Spark writer before commit. | Table remains at previous snapshot; orphan files, if any, are not committed. | Report failure detection time and cleanup requirements. |
| `acid-kill-during-commit` | Inject failure near commit publication where feasible. | Table is either old snapshot or complete new snapshot; no half-visible data. | Report snapshot lineage and recovery behavior. |
| `acid-conflicting-commits` | Run conflicting overwrite/update commits. | One commit succeeds or retries; failed commit does not corrupt table metadata. | Report retry count, final snapshot ID, and conflict error. |
| `acid-reader-isolation` | Hold long-running read while writer commits. | Reader returns one consistent snapshot; subsequent read sees new snapshot. | Report read latency and snapshot IDs observed. |

## Scenario 7: Incremental Pull

Goal: validate snapshot-based incremental consumption semantics and quantify the benefit versus full scans.

Cases:

| Case ID | Description | Required assertions | Performance conclusion |
| --- | --- | --- | --- |
| `incremental-append-only` | Capture snapshot A, append data, capture snapshot B, read only appended data. | Incremental rows equal appended batch; checksum matches. | Compare full scan bytes/time with incremental scan bytes/time and report saving ratio. |
| `incremental-multi-snapshot-window` | Append through multiple snapshots and pull a window from snapshot A to snapshot N. | Returned rows equal all appends in the window. | Report planning time versus number of snapshots in the window. |
| `incremental-with-delete-update-boundary` | Include update/delete between snapshots. | Report the exact semantics supported by Spark/Iceberg 1.10.1 for the chosen API; append-only validation must not be misrepresented as CDC. | Conclude whether this path is suitable for append-only sync or CDC-style sync. |
| `incremental-expired-snapshot` | Expire old snapshots and attempt incremental read from an expired base. | Read fails with clear reason or is skipped when unsupported. | Report retention requirement for incremental consumers. |

## Scenario 8: Time Travel

Goal: verify historical snapshot reads and retention behavior.

Cases:

| Case ID | Description | Required assertions | Performance conclusion |
| --- | --- | --- | --- |
| `time-travel-by-snapshot-id` | Query earlier snapshot by snapshot ID after appends and mutations. | Historical result matches expected row count and checksum. | Compare historical query latency with current snapshot latency. |
| `time-travel-by-timestamp` | Query table as of a timestamp between commits. | Result maps to the expected snapshot. | Report timestamp resolution and query latency. |
| `time-travel-after-schema-evolution` | Query old snapshots after schema evolution. | Historical schema and current projection behavior are correct. | Report additional planning/query cost when resolving older schema. |
| `time-travel-after-expire` | Expire snapshots and query an expired snapshot. | Expired snapshot is unavailable; retained snapshots remain queryable. | Report retention cutoff and maintenance effect. |

## Scenario 9: Small Files, Many Snapshots, and Compaction

Goal: measure how small files and snapshot accumulation affect Iceberg planning and query performance, then quantify compaction and metadata maintenance benefits.

Cases:

| Case ID | Description | Required assertions | Performance conclusion |
| --- | --- | --- | --- |
| `small-files-many-snapshots-build` | Generate 10/100/500/1000 small commits, each producing several small files and one snapshot. | Row count and checksum match expected total; snapshot count and file count match generated plan. | Report snapshot count, data file count, manifest count, metadata JSON count, planning time, and query latency trend. |
| `small-files-query-degradation` | Run fixed filter, aggregation, and full scan queries before maintenance. | Query results are correct. | Correlate query latency with snapshot count, file count, and manifest count. |
| `small-files-data-compaction` | Run Iceberg `rewrite_data_files`. | Row count and checksum remain unchanged; data file count decreases. | Report compaction duration, rewritten files, rewritten bytes, HDFS disk usage before/after, and query improvement. |
| `small-files-manifest-rewrite` | Run manifest rewrite after many snapshots/small files. | Manifest count decreases or is reorganized; data remains unchanged. | Report metadata planning improvement separately from data scan improvement. |
| `small-files-expire-snapshots` | Expire old snapshots after compaction. | Retained snapshots remain queryable; expired snapshots are unavailable. | Report metadata JSON count, HDFS disk usage change, and planning latency change. |

The report must distinguish data-file compaction, manifest rewrite, and snapshot expiration. These are different maintenance operations and should not be collapsed into one "compaction" number.

## Scenario Execution Flow

Each case follows the same lifecycle:

1. Preflight: verify Spark, Hive Metastore, HDFS, and Iceberg package configuration.
2. Create isolated namespace and table path.
3. Create table and seed baseline data.
4. Collect baseline metadata and HDFS usage.
5. Execute scenario action.
6. Run correctness assertions.
7. Collect post-action metadata and HDFS usage.
8. Run performance probes.
9. Compare against baseline.
10. Write result and evidence.
11. Cleanup unless `keepArtifacts` is true.

## HDFS EC Preflight and Safety

The EC scenario must inspect the live HDFS cluster before running:

- List enabled EC policies.
- Verify the requested policy is enabled.
- Count live DataNodes.
- Check whether the policy can be meaningfully tested with available DataNodes.
- Verify the runner has permission to set and unset EC policies.
- Verify replication baseline can be set to 2.

Fault injection rules:

- Never delete HDFS metadata or shared infrastructure volumes.
- Prefer stopping DataNode containers through the shared compose project over destructive block manipulation.
- Only run a failure case when the cluster has enough DataNodes for the target EC policy.
- Always restart stopped services and wait for health after the case.
- If the cluster cannot support a policy, record `SKIPPED` with evidence from HDFS policy and DataNode checks.

## Performance Conclusion Thresholds

Default thresholds should be configurable, but initial reporting can use these conservative labels:

- `GOOD`: target is at least 20 percent faster, uses at least 20 percent less disk, or has no more than 10 percent latency overhead while adding a required capability.
- `ACCEPTABLE`: target overhead is within 50 percent and all correctness assertions pass.
- `DEGRADED`: target overhead exceeds 50 percent, commit conflict rate exceeds configured limits, metadata growth causes measurable planning slowdown, or maintenance is required before the workload is viable.
- `NOT_COMPARABLE`: capability verification has no meaningful baseline.

The report must include raw numbers so the conclusion can be recalibrated later.

## Error Handling

Errors are case-scoped. One failed case should not abort the full run unless the case is marked `requiredForSuite`.

Scenario failures are grouped as:

- `INFRA_UNAVAILABLE`: Spark, HDFS, Hive Metastore, or Docker compose unavailable.
- `UNSUPPORTED_FEATURE`: Iceberg, Spark, or HDFS version does not expose required capability.
- `ASSERTION_FAILED`: functional correctness failed.
- `PERFORMANCE_DEGRADED`: correctness passed but threshold failed.
- `COMMAND_FAILED`: command execution failed unexpectedly.
- `CLEANUP_FAILED`: test completed but cleanup failed.

If cleanup fails, the report must include the table name, HDFS path, and suggested cleanup command.

## Testing Strategy

Unit tests:

- Config parsing and validation.
- Scenario registration and filtering.
- SQL template generation.
- Result model serialization.
- Conclusion threshold classification.
- Metadata parsing for snapshots, files, manifests, and HDFS usage.
- EC preflight skip decisions.

Integration tests:

- Use fake `CommandRunner` scripts to simulate Spark and HDFS outputs.
- Verify each scenario records expected evidence.
- Verify failure and skip cases are reported without aborting unrelated scenarios.

Manual compose verification:

- Run smoke profile against shared infrastructure.
- Run EC preflight and ensure unsupported EC policies are skipped cleanly on small local clusters.
- Run at least one complete schema evolution, time travel, and small-file compaction scenario.

## Implementation Boundaries

In scope:

- New `iceberg-validate` CLI subcommand.
- Iceberg validation config.
- Scenario framework.
- Spark SQL and HDFS command execution adapters.
- JSON and HTML validation reports.
- Scenario implementations for the listed cases.
- Upgrade or configure Iceberg runtime package to `1.10.1` for this validation module.

Out of scope for the first implementation:

- StarRocks external Iceberg validation.
- A full frontend report UI.
- Production-scale capacity planning.
- Destructive HDFS block-level corruption tests.
- Automatic changes to shared infrastructure service definitions unless a missing EC capability requires a documented shared-infra update.

## References

- Apache Iceberg 1.10.1 Spark writes: https://iceberg.apache.org/docs/1.10.1/spark-writes/
- Apache Iceberg 1.10.1 schema evolution: https://iceberg.apache.org/docs/1.10.1/evolution/
- Apache Iceberg 1.10.1 maintenance: https://iceberg.apache.org/docs/1.10.1/maintenance/
- Apache Iceberg 1.10.1 Spark procedures: https://iceberg.apache.org/docs/1.10.1/spark-procedures/
- Apache Iceberg 1.10.1 Spark queries: https://iceberg.apache.org/docs/1.10.1/spark-queries/
- Apache Iceberg 1.10.1 Spark structured streaming: https://iceberg.apache.org/docs/1.10.1/spark-structured-streaming/
- HDFS Erasure Coding: https://hadoop.apache.org/docs/r3.0.3/hadoop-project-dist/hadoop-hdfs/HDFSErasureCoding.html

## Self-Review Notes

- No implementation is included in this design.
- The module is independent from the existing StarRocks comparison runner.
- EC cases explicitly skip unsupported local topologies instead of pretending to validate tolerance.
- Policy-only EC changes are separated from physical rewrite conversion.
- Small-file compaction separates data file rewrite, manifest rewrite, and snapshot expiration.

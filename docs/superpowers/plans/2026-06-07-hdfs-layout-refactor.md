# HDFS Layout Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Standardize shared HDFS paths around `/services/<service-or-project>/<data-type>` and remove legacy `/data/generated` and `/benchmark` benchmark outputs.

**Architecture:** Keep `/warehouse/iceberg` for Hive Metastore / Iceberg managed tables. Move service-owned generated data under `/services`, where the second segment is the service or project name and the third segment is the data type. `data-benchmark` generated parquet uses `/services/data-benchmark/generated/...`.

**Tech Stack:** HDFS, Hive Metastore, Spark Iceberg, StarRocks external Iceberg catalog, Java/Maven, Docker Compose.

---

## Target HDFS Contract

```text
/warehouse/iceberg/<database>/<table>
/services/<service-or-project>/<data-type>/...
/user
/tmp
/workspace
```

`data-benchmark` paths:

```text
/services/data-benchmark/generated/kpi/compose-smoke
/services/data-benchmark/generated/kpi/kpi-1b
/services/data-benchmark/generated/kpi/default
```

Legacy paths to remove:

```text
/data
/benchmark
```

## Implementation Checklist

- [x] Update tests to expect `/services/data-benchmark/generated/...`.
- [x] Update `../shared-data-infra/compose.lakehouse.yaml` `hdfs-init` to create `/warehouse/iceberg` and `/services/data-benchmark/generated`.
- [x] Update compose HDFS configs:
  - `configs/benchmark-compose-smoke.yml`
  - `configs/benchmark-kpi-1b.yml`
- [x] Update `ComposeBenchmarkRunner` fallback for relative local output so compose publishes to `/services/data-benchmark/generated/kpi/default`.
- [x] Update README documentation in both repos.
- [ ] Delete legacy HDFS paths `/data` and `/benchmark` after confirming they only contain reproducible benchmark data.
- [ ] Create `/services/data-benchmark/generated` in HDFS and set benchmark-compatible permissions.
- [ ] Run focused tests.
- [ ] Run compose config validation for `data-benchmark` and `shared-data-infra`.
- [ ] Run `mvn package`.
- [ ] Run compose smoke and verify generated parquet lands under `/services/data-benchmark/generated/kpi/compose-smoke`.

## Verification Commands

```powershell
mvn "-Dtest=SharedInfraTopologyTest,BenchmarkConfigLoaderTest,SparkSubmitKpiDataGeneratorTest,KpiGenerationConfigTest,ComposeBenchmarkRunnerTest,SparkIcebergClientTest,HiveClientTest,SqlTemplatesTest,StarRocksClientTest" test
docker compose -f docker-compose.yml config
cd D:\agent-code\shared-data-infra
docker compose -f compose.yaml -f compose.lakehouse.yaml --profile lakehouse config
cd D:\agent-code\data-benchmark
mvn package
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-compose-smoke.yml --run-id compose-smoke-services-layout-check
```

## HDFS Cleanup Commands

```powershell
docker compose -f ..\shared-data-infra\compose.yaml -f ..\shared-data-infra\compose.lakehouse.yaml --profile lakehouse exec -T namenode /opt/hadoop-3.2.1/bin/hdfs dfs -rm -f -r -skipTrash /data /benchmark
docker compose -f ..\shared-data-infra\compose.yaml -f ..\shared-data-infra\compose.lakehouse.yaml --profile lakehouse exec -T namenode bash -lc "/opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p /warehouse/iceberg /services/data-benchmark/generated && /opt/hadoop-3.2.1/bin/hdfs dfs -chmod 777 /warehouse /warehouse/iceberg /services /services/data-benchmark /services/data-benchmark/generated"
docker compose -f ..\shared-data-infra\compose.yaml -f ..\shared-data-infra\compose.lakehouse.yaml --profile lakehouse exec -T namenode /opt/hadoop-3.2.1/bin/hdfs dfs -ls /
```

# data-benchmark

Benchmark design and deployment assets for the StarRocks and Iceberg data benchmark project.

## Staging Deployment

See [docs/deployment/staging.md](docs/deployment/staging.md) for the staging deployment workflow.

## Java 17 Local Benchmark MVP

Build and test:

```powershell
mvn test
```

Package the runner:

```powershell
mvn package
```

Run the CI-sized local smoke workflow:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --run-id local-smoke
```

Generate data only:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar generate --cells 10 --days 1 --row-cap 100
```

The default smoke config is [configs/benchmark-smoke.yml](configs/benchmark-smoke.yml). It preserves the spec values for `10,000` cells and `1` day, and uses `rowCap: 10000` so local verification does not accidentally generate all `14,400,000` rows.

Docker Compose uses the packaged runner jar from `target/`, so run `mvn package` before starting Compose. The runner service executes the real compose path:

```powershell
docker compose -f docker-compose.yml up benchmark-runner
```

The generator writes deterministic, partitioned Parquet files under `event_date=YYYY-MM-DD/part-00000.parquet`. Local mode remains a fast Java-only smoke path; compose mode runs the Spark/Iceberg and StarRocks engine path.

## HDFS Compose Benchmark

HDFS infrastructure is provisioned by Docker Compose. The HDFS Iceberg warehouse path is `hdfs://hdfs-namenode:8020/warehouse/iceberg`.

```powershell
mvn package
docker compose -f docker-compose.yml up -d hdfs-namenode hdfs-datanode hdfs-init hive-metastore spark starrocks-fe starrocks-be
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --run-id compose-smoke
```

Compose mode writes a standalone web report package under `reports/runs/<run_id>/`.
Open the report directly:

```text
reports/runs/compose-smoke/index.html
```

Each report directory contains `index.html`, `report.json`, and React assets. The HTML embeds the report data, so it can be opened directly from the filesystem.

If a Spark, StarRocks, or external Iceberg stage fails, the CLI exits nonzero after writing a DEGRADED report with the failing stage and error detail.

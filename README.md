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

The default verification config is [configs/benchmark-smoke.yml](configs/benchmark-smoke.yml). It preserves the spec values for `10,000` cells and `1` day, and uses `rowCap: 10000` for a 10k-row smoke dataset.

The formal KPI benchmark config is [configs/benchmark-kpi-10m.yml](configs/benchmark-kpi-10m.yml). It uses the same KPI shape with `rowCap: 10000000` for a 10m-row / 1000 万行 benchmark dataset.

Docker Compose uses the packaged runner jar from `target/`, so run `mvn package` before starting Compose. Build the benchmark-runner image after packaging so the container has Java 17, Spark, and the Hadoop `hdfs` CLI available for Hive HDFS Parquet publish. The runner service executes the real compose path:

```powershell
mvn package
docker compose -f docker-compose.yml build benchmark-runner
docker compose -f docker-compose.yml up benchmark-runner
```

The generator writes deterministic, partitioned Parquet files under `event_date=YYYY-MM-DD/part-00000.parquet`. Local mode remains a fast Java-only smoke path; compose mode runs the Spark/Iceberg and StarRocks engine path.

## HDFS Compose Benchmark

HDFS infrastructure is provisioned by Docker Compose. The HDFS Iceberg warehouse path is `hdfs://hdfs-namenode:8020/warehouse/iceberg`.

```powershell
mvn package
docker compose -f docker-compose.yml build benchmark-runner
docker compose -f docker-compose.yml up -d hdfs-namenode hdfs-datanode hdfs-init hive-metastore hive-server spark starrocks-fe starrocks-be
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-smoke.yml --run-id compose-smoke
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-kpi-10m.yml --run-id compose-kpi-10m
```

Current Docker Compose resource limits:

| Service | CPU | Memory |
| --- | ---: | ---: |
| starrocks-fe | 2 | 2GB |
| starrocks-be | 6 | 5GB |
| spark | 6 | 3GB |
| hive-metastore | 1 | 1GB |
| hive-server | 2 | 2GB |
| hdfs-namenode | 1 | 768MB |
| hdfs-datanode | 2 | 1.5GB |
| benchmark-runner | 2 | 1GB |

Compose mode writes a standalone web report package under `reports/runs/<run_id>/`.
Open the report directly:

```text
reports/runs/compose-smoke/index.html
```

Each report directory contains `index.html` and React assets. The HTML embeds the report data, so it can be opened directly from the filesystem without an external JSON file.

If a Spark, StarRocks, or external Iceberg stage fails, the CLI exits nonzero after writing a DEGRADED report with the failing stage and error detail.

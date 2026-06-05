# data-benchmark

Benchmark design and deployment assets for the StarRocks and Iceberg data benchmark project.

## Staging Deployment

See [docs/deployment/staging.md](docs/deployment/staging.md) for the staging deployment workflow.

Promote the current main branch to staging:

```sh
scripts/deploy-staging.sh
```

Promote a specific ref:

```sh
scripts/deploy-staging.sh --source-ref origin/main
scripts/deploy-staging.sh --source-ref 073fa4814b1833c9a74ec7c04a75ca05fad25e3a
```

Preview the push command from a dirty working tree:

```sh
scripts/deploy-staging.sh --dry-run --allow-dirty
```

## Java 17 Local Benchmark MVP

Build and test:

```sh
mvn test
```

Package the runner:

```sh
mvn package
```

Run the CI-sized local smoke workflow:

```sh
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --run-id local-smoke
```

Generate data only:

```sh
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar generate --cells 10 --days 1 --row-cap 100
```

KPI data generation uses Spark in both local and compose modes. Local commands use Spark `local[*]`; compose runs execute the same generation path inside the `spark` service before downstream route loading begins.

The default verification config is [configs/benchmark-smoke.yml](configs/benchmark-smoke.yml). It preserves the spec values for `10,000` cells and `1` day, and uses `rowCap: 10000` for a 10k-row smoke dataset.

The fast Compose smoke config is [configs/benchmark-compose-smoke.yml](configs/benchmark-compose-smoke.yml). It uses the same 10k-row dataset but sets `query.names` to run only `date_partition_pruning`, which is intended for quick four-route validation. Omit `query.names` to run every KPI SQL in `QueryCatalog`.

The formal KPI benchmark config is [configs/benchmark-kpi-10m.yml](configs/benchmark-kpi-10m.yml). It uses the same KPI shape with `rowCap: 10000000` for a 10m-row benchmark dataset.

The large KPI generation config is [configs/benchmark-kpi-1b.yml](configs/benchmark-kpi-1b.yml). It uses `rowCap: 1000000000` with 1024 Spark partitions for a 1b-row dataset.

Docker Compose uses the packaged runner jar from `target/`, so run `mvn package` before starting Compose. Build the benchmark-runner image after packaging so the container has Java 17, Spark, the Hadoop `hdfs` CLI, and `docker compose` available for Hive HDFS Parquet publish and route cold restarts. The runner service executes the real compose path:

```sh
mvn package
docker compose -f docker-compose.yml build benchmark-runner
docker compose -f docker-compose.yml up benchmark-runner
```

The `benchmark-runner` service mounts `/var/run/docker.sock:/var/run/docker.sock` so the Java controller inside the container can run `docker compose exec`, `stop`, and `start` for cold restart/readiness orchestration. On Windows Docker Desktop this Linux socket path is normally exposed to Linux containers. If you do not want to mount the Docker socket, start the dependent services with Compose and run the packaged jar directly from the host instead of using the runner container.

The generator writes deterministic, partitioned Parquet files under `event_date=YYYY-MM-DD/*.parquet`. Local mode uses Spark local execution for small smoke data; compose mode runs generation inside the Spark service and then executes the Spark/Iceberg, StarRocks, and Hive query routes.

## Shared Infra Compose Benchmark

Compose benchmark mode uses `../shared-data-infra` for HDFS, Hive Metastore, HDFS warehouse initialization, HiveServer2, Spark, and split StarRocks FE/BE services. The local `docker-compose.yml` keeps only `benchmark-runner`; it joins the external `shared-data-infra` network and controls shared service lifecycle through the Docker socket.

Start the benchmark-compatible shared infrastructure from `../shared-data-infra`:

```sh
docker compose -f compose.yaml -f compose.lakehouse.yaml -f compose.starrocks.yaml --profile lakehouse --profile lakehouse-tools --profile spark-tools --profile starrocks up -d
```

Shared service endpoints on the `shared-data-infra` network:

| Service | Endpoint |
| --- | --- |
| HDFS | `hdfs://hdfs-namenode:8020` |
| Hive Metastore | `thrift://hive-metastore:9083` |
| HiveServer2 | `jdbc:hive2://hive-server:10000/default` |
| Spark exec service | `docker compose ... exec spark /opt/spark/bin/spark-sql ...` |
| StarRocks FE MySQL | `starrocks-fe:9030` |
| StarRocks BE HTTP | `http://starrocks-be:8040` |

Compose benchmark environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `BENCHMARK_INFRA_PROJECT` | `shared-data-infra` | Docker Compose project name used when the Java runner controls shared services. |
| `BENCHMARK_INFRA_COMPOSE_FILES` | `../shared-data-infra/compose.yaml;../shared-data-infra/compose.lakehouse.yaml;../shared-data-infra/compose.starrocks.yaml` | Semicolon- or comma-separated shared infra compose files for host Java runs. |
| `BENCHMARK_INFRA_NETWORK` | `shared-data-infra` | Docker network used by ad hoc publisher containers and benchmark services. |
| `BENCHMARK_WORKSPACE` | `../data-benchmark` in shared infra | Host path mounted into the shared Spark service at `/workspace`; override it when the repositories are not checked out as siblings. |

Host Java runs use the `../shared-data-infra/...` compose file defaults. The `benchmark-runner` container cannot use those host-relative paths, so `docker-compose.yml` mounts `../shared-data-infra` read-only at `/shared-data-infra` and injects `/shared-data-infra/compose.yaml;/shared-data-infra/compose.lakehouse.yaml;/shared-data-infra/compose.starrocks.yaml` through `BENCHMARK_INFRA_COMPOSE_FILES`.

Resource ownership:

| Resource | Owner |
| --- | --- |
| `benchmark-runner` | local `data-benchmark/docker-compose.yml` |
| `hdfs-init` | `../shared-data-infra/compose.lakehouse.yaml` |
| `hive-server` | `../shared-data-infra/compose.lakehouse.yaml` |
| `spark` | `../shared-data-infra/compose.lakehouse.yaml` |
| `starrocks-fe` | `../shared-data-infra/compose.starrocks.yaml` |
| `starrocks-be` | `../shared-data-infra/compose.starrocks.yaml` |

Run the fast 10k Compose smoke validation:

```sh
mvn package
docker compose -f docker-compose.yml build benchmark-runner
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-compose-smoke.yml --run-id compose-smoke
```

Run the full 10k smoke validation with all KPI SQL:

```sh
mvn package
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-smoke.yml --run-id compose-smoke-full
```

Run the formal 10m-row KPI benchmark:

```sh
mvn package
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-kpi-10m.yml --run-id compose-kpi-10m
```

Run the 1b-row KPI generation and benchmark profile:

```sh
mvn package
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-kpi-1b.yml --run-id compose-kpi-1b
```

Use a unique `--run-id` for every real run, for example `kpi-10m-20260605-001`, so each report is written to its own directory under `reports/runs/`.

Current local Docker Compose resource limits:

| Service | CPU | Memory |
| --- | ---: | ---: |
| benchmark-runner | 2 | 1GB |

Cold restarts preserve shared service container filesystems for loaded benchmark data. The Java runner targets the shared infra compose project, using `stop` and `start` for `starrocks-be`, `starrocks-fe`, and `hive-server`, and `restart` for `spark`.

Compose mode writes a standalone web report package under `reports/runs/<run_id>/`.
Open the report directly:

```text
reports/runs/compose-smoke/index.html
```

Each report directory contains `index.html` and React assets. The HTML embeds the report data, so it can be opened directly from the filesystem without an external JSON file.

If a Spark, StarRocks, or external Iceberg stage fails, the CLI exits nonzero after writing a DEGRADED report with the failing stage and error detail.

After a successful run, verify the report data from a POSIX shell:

```sh
run_id="compose-kpi-10m"
node - "$run_id" <<'NODE'
const fs = require('fs');
const runId = process.argv[2];
const html = fs.readFileSync(`reports/runs/${runId}/index.html`, 'utf8');
const match = html.match(/window\.__BENCHMARK_REPORT__\s*=\s*(\{[\s\S]*?\});\s*<\/script>/);
if (!match) {
  throw new Error(`Report data not found for ${runId}`);
}
const report = JSON.parse(match[1]);
console.log(report.run.status);
console.log(report.dataset.rows);
console.log(report.performanceMatrix.length);
NODE
```

Expected values for the formal config are `SUCCESS`, `10000000`, and `10`.

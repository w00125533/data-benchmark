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

## HDFS Compose Benchmark

HDFS and the Hive Metastore are provided by the shared lakehouse infrastructure. Start that prerequisite from `D:\agent-code\shared-data-infra` before starting this benchmark runtime:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\infra-up.ps1 -Profiles lakehouse
```

The HDFS Iceberg warehouse path remains `hdfs://hdfs-namenode:8020/warehouse/iceberg`, and HiveServer2 uses the shared metastore at `thrift://hive-metastore:9083`.

This project still keeps `spark`, `hive-server`, and split `starrocks-fe`/`starrocks-be` services local. The Java runner shells into `spark` and `hive-server` with `docker compose exec`, and the benchmark needs direct FE/BE lifecycle control for StarRocks cold restart semantics.

Run the fast 10k Compose smoke validation:

```sh
mvn package
docker compose -f docker-compose.yml build benchmark-runner
docker compose -f docker-compose.yml up -d hdfs-init hive-server spark starrocks-fe starrocks-be
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-compose-smoke.yml --run-id compose-smoke
```

Run the full 10k smoke validation with all KPI SQL:

```sh
mvn package
docker compose -f docker-compose.yml up -d hdfs-init hive-server spark starrocks-fe starrocks-be
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-smoke.yml --run-id compose-smoke-full
```

Run the formal 10m-row KPI benchmark:

```sh
mvn package
docker compose -f docker-compose.yml down --remove-orphans
docker compose -f docker-compose.yml up -d hdfs-init hive-server spark starrocks-fe starrocks-be
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-kpi-10m.yml --run-id compose-kpi-10m
```

Run the 1b-row KPI generation and benchmark profile:

```sh
mvn package
docker compose -f docker-compose.yml down --remove-orphans
docker compose -f docker-compose.yml up -d hdfs-init hive-server spark starrocks-fe starrocks-be
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-kpi-1b.yml --run-id compose-kpi-1b
```

Use a unique `--run-id` for every real run, for example `kpi-10m-20260605-001`, so each report is written to its own directory under `reports/runs/`.

Current Docker Compose resource limits:

| Service | CPU | Memory |
| --- | ---: | ---: |
| starrocks-fe | 2 | 2GB |
| starrocks-be | 6 | 5GB |
| spark | 6 | 3GB |
| hive-server | 2 | 2GB |
| benchmark-runner | 2 | 1GB |

The default Compose network uses subnet `172.20.0.0/24`; `starrocks-fe` is pinned to `172.20.0.10` and `starrocks-be` to `172.20.0.11` so StarRocks metadata does not drift across container starts. After adopting or changing the fixed subnet, run `docker compose down --remove-orphans` once from this directory, or `docker compose -f docker-compose.yml down --remove-orphans`, so Docker recreates the old `databenchmark` network with the current IPAM settings. If `172.20.0.0/24` conflicts with a host route, VPN, or corporate network, change the subnet in `docker-compose.yml` and update both pinned FE/BE IP addresses consistently.

Cold restarts preserve container filesystems for loaded benchmark data: StarRocks uses `stop starrocks-be`, `stop starrocks-fe`, `start starrocks-fe`, wait for FE MySQL, then `start starrocks-be`; Hive uses `stop hive-server` then `start hive-server`. These routes intentionally avoid removing or force-recreating containers.

`hive-server` intentionally overrides the `apache/hive:4.0.0` default entrypoint and directly executes `/opt/hive/bin/hive --skiphadoopversion --skiphbasecp --service hiveserver2` with explicit `hive.metastore.uris=thrift://hive-metastore:9083` and `hive.server2.thrift.bind.host=0.0.0.0` settings. The image's `SERVICE_NAME=hiveserver2` entrypoint path can misread its own startup process as an already-running HiveServer2 after repeat cold restarts and exit with `HiveServer2 running as process ... Stop it first.` The explicit command keeps Compose stop/start cold restarts repeatable; schema and metastore lifecycle remain owned by the shared `hive-metastore` service.

The FE startup command rewrites `JAVA_OPTS` with `-Xmx1536m`, matching the 2GB FE container limit instead of the StarRocks default `-Xmx8192m`.

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

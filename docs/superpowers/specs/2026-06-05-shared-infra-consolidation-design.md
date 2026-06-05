# Shared Infra Consolidation Design

## Context

`data-benchmark` has already started using `../shared-data-infra` for common lakehouse services, but its local `docker-compose.yml` still owns several infrastructure services:

- `spark`
- `hive-server`
- `hdfs-init`
- `starrocks-fe`
- `starrocks-be`

The shared infrastructure repository already defines overlapping capabilities: HDFS, Hive Metastore, HiveServer2, Spark tools, and StarRocks. The current overlap makes the boundary unclear and risks configuration drift between benchmark runtime services and shared local infrastructure.

The benchmark still has stricter runtime requirements than a generic dev toolbox:

- Spark must be a long-running service that the Java runner can `docker compose exec` into.
- HiveServer2 must survive repeat stop/start cold restart cycles.
- StarRocks must expose separate FE and BE lifecycle control for benchmark cold restart semantics.
- HDFS must keep the stable benchmark endpoint `hdfs://hdfs-namenode:8020`.

## Decision

Consolidate the benchmark infrastructure into `../shared-data-infra` and update the shared services to the benchmark-compatible technical stack.

`data-benchmark/docker-compose.yml` will keep only project-local runtime services, primarily `benchmark-runner`. HDFS, Hive Metastore, HDFS initialization, HiveServer2, Spark, and StarRocks FE/BE will be owned by `shared-data-infra`.

The benchmark runner will remain project-local, but its service lifecycle commands will target the shared infra compose project through configurable compose files and project name.

## Goals

- Move all reusable infrastructure services out of `data-benchmark`.
- Make shared StarRocks use FE/BE split services rather than all-in-one StarRocks.
- Make shared Spark a long-running executable service compatible with `docker compose exec`.
- Make shared HiveServer2 use the explicit startup command that supports repeat cold restart cycles.
- Add shared lakehouse HDFS initialization for `/warehouse/iceberg`.
- Preserve benchmark route behavior and endpoint names.
- Keep existing shared infra profiles where practical:
  - `lakehouse`
  - `lakehouse-tools`
  - `spark-tools`
  - `starrocks`
- Make runner compose targeting configurable for local path differences.

## Non-Goals

- Do not change benchmark query semantics.
- Do not change report output behavior.
- Do not replace Docker Compose with another orchestrator.
- Do not introduce a benchmark-specific parallel StarRocks profile if the shared `starrocks` profile can be corrected.
- Do not keep duplicate project-local infrastructure unless a later implementation blocker proves isolation is required.

## Shared Infra Changes

### StarRocks

Update `../shared-data-infra/compose.starrocks.yaml` so the `starrocks` profile provides split FE and BE services:

- `starrocks-fe`
  - image: `starrocks/fe-ubuntu:3.3-latest`
  - hostname: `starrocks-fe-0`
  - ports: `${STARROCKS_HTTP_PORT:-8030}:8030`, `${STARROCKS_MYSQL_PORT:-9030}:9030`
  - command keeps the benchmark FE `JAVA_OPTS` rewrite:
    - remove existing `JAVA_OPTS`
    - set `-Xmx1536m`
    - preserve `-Dlog4j2.formatMsgNoLookups=true`
    - preserve StarRocks UDF security policy
- `starrocks-be`
  - image: `starrocks/be-ubuntu:3.3-latest`
  - hostname: `starrocks-be-0`
  - command: `/opt/starrocks/be_entrypoint.sh starrocks-fe`
  - ports: `${STARROCKS_BE_HTTP_PORT:-8040}:8040`
  - depends on `starrocks-fe`

The old all-in-one `starrocks` service is removed from the shared profile. Shared README and env documentation must replace `starrocks:9030` examples with `starrocks-fe:9030`.

### Lakehouse

Keep these shared services in `compose.lakehouse.yaml`:

- `hms-db`
- `namenode`
- `datanode`
- `hive-metastore`

Keep the `namenode` network alias:

```text
hdfs-namenode
```

The stable container endpoint for cross-project clients remains:

```text
hdfs://hdfs-namenode:8020
```

### HDFS Init

Add a shared one-shot `hdfs-init` service under the `lakehouse` profile.

It should wait for HDFS to accept commands, then run:

```sh
hdfs dfs -fs hdfs://hdfs-namenode:8020 -mkdir -p /warehouse/iceberg
hdfs dfs -fs hdfs://hdfs-namenode:8020 -chmod -R 777 /warehouse
```

`hdfs-init` belongs in shared infra because `/warehouse/iceberg` is the common lakehouse warehouse foundation, not benchmark-only state.

### HiveServer2

Update shared `hive-server` under `lakehouse-tools` to use the benchmark-compatible explicit command path rather than the image's `SERVICE_NAME=hiveserver2` entrypoint path.

The service should:

- use `apache/hive:4.0.0`
- keep hostname `hive-server`
- set `IS_RESUME=true`
- set `SKIP_SCHEMA_INIT=true`
- set `HADOOP_CLIENT_OPTS` with `-Xmx1G`
- execute:

```sh
/opt/hive/bin/hive --skiphadoopversion --skiphbasecp --service hiveserver2
```

with:

```text
hive.metastore.uris=thrift://hive-metastore:9083
hive.server2.thrift.bind.host=0.0.0.0
hive.server2.thrift.port=10000
```

HiveServer2 is the SQL Thrift/JDBC service on `hive-server:10000`. Hive Metastore remains the metadata Thrift service on `hive-metastore:9083`.

### Spark

Update shared `spark` under `spark-tools` to be a long-running service:

- image: `apache/spark:3.5.8-java17`
- hostname: `spark`
- working directory: `/workspace`
- command: `sleep infinity`
- mount: `${BENCHMARK_WORKSPACE:-../data-benchmark}:/workspace`
- environment:
  - `HADOOP_CONF_DIR`
  - `HIVE_CONF_DIR`
- depends on `hdfs-init` completing successfully

The old one-shot `spark-sql` tool behavior will be replaced with documented `docker compose exec spark /opt/spark/bin/spark-sql ...` usage.

## Data-Benchmark Changes

### Docker Compose

Update `data-benchmark/docker-compose.yml` to remove:

- `spark`
- `hive-server`
- `hdfs-init`
- `starrocks-fe`
- `starrocks-be`

Keep:

- `benchmark-runner`
- external network `shared-data-infra`

`benchmark-runner` should continue to mount:

```text
.:/workspace
/var/run/docker.sock:/var/run/docker.sock
```

It should keep endpoint environment variables pointed at shared services on the shared network:

```text
STARROCKS_JDBC_URL=jdbc:mysql://starrocks-fe:9030/?useSSL=false&allowPublicKeyRetrieval=true&allowMultiQueries=true
STARROCKS_STREAM_LOAD_URL=http://starrocks-be:8040/api/sr_internal/cell_kpi_1min/_stream_load
```

It should no longer declare `depends_on` for removed local infra services. Startup ordering becomes a documented prerequisite: shared infra must be up before benchmark runner starts.

### Runner Compose Targeting

Update `ComposeServiceController` so lifecycle commands target shared infra compose files instead of hard-coded `docker-compose.yml`.

Default compose files:

```text
../shared-data-infra/compose.yaml
../shared-data-infra/compose.lakehouse.yaml
../shared-data-infra/compose.starrocks.yaml
```

Default compose project:

```text
shared-data-infra
```

Environment overrides:

- `BENCHMARK_INFRA_COMPOSE_FILES`
  - semicolon-separated on Windows-friendly local configuration
  - implementation may also accept comma-separated values for container environments
- `BENCHMARK_INFRA_PROJECT`

Example generated command:

```sh
docker compose -p shared-data-infra \
  -f ../shared-data-infra/compose.yaml \
  -f ../shared-data-infra/compose.lakehouse.yaml \
  -f ../shared-data-infra/compose.starrocks.yaml \
  stop starrocks-be
```

Service names remain:

- `spark`
- `hive-server`
- `starrocks-fe`
- `starrocks-be`

Readiness and restart semantics remain unchanged:

- Spark route restarts `spark` and checks `spark-sql -e SELECT 1`.
- Hive route stops and starts `hive-server`, then checks Beeline at `jdbc:hive2://hive-server:10000/default`.
- StarRocks routes stop BE, stop FE, start FE, wait for FE MySQL, start BE, then verify backend readiness and blacklist state.

## Documentation Changes

### shared-data-infra README

Update profile documentation:

- `lakehouse`: HDFS, Hive Metastore, HDFS warehouse initialization.
- `lakehouse-tools`: long-running HiveServer2.
- `spark-tools`: long-running Spark tool service.
- `starrocks`: split StarRocks FE/BE.

Add a benchmark-compatible startup example:

```sh
docker compose -f compose.yaml -f compose.lakehouse.yaml -f compose.starrocks.yaml \
  --profile lakehouse --profile lakehouse-tools --profile spark-tools --profile starrocks \
  up -d
```

Document:

- `BENCHMARK_WORKSPACE`
- `STARROCKS_HTTP_PORT`
- `STARROCKS_MYSQL_PORT`
- `STARROCKS_BE_HTTP_PORT`
- HDFS endpoint `hdfs://hdfs-namenode:8020`
- Hive Metastore endpoint `thrift://hive-metastore:9083`
- HiveServer2 endpoint `jdbc:hive2://hive-server:10000/default`

### data-benchmark README

Update compose instructions:

1. Start shared infra from `../shared-data-infra`.
2. Run `mvn package`.
3. Build and run `benchmark-runner` from `data-benchmark`.

Document runner overrides:

- `BENCHMARK_INFRA_COMPOSE_FILES`
- `BENCHMARK_INFRA_PROJECT`
- `BENCHMARK_WORKSPACE`

Update resource tables to show that infrastructure resources are now owned by shared infra.

### AGENTS.md

Update `data-benchmark/AGENTS.md`:

- Remove the current exception that keeps `spark`, `hive-server`, `starrocks-fe`, and `starrocks-be` project-local.
- State that these services are provided by `../shared-data-infra`.
- Preserve the rule that any future infrastructure changes must first check shared infra.

Update `shared-data-infra/AGENTS.md` if needed to clarify:

- services used by multiple projects live in the relevant shared profile
- benchmark-compatible implementations are the default when they also satisfy common dev use
- isolated services require an explicit README reason

## Testing

Run shared infra config checks:

```sh
cd ../shared-data-infra
docker compose -f compose.yaml -f compose.lakehouse.yaml --profile lakehouse --profile lakehouse-tools --profile spark-tools config
docker compose -f compose.yaml -f compose.starrocks.yaml --profile starrocks config
```

Run data-benchmark checks:

```sh
cd ../data-benchmark
docker compose -f docker-compose.yml config
mvn test
```

If local resources allow, run a full smoke:

```sh
cd ../shared-data-infra
docker compose -f compose.yaml -f compose.lakehouse.yaml -f compose.starrocks.yaml \
  --profile lakehouse --profile lakehouse-tools --profile spark-tools --profile starrocks \
  up -d

cd ../data-benchmark
mvn package
docker compose -f docker-compose.yml build benchmark-runner
docker compose -f docker-compose.yml up benchmark-runner
```

## Risks

- Other projects may still expect `starrocks:9030`. Mitigation: update shared README/env and dependent project configs, or add a temporary network alias only if a dependent project requires it.
- `BENCHMARK_WORKSPACE` default uses a relative path from `shared-data-infra`; this must be documented and overridable for different checkout layouts.
- Cross-compose lifecycle control depends on the runner container having Docker CLI access and path visibility to shared compose files. The existing Docker socket mount remains required for containerized runner mode.
- HiveServer2 startup command is longer than the image default path, but it is needed for repeat cold restart stability.

## Acceptance Criteria

- `data-benchmark/docker-compose.yml` contains no infrastructure services other than `benchmark-runner`.
- `shared-data-infra` provides HDFS, Hive Metastore, `hdfs-init`, HiveServer2, Spark, and split StarRocks FE/BE.
- Runner lifecycle commands use configurable shared infra compose files and project name.
- Existing benchmark service names and internal endpoints remain usable by code:
  - `spark`
  - `hive-server`
  - `starrocks-fe`
  - `starrocks-be`
  - `hdfs-namenode`
  - `hive-metastore`
- Required compose config checks pass.
- `mvn test` passes in `data-benchmark`.

# Real Spark Iceberg StarRocks Engine Integration Design

## 目标

把当前 Java 17 benchmark 从“本地生成真实 Parquet 并生成 HTML 报告”升级为本地 Docker Compose 可验证的真实端到端 smoke benchmark。一次 run 应完成以下链路：

1. 生成 50 列无线 KPI Parquet 数据。
2. 通过 Spark SQL 写入 Iceberg 表。
3. 将同一份逻辑数据加载到 StarRocks internal table。
4. 在 StarRocks 中创建或刷新 external Iceberg catalog。
5. 在 Spark Iceberg、StarRocks internal、StarRocks external Iceberg 三种表形态上执行同一批 benchmark 查询。
6. 生成 HTML 报告，并暴露 Prometheus 指标供 Grafana dashboard 按 run id 查看。

本设计优先保证 smoke profile 在单机 Docker Compose 环境可运行、可诊断、可重复。它不是生产级容量规划，也不默认执行 4.032B 行 full profile。

## 范围

本轮实现覆盖：

- Docker Compose 真实服务编排和 readiness。
- HDFS warehouse 初始化。
- Spark SQL 写 Iceberg 表。
- StarRocks internal table DDL 和数据导入。
- StarRocks external Iceberg catalog DDL 和 refresh。
- 三种 engine/table shape 的查询执行和结果采集。
- Prometheus metrics endpoint。
- Grafana datasource 和 dashboard provisioning。
- HTML 报告展示 load/query 阶段、错误详情和 Grafana 链接。

本轮不覆盖：

- 生产级 Spark/StarRocks 集群调优。
- Kubernetes 部署。
- 多节点 StarRocks BE 扩容。
- 全量 4.032B 行默认执行。
- 对所有 Iceberg catalog 类型的兼容；本轮固定使用本地 Compose 中的 Hive Metastore + HDFS warehouse。

## 架构

### 服务拓扑

Docker Compose 提供以下服务：

- `starrocks-fe`：StarRocks SQL 入口，暴露 MySQL 协议端口 `9030` 和 HTTP 端口 `8030`。
- `starrocks-be`：StarRocks Backend，提供存储和导入执行。
- `spark`：Spark SQL 执行环境，用于读取 Parquet、写 Iceberg、执行 Spark 查询。
- `hdfs-namenode`：HDFS NameNode，提供 Iceberg warehouse 的分布式文件系统入口。
- `hdfs-datanode`：HDFS DataNode，保存 Iceberg warehouse 数据块。
- `hive-metastore`：Iceberg Hive catalog 元数据服务，warehouse 指向 HDFS。
- `prometheus`：采集 benchmark runner 和基础服务指标。
- `grafana`：自动加载 datasource 和 dashboard。
- `benchmark-runner`：Java 17 runner，编排数据生成、load、query、report 和 metrics。

Compose 应增加持久但本地的 mounted directories：

- `./data`：runner 生成的 Parquet 和可选导入中间文件。
- `./reports`：HTML 报告。
- `./sql`：Spark 和 StarRocks DDL/query 模板。
- `./monitoring/grafana`：Grafana provisioning。

### Java 模块边界

新增 `com.example.databenchmark.engine` 包：

- `EngineClient`：统一接口，定义 `prepare()`, `load(DatasetResult)`, `refresh()`, `runQueries(QueryCatalog)`。
- `SparkIcebergClient`：调用 Spark SQL 容器执行 Iceberg DDL、Parquet-to-Iceberg insert 和 Spark 查询。
- `StarRocksClient`：通过 JDBC 连接 StarRocks FE，执行 internal table DDL、导入、external catalog DDL/refresh 和 StarRocks 查询。
- `CommandRunner`：封装外部命令执行，捕获 stdout、stderr、exit code、duration。
- `JdbcExecutor`：封装 JDBC execute/query timing 和错误详情。
- `BenchmarkOrchestrator`：串联 generate、load、refresh、query、metrics、report。
- `EngineRunResult`：记录每个 load/query 阶段的 engine、table shape、rows、bytes、duration、success、error。

现有 `LocalBenchmarkRunner` 保留，用于无 Docker 依赖的快速本地 smoke。新增 CLI 子命令或 `run --mode compose` 用于真实 engine integration。

## 数据流

### 生成

Java runner 使用现有 `KpiDataGenerator` 生成真实 Parquet：

```text
data/generated/event_date=2026-01-01/part-00000.parquet
```

smoke 默认仍使用 `rowCap: 10000`，避免本地验证生成全部 14,400,000 行。真实 engine integration 可通过 CLI 参数关闭 row cap。

### HDFS warehouse 初始化

Compose 启动时必须创建 HDFS warehouse 目录，并保证 Spark、Hive Metastore 和 StarRocks external catalog 使用同一个路径：

```text
hdfs://hdfs-namenode:8020/warehouse/iceberg
```

初始化命令应至少完成：

```bash
hdfs dfs -mkdir -p /warehouse/iceberg
hdfs dfs -chmod -R 777 /warehouse
```

readiness 检查必须确认：

- NameNode Web UI 可访问。
- `hdfs dfs -ls /warehouse/iceberg` 成功。
- Hive Metastore 能连接到 HDFS warehouse 路径。

### Spark Iceberg 写入

Spark 读取 mounted `data/generated` 下的 Parquet，写入 Hive catalog 管理的 Iceberg 表。Spark catalog 配置固定使用 HDFS warehouse：

```text
spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_catalog.type=hive
spark.sql.catalog.iceberg_catalog.uri=thrift://hive-metastore:9083
spark.sql.catalog.iceberg_catalog.warehouse=hdfs://hdfs-namenode:8020/warehouse/iceberg
```

Spark SQL 表名使用 `iceberg_catalog.iceberg_db.cell_kpi_1min`：

```sql
CREATE DATABASE IF NOT EXISTS iceberg_catalog.iceberg_db;

CREATE TABLE IF NOT EXISTS iceberg_catalog.iceberg_db.cell_kpi_1min (
  event_time TIMESTAMP,
  cell_id STRING,
  province STRING,
  city STRING,
  district STRING,
  grid_id STRING,
  vendor STRING,
  rat STRING,
  band STRING,
  arfcn INT,
  pci INT,
  site_id STRING,
  longitude DOUBLE,
  latitude DOUBLE,
  rsrp_avg DOUBLE,
  rsrp_p10 DOUBLE,
  rsrq_avg DOUBLE,
  sinr_avg DOUBLE,
  prb_dl_util DOUBLE,
  prb_ul_util DOUBLE,
  rrc_users INT,
  active_users INT,
  dl_traffic_mb DOUBLE,
  ul_traffic_mb DOUBLE,
  dl_throughput_mbps DOUBLE,
  ul_throughput_mbps DOUBLE,
  drop_rate DOUBLE,
  handover_success_rate DOUBLE,
  access_success_rate DOUBLE,
  volte_drop_rate DOUBLE,
  latency_ms DOUBLE,
  availability_rate DOUBLE,
  alarm_count INT,
  interference_score DOUBLE,
  load_score DOUBLE,
  packet_loss_rate DOUBLE,
  cqi_avg DOUBLE,
  mcs_avg DOUBLE,
  ta_avg DOUBLE,
  ul_noise_avg DOUBLE,
  connected_users_peak INT,
  rrc_setup_attempts INT,
  rrc_setup_successes INT,
  erab_setup_attempts INT,
  erab_setup_successes INT,
  handover_attempts INT,
  handover_successes INT,
  retransmission_rate DOUBLE,
  backhaul_util DOUBLE,
  energy_kwh DOUBLE
)
USING iceberg
PARTITIONED BY (days(event_time));
```

写入前 smoke run 默认清理目标表，保证相同 run 可重复。

### StarRocks internal load

StarRocks internal table 使用与 `KpiSchema` 等价的字段。内部表按 `event_date` 分区或使用 smoke-friendly 单分区布局，按 `cell_id` 分桶。

导入路径优先级：

1. 如果 StarRocks 可直接通过 HTTP Stream Load 读取 runner 生成的 CSV，则 runner 生成 StarRocks 专用 CSV 并 Stream Load。
2. 如果 CSV 链路在当前镜像上不稳定，则使用 Spark 从 Iceberg 导出 CSV 到 mounted volume，再 Stream Load。

本轮实现选择第一种：新增 `StarRocksCsvExporter`，从 Parquet record 读取并输出 headerless CSV，字段顺序与 StarRocks internal table 一致。这样不改变主数据源 Parquet/Iceberg，只为 StarRocks internal load 提供导入格式。

### StarRocks external Iceberg catalog

StarRocks 通过 external catalog 访问同一 Iceberg warehouse。runner 执行：

```sql
CREATE EXTERNAL CATALOG IF NOT EXISTS sr_external_iceberg
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hive",
  "iceberg.catalog.hive.metastore.uris" = "thrift://hive-metastore:9083"
);
```

Iceberg 数据文件存储在 HDFS，StarRocks FE/BE 必须能解析并访问 `hdfs://hdfs-namenode:8020/warehouse/iceberg`。如果 StarRocks 镜像版本要求额外 HDFS 或 catalog 属性，runner 必须把失败 SQL 和错误信息写入报告，不吞掉异常。

## 查询执行

`QueryCatalog` 保留逻辑查询定义。新增 SQL renderer 将同一查询渲染到：

- `spark_iceberg`：`iceberg_db.cell_kpi_1min`
- `starrocks_internal`：`sr_internal.cell_kpi_1min`
- `starrocks_external_iceberg`：`sr_external_iceberg.iceberg_db.cell_kpi_1min`

smoke 查询至少包含：

- 单小区一天趋势。
- 城市/厂商/频段/制式聚合。
- 高负载 Top N 小区。
- 弱覆盖小区筛选。
- 日期分区裁剪。

每条查询记录：

- run id
- profile
- engine
- table shape
- query name
- cold/warm run type
- duration seconds
- returned rows
- success/failure
- error message

如果某个 engine 失败，其它 engine 继续执行，最终报告整体标记为 degraded 而不是直接丢失报告。

## Metrics 和 Grafana

runner 暴露 HTTP metrics endpoint，默认端口 `9108`：

- `benchmark_load_duration_seconds`
- `benchmark_load_rows_total`
- `benchmark_load_bytes_total`
- `benchmark_query_duration_seconds`
- `benchmark_query_rows_total`
- `benchmark_query_failures_total`

标签：

- `run_id`
- `profile`
- `engine`
- `table_shape`
- `stage`
- `query_name`

Prometheus 增加 scrape target：

```yaml
- job_name: benchmark-runner
  static_configs:
    - targets: ["benchmark-runner:9108"]
```

Grafana provisioning 包含：

- Prometheus datasource。
- `benchmark` dashboard，uid 固定为 `benchmark`。
- dashboard variable `run_id`，用于报告链接：

```text
http://localhost:3000/d/benchmark?var-run_id=<run_id>
```

Dashboard 至少包含：

- Load duration by engine/table shape。
- Load rows/bytes。
- Query latency by engine/table shape/query。
- Query returned rows。
- Query failures。
- Latest run metadata text panel。

## CLI

保留现有命令：

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --run-id local-smoke
```

新增真实集成模式：

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --run-id compose-smoke
```

Compose 中 `benchmark-runner` 默认使用真实集成模式。

本地无 Docker 服务时，默认 `run` 仍使用 local mode，避免普通开发测试被 StarRocks/Spark 依赖阻塞。

## 错误处理

- readiness 超时要说明具体服务和 URL/端口。
- Spark command 失败要记录 command、exit code、stdout、stderr。
- StarRocks SQL 失败要记录 SQL 名称、SQL 文本摘要和 JDBC 错误。
- load/query 阶段失败不应阻止 HTML 报告生成。
- 如果关键准备阶段失败，例如 StarRocks FE 完全不可连接，runner 应返回非零退出码，但仍尽力写出失败报告。

## 测试策略

### 单元测试

- SQL renderer：同一 logical query 渲染到三种 table shape。
- StarRocks DDL generator：字段数量、字段顺序、关键分区/分桶语句。
- Spark SQL generator：Iceberg table DDL 和 insert SQL。
- CSV exporter：字段顺序、日期/时间格式、null-free smoke rows。
- Metrics registry：指标名和标签完整。
- Report writer：成功和 degraded run 都能生成 HTML。
- Grafana provisioning：datasource 和 dashboard JSON 可解析，uid 为 `benchmark`。

### 集成测试

- `docker compose -f docker-compose.yml config` 必须通过。
- `mvn test` 和 `mvn package` 必须通过。
- 可选手动 smoke：

```powershell
docker compose -f docker-compose.yml up -d hdfs-namenode hdfs-datanode hive-metastore spark starrocks-fe starrocks-be prometheus grafana
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --run-id compose-smoke
```

### 验收测试

- `reports/runs/compose-smoke/index.html` 存在。
- 报告中有三种 table shape 的 load/query 记录，失败时包含错误详情。
- Grafana 健康检查返回 200。
- `http://localhost:3000/d/benchmark?var-run_id=compose-smoke` 可打开 dashboard。
- Prometheus targets 中包含 benchmark-runner。

## 实施顺序

1. Grafana/Prometheus provisioning 和 tests。
2. SQL/DDL renderer 和 tests。
3. StarRocks CSV exporter 和 tests。
4. Engine result model、report 扩展和 metrics。
5. Spark command client。
6. StarRocks JDBC client。
7. Compose mode orchestrator。
8. Docker Compose 更新。
9. 本地 smoke 验证、报告生成、staging 部署。

这个顺序让 dashboard 和报告先变得可验证，再逐步接入真实 engine。即使 StarRocks external catalog 因镜像属性差异失败，runner 也能输出 degraded 报告并保留具体错误，便于下一轮定位。

## 成功标准

- `mvn test` 通过。
- `mvn package` 通过。
- `docker compose -f docker-compose.yml config` 通过。
- Grafana dashboard provisioning 文件存在并被 Compose 挂载。
- Prometheus scrape config 包含 `benchmark-runner:9108`。
- local mode 仍可运行并生成报告。
- compose mode 在服务可用时执行真实 Spark/StarRocks/Iceberg 链路；如果某个 engine 阶段失败，报告包含失败详情和其它阶段结果。
- staging 分支部署成功。

## 风险

- StarRocks external Iceberg catalog 属性名随版本变化。应把 DDL 集中在一个 generator 中，并把失败 SQL 记录到报告。
- bitnami Spark 镜像默认不一定包含 Iceberg runtime jar。实现时需要在 Compose 中通过 package 参数或 mounted jars 固定 Iceberg runtime。
- Hive Metastore 和 HDFS warehouse 初始化顺序容易失败。需要 readiness 和初始化命令重试。
- StarRocks 访问 HDFS 上的 Iceberg 数据时可能需要镜像内 Hadoop client 配置。实现时需要把 HDFS endpoint、core-site.xml 或等价 catalog 属性集中管理，并把访问失败写入报告。
- Windows 路径挂载到 Linux 容器时路径格式不同。所有容器内路径必须使用 `/workspace/data/generated`、`/workspace/reports/runs` 这类显式路径，Java 本地路径和容器路径要显式转换。
- Full profile 不适合默认本地运行。CLI 必须继续要求显式 opt-in。

## 自检

- Placeholder scan: 本 spec 没有待填占位符。
- Consistency check: local mode 和 compose mode 边界清楚；真实集成只在 compose mode 中启用。
- Scope check: 本 spec 仍然较大，但所有工作服务于一个可验证端到端 smoke benchmark。实施计划应拆成多个 task，并优先让每个 task 可单独测试。
- Ambiguity check: StarRocks internal load 路径明确选择 CSV + Stream Load；Spark Iceberg 使用 Hive Metastore + HDFS；Grafana dashboard uid 固定为 `benchmark`。

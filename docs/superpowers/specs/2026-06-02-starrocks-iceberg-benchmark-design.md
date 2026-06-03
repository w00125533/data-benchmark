# StarRocks 与 Iceberg 性能验证工程设计

## 目标

构建一个可复现的 Docker Compose 性能验证工程，用于对比以下三种数据形态的端到端性能：

- StarRocks 内表。
- Spark SQL 原生读写 Iceberg 表。
- StarRocks 外表挂接 Iceberg。

工程需要覆盖无线领域小区级 1 分钟 KPI 大宽表数据生成、数据导入、典型查询、混合链路性能验证、Prometheus/Grafana 监控，以及每次运行后的 HTML 测试报告。

## 范围

工程优先面向本机 Docker Compose 环境，要求能够快速完成 smoke 验证，也能通过参数扩展到更大规模主机上执行正式压力验证。

默认 smoke profile：

- 小区数：`10,000`。
- 时间范围：`1` 天。
- 数据粒度：每小区每分钟 1 条记录。
- 总记录数：`14,400,000`。
- 字段数：`50`。

全量 full profile：

- 小区数：`400,000`。
- 时间范围：`7` 天。
- 数据粒度：每小区每分钟 1 条记录。
- 总记录数：`4,032,000,000`。
- 字段数：`50`。

全量 profile 不作为默认 smoke 测试执行，必须显式开启。原因是本机 Docker 环境下，CPU、内存、磁盘和对象存储吞吐很容易主导结果，导致基准结论失真。

## 总体架构

工程由一个自包含的基准测试栈组成：

- Docker Compose 启动 StarRocks、Spark、Iceberg catalog、HDFS、Prometheus、Grafana 和 benchmark runner。
- Benchmark runner 负责生成无线 KPI 数据、写入 Iceberg、导入 StarRocks 内表、执行查询集、采集指标并生成 HTML 报告。
- Spark SQL 代表原生 Iceberg 的读写、表维护和批查询能力。
- StarRocks 同时验证内表查询性能，以及通过 external catalog 查询 Iceberg 表的性能。
- Prometheus 采集系统指标和 benchmark 自定义指标。
- Grafana 通过 provisioning 自动加载 dashboard，按运行批次、引擎、表形态、查询类型和无线业务维度展示结果。

## 组件设计

### Docker Compose 栈

Compose 需要定义以下服务：

- `starrocks-fe`：StarRocks Frontend。
- `starrocks-be`：StarRocks Backend。
- `spark`：Spark SQL 运行环境，用于原生 Iceberg 读写和查询。
- `hive-metastore`：Iceberg catalog 元数据服务。
- `hdfs-namenode` / `hdfs-datanode`：HDFS 存储服务，作为 Iceberg warehouse。
- `prometheus`：指标采集。
- `grafana`：可视化看板。
- `benchmark-runner`：编排数据生成、导入、查询、指标暴露和报告生成。

栈启动后需要暴露以下入口：

- StarRocks SQL 连接端口。
- Spark SQL 执行入口。
- HDFS NameNode Web UI。
- Prometheus UI。
- Grafana UI。

### 无线 KPI 数据生成器

数据生成器负责创建 `cell_kpi_1min` 大宽表。生成器必须支持固定随机种子，保证相同参数下的数据可重复生成，便于多轮对比。

基础维度字段：

- `event_time`：分钟级时间戳。
- `cell_id`：稳定的小区标识。
- `province`、`city`、`district`、`grid_id`。
- `vendor`：Huawei、ZTE、Ericsson、Nokia、Samsung。
- `rat`：4G 或 5G。
- `band`、`arfcn`、`pci`、`site_id`。
- `longitude`、`latitude`。

KPI 字段组：

- 覆盖类：`rsrp_avg`、`rsrp_p10`、`rsrq_avg`、`sinr_avg`。
- 容量类：`prb_dl_util`、`prb_ul_util`、`rrc_users`、`active_users`。
- 流量类：`dl_traffic_mb`、`ul_traffic_mb`、`dl_throughput_mbps`、`ul_throughput_mbps`。
- 质量类：`drop_rate`、`handover_success_rate`、`access_success_rate`、`volte_drop_rate`。
- 时延与可用性：`latency_ms`、`availability_rate`。
- 故障与负载：`alarm_count`、`interference_score`、`load_score`。
- 其他补充数值字段：用于补齐到 50 个字段，并保持与无线网络状态相关的合理分布和相关性。

生成器默认输出分区 Parquet 文件，用于提升后续 Iceberg 写入和 StarRocks 导入效率。CSV 仅作为可选格式，用于特定 StarRocks 导入链路需要时启用。

数据规模 profile：

- `smoke`：`10,000` 小区，`1` 天。
- `medium`：默认 `50,000` 小区，`3` 天，可配置。
- `full`：`400,000` 小区，`7` 天。

生成器支持显式参数覆盖：

- `--profile`
- `--cells`
- `--start`
- `--days`
- `--seed`
- `--output`

### 三种表形态

工程需要创建逻辑等价的三种表：

- `sr_internal.cell_kpi_1min`：StarRocks 内表。
- `iceberg_db.cell_kpi_1min`：Spark SQL 原生维护和查询的 Iceberg 表。
- `sr_external_iceberg.cell_kpi_1min`：StarRocks external catalog 挂接 Iceberg 后看到的外表。

物理组织建议：

- Iceberg 表按 `days(event_time)` 分区；如果写入工具支持，可进一步围绕 `cell_id` 做聚簇或 bucket。
- StarRocks 内表按日期分区，并按 `cell_id` hash 分桶。

三种表的字段语义必须一致。不同引擎只允许在物理布局、DDL 语法、导入方式上存在差异。

## 基准测试流程

### 导入流程

Benchmark runner 按顺序执行：

1. 生成无线 KPI 数据。
2. 通过 Spark 写入 Iceberg 表。
3. 将同一份逻辑数据导入 StarRocks 内表。
4. 刷新 StarRocks external Iceberg catalog 元数据。
5. 记录每个阶段的耗时、行数、字节数、records/s 和 MB/s。

### 查询流程

Runner 使用同一组逻辑查询，分别在以下目标上执行：

- Spark SQL 原生 Iceberg。
- StarRocks 内表。
- StarRocks 外表挂接 Iceberg。

必备查询场景：

- 单小区一天趋势查询。
- 单小区一周趋势查询。
- 按城市、厂家、频段、制式做分钟级聚合。
- 按 `prb_dl_util`、`active_users`、`load_score` 查询 Top N 高负载小区。
- 基于 `rsrp_avg`、`rsrp_p10`、`sinr_avg` 筛选弱覆盖小区。
- 相邻时间窗口异常 KPI 突增检测。
- 最近 N 分钟热点小区查询。
- 大宽表多字段过滤加 group by。
- 日期分区裁剪查询。
- 数百到数千小区 ID 的列表过滤查询。

每次查询需要记录：

- 查询引擎。
- 表形态。
- 查询名称。
- profile。
- 开始时间和结束时间。
- 耗时毫秒数。
- 返回行数。
- 成功或失败状态。
- 失败时的错误信息。

### 冷热查询

查询 runner 支持两类执行模式：

- `cold`：服务启动后或尽可能清理缓存后的首次查询。
- `warm`：数据和元数据加载完成后的重复查询。

默认 smoke benchmark 至少执行：

- 每个查询 1 次 cold run。
- 每个查询 3 次 warm run。

## 监控设计

Prometheus 需要采集：

- Benchmark runner 自定义指标。
- StarRocks FE 和 BE 指标。
- HDFS NameNode 和 DataNode 指标。
- 通过 Docker 兼容 exporter 采集容器 CPU、内存、网络、磁盘指标。
- Spark 指标，前提是所选镜像和配置能够稳定暴露。

Benchmark runner 必须暴露以下指标：

- `benchmark_load_duration_seconds`
- `benchmark_load_rows_total`
- `benchmark_load_bytes_total`
- `benchmark_query_duration_seconds`
- `benchmark_query_rows_total`
- `benchmark_query_failures_total`

指标标签必须包含：

- `run_id`
- `profile`
- `engine`
- `table_shape`
- `stage`
- `query_name`

Grafana 需要通过 provisioning 自动加载 dashboard，至少包含：

- 运行批次总览。
- 导入吞吐对比。
- 查询延迟对比，按引擎和表形态分组。
- 无线 KPI 查询场景分组。
- 错误率和失败查询。
- StarRocks FE/BE 资源使用。
- HDFS NameNode/DataNode 容量、块状态、读写吞吐和错误指标。
- Benchmark runner 进度和吞吐。

## HTML 测试报告

每次 benchmark run 只要求默认生成一个 HTML 报告，不要求默认生成 Markdown、JSON 或 CSV 报告。

报告路径：

```text
reports/runs/<run_id>/index.html
```

HTML 报告必须包含：

- 运行元信息：run id、profile、开始时间、结束时间、软件版本、主机资源提示。
- 数据集摘要：小区数、天数、行数、字段数、生成数据字节数。
- 导入摘要：各阶段耗时和吞吐。
- 查询摘要：Spark Iceberg、StarRocks 内表、StarRocks 外表挂 Iceberg 的延迟对比。
- 如果存在重复查询，展示每个查询组的 P50、P95、P99。
- 失败查询表和错误详情。
- 指向同一 run id 的 Grafana dashboard 链接。
- 当未执行 full profile 时，在报告中明确说明当前不是 4.032B 行全量验证。

HTML 报告应是静态文件，既可以直接从文件系统打开，也可以由轻量服务暴露。

## 配置设计

Benchmark 使用配置文件，并允许 CLI 参数覆盖配置。

推荐默认配置：

```yaml
profile: smoke
seed: 20260602
dataset:
  cells: 10000
  days: 1
  columns: 50
  start_time: "2026-01-01T00:00:00"
query:
  cold_runs: 1
  warm_runs: 3
  concurrency: 1
report:
  format: html
monitoring:
  prometheus: true
  grafana: true
```

## 成功标准

工程完成后需要满足：

- `docker compose up` 能启动完整基准环境。
- Smoke run 能生成 `14,400,000` 条无线 KPI 记录。
- Runner 能通过 Spark 写入 Iceberg 表。
- Runner 能将等价数据导入 StarRocks 内表。
- StarRocks 能通过 external catalog 查询 Iceberg 表。
- 同一套查询能在三种表形态上执行。
- Prometheus 能采集 benchmark 指标。
- Grafana 能按 run id、引擎、表形态、查询组和无线业务维度展示结果。
- 每次运行能生成 `reports/runs/<run_id>/index.html`。

## 非目标

本阶段不覆盖：

- StarRocks 或 Spark 的生产级容量规划。
- Kubernetes 部署。
- 云对象存储自动开通。
- 与所有生产 Iceberg catalog 形态完全等价。
- 在本机 Docker 环境默认执行 4.032B 行全量 profile。

## 风险与应对

- 本机硬件可能主导性能结果。应对方式：报告中记录 profile 和主机资源提示，全量 profile 必须显式开启。
- Spark 和 StarRocks SQL 方言存在差异。应对方式：查询使用逻辑模板，并通过引擎渲染器生成具体 SQL。
- 全量数据对多数本机环境过大。应对方式：先用 smoke 和 medium profile 验证链路，再在强主机上执行 full profile。
- 不同镜像版本暴露的监控指标可能不一致。应对方式：benchmark runner 自定义指标必须稳定提供，基础设施指标按组件能力采集。
- StarRocks external Iceberg 配置对版本敏感。应对方式：固定镜像版本，并在工程内沉淀 catalog DDL 和启动顺序。

## 自检结果

- 占位内容检查：没有保留待补充占位符。
- 一致性检查：smoke profile、full profile、HTML-only 报告、Spark 原生 Iceberg、Prometheus/Grafana、三种表形态定义一致。
- 范围检查：设计聚焦在一个性能验证工程，可进入单个分阶段 implementation plan。
- 歧义检查：原生 Iceberg 明确为 Spark SQL；默认报告明确为 HTML；全量数据明确为显式 opt-in。

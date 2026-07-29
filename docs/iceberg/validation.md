# Iceberg 验证模块

本模块通过 `iceberg-validate` 子命令独立验证 Apache Iceberg `1.10.1` 的关键能力，不依赖 StarRocks 查询链路。默认配置为 [configs/iceberg-validation.yml](../../configs/iceberg-validation.yml)，报告输出到 `reports/iceberg-validation/<run_id>/`。

## 运行方式

```sh
mvn package
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar iceberg-validate --config configs/iceberg-validation.yml --run-id iceberg-validation-smoke
```

按场景或用例过滤：

```sh
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar iceberg-validate --scenario schemaEvolution --case schema-type-promotion --run-id iceberg-schema-smoke
```

需要保留表、目录和中间证据时使用：

```sh
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar iceberg-validate --keep-artifacts --run-id iceberg-debug
```

## 基础约束

- Iceberg 版本固定为 `1.10.1`。
- HDFS replication 基线为双副本：`replicationBaseline: 2`。
- 共享基础设施由 `../shared-data-infra` 提供，包括 HDFS、Hive Metastore、Spark、StarRocks FE/BE。
- 本仓库不新增 HDFS、Hive、Spark、StarRocks 等重复 Compose 服务。
- 默认 EC policy 覆盖 `RS-3-2-1024k`、`RS-6-3-1024k`、`RS-10-4-1024k`、`XOR-2-1-1024k`。

## 场景覆盖

| 场景 | 用例 | 验证重点 | 性能结论口径 |
| --- | --- | --- | --- |
| Schema 长期演进 | `schema-add-drop-rename`、`schema-type-promotion`、`schema-nested-struct`、`schema-complex-types`、`schema-long-chain-history` | 字段增删改名、数值类型提升、decimal 扩容、嵌套字段变化、多 schema 历史版本兼容读取 | 对比演进前后 planning/query 耗时，结论关注历史快照是否可读，以及延迟是否落在可接受范围 |
| HDFS 纠删码 | `ec-policy-write-read`、`ec-rs-10-4-failure-tolerance`、`ec-policy-matrix-failure`、`ec-file-count-and-disk-usage` | replication=2 与多 EC policy 写读、失效副本容错、文件数和 HDFS 磁盘占用 | 统计 logical bytes、disk bytes、file count、write/read latency，输出 EC 相对双副本的磁盘节省率和延迟倍率 |
| EC/replication 转换 | `replication-to-ec-policy-only`、`replication-to-ec-rewrite`、`ec-to-replication-policy-only`、`ec-to-replication-rewrite` | 目录 policy 切换和物理 rewrite 两类转换路径 | 统计转换耗时、吞吐、转换前后 disk bytes、file count、query latency |
| 多进程并发写入 | `concurrent-append-disjoint-partitions`、`concurrent-append-same-partition`、`concurrent-update-overlap`、`concurrent-mixed-read-write` | 并发 append、同分区提交、重叠 update 冲突、读写隔离 | 统计 writer 数、成功/失败提交数、冲突数、总耗时、提交延迟 |
| 行级更新删除 | `row-update-single-range`、`row-delete-partition-prunable`、`row-delete-selective`、`row-merge-upsert-delete` | Iceberg v2 的 UPDATE、DELETE、MERGE 行级变更 | 统计 mutation 耗时、rewrite data files、delete files、变更后查询耗时 |
| ACID 事务保证 | `acid-kill-before-commit`、`acid-kill-during-commit`、`acid-conflicting-commits`、`acid-reader-isolation` | 快照原子发布、失败写入不可见、冲突隔离、读一致性 | 统计提交前后 snapshot lineage、row count、冲突错误和读延迟 |
| 增量拉取 | `incremental-append-only`、`incremental-multi-snapshot-window`、`incremental-with-delete-update-boundary`、`incremental-expired-snapshot` | snapshot A 到 B 的 append-only 增量窗口，update/delete 边界语义和过期快照行为 | 对比 full scan 与 incremental scan 耗时，输出增量节省率和 snapshot window |
| 时间旅行 | `time-travel-by-snapshot-id`、`time-travel-by-timestamp`、`time-travel-after-schema-evolution`、`time-travel-after-expire` | 按 snapshot id、timestamp 读取历史，schema 演进后的历史投影，过期快照失败语义 | 对比当前快照和历史快照 planning/query 耗时 |
| 小文件 Compaction | `small-files-many-snapshots-build`、`small-files-query-degradation`、`small-files-data-compaction`、`small-files-manifest-rewrite`、`small-files-expire-snapshots` | 构造多 snapshot 多小文件，验证小文件对查询和 planning 的影响，以及 data file/manifest/snapshot 维护效果 | 统计 compaction 前后 data file、manifest、snapshot、metadata JSON、HDFS disk bytes、query latency |

## EC 故障注入

`RS-10-4-1024k` 需要至少 `14` 个 live DataNode 才能完整验证 10 data + 4 parity 的容错场景。当前环境 DataNode 数不足时，用例会返回 `SKIPPED`，报告 evidence 会包含：

```text
policy=RS-10-4-1024k
liveDataNodes=<actual>
requiredDataNodes=14
```

这类跳过不代表 Iceberg 功能失败，只代表当前共享 HDFS 集群规模不足以做该 EC policy 的失效副本验证。

## 报告字段

每个 case 会写入：

- `setupCommands`：建 namespace、drop/create table、基线数据写入脚本。
- `actionCommands`：场景动作脚本，例如 schema evolution、MERGE、EC policy 切换或 compaction。
- `assertions`：功能断言。
- `metrics`：场景采集指标名和值或采集口径。
- `baseline`：replication=2、演进前、compaction 前等基线值。
- `comparison`：延迟倍率、磁盘节省率、文件数差异、转换吞吐等对比结论。
- `evidence`：表名、HDFS location、snapshot、错误或跳过原因。

JSON 和 HTML 报告会同时生成。JSON 便于自动化消费，HTML 用表格呈现验证项、需求关键要素、指标、基线/对比和证据，便于人工审阅。

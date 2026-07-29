# Iceberg 单 DataNode EC 与 Schema 历史读报告增强设计

## 背景

当前 `iceberg-validate` 报告已经改为 HTML + JSON 输出，并移除了将指标名称列表冒充性能结论的问题。但报告仍有几个影响审阅的问题：

- Schema 演进没有展示历史数据查询 SQL，也没有在 ALTER 后写入新数据，因此看不出历史快照和当前快照在 Schema 变化后的读取差异。
- Schema 的元数据/性能指标只展示 `snapshotCount`、`schemaHistoryLength` 等静态值，缺少查询耗时、返回行数和样例结果集。
- EC 纠删码报告使用 `ecPolicyCount=4` 聚合表达，无法对比不同 policy，也没有展示 set policy 的位置和命令。
- 用户的验证环境只有 1 个 DataNode，无法完成 RS 类 EC policy 的真实 block group 编码和故障容忍验证，但仍希望验证磁盘占用和查询性能。

## 目标

1. Schema 演进报告必须展示历史查询 SQL、当前查询 SQL、查询耗时、返回行数和样例数据集。
2. Schema 演进必须在 ALTER 后写入新数据，使报告能体现历史数据和新数据在不同 Schema 状态下的读取影响。
3. 每个验证分类增加“验证点说明”列，用中文说明该用例验证什么。
4. EC 纠删码报告按 baseline 和 policy 拆行展示，不再使用 `ecPolicyCount=4` 作为主结论。
5. 在 1 个 DataNode 下引入明确的 Single-DN EC 评估模式：
   - 真实测量 replication 路径的文件数、HDFS 磁盘占用和查询耗时。
   - 对 EC policy 执行可用性和 path policy 检查。
   - 对无法真实 EC 编码落盘的 policy 输出理论存储估算，并明确标记为 theoretical，不冒充真实 HDFS `du`。
6. EC 故障容忍场景在 DataNode 不足时不再只展示 skip，而是输出“不可真实验证的原因”和“需要补充的真实指标”；如果 policy 只能 policy-only，则不输出故障后查询性能。

## 非目标

- 不在 `data-benchmark` 本仓库新增 HDFS、Spark、Hive、StarRocks 等 Compose 基础设施。
- 不把 1 个 DataNode 下的理论 EC 存储节省标记为真实落盘节省。
- 不声称 RS-3-2、RS-6-3、RS-10-4 在 1 个 DataNode 环境中完成了真实故障容忍验证。
- 本次不强制扩容到 14 个 DataNode。若后续需要完整 RS-10-4 容错验证，应在 `../shared-data-infra` 单独设计 multi-datanode profile。

## Schema 演进设计

每个 Schema case 的执行脚本分为五段：

1. 初始化表并写入 baseline 数据。
2. 采集 `baselineSnapshotId`。
3. 执行该 case 的 ALTER TABLE 操作。
4. 写入 `after_evolution` 新数据。
5. 分别执行历史快照查询和当前快照查询。

报告主表新增“验证点说明”列，Schema section 建议列为：

| 列名 | 内容 |
| --- | --- |
| 用例 | `caseId` |
| 验证点说明 | 中文说明，例如“验证字段 rename 后历史快照按字段 ID 兼容读取，ALTER 后新写入数据在当前 Schema 可见” |
| Schema 变化 | 变更类型、变更数量、关键 ALTER DDL |
| 历史数据查询 | 历史查询 SQL 摘要、`baselineSnapshotId`、`historicalRows`、`historicalQuerySeconds` |
| 当前数据查询 | 当前查询 SQL 摘要、`postAlterSnapshotId`、`currentRows`、`currentQuerySeconds` |
| 元数据/性能指标 | `snapshotCount`、`schemaHistoryLength`、`metadataJsonCount`、查询耗时对比 |
| 状态 | 功能/性能状态 |
| 执行脚本与证据 | 默认折叠，包含完整 SQL、stdout/stderr、返回样例数据集 |

返回数据集默认隐藏，放在行内 `<details>` 中，内容包括：

- `historicalQuerySql`
- `historicalRows`
- `historicalQuerySeconds`
- `historicalSampleRows`
- `currentQuerySql`
- `currentRows`
- `currentQuerySeconds`
- `currentSampleRows`

## EC 单 DataNode 评估设计

EC section 不再输出单行 `ecPolicyCount=4`，改为多行矩阵：

- `replication=1 actual`
- `replication=2 target baseline`
- `RS-3-2-1024k`
- `RS-6-3-1024k`
- `RS-10-4-1024k`
- `XOR-2-1-1024k`

报告主表新增“验证点说明”列，EC section 建议列为：

| 列名 | 内容 |
| --- | --- |
| 用例 | `caseId` 或 policy case id |
| 验证点说明 | 中文说明，例如“在 1 个 DataNode 下验证 policy 设置能力，并用理论值估算 EC 存储开销” |
| Policy/模式 | `replication=1 actual`、`replication=2 target`、具体 EC policy |
| EC 设置位置 | `hdfs ec -setPolicy -path <path> -policy <policy>` 和实际 `getPolicy` 结果 |
| DataNode 条件 | `liveDataNodes`、`requiredDataNodes`、`physicalEcWritable` |
| 相同数据量 | `rowCount`、`logicalBytes`、`checksum` |
| 文件数量 | `fileCount` 或 `fileCountStatus` |
| 磁盘占用 | 真实 `hdfsDiskBytes`；EC 不可真实落盘时展示 `theoreticalEcDiskBytes` |
| 查询效率 | 真实可执行路径的 `querySeconds`；EC policy-only 时标记 `queryPerformanceStatus=notRepresentative` |
| 结论 | actual/theoretical/skipped 的明确结论 |
| 状态 | 功能/性能状态 |
| 执行脚本与证据 | 默认折叠，包含 HDFS/Spark 命令和结果 |

### 1 个 DataNode 下的状态规则

| 条件 | 功能状态 | 性能状态 | 报告结论 |
| --- | --- | --- | --- |
| replication=1 实际写入和查询成功 | PASS | ACCEPTABLE 或 DEGRADED | 真实磁盘和查询性能可用 |
| replication=2 在 1 DN 下写入成功但 under-replicated | DEGRADED | ACCEPTABLE 或 NOT_COMPARABLE | 可作为目标双副本基线，但要标记副本不足 |
| EC policy set 成功但无法真实形成 block group | DEGRADED | NOT_COMPARABLE | policy-only 成功，磁盘节省仅理论估算 |
| EC policy set 失败 | SKIPPED 或 FAIL | NOT_COMPARABLE | 展示失败命令、exit code 和错误 |
| EC 故障容忍因 DataNode 不足 | SKIPPED | NOT_COMPARABLE | 展示 `liveDataNodes`、`requiredDataNodes`，不输出故障后查询性能 |

## EC 理论存储估算

当 `liveDataNodes < requiredDataNodes` 时，报告输出理论字段：

```text
theoreticalEcDiskBytes = logicalBytes * (dataBlocks + parityBlocks) / dataBlocks
theoreticalSavingVsReplication2 = 1 - theoreticalEcDiskBytes / (logicalBytes * 2)
```

policy 参数：

| Policy | dataBlocks | parityBlocks | requiredDataNodes |
| --- | ---: | ---: | ---: |
| RS-3-2-1024k | 3 | 2 | 5 |
| RS-6-3-1024k | 6 | 3 | 9 |
| RS-10-4-1024k | 10 | 4 | 14 |
| XOR-2-1-1024k | 2 | 1 | 3 |

理论字段必须带 `theoretical` 前缀，真实 HDFS `du` 字段必须带 `actual` 或直接使用 `hdfsDiskBytes`。报告不得把理论值放入真实 HDFS 磁盘占用列而不标注。

## 数据流

1. `IcebergValidationRunner` 调用 scenario。
2. Schema scenario 使用 Spark SQL executor 执行建表、插入、ALTER、历史查询和当前查询，并将每次执行写入 `IcebergExecutionEvidence`。
3. EC scenario 使用 Spark SQL executor 写入相同规模数据，使用 HDFS CLI 采集 `du/count/getPolicy/setPolicy` 结果。
4. scenario 写入结构化 metrics：
   - 实测字段放入 `metrics`、`baseline`、`comparison`。
   - 不可实测字段放入 `metricCollectionStatus`、`notExecutedReason`、`theoretical*` 字段。
5. HTML writer 按场景列渲染主指标，执行脚本和返回数据集放入默认隐藏的 details。

## 错误处理

- Spark SQL 或 HDFS 命令失败时保留 exit code、stdout、stderr。
- 单个 policy 失败不应导致整个 EC section 丢失，失败行单独展示。
- DataNode 不足属于环境约束，不等同 Iceberg 功能失败；必须展示为 `SKIPPED` 或 `DEGRADED / NOT_COMPARABLE`。
- 任何未真实采集的耗时、文件数、磁盘占用不得伪造默认值。

## 测试策略

- Schema 测试：
  - 先写 failing test，断言 action commands 包含历史查询 SQL 和 ALTER 后插入。
  - 断言 metrics 包含 `historicalRows`、`currentRows`、`historicalQuerySeconds`、`currentQuerySeconds` 或明确的 not executed 状态。
  - 断言 HTML 中出现“验证点说明”，并且返回数据集默认隐藏。
- EC 测试：
  - 断言不再输出 `ecPolicyCount`。
  - 断言按 policy 拆行，包含 replication baseline 行。
  - 断言 `setPolicy`、`getPolicy`、`du`、`count` 命令进入执行证据。
  - 断言 1 DataNode 下 EC 行使用 `theoreticalEcDiskBytes`，并标记 `physicalEcWritable=false`。
  - 断言故障容忍 DataNode 不足时没有伪造 `querySecondsAfterFailure`。
- 报告审计：
  - HTML/JSON 不出现旧占位字段。
  - HTML 出现 `actual`、`theoretical`、`notRepresentative` 或 `NOT_COMPARABLE` 等区分真实与估算的状态。

## 验收标准

- Schema 报告能看到历史查询 SQL、当前查询 SQL、ALTER 后新数据插入、查询耗时、返回行数和默认隐藏的数据集样例。
- EC 报告能看到相同数据量下 replication baseline 和每个 EC policy 的文件数、磁盘占用、查询效率字段。
- 1 个 DataNode 下的 EC 磁盘节省只作为理论估算展示，真实 `hdfsDiskBytes` 仅用于实际可执行路径。
- 纠删码故障容忍场景展示 DataNode 不足的硬约束和跳过原因，不展示伪造的故障后查询性能。
- 不修改 `data-benchmark/docker-compose.yml` 添加本地 HDFS 等重复基础设施。

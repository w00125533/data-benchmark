# AGENTS.md

## 公共基础设施约束

- 新增或修改 Docker Compose 基础设施前，必须先检查 `../shared-data-infra` 是否已经定义同类服务或 profile。
- 如果 `../shared-data-infra` 已定义 HDFS、Hive、Spark、YARN、Kafka、ZooKeeper、StarRocks、Prometheus、Grafana 等能力，不要在本工程重复新增；通过 external network、环境变量和项目级命名空间复用。
- 只有 benchmark 隔离、性能对比或现有 runner 兼容性明确需要时，才允许保留 project-local 基础设施，并在 compose 或 README 中写明原因。
- 修改基础设施后，至少运行 `docker compose -f docker-compose.yml config`。

## 当前例外

- `spark` 和 `hive-server` 保持 project-local，因为 compose runner 需要 `docker compose exec` 到这些服务。
- `starrocks-fe`/`starrocks-be` 保持 project-local，因为 benchmark 场景需要独立 FE/BE 生命周期控制。

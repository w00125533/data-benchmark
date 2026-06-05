# AGENTS.md

## 项目概览

`data-benchmark` 是本地数据平台 benchmark 工程，使用 Docker Compose 启动 StarRocks、Spark、Hive、HDFS 等组件，并通过 benchmark runner 执行数据生成、装载、查询和报告输出。

## 公共基础设施约束

- 在新增或修改 Docker Compose 基础设施前，先检查同级目录下其他工程以及计划中的公共基础设施是否已经提供相同能力，尤其是 HDFS、Hive Metastore、HiveServer2、Spark、YARN、Kafka、ZooKeeper、StarRocks、Prometheus、Grafana 等。
- 能复用公共基础设施时，不要在本工程再次新增一套同类服务；业务容器应通过 external network、环境变量和独立命名空间连接公共服务。
- 本工程需要隔离时，优先使用独立 HDFS 路径、Hive database、Kafka topic prefix、StarRocks database、checkpoint 路径和数据卷，而不是复制基础设施容器。
- 只有当测试、性能对比或故障隔离明确要求独占实例时，才允许在本工程保留 project-local 基础设施，并在相关 compose 或文档中写明原因。
- 避免新增固定 `container_name` 和固定宿主端口，除非该服务是公共入口或已有约定；新增端口前要确认不会与其他工程冲突。
- 修改基础设施后，更新 README/AGENTS 中的启动说明，并运行对应 `docker compose ... config` 验证配置有效。

## 常用验证

```bash
docker compose -f docker-compose.yml config
mvn test
```

## Current project-local exceptions

- Keep `spark` and `hive-server` project-local because the Java compose runner uses `docker compose exec` against those services.
- Keep split `starrocks-fe` and `starrocks-be` project-local with fixed IPs because benchmark cold restart semantics require FE/BE lifecycle control.

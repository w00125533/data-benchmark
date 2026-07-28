# Iceberg Validation Module Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增独立 `iceberg-validate` 验证模块，用 Apache Iceberg `1.10.1` 验证 schema 演进、HDFS 纠删码、并发写入、行级更新删除、ACID、增量拉取、时间旅行、小文件与多 snapshot compaction，并输出 JSON/Markdown 结论报告。

**Architecture:** 新模块放在 `com.example.databenchmark.iceberg`，独立于现有 KPI/TPC-H `ComposeBenchmarkRunner`。模块复用 `CommandRunner` 和 shared infra compose 约定，通过 Spark SQL/HDFS CLI 执行验证，用 scenario/case/result 模型沉淀证据和性能结论。

**Tech Stack:** Java 17, Picocli, Jackson YAML/JSON, Spark SQL, Apache Iceberg Spark runtime `1.10.1`, HDFS CLI, JUnit 5, AssertJ.

---

## 文件结构

新增文件：

- `configs/iceberg-validation.yml`：默认 smoke 配置。
- `src/main/java/com/example/databenchmark/iceberg/IcebergValidationConfig.java`：配置 record 与默认值。
- `src/main/java/com/example/databenchmark/iceberg/IcebergValidationConfigLoader.java`：YAML 加载、默认值归一化、校验。
- `src/main/java/com/example/databenchmark/iceberg/IcebergValidationCase.java`：用例定义。
- `src/main/java/com/example/databenchmark/iceberg/IcebergValidationContext.java`：运行上下文，持有 config、runId、workspace、执行器、报告根目录。
- `src/main/java/com/example/databenchmark/iceberg/IcebergValidationScenario.java`：场景接口。
- `src/main/java/com/example/databenchmark/iceberg/IcebergValidationResult.java`：单用例结果。
- `src/main/java/com/example/databenchmark/iceberg/IcebergValidationReport.java`：整次运行报告。
- `src/main/java/com/example/databenchmark/iceberg/IcebergConclusion.java`：功能状态、性能状态、错误类型枚举。
- `src/main/java/com/example/databenchmark/iceberg/IcebergValidationRunner.java`：场景过滤、执行、错误隔离、汇总。
- `src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java`：JSON/Markdown 输出。
- `src/main/java/com/example/databenchmark/iceberg/IcebergValidateCommand.java`：Picocli 子命令。
- `src/main/java/com/example/databenchmark/iceberg/sql/SparkSqlScriptBuilder.java`：Spark SQL 脚本构造。
- `src/main/java/com/example/databenchmark/iceberg/sql/IcebergSqlTemplates.java`：建表、插入、演进、mutation、维护 SQL。
- `src/main/java/com/example/databenchmark/iceberg/sql/MetadataTableQueries.java`：snapshots/files/manifests/history 查询。
- `src/main/java/com/example/databenchmark/iceberg/exec/SparkSqlExecutor.java`：通过 shared infra Spark 容器执行 SQL。
- `src/main/java/com/example/databenchmark/iceberg/hdfs/HdfsCliClient.java`：HDFS `dfs` 命令封装。
- `src/main/java/com/example/databenchmark/iceberg/hdfs/HdfsEcPolicyClient.java`：EC policy 管理和查询。
- `src/main/java/com/example/databenchmark/iceberg/hdfs/HdfsUsageCollector.java`：`hdfs dfs -du -s` 与 `-count` 解析。
- `src/main/java/com/example/databenchmark/iceberg/hdfs/HdfsFaultInjector.java`：DataNode 停止/恢复的安全封装。
- `src/main/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenario.java`
- `src/main/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenario.java`
- `src/main/java/com/example/databenchmark/iceberg/scenario/ErasureCodingConversionScenario.java`
- `src/main/java/com/example/databenchmark/iceberg/scenario/ConcurrentWriteScenario.java`
- `src/main/java/com/example/databenchmark/iceberg/scenario/RowLevelMutationScenario.java`
- `src/main/java/com/example/databenchmark/iceberg/scenario/AcidTransactionScenario.java`
- `src/main/java/com/example/databenchmark/iceberg/scenario/IncrementalPullScenario.java`
- `src/main/java/com/example/databenchmark/iceberg/scenario/TimeTravelScenario.java`
- `src/main/java/com/example/databenchmark/iceberg/scenario/SmallFileCompactionScenario.java`

修改文件：

- `src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java`：注册 `iceberg-validate` 子命令，扩展 `RunnerFactory`。

新增测试：

- `src/test/java/com/example/databenchmark/iceberg/IcebergValidationConfigLoaderTest.java`
- `src/test/java/com/example/databenchmark/iceberg/IcebergValidateCommandTest.java`
- `src/test/java/com/example/databenchmark/iceberg/IcebergValidationRunnerTest.java`
- `src/test/java/com/example/databenchmark/iceberg/IcebergValidationReportWriterTest.java`
- `src/test/java/com/example/databenchmark/iceberg/sql/IcebergSqlTemplatesTest.java`
- `src/test/java/com/example/databenchmark/iceberg/exec/SparkSqlExecutorTest.java`
- `src/test/java/com/example/databenchmark/iceberg/hdfs/HdfsUsageCollectorTest.java`
- `src/test/java/com/example/databenchmark/iceberg/hdfs/HdfsEcPolicyClientTest.java`
- `src/test/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenarioTest.java`
- `src/test/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenarioTest.java`
- `src/test/java/com/example/databenchmark/iceberg/scenario/ErasureCodingConversionScenarioTest.java`
- `src/test/java/com/example/databenchmark/iceberg/scenario/ConcurrentWriteScenarioTest.java`
- `src/test/java/com/example/databenchmark/iceberg/scenario/RowLevelMutationScenarioTest.java`
- `src/test/java/com/example/databenchmark/iceberg/scenario/AcidTransactionScenarioTest.java`
- `src/test/java/com/example/databenchmark/iceberg/scenario/IncrementalPullScenarioTest.java`
- `src/test/java/com/example/databenchmark/iceberg/scenario/TimeTravelScenarioTest.java`
- `src/test/java/com/example/databenchmark/iceberg/scenario/SmallFileCompactionScenarioTest.java`

---

### Task 1: 配置模型与默认配置

**Files:**
- Create: `configs/iceberg-validation.yml`
- Create: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationConfig.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationConfigLoader.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/IcebergValidationConfigLoaderTest.java`

- [ ] **Step 1: 写配置加载失败测试**

在 `IcebergValidationConfigLoaderTest` 添加：

```java
@Test
void rejectsUnsupportedIcebergVersion() throws Exception {
    Path config = tempDir.resolve("iceberg-validation.yml");
    Files.writeString(config, """
        iceberg:
          version: "1.7.1"
          catalog: "iceberg_catalog"
          namespace: "iceberg_validation"
          warehouse: "hdfs://hdfs-namenode:8020/warehouse/iceberg"
          formatVersion: 2
        spark:
          service: "spark"
          timeoutSeconds: 900
          packages:
            - "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1"
          extensions:
            - "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        hdfs:
          defaultFs: "hdfs://hdfs-namenode:8020"
          replicationBaseline: 2
          ecPolicies: ["RS-10-4-1024k"]
        scale:
          profile: "smoke"
          rows: 100000
          partitions: 8
          smallFileCommits: 100
          filesPerCommit: 4
          concurrentWriters: [2, 4, 8]
        scenarios:
          schemaEvolution:
            enabled: true
        report:
          output: "reports/iceberg-validation"
          formats: ["json", "markdown"]
        """);

    assertThatThrownBy(() -> new IcebergValidationConfigLoader().load(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("iceberg.version must be 1.10.1");
}
```

- [ ] **Step 2: 运行失败测试**

Run: `python -m pytest` 不适用本仓库。使用：

```powershell
mvn -Dtest=IcebergValidationConfigLoaderTest test
```

Expected: 编译失败，提示 `IcebergValidationConfigLoader` 不存在。

- [ ] **Step 3: 新增默认配置文件**

创建 `configs/iceberg-validation.yml`：

```yaml
iceberg:
  version: "1.10.1"
  catalog: "iceberg_catalog"
  namespace: "iceberg_validation"
  warehouse: "hdfs://hdfs-namenode:8020/warehouse/iceberg"
  formatVersion: 2
spark:
  service: "spark"
  timeoutSeconds: 900
  packages:
    - "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1"
  extensions:
    - "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
hdfs:
  defaultFs: "hdfs://hdfs-namenode:8020"
  replicationBaseline: 2
  ecPolicies:
    - "RS-3-2-1024k"
    - "RS-6-3-1024k"
    - "RS-10-4-1024k"
    - "XOR-2-1-1024k"
scale:
  profile: "smoke"
  rows: 100000
  partitions: 8
  smallFileCommits: 100
  filesPerCommit: 4
  concurrentWriters: [2, 4, 8]
scenarios:
  schemaEvolution:
    enabled: true
  erasureCoding:
    enabled: true
  erasureCodingConversion:
    enabled: true
  concurrentWrite:
    enabled: true
  rowLevelMutation:
    enabled: true
  acidTransaction:
    enabled: true
  incrementalPull:
    enabled: true
  timeTravel:
    enabled: true
  smallFileCompaction:
    enabled: true
report:
  output: "reports/iceberg-validation"
  formats: ["json", "markdown"]
```

- [ ] **Step 4: 实现配置 record**

`IcebergValidationConfig.java`：

```java
package com.example.databenchmark.iceberg;

import java.util.List;
import java.util.Map;

public record IcebergValidationConfig(
    IcebergConfig iceberg,
    SparkConfig spark,
    HdfsConfig hdfs,
    ScaleConfig scale,
    Map<String, ScenarioConfig> scenarios,
    ReportConfig report
) {
    public record IcebergConfig(String version, String catalog, String namespace, String warehouse, int formatVersion) {}

    public record SparkConfig(String service, int timeoutSeconds, List<String> packages, List<String> extensions) {}

    public record HdfsConfig(String defaultFs, int replicationBaseline, List<String> ecPolicies) {}

    public record ScaleConfig(
        String profile,
        long rows,
        int partitions,
        int smallFileCommits,
        int filesPerCommit,
        List<Integer> concurrentWriters
    ) {}

    public record ScenarioConfig(boolean enabled) {}

    public record ReportConfig(String output, List<String> formats) {}
}
```

- [ ] **Step 5: 实现配置 loader**

`IcebergValidationConfigLoader.java`：

```java
package com.example.databenchmark.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;

public class IcebergValidationConfigLoader {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory()).findAndRegisterModules();

    public IcebergValidationConfig load(Path path) throws IOException {
        IcebergValidationConfig config = mapper.readValue(path.toFile(), IcebergValidationConfig.class);
        validate(config);
        return config;
    }

    private static void validate(IcebergValidationConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (config.iceberg() == null) {
            throw new IllegalArgumentException("iceberg must not be null");
        }
        requireEquals(config.iceberg().version(), "1.10.1", "iceberg.version");
        requireNonBlank(config.iceberg().catalog(), "iceberg.catalog");
        requireNonBlank(config.iceberg().namespace(), "iceberg.namespace");
        requireNonBlank(config.iceberg().warehouse(), "iceberg.warehouse");
        if (config.iceberg().formatVersion() != 2) {
            throw new IllegalArgumentException("iceberg.formatVersion must be 2");
        }
        if (config.spark() == null) {
            throw new IllegalArgumentException("spark must not be null");
        }
        requireNonBlank(config.spark().service(), "spark.service");
        requirePositive(config.spark().timeoutSeconds(), "spark.timeoutSeconds");
        requireNotEmpty(config.spark().packages(), "spark.packages");
        requireNotEmpty(config.spark().extensions(), "spark.extensions");
        if (config.spark().packages().stream().noneMatch(value -> value.contains(":1.10.1"))) {
            throw new IllegalArgumentException("spark.packages must include Iceberg 1.10.1");
        }
        if (config.hdfs() == null) {
            throw new IllegalArgumentException("hdfs must not be null");
        }
        requireNonBlank(config.hdfs().defaultFs(), "hdfs.defaultFs");
        if (config.hdfs().replicationBaseline() != 2) {
            throw new IllegalArgumentException("hdfs.replicationBaseline must be 2");
        }
        requireNotEmpty(config.hdfs().ecPolicies(), "hdfs.ecPolicies");
        if (config.scale() == null) {
            throw new IllegalArgumentException("scale must not be null");
        }
        requireNonBlank(config.scale().profile(), "scale.profile");
        requirePositive(config.scale().rows(), "scale.rows");
        requirePositive(config.scale().partitions(), "scale.partitions");
        requirePositive(config.scale().smallFileCommits(), "scale.smallFileCommits");
        requirePositive(config.scale().filesPerCommit(), "scale.filesPerCommit");
        requireNotEmpty(config.scale().concurrentWriters(), "scale.concurrentWriters");
        if (config.scenarios() == null || config.scenarios().isEmpty()) {
            throw new IllegalArgumentException("scenarios must not be empty");
        }
        if (config.report() == null) {
            throw new IllegalArgumentException("report must not be null");
        }
        requireNonBlank(config.report().output(), "report.output");
        requireNotEmpty(config.report().formats(), "report.formats");
    }

    private static void requireEquals(String actual, String expected, String field) {
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException(field + " must be " + expected);
        }
    }

    private static void requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }

    private static void requirePositive(long value, String field) {
        if (value <= 0) {
            throw new IllegalArgumentException(field + " must be positive");
        }
    }

    private static void requireNotEmpty(java.util.Collection<?> values, String field) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be empty");
        }
    }
}
```

- [ ] **Step 6: 补充成功加载测试**

在 `IcebergValidationConfigLoaderTest` 添加：

```java
@Test
void loadsDefaultIcebergValidationConfig() throws Exception {
    IcebergValidationConfig config = new IcebergValidationConfigLoader()
        .load(Path.of("configs", "iceberg-validation.yml"));

    assertThat(config.iceberg().version()).isEqualTo("1.10.1");
    assertThat(config.hdfs().replicationBaseline()).isEqualTo(2);
    assertThat(config.hdfs().ecPolicies()).contains("RS-10-4-1024k");
    assertThat(config.scale().smallFileCommits()).isEqualTo(100);
    assertThat(config.scenarios()).containsKeys(
        "schemaEvolution",
        "erasureCoding",
        "erasureCodingConversion",
        "smallFileCompaction"
    );
}
```

- [ ] **Step 7: 运行配置测试**

```powershell
mvn -Dtest=IcebergValidationConfigLoaderTest test
```

Expected: `BUILD SUCCESS`，测试数至少 2。

- [ ] **Step 8: 提交**

```powershell
git add configs/iceberg-validation.yml src/main/java/com/example/databenchmark/iceberg/IcebergValidationConfig.java src/main/java/com/example/databenchmark/iceberg/IcebergValidationConfigLoader.java src/test/java/com/example/databenchmark/iceberg/IcebergValidationConfigLoaderTest.java
git commit -m "feat: add Iceberg validation config"
```

---

### Task 2: 结果模型、结论枚举和报告 writer

**Files:**
- Create: `src/main/java/com/example/databenchmark/iceberg/IcebergConclusion.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationCase.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationResult.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationReport.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/IcebergValidationReportWriterTest.java`

- [ ] **Step 1: 写报告序列化测试**

```java
@Test
void writesJsonAndMarkdownReports() throws Exception {
    IcebergValidationReport report = new IcebergValidationReport(
        "run-1",
        "1.10.1",
        "smoke",
        "SUCCESS",
        "2026-07-28T00:00:00Z",
        "2026-07-28T00:00:01Z",
        List.of(new IcebergValidationResult(
            "schemaEvolution",
            "schema-add-drop-rename",
            "verify schema compatibility",
            Map.of("rows", "100000"),
            List.of("CREATE TABLE ..."),
            List.of("ALTER TABLE ..."),
            List.of("row count matched"),
            Map.of("snapshotCount", "3", "queryMs", "12.5"),
            Map.of("queryMs", "10.0"),
            Map.of("latencyRatio", "1.25"),
            IcebergConclusion.FunctionStatus.PASS,
            IcebergConclusion.PerformanceStatus.ACCEPTABLE,
            "历史数据兼容读取通过，查询延迟为基线 1.25 倍。",
            List.of("snapshotId=123"),
            List.of()
        ))
    );

    Path root = tempDir.resolve("reports");
    Path index = new IcebergValidationReportWriter().write(report, root);

    assertThat(index).isEqualTo(root.resolve("run-1").resolve("report.md"));
    assertThat(root.resolve("run-1").resolve("report.json")).exists();
    assertThat(root.resolve("run-1").resolve("report.md")).exists();
    assertThat(Files.readString(root.resolve("run-1").resolve("report.md")))
        .contains("# Iceberg Validation Report")
        .contains("schema-add-drop-rename")
        .contains("历史数据兼容读取通过");
}
```

- [ ] **Step 2: 运行失败测试**

```powershell
mvn -Dtest=IcebergValidationReportWriterTest test
```

Expected: 编译失败，提示报告类不存在。

- [ ] **Step 3: 实现枚举和 record**

`IcebergConclusion.java`：

```java
package com.example.databenchmark.iceberg;

public final class IcebergConclusion {
    private IcebergConclusion() {}

    public enum FunctionStatus {
        PASS,
        FAIL,
        SKIPPED,
        DEGRADED
    }

    public enum PerformanceStatus {
        GOOD,
        ACCEPTABLE,
        DEGRADED,
        NOT_COMPARABLE
    }

    public enum ErrorType {
        INFRA_UNAVAILABLE,
        UNSUPPORTED_FEATURE,
        ASSERTION_FAILED,
        PERFORMANCE_DEGRADED,
        COMMAND_FAILED,
        CLEANUP_FAILED
    }
}
```

`IcebergValidationCase.java`：

```java
package com.example.databenchmark.iceberg;

import java.util.Map;

public record IcebergValidationCase(
    String scenario,
    String caseId,
    String purpose,
    Map<String, String> parameters,
    boolean requiredForSuite
) {}
```

`IcebergValidationResult.java`：

```java
package com.example.databenchmark.iceberg;

import java.util.List;
import java.util.Map;

public record IcebergValidationResult(
    String scenario,
    String caseId,
    String purpose,
    Map<String, String> dataScale,
    List<String> setupCommands,
    List<String> actionCommands,
    List<String> assertions,
    Map<String, String> metrics,
    Map<String, String> baseline,
    Map<String, String> comparison,
    IcebergConclusion.FunctionStatus functionStatus,
    IcebergConclusion.PerformanceStatus performanceStatus,
    String conclusion,
    List<String> evidence,
    List<String> errors
) {
    public boolean successful() {
        return functionStatus == IcebergConclusion.FunctionStatus.PASS
            || functionStatus == IcebergConclusion.FunctionStatus.DEGRADED;
    }
}
```

`IcebergValidationReport.java`：

```java
package com.example.databenchmark.iceberg;

import java.util.List;

public record IcebergValidationReport(
    String runId,
    String icebergVersion,
    String profile,
    String status,
    String startedAt,
    String endedAt,
    List<IcebergValidationResult> results
) {}
```

- [ ] **Step 4: 实现报告 writer**

`IcebergValidationReportWriter.java`：

```java
package com.example.databenchmark.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class IcebergValidationReportWriter {
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()
        .enable(SerializationFeature.INDENT_OUTPUT);

    public Path write(IcebergValidationReport report, Path reportRoot) throws IOException {
        Path runDir = reportRoot.resolve(report.runId());
        Files.createDirectories(runDir);
        mapper.writeValue(runDir.resolve("report.json").toFile(), report);
        Files.writeString(runDir.resolve("report.md"), markdown(report));
        return runDir.resolve("report.md");
    }

    private static String markdown(IcebergValidationReport report) {
        StringBuilder builder = new StringBuilder();
        builder.append("# Iceberg Validation Report\n\n");
        builder.append("- Run ID: `").append(report.runId()).append("`\n");
        builder.append("- Iceberg Version: `").append(report.icebergVersion()).append("`\n");
        builder.append("- Profile: `").append(report.profile()).append("`\n");
        builder.append("- Status: `").append(report.status()).append("`\n\n");
        builder.append("| Scenario | Case | Function | Performance | Conclusion |\n");
        builder.append("| --- | --- | --- | --- | --- |\n");
        for (IcebergValidationResult result : report.results()) {
            builder.append("| ")
                .append(result.scenario()).append(" | ")
                .append(result.caseId()).append(" | ")
                .append(result.functionStatus()).append(" | ")
                .append(result.performanceStatus()).append(" | ")
                .append(escape(result.conclusion())).append(" |\n");
        }
        builder.append("\n## Evidence\n\n");
        for (IcebergValidationResult result : report.results()) {
            builder.append("### ").append(result.scenario()).append(" / ").append(result.caseId()).append("\n\n");
            for (String evidence : result.evidence()) {
                builder.append("- ").append(evidence).append("\n");
            }
            for (String error : result.errors()) {
                builder.append("- ERROR: ").append(error).append("\n");
            }
            builder.append("\n");
        }
        return builder.toString();
    }

    private static String escape(String value) {
        return value == null ? "" : value.replace("|", "\\|").replace("\r", " ").replace("\n", " ");
    }
}
```

- [ ] **Step 5: 运行报告测试**

```powershell
mvn -Dtest=IcebergValidationReportWriterTest test
```

Expected: `BUILD SUCCESS`。

- [ ] **Step 6: 提交**

```powershell
git add src/main/java/com/example/databenchmark/iceberg src/test/java/com/example/databenchmark/iceberg/IcebergValidationReportWriterTest.java
git commit -m "feat: add Iceberg validation report model"
```

---

### Task 3: CLI 子命令和 runner 骨架

**Files:**
- Modify: `src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/IcebergValidateCommand.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationRunner.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationContext.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationScenario.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/IcebergValidateCommandTest.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/IcebergValidationRunnerTest.java`
- Modify: `src/test/java/com/example/databenchmark/BenchmarkRunnerAppTest.java`

- [ ] **Step 1: 写 CLI 暴露测试**

在 `BenchmarkRunnerAppTest.cliExposesCoreCommandsAndStandardOptions` 断言中加入：

```java
assertThat(commandLine.getSubcommands().keySet()).contains("iceberg-validate");
```

在 `helpListsCoreCommands` 加入：

```java
assertThat(usage).contains("iceberg-validate");
```

- [ ] **Step 2: 写命令调度测试**

`IcebergValidateCommandTest.java`：

```java
@Test
void commandDispatchesToIcebergValidationRunner() throws Exception {
    Path reportDir = tempDir.resolve("reports");
    Path config = tempDir.resolve("iceberg-validation.yml");
    Files.writeString(config, validConfig(reportDir));
    FakeRunnerFactory runners = new FakeRunnerFactory();

    CommandResult result = execute(new BenchmarkRunnerApp(runners),
        "iceberg-validate",
        "--config", config.toString(),
        "--run-id", "iceberg-run",
        "--scenario", "schemaEvolution",
        "--case", "schema-add-drop-rename",
        "--keep-artifacts"
    );

    assertThat(result.exitCode()).isZero();
    assertThat(result.out()).contains("cases=1");
    assertThat(result.out()).contains("report=" + reportDir.resolve("iceberg-run").resolve("report.md"));
    assertThat(runners.calls).containsExactly("iceberg:iceberg-run:[schemaEvolution]:[schema-add-drop-rename]:true");
}
```

- [ ] **Step 3: 运行失败测试**

```powershell
mvn -Dtest=BenchmarkRunnerAppTest,IcebergValidateCommandTest test
```

Expected: 编译失败或断言失败，因为子命令尚未注册。

- [ ] **Step 4: 实现子命令类**

`IcebergValidateCommand.java`：

```java
package com.example.databenchmark.iceberg;

import com.example.databenchmark.BenchmarkRunnerApp;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(name = "iceberg-validate", description = "Run Apache Iceberg capability validation scenarios.")
public class IcebergValidateCommand implements Callable<Integer> {
    @ParentCommand
    BenchmarkRunnerApp parent;

    @Spec
    CommandSpec spec;

    @CommandLine.Option(names = "--config", defaultValue = "configs/iceberg-validation.yml")
    Path configPath;

    @CommandLine.Option(names = "--run-id")
    String runId;

    @CommandLine.Option(names = "--scenario")
    List<String> scenarios = new ArrayList<>();

    @CommandLine.Option(names = "--case")
    List<String> cases = new ArrayList<>();

    @CommandLine.Option(names = "--keep-artifacts")
    boolean keepArtifacts;

    @Override
    public Integer call() throws Exception {
        IcebergValidationConfig config = new IcebergValidationConfigLoader().load(configPath);
        IcebergValidationReport report = parent.runnerFactory()
            .runIcebergValidation(config, runId, scenarios, cases, keepArtifacts);
        Path reportPath = Path.of(config.report().output()).resolve(report.runId()).resolve("report.md");
        spec.commandLine().getOut().printf(
            "cases=%d report=%s%n",
            report.results().size(),
            reportPath
        );
        return "SUCCESS".equals(report.status()) ? 0 : 1;
    }
}
```

- [ ] **Step 5: 修改 `BenchmarkRunnerApp`**

改动点：

```java
import com.example.databenchmark.iceberg.IcebergValidateCommand;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationReport;
import com.example.databenchmark.iceberg.IcebergValidationRunner;
import java.util.List;
```

在 `@Command(subcommands = { ... })` 加入：

```java
IcebergValidateCommand.class
```

把构造器可见性保持包内即可，但新增访问器：

```java
public RunnerFactory runnerFactory() {
    return runnerFactory;
}
```

扩展 `RunnerFactory`：

```java
IcebergValidationReport runIcebergValidation(
    IcebergValidationConfig config,
    String runId,
    List<String> scenarios,
    List<String> cases,
    boolean keepArtifacts
) throws Exception;
```

在 `DefaultRunnerFactory` 实现：

```java
@Override
public IcebergValidationReport runIcebergValidation(
    IcebergValidationConfig config,
    String runId,
    List<String> scenarios,
    List<String> cases,
    boolean keepArtifacts
) throws Exception {
    return new IcebergValidationRunner().run(config, runId, scenarios, cases, keepArtifacts);
}
```

- [ ] **Step 6: 实现 runner 最小骨架**

`IcebergValidationScenario.java`：

```java
package com.example.databenchmark.iceberg;

import java.util.List;

public interface IcebergValidationScenario {
    String name();
    List<IcebergValidationCase> cases(IcebergValidationConfig config);
    IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) throws Exception;
}
```

`IcebergValidationContext.java`：

```java
package com.example.databenchmark.iceberg;

import java.nio.file.Path;

public record IcebergValidationContext(
    IcebergValidationConfig config,
    String runId,
    Path workspace,
    Path reportRoot,
    boolean keepArtifacts
) {}
```

`IcebergValidationRunner.java`：

```java
package com.example.databenchmark.iceberg;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public class IcebergValidationRunner {
    private final List<IcebergValidationScenario> scenarios;
    private final IcebergValidationReportWriter reportWriter;

    public IcebergValidationRunner() {
        this(List.of(), new IcebergValidationReportWriter());
    }

    IcebergValidationRunner(List<IcebergValidationScenario> scenarios, IcebergValidationReportWriter reportWriter) {
        this.scenarios = List.copyOf(scenarios);
        this.reportWriter = reportWriter;
    }

    public IcebergValidationReport run(
        IcebergValidationConfig config,
        String runId,
        List<String> scenarioFilter,
        List<String> caseFilter,
        boolean keepArtifacts
    ) throws Exception {
        String actualRunId = runId == null || runId.isBlank() ? "iceberg-validation-" + Instant.now().toEpochMilli() : runId;
        Instant started = Instant.now();
        IcebergValidationContext context = new IcebergValidationContext(
            config,
            actualRunId,
            Path.of(".").toAbsolutePath().normalize(),
            Path.of(config.report().output()),
            keepArtifacts
        );
        List<IcebergValidationResult> results = scenarios.stream()
            .filter(scenario -> scenarioFilter == null || scenarioFilter.isEmpty() || scenarioFilter.contains(scenario.name()))
            .flatMap(scenario -> scenario.cases(config).stream()
                .filter(testCase -> caseFilter == null || caseFilter.isEmpty() || caseFilter.contains(testCase.caseId()))
                .map(testCase -> runCase(scenario, testCase, context)))
            .toList();
        String status = results.stream().allMatch(IcebergValidationResult::successful) ? "SUCCESS" : "DEGRADED";
        IcebergValidationReport report = new IcebergValidationReport(
            actualRunId,
            config.iceberg().version(),
            config.scale().profile(),
            status,
            started.toString(),
            Instant.now().toString(),
            results
        );
        reportWriter.write(report, Path.of(config.report().output()));
        return report;
    }

    private static IcebergValidationResult runCase(
        IcebergValidationScenario scenario,
        IcebergValidationCase testCase,
        IcebergValidationContext context
    ) {
        try {
            return scenario.run(testCase, context);
        } catch (Exception exception) {
            return new IcebergValidationResult(
                scenario.name(),
                testCase.caseId(),
                testCase.purpose(),
                testCase.parameters(),
                List.of(),
                List.of(),
                List.of(),
                java.util.Map.of(),
                java.util.Map.of(),
                java.util.Map.of(),
                IcebergConclusion.FunctionStatus.FAIL,
                IcebergConclusion.PerformanceStatus.NOT_COMPARABLE,
                "用例执行失败。",
                List.of(),
                List.of(exception.getMessage())
            );
        }
    }
}
```

- [ ] **Step 7: 修正 fake factory**

在 `BenchmarkRunnerAppTest.FakeRunnerFactory` 和 `IcebergValidateCommandTest.FakeRunnerFactory` 实现新增方法，返回含一个 result 的 `IcebergValidationReport`。

- [ ] **Step 8: 运行 CLI 测试**

```powershell
mvn -Dtest=BenchmarkRunnerAppTest,IcebergValidateCommandTest,IcebergValidationRunnerTest test
```

Expected: `BUILD SUCCESS`。

- [ ] **Step 9: 提交**

```powershell
git add src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java src/main/java/com/example/databenchmark/iceberg src/test/java/com/example/databenchmark/BenchmarkRunnerAppTest.java src/test/java/com/example/databenchmark/iceberg
git commit -m "feat: add Iceberg validation CLI"
```

---

### Task 4: Spark SQL 执行器和 SQL 模板

**Files:**
- Create: `src/main/java/com/example/databenchmark/iceberg/exec/SparkSqlExecutor.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/sql/SparkSqlScriptBuilder.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/sql/IcebergSqlTemplates.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/sql/MetadataTableQueries.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/exec/SparkSqlExecutorTest.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/sql/IcebergSqlTemplatesTest.java`

- [ ] **Step 1: 写 Spark 命令测试**

```java
@Test
void sparkSqlCommandUsesIceberg1101RuntimeAndExtensions() {
    IcebergValidationConfig config = validConfig();

    List<String> command = new SparkSqlExecutor(new FakeCommandRunner())
        .commandFor(config, "SELECT 1");

    assertThat(command).contains(
        "docker", "compose",
        "-f", "../shared-data-infra/compose.yaml",
        "-f", "../shared-data-infra/compose.lakehouse.yaml",
        "-f", "../shared-data-infra/compose.starrocks.yaml",
        "--profile", "lakehouse",
        "--profile", "lakehouse-tools",
        "--profile", "spark-tools",
        "--profile", "starrocks",
        "exec", "-T", "spark",
        "/opt/spark/bin/spark-sql"
    );
    assertThat(command).anySatisfy(value ->
        assertThat(value).contains("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1"));
    assertThat(command).anySatisfy(value ->
        assertThat(value).contains("spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"));
}
```

- [ ] **Step 2: 写 SQL 模板测试**

```java
@Test
void createsValidationTableWithFormatVersionTwoAndExplicitLocation() {
    String sql = IcebergSqlTemplates.createBaseTable(
        "iceberg_catalog.iceberg_validation.schema_case_run",
        "hdfs://hdfs-namenode:8020/warehouse/iceberg/iceberg_validation/schema/case/run"
    );

    assertThat(sql).contains("CREATE TABLE iceberg_catalog.iceberg_validation.schema_case_run");
    assertThat(sql).contains("USING iceberg");
    assertThat(sql).contains("'format-version'='2'");
    assertThat(sql).contains("LOCATION 'hdfs://hdfs-namenode:8020/warehouse/iceberg/iceberg_validation/schema/case/run'");
    assertThat(sql).contains("id BIGINT");
    assertThat(sql).contains("payload STRUCT");
}
```

- [ ] **Step 3: 运行失败测试**

```powershell
mvn -Dtest=SparkSqlExecutorTest,IcebergSqlTemplatesTest test
```

Expected: 编译失败，提示执行器和模板不存在。

- [ ] **Step 4: 实现 Spark SQL script builder**

`SparkSqlScriptBuilder.java`：

```java
package com.example.databenchmark.iceberg.sql;

import java.util.ArrayList;
import java.util.List;

public class SparkSqlScriptBuilder {
    private final List<String> statements = new ArrayList<>();

    public SparkSqlScriptBuilder add(String sql) {
        if (sql != null && !sql.isBlank()) {
            statements.add(sql.strip());
        }
        return this;
    }

    public String build() {
        return String.join(";\n", statements) + ";\n";
    }
}
```

- [ ] **Step 5: 实现核心 SQL 模板**

`IcebergSqlTemplates.java` 至少包含：

```java
public static String createNamespace(String catalog, String namespace) {
    return "CREATE NAMESPACE IF NOT EXISTS " + catalog + "." + namespace;
}

public static String dropTable(String table) {
    return "DROP TABLE IF EXISTS " + table + " PURGE";
}

public static String createBaseTable(String table, String location) {
    return """
        CREATE TABLE %s (
          id BIGINT,
          event_day DATE,
          region STRING,
          metric_int INT,
          metric_float FLOAT,
          amount DECIMAL(12, 2),
          payload STRUCT<vendor: STRING, score: INT>,
          tags ARRAY<STRING>,
          attrs MAP<STRING, STRING>
        )
        USING iceberg
        PARTITIONED BY (event_day)
        LOCATION '%s'
        TBLPROPERTIES ('format-version'='2')
        """.formatted(table, location);
}

public static String insertRange(String table, long startInclusive, long endExclusive, String region) {
    return """
        INSERT INTO %s
        SELECT id,
               DATE '2026-01-01' + CAST(id %% 7 AS INT),
               '%s',
               CAST(id AS INT),
               CAST(id * 1.0 AS FLOAT),
               CAST(id * 1.25 AS DECIMAL(12, 2)),
               named_struct('vendor', 'vendor-' || CAST(id %% 3 AS STRING), 'score', CAST(id %% 100 AS INT)),
               array('kpi', 'iceberg'),
               map('source', 'validation')
        FROM range(%d, %d)
        """.formatted(table, region, startInclusive, endExclusive);
}
```

- [ ] **Step 6: 实现 metadata table queries**

`MetadataTableQueries.java`：

```java
public static String snapshotCount(String table) {
    return "SELECT COUNT(*) AS snapshot_count FROM " + table + ".snapshots";
}

public static String dataFileStats(String table) {
    return "SELECT COUNT(*) AS file_count, COALESCE(SUM(file_size_in_bytes), 0) AS logical_bytes FROM " + table + ".files";
}

public static String manifestCount(String table) {
    return "SELECT COUNT(*) AS manifest_count FROM " + table + ".manifests";
}

public static String currentSnapshot(String table) {
    return "SELECT snapshot_id FROM " + table + ".snapshots ORDER BY committed_at DESC LIMIT 1";
}
```

- [ ] **Step 7: 实现 SparkSqlExecutor**

`SparkSqlExecutor` 用 `CommandRunner.run(command, Path.of("."), Duration.ofSeconds(config.spark().timeoutSeconds()))` 执行命令，命令结构参考当前 `SparkIcebergClient`，但 package 版本使用 config 中的 `1.10.1`。

失败时抛出：

```java
throw new IllegalStateException("Spark SQL failed: " + result.stderr());
```

- [ ] **Step 8: 运行测试**

```powershell
mvn -Dtest=SparkSqlExecutorTest,IcebergSqlTemplatesTest test
```

Expected: `BUILD SUCCESS`。

- [ ] **Step 9: 提交**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/exec src/main/java/com/example/databenchmark/iceberg/sql src/test/java/com/example/databenchmark/iceberg/exec src/test/java/com/example/databenchmark/iceberg/sql
git commit -m "feat: add Iceberg Spark SQL execution helpers"
```

---

### Task 5: HDFS usage、EC policy 和安全故障注入适配

**Files:**
- Create: `src/main/java/com/example/databenchmark/iceberg/hdfs/HdfsCliClient.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/hdfs/HdfsUsageCollector.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/hdfs/HdfsEcPolicyClient.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/hdfs/HdfsFaultInjector.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/hdfs/HdfsUsageCollectorTest.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/hdfs/HdfsEcPolicyClientTest.java`

- [ ] **Step 1: 写 HDFS usage 解析测试**

```java
@Test
void parsesDuAndCountOutput() {
    HdfsUsageCollector.Usage usage = HdfsUsageCollector.parse(
        "4096 8192 hdfs://hdfs-namenode:8020/warehouse/iceberg/table\n",
        "           2           5              4096 hdfs://hdfs-namenode:8020/warehouse/iceberg/table\n"
    );

    assertThat(usage.logicalBytes()).isEqualTo(4096L);
    assertThat(usage.diskBytes()).isEqualTo(8192L);
    assertThat(usage.directoryCount()).isEqualTo(2L);
    assertThat(usage.fileCount()).isEqualTo(5L);
}
```

- [ ] **Step 2: 写 EC skip 决策测试**

```java
@Test
void skipsRsTenFourWhenLiveDataNodesAreInsufficient() {
    HdfsEcPolicyClient.EcPreflight preflight = new HdfsEcPolicyClient.EcPreflight(
        "RS-10-4-1024k",
        true,
        3,
        false,
        "RS-10-4-1024k requires at least 14 live DataNodes for full policy tolerance validation"
    );

    assertThat(preflight.canRunFaultTolerance()).isFalse();
    assertThat(preflight.reason()).contains("14 live DataNodes");
}
```

- [ ] **Step 3: 运行失败测试**

```powershell
mvn -Dtest=HdfsUsageCollectorTest,HdfsEcPolicyClientTest test
```

Expected: 编译失败。

- [ ] **Step 4: 实现 `HdfsUsageCollector`**

实现 `Usage` record：

```java
public record Usage(long logicalBytes, long diskBytes, long directoryCount, long fileCount) {}
```

`parse` 规则：

- `du -s` 第一列是 logical bytes，第二列是 disk space consumed。
- `count` 前三列按 directory count、file count、content size 解析。
- 解析失败抛 `IllegalArgumentException("Unable to parse HDFS usage output")`。

- [ ] **Step 5: 实现 `HdfsEcPolicyClient`**

关键方法：

```java
public List<String> enabledPolicies()
public void setPolicy(String path, String policy)
public void unsetPolicy(String path)
public String getPolicy(String path)
public EcPreflight preflight(String policy, int liveDataNodes)
```

容忍 DataNode 数规则：

```java
private static int requiredDataNodes(String policy) {
    if (policy.startsWith("RS-3-2")) return 5;
    if (policy.startsWith("RS-6-3")) return 9;
    if (policy.startsWith("RS-10-4")) return 14;
    if (policy.startsWith("XOR-2-1")) return 3;
    return 1;
}
```

- [ ] **Step 6: 实现 `HdfsFaultInjector`**

只提供安全动作：

```java
public void stopDataNode(String serviceName)
public void startDataNode(String serviceName)
public void restartStoppedDataNodes()
```

命令必须使用 shared infra compose 文件和 profile，禁止删除 HDFS block 文件。失败时抛 `IllegalStateException`，并把命令 stderr 放进消息。

- [ ] **Step 7: 运行 HDFS 测试**

```powershell
mvn -Dtest=HdfsUsageCollectorTest,HdfsEcPolicyClientTest test
```

Expected: `BUILD SUCCESS`。

- [ ] **Step 8: 提交**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/hdfs src/test/java/com/example/databenchmark/iceberg/hdfs
git commit -m "feat: add HDFS metrics and EC helpers"
```

---

### Task 6: Scenario 注册和通用执行工具

**Files:**
- Modify: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationRunner.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/IcebergScenarioRegistry.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/IcebergScenarioSupport.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/IcebergValidationRunnerTest.java`

- [ ] **Step 1: 写过滤和错误隔离测试**

```java
@Test
void runnerFiltersScenariosAndKeepsExecutingAfterCaseFailure() throws Exception {
    IcebergValidationScenario passing = fakeScenario("schemaEvolution", "schema-add-drop-rename", false);
    IcebergValidationScenario failing = fakeScenario("timeTravel", "time-travel-by-snapshot-id", true);
    IcebergValidationRunner runner = new IcebergValidationRunner(
        List.of(passing, failing),
        new IcebergValidationReportWriter()
    );

    IcebergValidationReport report = runner.run(
        validConfig(tempDir.resolve("reports")),
        "run-filter",
        List.of("schemaEvolution", "timeTravel"),
        List.of(),
        false
    );

    assertThat(report.results()).hasSize(2);
    assertThat(report.status()).isEqualTo("DEGRADED");
    assertThat(report.results()).extracting(IcebergValidationResult::caseId)
        .contains("schema-add-drop-rename", "time-travel-by-snapshot-id");
}
```

- [ ] **Step 2: 实现 registry**

`IcebergScenarioRegistry.defaultScenarios()` 返回 9 个 scenario，顺序固定：

```java
return List.of(
    new SchemaEvolutionScenario(),
    new ErasureCodingScenario(),
    new ErasureCodingConversionScenario(),
    new ConcurrentWriteScenario(),
    new RowLevelMutationScenario(),
    new AcidTransactionScenario(),
    new IncrementalPullScenario(),
    new TimeTravelScenario(),
    new SmallFileCompactionScenario()
);
```

- [ ] **Step 3: 修改 runner 默认构造器**

```java
public IcebergValidationRunner() {
    this(IcebergScenarioRegistry.defaultScenarios(), new IcebergValidationReportWriter());
}
```

- [ ] **Step 4: 实现 support 工具**

`IcebergScenarioSupport` 提供：

```java
public static String tableName(IcebergValidationContext context, String scenario, String caseId)
public static String tableLocation(IcebergValidationContext context, String scenario, String caseId)
public static Map<String, String> dataScale(IcebergValidationConfig config)
public static IcebergValidationResult pass(...)
public static IcebergValidationResult skipped(...)
public static IcebergValidationResult fail(...)
```

`tableName` 要清理 run id 中非 `[a-zA-Z0-9_]` 字符，转为 `_`。

- [ ] **Step 5: 运行 runner 测试**

```powershell
mvn -Dtest=IcebergValidationRunnerTest test
```

Expected: `BUILD SUCCESS`。

- [ ] **Step 6: 提交**

```powershell
git add src/main/java/com/example/databenchmark/iceberg src/test/java/com/example/databenchmark/iceberg/IcebergValidationRunnerTest.java
git commit -m "feat: register Iceberg validation scenarios"
```

---

### Task 7: Schema evolution、time travel、incremental pull 场景

**Files:**
- Create: `src/main/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenario.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/scenario/TimeTravelScenario.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/scenario/IncrementalPullScenario.java`
- Modify: `src/main/java/com/example/databenchmark/iceberg/sql/IcebergSqlTemplates.java`
- Test: corresponding scenario tests

- [ ] **Step 1: 写 schema evolution case 列表测试**

```java
@Test
void schemaEvolutionDefinesCompatibilityCases() {
    List<IcebergValidationCase> cases = new SchemaEvolutionScenario().cases(validConfig());

    assertThat(cases).extracting(IcebergValidationCase::caseId).containsExactly(
        "schema-add-drop-rename",
        "schema-type-promotion",
        "schema-nested-struct",
        "schema-complex-types",
        "schema-long-chain-history"
    );
}
```

- [ ] **Step 2: 写 time travel case 列表测试**

```java
@Test
void timeTravelDefinesSnapshotAndTimestampCases() {
    List<IcebergValidationCase> cases = new TimeTravelScenario().cases(validConfig());

    assertThat(cases).extracting(IcebergValidationCase::caseId).containsExactly(
        "time-travel-by-snapshot-id",
        "time-travel-by-timestamp",
        "time-travel-after-schema-evolution",
        "time-travel-after-expire"
    );
}
```

- [ ] **Step 3: 写 incremental case 列表测试**

```java
@Test
void incrementalPullDefinesAppendAndBoundaryCases() {
    List<IcebergValidationCase> cases = new IncrementalPullScenario().cases(validConfig());

    assertThat(cases).extracting(IcebergValidationCase::caseId).containsExactly(
        "incremental-append-only",
        "incremental-multi-snapshot-window",
        "incremental-with-delete-update-boundary",
        "incremental-expired-snapshot"
    );
}
```

- [ ] **Step 4: 运行失败测试**

```powershell
mvn -Dtest=SchemaEvolutionScenarioTest,TimeTravelScenarioTest,IncrementalPullScenarioTest test
```

Expected: 编译失败。

- [ ] **Step 5: 实现场景 cases**

每个 scenario 的 `cases` 返回 spec 中列出的 case ID、purpose、参数：

```java
new IcebergValidationCase(
    name(),
    "schema-type-promotion",
    "Validate compatible primitive and decimal type promotion with historical reads.",
    Map.of("changes", "int->long,float->double,decimal precision expansion"),
    true
)
```

- [ ] **Step 6: 实现场景 run 的第一版**

每个 case 先执行完整 Spark SQL 脚本并记录命令。Schema evolution 脚本必须包含：

```sql
ALTER TABLE <table> ADD COLUMN added_text STRING;
ALTER TABLE <table> RENAME COLUMN region TO service_region;
ALTER TABLE <table> ALTER COLUMN metric_int TYPE BIGINT;
ALTER TABLE <table> ALTER COLUMN metric_float TYPE DOUBLE;
ALTER TABLE <table> ALTER COLUMN amount TYPE DECIMAL(18, 2);
ALTER TABLE <table> ADD COLUMN payload.vendor_code STRING;
```

Time travel 脚本必须包含 snapshot ID 查询、`VERSION AS OF` 查询和 `TIMESTAMP AS OF` 查询。

Incremental append-only case 必须记录 base snapshot、end snapshot、append rows、full scan rows、incremental rows。

- [ ] **Step 7: 运行场景测试**

```powershell
mvn -Dtest=SchemaEvolutionScenarioTest,TimeTravelScenarioTest,IncrementalPullScenarioTest test
```

Expected: `BUILD SUCCESS`。

- [ ] **Step 8: 提交**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/scenario src/main/java/com/example/databenchmark/iceberg/sql src/test/java/com/example/databenchmark/iceberg/scenario
git commit -m "feat: add Iceberg history read scenarios"
```

---

### Task 8: Row-level mutation、ACID、并发写入场景

**Files:**
- Create: `RowLevelMutationScenario.java`
- Create: `AcidTransactionScenario.java`
- Create: `ConcurrentWriteScenario.java`
- Modify: `IcebergSqlTemplates.java`
- Test: corresponding scenario tests

- [ ] **Step 1: 写 case 列表测试**

Row-level mutation 必须包含：

```java
assertThat(cases).extracting(IcebergValidationCase::caseId).containsExactly(
    "row-update-single-range",
    "row-delete-partition-prunable",
    "row-delete-selective",
    "row-merge-upsert-delete"
);
```

ACID 必须包含：

```java
assertThat(cases).extracting(IcebergValidationCase::caseId).containsExactly(
    "acid-kill-before-commit",
    "acid-kill-during-commit",
    "acid-conflicting-commits",
    "acid-reader-isolation"
);
```

Concurrent write 必须包含：

```java
assertThat(cases).extracting(IcebergValidationCase::caseId).containsExactly(
    "concurrent-append-disjoint-partitions",
    "concurrent-append-same-partition",
    "concurrent-update-overlap",
    "concurrent-mixed-read-write"
);
```

- [ ] **Step 2: 运行失败测试**

```powershell
mvn -Dtest=RowLevelMutationScenarioTest,AcidTransactionScenarioTest,ConcurrentWriteScenarioTest test
```

Expected: 编译失败。

- [ ] **Step 3: 增加 mutation SQL 模板**

`IcebergSqlTemplates` 添加：

```java
public static String updateRange(String table, long minIdInclusive, long maxIdExclusive) {
    return "UPDATE " + table + " SET region = 'updated' WHERE id >= "
        + minIdInclusive + " AND id < " + maxIdExclusive;
}

public static String deleteRange(String table, long minIdInclusive, long maxIdExclusive) {
    return "DELETE FROM " + table + " WHERE id >= "
        + minIdInclusive + " AND id < " + maxIdExclusive;
}

public static String mergeUpsertDelete(String table, String sourceView) {
    return """
        MERGE INTO %s t
        USING %s s
        ON t.id = s.id
        WHEN MATCHED AND s.op = 'delete' THEN DELETE
        WHEN MATCHED AND s.op = 'update' THEN UPDATE SET region = s.region
        WHEN NOT MATCHED THEN INSERT *
        """.formatted(table, sourceView);
}
```

- [ ] **Step 4: 实现场景 cases 和 run**

Row-level case 记录：

- mutation SQL。
- mutation duration。
- before/after row count。
- before/after data file count。
- before/after delete file count。
- historical snapshot query result。

ACID case 使用 fake runner 单元测试覆盖命令序列，真实故障注入在 compose smoke 手工验证中执行。`acid-kill-during-commit` 如果没有可靠注入点，返回 `SKIPPED`，原因必须是：

```text
No deterministic commit-phase injection hook is available in the current Spark SQL executor.
```

Concurrent write case 通过 `SparkSqlExecutor.runAsync` 或 `ProcessBuilder` 并发执行多个 append/update 脚本，记录成功数、失败数、耗时和错误。

- [ ] **Step 5: 运行测试**

```powershell
mvn -Dtest=RowLevelMutationScenarioTest,AcidTransactionScenarioTest,ConcurrentWriteScenarioTest test
```

Expected: `BUILD SUCCESS`。

- [ ] **Step 6: 提交**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/scenario src/main/java/com/example/databenchmark/iceberg/sql src/test/java/com/example/databenchmark/iceberg/scenario
git commit -m "feat: add Iceberg mutation and concurrency scenarios"
```

---

### Task 9: HDFS EC 和 EC/replication 转换场景

**Files:**
- Create: `ErasureCodingScenario.java`
- Create: `ErasureCodingConversionScenario.java`
- Modify: HDFS helper classes as needed
- Test: `ErasureCodingScenarioTest.java`
- Test: `ErasureCodingConversionScenarioTest.java`

- [ ] **Step 1: 写 EC case 列表测试**

```java
assertThat(new ErasureCodingScenario().cases(validConfig()))
    .extracting(IcebergValidationCase::caseId)
    .containsExactly(
        "ec-policy-write-read",
        "ec-rs-10-4-failure-tolerance",
        "ec-policy-matrix-failure",
        "ec-file-count-and-disk-usage"
    );
```

- [ ] **Step 2: 写转换 case 列表测试**

```java
assertThat(new ErasureCodingConversionScenario().cases(validConfig()))
    .extracting(IcebergValidationCase::caseId)
    .containsExactly(
        "replication-to-ec-policy-only",
        "replication-to-ec-rewrite",
        "ec-to-replication-policy-only",
        "ec-to-replication-rewrite"
    );
```

- [ ] **Step 3: 运行失败测试**

```powershell
mvn -Dtest=ErasureCodingScenarioTest,ErasureCodingConversionScenarioTest test
```

Expected: 编译失败。

- [ ] **Step 4: 实现 EC write/read 场景**

每个 policy 执行：

1. 创建 HDFS 目标目录。
2. baseline 目录设置 `REPLICATION`，写入 replication=2 表。
3. EC 目录设置指定 policy，写入同规模表。
4. 查询 row count 和 checksum。
5. 收集 `files` metadata、manifest count、HDFS `du -s`、HDFS `count`。
6. 生成 comparison：

```text
diskSavingRatio = 1 - ecDiskBytes / replicationDiskBytes
writeLatencyRatio = ecWriteSeconds / replicationWriteSeconds
readLatencyRatio = ecReadSeconds / replicationReadSeconds
fileCountDelta = ecFileCount - replicationFileCount
```

- [ ] **Step 5: 实现故障注入 skip 和执行**

`RS-10-4` 需要至少 14 个 live DataNode 才执行完整容错验证。DataNode 不足时返回 `SKIPPED`，evidence 包含：

```text
policy=RS-10-4-1024k
liveDataNodes=<actual>
requiredDataNodes=14
```

如果满足条件，停止不超过 parity 数的 DataNode，执行 checksum 查询，再恢复 DataNode。

- [ ] **Step 6: 实现转换场景**

policy-only case 只设置目录 policy 并 append 新数据，必须报告旧文件和新文件 policy 分布。

rewrite case 使用 Spark SQL 创建目标表并执行：

```sql
INSERT INTO <target_table> SELECT * FROM <source_table>;
```

转换效率指标：

```text
conversionSeconds
convertedLogicalBytes
convertedDiskBytesBefore
convertedDiskBytesAfter
throughputMbPerSecond
fileCountBefore
fileCountAfter
queryMsBefore
queryMsAfter
```

- [ ] **Step 7: 运行 EC 测试**

```powershell
mvn -Dtest=ErasureCodingScenarioTest,ErasureCodingConversionScenarioTest test
```

Expected: `BUILD SUCCESS`。

- [ ] **Step 8: 提交**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/scenario src/test/java/com/example/databenchmark/iceberg/scenario
git commit -m "feat: add Iceberg HDFS erasure coding scenarios"
```

---

### Task 10: 多 snapshot 小文件和维护场景

**Files:**
- Create: `SmallFileCompactionScenario.java`
- Modify: `IcebergSqlTemplates.java`
- Modify: `MetadataTableQueries.java`
- Test: `SmallFileCompactionScenarioTest.java`

- [ ] **Step 1: 写 case 列表测试**

```java
assertThat(new SmallFileCompactionScenario().cases(validConfig()))
    .extracting(IcebergValidationCase::caseId)
    .containsExactly(
        "small-files-many-snapshots-build",
        "small-files-query-degradation",
        "small-files-data-compaction",
        "small-files-manifest-rewrite",
        "small-files-expire-snapshots"
    );
```

- [ ] **Step 2: 运行失败测试**

```powershell
mvn -Dtest=SmallFileCompactionScenarioTest test
```

Expected: 编译失败。

- [ ] **Step 3: 增加 maintenance SQL 模板**

```java
public static String rewriteDataFiles(String catalog, String table) {
    return "CALL " + catalog + ".system.rewrite_data_files(table => '" + table + "')";
}

public static String rewriteManifests(String catalog, String table) {
    return "CALL " + catalog + ".system.rewrite_manifests('" + table + "')";
}

public static String expireSnapshots(String catalog, String table, String olderThanTimestamp) {
    return "CALL " + catalog + ".system.expire_snapshots(table => '" + table
        + "', older_than => TIMESTAMP '" + olderThanTimestamp + "')";
}
```

- [ ] **Step 4: 实现 many snapshots 构造**

循环 `config.scale().smallFileCommits()` 次，每次执行 `config.scale().filesPerCommit()` 个小范围 insert。每次 commit 后记录 snapshot count、data file count、manifest count、metadata JSON count。

测试中用 fake executor 验证生成的 action commands 数量：

```java
assertThat(result.actionCommands()).filteredOn(command -> command.contains("INSERT INTO")).hasSize(100);
assertThat(result.metrics()).containsEntry("targetSnapshots", "100");
```

- [ ] **Step 5: 实现 compaction 对比**

compaction 前后必须收集：

- `dataFileCountBefore`
- `dataFileCountAfter`
- `manifestCountBefore`
- `manifestCountAfter`
- `snapshotCountBefore`
- `snapshotCountAfter`
- `metadataJsonCountBefore`
- `metadataJsonCountAfter`
- `hdfsDiskBytesBefore`
- `hdfsDiskBytesAfter`
- `queryMsBefore`
- `queryMsAfter`
- `maintenanceSeconds`

- [ ] **Step 6: 运行测试**

```powershell
mvn -Dtest=SmallFileCompactionScenarioTest test
```

Expected: `BUILD SUCCESS`。

- [ ] **Step 7: 提交**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/scenario/SmallFileCompactionScenario.java src/main/java/com/example/databenchmark/iceberg/sql src/test/java/com/example/databenchmark/iceberg/scenario/SmallFileCompactionScenarioTest.java
git commit -m "feat: add Iceberg small-file compaction scenario"
```

---

### Task 11: 文档、README 和 compose 手工验证

**Files:**
- Modify: `README.md`
- Create: `docs/iceberg/validation.md`
- Modify: `docs/superpowers/specs/2026-07-28-iceberg-validation-module-design.md` only if implementation reveals a real spec correction

- [ ] **Step 1: 更新 README**

在 Shared Infra Compose Benchmark 后增加：

```markdown
## Iceberg Capability Validation

Run the standalone Iceberg validation suite:

```sh
mvn package
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar iceberg-validate --config configs/iceberg-validation.yml --run-id iceberg-validation-smoke
```

Reports are written under `reports/iceberg-validation/<run_id>/`.
The suite uses Apache Iceberg `1.10.1` and validates Iceberg behavior independently from StarRocks route comparisons.
```

- [ ] **Step 2: 新增专项文档**

`docs/iceberg/validation.md` 内容包含：

- 模块目标。
- 8 类能力和 EC conversion 子场景。
- smoke 运行命令。
- EC 场景 DataNode 数不足时 skip 的说明。
- `keep-artifacts` 排障说明。
- 报告字段解释。

- [ ] **Step 3: 运行文档 grep 检查**

```powershell
rg -n "1.10.1|iceberg-validate|RS-10-4|replicationBaseline" README.md docs/iceberg/validation.md configs/iceberg-validation.yml
```

Expected: 输出包含 README、docs、config 三处。

- [ ] **Step 4: 执行完整单元测试**

```powershell
mvn test
```

Expected: `BUILD SUCCESS`。如果 `SharedInfraTopologyTest` 因 `../shared-data-infra` 当前 JVM 参数变化失败，先记录根因，不在本任务里静默修改共享基础设施断言。

- [ ] **Step 5: 执行 smoke 手工验证**

需要能访问 Docker Desktop 和 shared infra：

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar iceberg-validate --config configs/iceberg-validation.yml --run-id iceberg-validation-smoke --scenario schemaEvolution --scenario timeTravel --scenario smallFileCompaction
```

Expected:

- exit code `0` 或在不支持的 case 上返回 `DEGRADED` 且报告有明确 skip/fail 原因。
- `reports/iceberg-validation/iceberg-validation-smoke/report.json` 存在。
- `reports/iceberg-validation/iceberg-validation-smoke/report.md` 存在。
- report 中包含 `schema-add-drop-rename`、`time-travel-by-snapshot-id`、`small-files-data-compaction`。

- [ ] **Step 6: 提交**

```powershell
git add README.md docs/iceberg/validation.md
git commit -m "docs: document Iceberg validation suite"
```

---

### Task 12: 最终验收和推送准备

**Files:**
- No code changes unless verification exposes a defect.

- [ ] **Step 1: 检查工作区**

```powershell
git status --short --branch
```

Expected: 只有用户已有的 `outputs/`、`work/` 未跟踪内容；没有未提交 tracked 文件。

- [ ] **Step 2: 运行全量构建测试**

```powershell
mvn test
```

Expected: `BUILD SUCCESS`。如果外部 shared infra 断言失败，记录具体失败文件和外部配置差异。

- [ ] **Step 3: 运行 compose config 校验**

按 `AGENTS.md` 要求：

```powershell
docker compose -f docker-compose.yml config
```

Expected: 输出有效 compose config，且本仓库仍只保留 `benchmark-runner`。

- [ ] **Step 4: 运行 Iceberg smoke 验证**

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar iceberg-validate --config configs/iceberg-validation.yml --run-id iceberg-validation-final-smoke --scenario schemaEvolution --scenario timeTravel --scenario smallFileCompaction
```

Expected: 报告生成在 `reports/iceberg-validation/iceberg-validation-final-smoke/`，功能断言通过；不支持的 EC 故障注入 case 在未选择时不影响 smoke。

- [ ] **Step 5: 检查报告内容**

```powershell
Get-Content -LiteralPath reports/iceberg-validation/iceberg-validation-final-smoke/report.md -TotalCount 80
```

Expected: 包含 `Iceberg Version: \`1.10.1\``、场景名、case 表格、evidence。

- [ ] **Step 6: 提交任何验证修复**

如果 Step 2-5 暴露代码缺陷，按最小修复提交：

```powershell
git add <fixed-files>
git commit -m "fix: stabilize Iceberg validation smoke run"
```

- [ ] **Step 7: 不自动推送，除非用户明确要求**

最终向用户报告：

- 已完成的提交列表。
- 测试命令和结果。
- smoke 报告路径。
- 剩余未跟踪文件。

---

## 自检

Spec 覆盖：

- 独立 CLI：Task 3。
- Iceberg 1.10.1：Task 1、Task 4、Task 11。
- Schema 历史兼容读取：Task 7。
- HDFS EC 多 policy、RS-10-4、故障注入、replication=2、文件数和磁盘占用：Task 5、Task 9。
- EC/replication 双向转换效率：Task 9。
- 并发写入：Task 8。
- 行级更新删除：Task 8。
- ACID：Task 8。
- 增量拉取：Task 7。
- 时间旅行：Task 7。
- 多 snapshot 小文件和 compaction：Task 10。
- JSON/Markdown 报告：Task 2。
- 文档和最终验证：Task 11、Task 12。

占位符检查：

- 本计划不使用未定义的后续补空步骤。
- 对不可靠故障注入明确返回 `SKIPPED`，并要求报告原因。
- 对 shared infra 变更遵守 `AGENTS.md`，本计划不新增本仓库基础设施服务。

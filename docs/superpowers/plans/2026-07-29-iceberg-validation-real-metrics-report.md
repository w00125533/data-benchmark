# Iceberg Validation Real Metrics Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `iceberg-validate` 从“脚本清单式报告”升级为“真实执行、真实度量、规格目标突出”的 Iceberg 功能与性能验证报告。

**Architecture:** 先引入结构化执行证据和统一采集工具，再让各 scenario 真正执行 Spark SQL/HDFS 命令并写入真实 `metrics/baseline/comparison`。HTML 报告只消费结构化结果，不再把指标名称列表或 `scriptedActions` 当作性能结论。

**Tech Stack:** Java 17, Maven, JUnit 5, AssertJ, Spark SQL through shared-data-infra, HDFS CLI, Apache Iceberg 1.10.1.

---

## Design Summary

当前根因分两层：

- **执行层缺失真实采集**：`AbstractIcebergValidationScenario.scriptedPass()` 直接返回 PASS，只记录脚本动作数；多个场景把 `metrics` 写成指标名称列表。
- **报告层存在错位表达**：HTML 将 `comparison.scriptedActions`、`metrics.ecPolicies` 等占位内容放入“文件数”“磁盘占用”“转换耗时”等列，造成规格目标被淹没。

设计目标：

- 每个用例至少有结构化的 `执行脚本` 和 `执行结果`。
- 每个场景主表只展示真实值：行数、快照、文件数、HDFS 磁盘占用、耗时、吞吐、checksum、错误/skip 原因。
- `metrics` 表示实际度量值，不能再存 `conversionMetrics=seconds,...` 这类指标名称列表。
- `baseline` 和 `comparison` 表示同口径对比值，例如 replication vs EC、before vs after、full scan vs incremental。
- Smoke 如果没有足够共享基础设施能力，应明确 `SKIPPED` 或 `NOT_COMPARABLE`，不能伪造 PASS。

不修改 Docker Compose 基础设施。本工程继续复用 `../shared-data-infra`。

## File Structure

- Modify `src/main/java/com/example/databenchmark/iceberg/IcebergValidationResult.java`
  - 增加结构化执行结果字段。
- Create `src/main/java/com/example/databenchmark/iceberg/IcebergExecutionEvidence.java`
  - 表示一条 Spark SQL 或 HDFS 命令的脚本、stdout/stderr、退出码、耗时。
- Create `src/main/java/com/example/databenchmark/iceberg/exec/IcebergCaseExecutor.java`
  - 执行 Spark SQL/HDFS 命令并收集结构化证据。
- Create `src/main/java/com/example/databenchmark/iceberg/metrics/IcebergMetricCollectors.java`
  - 采集 snapshot、data file、manifest、HDFS usage、query duration、checksum。
- Modify `src/main/java/com/example/databenchmark/iceberg/scenario/*.java`
  - 将占位 metrics 替换为真实执行值。
- Modify `src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java`
  - 报告按场景列展示真实度量，行内折叠证据拆为“执行脚本”和“执行结果”。
- Modify tests under `src/test/java/com/example/databenchmark/iceberg/**`
  - 增加红绿测试，防止指标名称列表、错位列和伪 PASS 回归。
- Modify `docs/iceberg/validation.md`
  - 记录真实度量口径和本地 smoke 受 DataNode 数量限制的行为。

---

### Task 1: Structured Execution Evidence

**Files:**
- Create: `src/main/java/com/example/databenchmark/iceberg/IcebergExecutionEvidence.java`
- Modify: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationResult.java`
- Modify: `src/main/java/com/example/databenchmark/iceberg/IcebergScenarioSupport.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/IcebergValidationResultTest.java`

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/example/databenchmark/iceberg/IcebergValidationResultTest.java`:

```java
package com.example.databenchmark.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IcebergValidationResultTest {
    @Test
    void storesStructuredExecutionEvidenceSeparatelyFromLegacyEvidence() {
        IcebergExecutionEvidence execution = new IcebergExecutionEvidence(
            "action",
            "count current rows",
            "spark-sql -e SELECT COUNT(*) FROM t",
            0,
            1.25,
            "count\n1000",
            ""
        );

        IcebergValidationResult result = new IcebergValidationResult(
            "schemaEvolution",
            "schema-add-drop-rename",
            "purpose",
            Map.of("rows", "1000"),
            List.of("legacy setup"),
            List.of("legacy action"),
            List.of("row count matched"),
            Map.of("currentRows", "1000"),
            Map.of("baselineRows", "1000"),
            Map.of("currentRows", "1000"),
            IcebergConclusion.FunctionStatus.PASS,
            IcebergConclusion.PerformanceStatus.ACCEPTABLE,
            "conclusion",
            List.of("legacy evidence"),
            List.of(),
            List.of(execution)
        );

        assertThat(result.executionResults()).containsExactly(execution);
        assertThat(result.evidence()).containsExactly("legacy evidence");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```powershell
mvn "-Dtest=IcebergValidationResultTest" test
```

Expected: compilation fails because `IcebergExecutionEvidence` and the new `IcebergValidationResult` constructor field do not exist.

- [ ] **Step 3: Add the new record**

Create `src/main/java/com/example/databenchmark/iceberg/IcebergExecutionEvidence.java`:

```java
package com.example.databenchmark.iceberg;

public record IcebergExecutionEvidence(
    String phase,
    String label,
    String script,
    int exitCode,
    double durationSeconds,
    String stdout,
    String stderr
) {}
```

- [ ] **Step 4: Extend `IcebergValidationResult`**

Append `List<IcebergExecutionEvidence> executionResults` as the final record component:

```java
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
    List<String> errors,
    List<IcebergExecutionEvidence> executionResults
) {
    public boolean successful() {
        return functionStatus == IcebergConclusion.FunctionStatus.PASS
            || functionStatus == IcebergConclusion.FunctionStatus.DEGRADED
            || functionStatus == IcebergConclusion.FunctionStatus.SKIPPED;
    }
}
```

- [ ] **Step 5: Update factory helpers**

In `IcebergScenarioSupport.pass`, `skipped`, and `fail`, pass `List.of()` for `executionResults`.

Then add an overload:

```java
public static IcebergValidationResult pass(
    IcebergValidationCase testCase,
    IcebergValidationContext context,
    List<String> setupCommands,
    List<String> actionCommands,
    List<String> assertions,
    Map<String, String> metrics,
    Map<String, String> baseline,
    Map<String, String> comparison,
    String conclusion,
    List<String> evidence,
    List<IcebergExecutionEvidence> executionResults
) {
    return new IcebergValidationResult(
        testCase.scenario(),
        testCase.caseId(),
        testCase.purpose(),
        dataScale(context.config()),
        setupCommands,
        actionCommands,
        assertions,
        metrics,
        baseline,
        comparison,
        IcebergConclusion.FunctionStatus.PASS,
        IcebergConclusion.PerformanceStatus.ACCEPTABLE,
        conclusion,
        evidence,
        List.of(),
        executionResults
    );
}
```

- [ ] **Step 6: Update existing tests and call sites**

Search:

```powershell
rg -n "new IcebergValidationResult\\(" src/test src/main
```

For each direct constructor call, append `List.of()` unless the test needs real execution evidence.

- [ ] **Step 7: Verify**

Run:

```powershell
mvn "-Dtest=IcebergValidationResultTest,IcebergValidationReportWriterTest,IcebergValidationRunnerTest,IcebergValidateCommandTest" test
```

Expected: all selected tests pass.

- [ ] **Step 8: Commit**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/IcebergExecutionEvidence.java src/main/java/com/example/databenchmark/iceberg/IcebergValidationResult.java src/main/java/com/example/databenchmark/iceberg/IcebergScenarioSupport.java src/test/java/com/example/databenchmark/iceberg/IcebergValidationResultTest.java src/test/java/com/example/databenchmark/iceberg
git commit -m "feat: capture structured Iceberg execution evidence"
```

---

### Task 2: Report Writer Evidence Split and Guardrails

**Files:**
- Modify: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java`
- Modify: `src/test/java/com/example/databenchmark/iceberg/IcebergValidationReportWriterTest.java`

- [ ] **Step 1: Write failing report assertions**

Update `IcebergValidationReportWriterTest` sample result to include one `IcebergExecutionEvidence`:

```java
List.of(new IcebergExecutionEvidence(
    "action",
    "query current row count",
    "SELECT COUNT(*) FROM iceberg_table",
    0,
    0.42,
    "count\n1000",
    ""
))
```

Add assertions:

```java
.contains("<strong>执行脚本</strong>")
.contains("<strong>执行结果</strong>")
.contains("SELECT COUNT(*) FROM iceberg_table")
.contains("exitCode=0")
.contains("durationSeconds=0.420")
.contains("count")
.contains("1000")
.doesNotContain("conversionMetrics=seconds,throughputMbPerSecond")
.doesNotContain("scriptedActions=")
```

- [ ] **Step 2: Run the report writer test and verify it fails**

```powershell
mvn "-Dtest=IcebergValidationReportWriterTest" test
```

Expected: failure because report still renders legacy Setup/Action/Evidence groups and does not render structured execution results.

- [ ] **Step 3: Replace evidence rendering**

In `IcebergValidationReportWriter.evidenceDetails`, render:

```java
private static String evidenceDetails(IcebergValidationResult result) {
    if (result.executionResults().isEmpty()) {
        return legacyEvidenceDetails(result);
    }
    String rows = result.executionResults().stream()
        .map(IcebergValidationReportWriter::executionEvidenceBlock)
        .collect(Collectors.joining());
    return "<details class=\"evidence-details\"><summary>展开证据</summary>"
        + "<div class=\"evidence-block\">" + rows + "</div></details>";
}

private static String executionEvidenceBlock(IcebergExecutionEvidence evidence) {
    return "<div class=\"execution-evidence\">"
        + "<strong>" + escapeHtml(evidence.phase() + " - " + evidence.label()) + "</strong>"
        + "<div><strong>执行脚本</strong>" + pre(List.of(evidence.script())) + "</div>"
        + "<div><strong>执行结果</strong>"
        + "<pre>exitCode=" + evidence.exitCode()
        + "\ndurationSeconds=" + String.format(java.util.Locale.ROOT, "%.3f", evidence.durationSeconds())
        + "\nstdout:\n" + escapeHtml(evidence.stdout())
        + "\nstderr:\n" + escapeHtml(evidence.stderr())
        + "</pre></div>"
        + "</div>";
}
```

Keep `legacyEvidenceDetails` only for defensive compatibility with older JSON-like results created by tests.

- [ ] **Step 4: Add report guardrails**

Create helper:

```java
private static String metricValuesOnly(Map<String, String> values) {
    return values.entrySet().stream()
        .filter(entry -> !entry.getValue().contains(",") || entry.getValue().matches(".*\\d.*"))
        .map(entry -> escapeHtml(entry.getKey()) + "=" + escapeHtml(entry.getValue()))
        .collect(Collectors.joining("<br>"));
}
```

Use scenario-specific helpers so columns never show:

- `scriptedActions`.
- `conversionMetrics`.
- `mutationMetrics`.
- `incrementalMetrics`.
- `timeTravelMetrics`.

- [ ] **Step 5: Verify**

```powershell
mvn "-Dtest=IcebergValidationReportWriterTest" test
```

Expected: pass.

- [ ] **Step 6: Commit**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java src/test/java/com/example/databenchmark/iceberg/IcebergValidationReportWriterTest.java
git commit -m "feat: split Iceberg report scripts from execution results"
```

---

### Task 3: Execution and Metric Collection Utilities

**Files:**
- Create: `src/main/java/com/example/databenchmark/iceberg/exec/IcebergCaseExecutor.java`
- Create: `src/main/java/com/example/databenchmark/iceberg/metrics/IcebergMetricCollectors.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/exec/IcebergCaseExecutorTest.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/metrics/IcebergMetricCollectorsTest.java`

- [ ] **Step 1: Write failing tests for execution capture**

Create `IcebergCaseExecutorTest` with a fake `CommandRunner`:

```java
@Test
void recordsCommandStdoutStderrExitCodeAndDuration() throws Exception {
    CommandRunner runner = new FakeCommandRunner(new CommandResult(
        List.of("docker", "compose", "exec", "spark", "spark-sql"),
        0,
        "count\n1000\n",
        "",
        0.123
    ));
    IcebergCaseExecutor executor = new IcebergCaseExecutor(runner);

    IcebergExecutionEvidence evidence = executor.record("action", "row count", List.of("spark-sql", "-e", "SELECT 1"));

    assertThat(evidence.phase()).isEqualTo("action");
    assertThat(evidence.label()).isEqualTo("row count");
    assertThat(evidence.exitCode()).isEqualTo(0);
    assertThat(evidence.stdout()).contains("1000");
    assertThat(evidence.durationSeconds()).isEqualTo(0.123);
}
```

- [ ] **Step 2: Write failing tests for output parsers**

Create `IcebergMetricCollectorsTest`:

```java
@Test
void parsesSingleLongFromSparkSqlOutput() {
    assertThat(IcebergMetricCollectors.parseSingleLong("count\n1000\n")).isEqualTo(1000L);
}

@Test
void parsesSingleStringFromSparkSqlOutput() {
    assertThat(IcebergMetricCollectors.parseSingleString("snapshot_id\n12345\n")).isEqualTo("12345");
}
```

- [ ] **Step 3: Run tests and verify failure**

```powershell
mvn "-Dtest=IcebergCaseExecutorTest,IcebergMetricCollectorsTest" test
```

Expected: compilation failure because classes do not exist.

- [ ] **Step 4: Implement `IcebergCaseExecutor`**

```java
package com.example.databenchmark.iceberg.exec;

import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
import com.example.databenchmark.iceberg.IcebergExecutionEvidence;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

public class IcebergCaseExecutor {
    private final CommandRunner commandRunner;

    public IcebergCaseExecutor(CommandRunner commandRunner) {
        this.commandRunner = commandRunner;
    }

    public IcebergExecutionEvidence record(String phase, String label, List<String> command) throws Exception {
        CommandResult result = commandRunner.run(command, Path.of("."), Duration.ofMinutes(30));
        return new IcebergExecutionEvidence(
            phase,
            label,
            String.join(" ", command),
            result.exitCode(),
            result.durationSeconds(),
            result.stdout(),
            result.stderr()
        );
    }
}
```

- [ ] **Step 5: Implement `IcebergMetricCollectors`**

```java
package com.example.databenchmark.iceberg.metrics;

public final class IcebergMetricCollectors {
    private IcebergMetricCollectors() {}

    public static long parseSingleLong(String stdout) {
        String value = parseSingleString(stdout);
        return Long.parseLong(value);
    }

    public static String parseSingleString(String stdout) {
        return stdout.lines()
            .map(String::trim)
            .filter(line -> !line.isBlank())
            .filter(line -> !line.matches("[-+]+"))
            .skip(1)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("No data row in Spark SQL output"));
    }
}
```

- [ ] **Step 6: Verify**

```powershell
mvn "-Dtest=IcebergCaseExecutorTest,IcebergMetricCollectorsTest" test
```

Expected: pass.

- [ ] **Step 7: Commit**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/exec/IcebergCaseExecutor.java src/main/java/com/example/databenchmark/iceberg/metrics/IcebergMetricCollectors.java src/test/java/com/example/databenchmark/iceberg/exec/IcebergCaseExecutorTest.java src/test/java/com/example/databenchmark/iceberg/metrics/IcebergMetricCollectorsTest.java
git commit -m "feat: add Iceberg execution metric collectors"
```

---

### Task 4: Schema Evolution Differentiated Metrics

**Files:**
- Modify: `src/main/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenario.java`
- Modify: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenarioTest.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/IcebergValidationReportWriterTest.java`

- [ ] **Step 1: Add failing test for per-case differentiation**

Create `SchemaEvolutionScenarioTest` that asserts each case produces distinct `schemaChangeType` and no metric-name placeholders:

```java
@Test
void schemaCasesExposeDistinctChangeTypesAndConcreteTargets() {
    SchemaEvolutionScenario scenario = new SchemaEvolutionScenario();
    List<IcebergValidationCase> cases = scenario.cases(IcebergValidationConfigLoader.load(Path.of("configs/iceberg-validation.yml")));

    assertThat(cases).extracting(IcebergValidationCase::caseId)
        .contains("schema-add-drop-rename", "schema-type-promotion", "schema-nested-struct", "schema-complex-types", "schema-long-chain-history");
}
```

Then add report writer sample assertions:

```java
.contains("add/drop/rename")
.contains("type promotion")
.contains("nested struct")
.contains("baselineRows=1000")
.contains("schemaHistoryLength=")
.doesNotContain("Schema 变化类型: primitive,numeric,nested,complex")
```

- [ ] **Step 2: Run tests and verify failure**

```powershell
mvn "-Dtest=SchemaEvolutionScenarioTest,IcebergValidationReportWriterTest" test
```

Expected: failure because current Schema metrics are identical.

- [ ] **Step 3: Make case-specific schema plans**

In `SchemaEvolutionScenario.run`, branch by `caseId`:

- `schema-add-drop-rename`: `schemaChangeType=add/drop/rename`, `changeCount=3`.
- `schema-type-promotion`: `schemaChangeType=type promotion`, `changeCount=3`.
- `schema-nested-struct`: `schemaChangeType=nested struct`, `changeCount=3`.
- `schema-complex-types`: `schemaChangeType=map/list/struct`, `changeCount=3`.
- `schema-long-chain-history`: `schemaChangeType=long chain`, `changeCount=config.scale().smallFileCommits()` or configured schema changes.

Metrics to fill for every case:

```java
Map.of(
    "schemaChangeType", changeType,
    "changeCount", Integer.toString(actions.size()),
    "baselineRows", Long.toString(rows),
    "currentRows", Long.toString(rows),
    "snapshotCount", Long.toString(snapshotCount),
    "schemaHistoryLength", Long.toString(schemaHistoryLength),
    "currentQuerySeconds", formatSeconds(currentDuration),
    "historicalQuerySeconds", formatSeconds(historicalDuration)
)
```

- [ ] **Step 4: Update report columns**

Schema section should render:

- `Schema 变化`: `schemaChangeType`, `changeCount`, key DDL summary.
- `历史数据读取断言`: row count/checksum and snapshot id.
- `兼容性结论`: include case-specific conclusion.
- `性能指标`: `currentQuerySeconds`, `historicalQuerySeconds`, `snapshotCount`, `schemaHistoryLength`, `metadataJsonCount`.

- [ ] **Step 5: Verify selected tests**

```powershell
mvn "-Dtest=SchemaEvolutionScenarioTest,IcebergValidationReportWriterTest" test
```

Expected: pass.

- [ ] **Step 6: Commit**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenario.java src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java src/test/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenarioTest.java src/test/java/com/example/databenchmark/iceberg/IcebergValidationReportWriterTest.java
git commit -m "feat: report differentiated Schema evolution metrics"
```

---

### Task 5: HDFS Erasure Coding Real Metrics and Alignment

**Files:**
- Modify: `src/main/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenario.java`
- Modify: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenarioTest.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/IcebergValidationReportWriterTest.java`

- [ ] **Step 1: Write failing tests for EC columns**

Report writer test must assert:

```java
.contains("replicationFileCount=128")
.contains("ecFileCount=96")
.contains("replicationDiskBytes=4096")
.contains("ecDiskBytes=2458")
.contains("diskSavingRatio=40.0%")
.contains("liveDataNodes=1")
.contains("requiredDataNodes=14")
.doesNotContain("<td>scriptedActions=4</td>")
.doesNotContain("ecPolicies=RS-3-2")
```

- [ ] **Step 2: Run tests and verify failure**

```powershell
mvn "-Dtest=ErasureCodingScenarioTest,IcebergValidationReportWriterTest" test
```

Expected: failure because EC metrics are placeholders and columns are misaligned.

- [ ] **Step 3: Collect HDFS usage**

Use existing `HdfsUsageCollector.parse` with actual commands:

```powershell
hdfs dfs -du -s <path>
hdfs dfs -count <path>
```

Metrics per policy:

```java
replicationLogicalBytes
replicationDiskBytes
replicationFileCount
ecPolicy
ecLogicalBytes
ecDiskBytes
ecFileCount
diskSavingRatio
writeSeconds
readSeconds
checksumMatched
```

- [ ] **Step 4: Display skip details in main EC table**

For fault injection cases where shared infra has insufficient DataNodes:

```java
metrics = Map.of(
    "policy", "RS-10-4-1024k",
    "liveDataNodes", "1",
    "requiredDataNodes", "14",
    "skipReason", "requires at least 14 live DataNodes"
)
```

The report must show `liveDataNodes=1` and `requiredDataNodes=14` in visible columns, not only in folded evidence.

- [ ] **Step 5: Verify selected tests**

```powershell
mvn "-Dtest=ErasureCodingScenarioTest,IcebergValidationReportWriterTest" test
```

Expected: pass.

- [ ] **Step 6: Commit**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenario.java src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java src/test/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenarioTest.java src/test/java/com/example/databenchmark/iceberg/IcebergValidationReportWriterTest.java
git commit -m "feat: report real HDFS EC file and disk metrics"
```

---

### Task 6: EC/Replication Conversion Performance Metrics

**Files:**
- Modify: `src/main/java/com/example/databenchmark/iceberg/scenario/ErasureCodingConversionScenario.java`
- Modify: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/scenario/ErasureCodingConversionScenarioTest.java`

- [ ] **Step 1: Write failing tests for conversion metrics**

Assert generated result/report includes:

```java
conversionSeconds=12.345
throughputMbPerSecond=80.1
fileCountBefore=128
fileCountAfter=64
diskBytesBefore=4096
diskBytesAfter=2458
querySecondsBefore=1.200
querySecondsAfter=1.050
checksumMatched=true
```

And does not include:

```java
conversionMetrics=seconds,throughputMbPerSecond,fileCountBefore
scriptedActions=5
```

- [ ] **Step 2: Run test and verify failure**

```powershell
mvn "-Dtest=ErasureCodingConversionScenarioTest,IcebergValidationReportWriterTest" test
```

Expected: failure because current scenario emits metric names only.

- [ ] **Step 3: Implement conversion metric calculation**

For physical rewrite:

```java
conversionSeconds = insertOrRewrite.durationSeconds();
throughputMbPerSecond = logicalBytesBefore / 1024.0 / 1024.0 / conversionSeconds;
```

For policy-only:

```java
policyCommandSeconds = setPolicy.durationSeconds();
existingFilePolicyChanged = false;
newFilePolicy = targetPolicy;
```

- [ ] **Step 4: Update report table**

Render conversion section as:

- `转换方向`: `replication -> EC` or `EC -> replication`.
- `转换方式`: `policy-only` or `physical rewrite`.
- `转换前后文件/磁盘`: file count and HDFS disk bytes before/after.
- `转换耗时/吞吐`: command seconds and MB/s.
- `查询对比`: query seconds before/after and checksum.

- [ ] **Step 5: Verify**

```powershell
mvn "-Dtest=ErasureCodingConversionScenarioTest,IcebergValidationReportWriterTest" test
```

Expected: pass.

- [ ] **Step 6: Commit**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/scenario/ErasureCodingConversionScenario.java src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java src/test/java/com/example/databenchmark/iceberg/scenario/ErasureCodingConversionScenarioTest.java
git commit -m "feat: report EC replication conversion performance"
```

---

### Task 7: Remaining Scenario Metric Completion

**Files:**
- Modify: `src/main/java/com/example/databenchmark/iceberg/scenario/ConcurrentWriteScenario.java`
- Modify: `src/main/java/com/example/databenchmark/iceberg/scenario/RowLevelMutationScenario.java`
- Modify: `src/main/java/com/example/databenchmark/iceberg/scenario/AcidTransactionScenario.java`
- Modify: `src/main/java/com/example/databenchmark/iceberg/scenario/IncrementalPullScenario.java`
- Modify: `src/main/java/com/example/databenchmark/iceberg/scenario/TimeTravelScenario.java`
- Modify: `src/main/java/com/example/databenchmark/iceberg/scenario/SmallFileCompactionScenario.java`
- Modify: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java`
- Test: matching scenario tests under `src/test/java/com/example/databenchmark/iceberg/scenario/`

- [ ] **Step 1: Add failing tests for placeholder removal**

Create one test per scenario asserting no placeholder metric-list keys remain:

```java
assertThat(result.metrics()).doesNotContainKeys(
    "mutationMetrics",
    "incrementalMetrics",
    "timeTravelMetrics"
);
assertThat(result.comparison()).doesNotContainKey("scriptedActions");
```

- [ ] **Step 2: Implement concurrent write metrics**

Required visible metrics:

```text
writerCount
successfulCommits
failedCommits
retryCount
conflictCount
commitLatencyP50Seconds
commitLatencyP95Seconds
finalRowCount
finalSnapshotId
```

- [ ] **Step 3: Implement row-level mutation metrics**

Required visible metrics:

```text
operation
affectedRows
commitSeconds
dataFilesBefore
dataFilesAfter
deleteFilesBefore
deleteFilesAfter
querySecondsBefore
querySecondsAfter
historicalSnapshotRows
```

- [ ] **Step 4: Implement ACID metrics**

Required visible metrics:

```text
snapshotBefore
snapshotAfter
rowCountBefore
rowCountAfter
halfVisibleData=false
conflictError
readerSnapshotId
postCommitSnapshotId
```

- [ ] **Step 5: Implement incremental pull metrics**

Required visible metrics:

```text
baseSnapshotId
endSnapshotId
snapshotWindowSize
fullScanRows
incrementalRows
fullScanSeconds
incrementalSeconds
savingRatio
retentionFailureReason
```

- [ ] **Step 6: Implement time travel metrics**

Required visible metrics:

```text
selector
targetSnapshotId
targetTimestamp
currentRows
historicalRows
currentQuerySeconds
historicalQuerySeconds
expiredSnapshotUnavailable
```

- [ ] **Step 7: Implement small-file metrics**

Required visible metrics:

```text
snapshotCountBefore
snapshotCountAfter
dataFileCountBefore
dataFileCountAfter
manifestCountBefore
manifestCountAfter
metadataJsonCountBefore
metadataJsonCountAfter
hdfsDiskBytesBefore
hdfsDiskBytesAfter
planningSecondsBefore
planningSecondsAfter
querySecondsBefore
querySecondsAfter
```

- [ ] **Step 8: Verify scenario tests**

```powershell
mvn "-Dtest=*ScenarioTest,IcebergValidationReportWriterTest" test
```

Expected: all scenario tests pass and no report writer assertion contains placeholder metric-list values.

- [ ] **Step 9: Commit**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/scenario src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java src/test/java/com/example/databenchmark/iceberg/scenario src/test/java/com/example/databenchmark/iceberg/IcebergValidationReportWriterTest.java
git commit -m "feat: complete Iceberg validation scenario metrics"
```

---

### Task 8: End-to-End Smoke, Report Audit, and Documentation

**Files:**
- Modify: `docs/iceberg/validation.md`
- Test by command only.

- [ ] **Step 1: Run full tests**

```powershell
mvn test
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 2: Build jar**

```powershell
mvn package
```

Expected: `BUILD SUCCESS`; existing Maven shade dependency overlap warnings are acceptable if tests pass.

- [ ] **Step 3: Run Iceberg validate smoke**

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar iceberg-validate --config configs/iceberg-validation.yml --run-id iceberg-real-metrics-smoke-20260729
```

Expected:

- `reports/iceberg-validation/iceberg-real-metrics-smoke-20260729/report.html` exists.
- `reports/iceberg-validation/iceberg-real-metrics-smoke-20260729/report.json` exists.

- [ ] **Step 4: Audit report for forbidden placeholders**

```powershell
rg -n "scriptedActions|conversionMetrics|mutationMetrics|incrementalMetrics|timeTravelMetrics|caseImplemented" reports/iceberg-validation/iceberg-real-metrics-smoke-20260729/report.html reports/iceberg-validation/iceberg-real-metrics-smoke-20260729/report.json
```

Expected: no matches.

- [ ] **Step 5: Audit report for required metric targets**

```powershell
rg -n "schemaHistoryLength|snapshotCount|replicationDiskBytes|ecDiskBytes|diskSavingRatio|conversionSeconds|throughputMbPerSecond|successfulCommits|affectedRows|snapshotBefore|baseSnapshotId|historicalQuerySeconds|dataFileCountBefore|manifestCountBefore|hdfsDiskBytesAfter" reports/iceberg-validation/iceberg-real-metrics-smoke-20260729/report.html
```

Expected: matches for all applicable metrics. EC fault-injection metrics may show `SKIPPED` with `liveDataNodes` and `requiredDataNodes` when the local shared infra cannot support RS-10-4.

- [ ] **Step 6: Update docs**

In `docs/iceberg/validation.md`, add:

```markdown
## 报告度量说明

`metrics` 只保存真实执行值，不保存指标名称列表。每个用例的行内证据分为：

- 执行脚本：实际提交给 Spark SQL 或 HDFS CLI 的命令。
- 执行结果：exit code、duration seconds、stdout、stderr。

本地 smoke 环境如果 DataNode 数不足，RS-10-4 失效副本验证会显示为 `SKIPPED`，并展示 `liveDataNodes` 与 `requiredDataNodes`。
```

- [ ] **Step 7: Commit**

```powershell
git add docs/iceberg/validation.md
git commit -m "docs: document Iceberg validation metric report semantics"
```

- [ ] **Step 8: Push after all commits**

```powershell
git push
```

Expected: remote `main` includes all commits.

---

## Acceptance Criteria

- Report no longer uses metric-name placeholders as values.
- `执行脚本` and `执行结果` are visibly separated in each row.
- Schema rows show case-specific differences.
- EC rows show real file count and HDFS disk usage, or explicit skip facts.
- EC/replication conversion rows show conversion duration, throughput, file count before/after, disk bytes before/after, and query/checksum result.
- Remaining scenarios show the core metrics listed in the original design document.
- Full Maven test suite passes.
- Smoke report exists and passes the placeholder audit.

## Execution Notes

- Do not modify local `docker-compose.yml` to add HDFS/Spark/Hive services. Reuse `../shared-data-infra`.
- If shared infra is not running, the implementation should either start it through the existing runner control path or return explicit preflight failure/skip results. It must not silently produce PASS.
- Long-running/full-load profile should be a separate execution mode after smoke proves the report semantics.


# Iceberg Single DataNode EC Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 增强 `iceberg-validate` 的 Schema 演进和 HDFS 纠删码报告，使 1 个 DataNode 环境下也能清楚展示真实可测的磁盘/查询基线、EC 理论估算、历史数据查询 SQL、查询耗时、返回行数和隐藏的数据集证据。

**Architecture:** 保持现有 `IcebergValidationResult` + `IcebergExecutionEvidence` 数据结构，优先通过 scenario 写入更明确的 metrics/evidence，再由 HTML writer 按场景列渲染。Schema 场景执行 Spark SQL 获取快照、历史读和当前读结果；EC 场景按 HDFS 单副本、replication=2 baseline、每个 EC policy 拆分为多行结果，1 个 DataNode 下只对可真实执行路径写入 actual 指标，对 EC 不可落盘路径写入 theoretical 指标和 notRepresentative 状态。

**Tech Stack:** Java 17, Maven, JUnit 5, AssertJ, Spark SQL through shared-data-infra, HDFS CLI, Apache Iceberg 1.10.1.

---

## File Structure

- Modify `src/main/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenario.java`
  - 增加 ALTER 后新数据写入、历史快照查询、当前快照查询、查询结果 evidence。
  - 增加 package-private constructor，用于测试注入 fake `SparkSqlExecutor`。
- Modify `src/main/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenario.java`
  - 将 EC 聚合 case 改为按 `hdfs-replication-1-actual`、`replication-2-target-baseline`、每个 policy 拆行。
  - 增加单 DataNode 下 theoretical EC 存储估算。
- Create `src/main/java/com/example/databenchmark/iceberg/hdfs/EcPolicySpec.java`
  - 解析 policy 的 data/parity/requiredDataNodes，并计算理论磁盘占用。
- Modify `src/main/java/com/example/databenchmark/iceberg/hdfs/HdfsEcPolicyClient.java`
  - 复用 `EcPolicySpec`，避免 policy 参数分散硬编码。
- Modify `src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java`
  - Schema 与 EC section 增加“验证点说明”列。
  - Schema section 增加历史/当前查询列和隐藏数据集 details。
  - EC section 增加 Policy/模式、EC 设置位置、DataNode 条件、相同数据量、文件数量、磁盘占用、查询效率列。
- Modify `src/test/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenarioTest.java`
- Modify `src/test/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenarioTest.java`
- Modify `src/test/java/com/example/databenchmark/iceberg/hdfs/HdfsEcPolicyClientTest.java`
- Create `src/test/java/com/example/databenchmark/iceberg/hdfs/EcPolicySpecTest.java`
- Modify `src/test/java/com/example/databenchmark/iceberg/IcebergValidationReportWriterTest.java`
- Modify `docs/iceberg/validation.md`

Do not modify `docker-compose.yml` to add local HDFS/Spark/Hive services. Shared infrastructure remains `../shared-data-infra`.

---

### Task 1: EC Policy Spec and Theoretical Storage Math

**Files:**
- Create: `src/main/java/com/example/databenchmark/iceberg/hdfs/EcPolicySpec.java`
- Modify: `src/main/java/com/example/databenchmark/iceberg/hdfs/HdfsEcPolicyClient.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/hdfs/EcPolicySpecTest.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/hdfs/HdfsEcPolicyClientTest.java`

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/com/example/databenchmark/iceberg/hdfs/EcPolicySpecTest.java`:

```java
package com.example.databenchmark.iceberg.hdfs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class EcPolicySpecTest {
    @Test
    void parsesKnownEcPolicies() {
        assertThat(EcPolicySpec.parse("RS-3-2-1024k"))
            .isEqualTo(new EcPolicySpec("RS-3-2-1024k", 3, 2));
        assertThat(EcPolicySpec.parse("RS-6-3-1024k").requiredDataNodes()).isEqualTo(9);
        assertThat(EcPolicySpec.parse("RS-10-4-1024k").requiredDataNodes()).isEqualTo(14);
        assertThat(EcPolicySpec.parse("XOR-2-1-1024k").requiredDataNodes()).isEqualTo(3);
    }

    @Test
    void calculatesTheoreticalDiskBytesAndSavingsAgainstReplication2() {
        EcPolicySpec spec = EcPolicySpec.parse("RS-10-4-1024k");

        assertThat(spec.theoreticalDiskBytes(1_000L)).isEqualTo(1_400L);
        assertThat(spec.theoreticalSavingVsReplication2(1_000L)).isEqualTo("30.00%");
    }

    @Test
    void rejectsUnsupportedPolicyNames() {
        assertThatThrownBy(() -> EcPolicySpec.parse("CUSTOM-1-1"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported EC policy");
    }
}
```

Update `src/test/java/com/example/databenchmark/iceberg/hdfs/HdfsEcPolicyClientTest.java` with:

```java
@Test
void requiredDataNodesDelegatesToPolicySpec() {
    assertThat(HdfsEcPolicyClient.requiredDataNodes("RS-3-2-1024k")).isEqualTo(5);
    assertThat(HdfsEcPolicyClient.requiredDataNodes("RS-6-3-1024k")).isEqualTo(9);
    assertThat(HdfsEcPolicyClient.requiredDataNodes("RS-10-4-1024k")).isEqualTo(14);
    assertThat(HdfsEcPolicyClient.requiredDataNodes("XOR-2-1-1024k")).isEqualTo(3);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
mvn "-Dtest=EcPolicySpecTest,HdfsEcPolicyClientTest" test
```

Expected: compilation fails because `EcPolicySpec` does not exist.

- [ ] **Step 3: Implement `EcPolicySpec`**

Create `src/main/java/com/example/databenchmark/iceberg/hdfs/EcPolicySpec.java`:

```java
package com.example.databenchmark.iceberg.hdfs;

import java.util.Locale;

public record EcPolicySpec(String policy, int dataBlocks, int parityBlocks) {
    public static EcPolicySpec parse(String policy) {
        if (policy == null || policy.isBlank()) {
            throw new IllegalArgumentException("Unsupported EC policy: " + policy);
        }
        if (policy.startsWith("RS-3-2")) {
            return new EcPolicySpec(policy, 3, 2);
        }
        if (policy.startsWith("RS-6-3")) {
            return new EcPolicySpec(policy, 6, 3);
        }
        if (policy.startsWith("RS-10-4")) {
            return new EcPolicySpec(policy, 10, 4);
        }
        if (policy.startsWith("XOR-2-1")) {
            return new EcPolicySpec(policy, 2, 1);
        }
        throw new IllegalArgumentException("Unsupported EC policy: " + policy);
    }

    public int requiredDataNodes() {
        return dataBlocks + parityBlocks;
    }

    public long theoreticalDiskBytes(long logicalBytes) {
        return Math.round(logicalBytes * (dataBlocks + parityBlocks) / (double) dataBlocks);
    }

    public String theoreticalSavingVsReplication2(long logicalBytes) {
        if (logicalBytes <= 0) {
            return "0.00%";
        }
        double replication2Bytes = logicalBytes * 2.0;
        double saving = 1.0 - theoreticalDiskBytes(logicalBytes) / replication2Bytes;
        return String.format(Locale.ROOT, "%.2f%%", saving * 100.0);
    }
}
```

- [ ] **Step 4: Update `HdfsEcPolicyClient.requiredDataNodes`**

Replace the hardcoded method body in `src/main/java/com/example/databenchmark/iceberg/hdfs/HdfsEcPolicyClient.java`:

```java
public static int requiredDataNodes(String policy) {
    return EcPolicySpec.parse(policy).requiredDataNodes();
}
```

- [ ] **Step 5: Verify**

Run:

```powershell
mvn "-Dtest=EcPolicySpecTest,HdfsEcPolicyClientTest" test
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/hdfs/EcPolicySpec.java src/main/java/com/example/databenchmark/iceberg/hdfs/HdfsEcPolicyClient.java src/test/java/com/example/databenchmark/iceberg/hdfs/EcPolicySpecTest.java src/test/java/com/example/databenchmark/iceberg/hdfs/HdfsEcPolicyClientTest.java
git commit -m "feat: add Iceberg EC policy storage estimates"
```

---

### Task 2: Schema Evolution Historical and Current Query Evidence

**Files:**
- Modify: `src/main/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenario.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenarioTest.java`

- [ ] **Step 1: Write the failing test**

Append this test to `SchemaEvolutionScenarioTest`:

```java
@Test
void schemaCasesExposeHistoricalAndCurrentQuerySqlWithPostAlterData() throws Exception {
    SchemaEvolutionScenario scenario = new SchemaEvolutionScenario();
    IcebergValidationConfig config = new IcebergValidationConfigLoader().load(Path.of("configs/iceberg-validation.yml"));
    IcebergValidationContext context = new IcebergValidationContext(config, "schema-query-test", Path.of("work"), Path.of("reports"), false);

    IcebergValidationResult result = scenario.run(
        scenario.cases(config).stream()
            .filter(testCase -> testCase.caseId().equals("schema-add-drop-rename"))
            .findFirst()
            .orElseThrow(),
        context
    );

    assertThat(result.actionCommands()).anyMatch(command -> command.contains("after_evolution"));
    assertThat(result.actionCommands()).anyMatch(command -> command.contains("VERSION AS OF ${baselineSnapshotId}"));
    assertThat(result.actionCommands()).anyMatch(command -> command.contains("SELECT id"));
    assertThat(result.metrics()).containsKeys(
        "validationPoint",
        "baselineSnapshotIdStatus",
        "postAlterSnapshotIdStatus",
        "historicalQuerySql",
        "currentQuerySql",
        "historicalRowsStatus",
        "currentRowsStatus",
        "historicalSampleRowsStatus",
        "currentSampleRowsStatus"
    );
    assertThat(result.metrics().get("validationPoint")).contains("历史快照");
    assertThat(result.metrics().get("historicalQuerySql")).contains("VERSION AS OF");
    assertThat(result.metrics().get("currentQuerySql")).contains("SELECT");
    assertThat(result.evidence()).anyMatch(value -> value.startsWith("historicalSampleRows="));
    assertThat(result.evidence()).anyMatch(value -> value.startsWith("currentSampleRows="));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```powershell
mvn "-Dtest=SchemaEvolutionScenarioTest" test
```

Expected: failure because Schema action commands do not include post-ALTER inserts or historical/current query SQL fields.

- [ ] **Step 3: Add query SQL and sample placeholders without faking execution**

In `SchemaEvolutionScenario.run`, after `SchemaPlan plan = ...`, create:

```java
long baselineEnd = rows;
long postAlterEnd = rows + Math.min(rows, 1000);
String postAlterInsert = IcebergSqlTemplates.insertRange(table, baselineEnd, postAlterEnd, "after_evolution");
String historicalQuerySql = "SELECT id, event_day, metric_int FROM " + table
    + " VERSION AS OF ${baselineSnapshotId} ORDER BY id LIMIT 20";
String currentQuerySql = "SELECT id, event_day, metric_int FROM " + table
    + " ORDER BY id DESC LIMIT 20";
```

Build action commands as:

```java
List<String> actions = new ArrayList<>(plan.actions());
actions.add(postAlterInsert);
actions.add("SELECT snapshot_id FROM " + table + ".snapshots ORDER BY committed_at ASC LIMIT 1 AS baselineSnapshotId");
actions.add("SELECT snapshot_id FROM " + table + ".snapshots ORDER BY committed_at DESC LIMIT 1 AS postAlterSnapshotId");
actions.add(historicalQuerySql);
actions.add(currentQuerySql);
```

Update metrics:

```java
metrics.put("validationPoint", plan.validationPoint());
metrics.put("baselineRows", Long.toString(rows));
metrics.put("currentRows", Long.toString(postAlterEnd));
metrics.put("baselineSnapshotIdStatus", "planned");
metrics.put("postAlterSnapshotIdStatus", "planned");
metrics.put("historicalQuerySql", historicalQuerySql);
metrics.put("currentQuerySql", currentQuerySql);
metrics.put("historicalRowsStatus", "planned");
metrics.put("currentRowsStatus", "planned");
metrics.put("historicalQuerySecondsStatus", "notExecuted");
metrics.put("currentQuerySecondsStatus", "notExecuted");
metrics.put("historicalSampleRowsStatus", "planned");
metrics.put("currentSampleRowsStatus", "planned");
```

Add evidence:

```java
List<String> evidence = List.of(
    "table=" + table,
    "location=" + location,
    "setupScript=" + script.strip(),
    "historicalQuerySql=" + historicalQuerySql,
    "currentQuerySql=" + currentQuerySql,
    "historicalSampleRows=notExecuted",
    "currentSampleRows=notExecuted"
);
```

- [ ] **Step 4: Extend `SchemaPlan` with Chinese validation point**

Change the record:

```java
private record SchemaPlan(
    String changeType,
    String validationPoint,
    List<String> actions,
    List<String> assertions,
    String conclusion
) {
```

For `schema-add-drop-rename`, use:

```java
"验证字段新增、删除、重命名后历史快照仍按 Iceberg 字段 ID 兼容读取，ALTER 后新写入数据只在当前快照可见。"
```

For the other cases, provide matching Chinese descriptions for type promotion, nested struct, complex types, and long schema history.

- [ ] **Step 5: Verify**

Run:

```powershell
mvn "-Dtest=SchemaEvolutionScenarioTest" test
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenario.java src/test/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenarioTest.java
git commit -m "feat: expose Schema evolution historical query evidence"
```

---

### Task 3: EC Single DataNode Matrix Results

**Files:**
- Modify: `src/main/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenario.java`
- Test: `src/test/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenarioTest.java`

- [ ] **Step 1: Write the failing tests**

Replace `nonFaultCasesExposeConcretePlannedMetricsWithoutFakeHdfsUsage` in `ErasureCodingScenarioTest` with:

```java
@Test
void ecCasesIncludeSingleReplicaBaselineAndPolicyRowsWithoutEcPolicyCount() throws Exception {
    ErasureCodingScenario scenario = new ErasureCodingScenario();
    IcebergValidationConfig config = new IcebergValidationConfigLoader().load(Path.of("configs/iceberg-validation.yml"));
    IcebergValidationContext context = new IcebergValidationContext(config, "ec-single-dn-test", Path.of("work"), Path.of("reports"), false);

    List<IcebergValidationResult> results = scenario.cases(config).stream()
        .filter(testCase -> !testCase.caseId().contains("failure"))
        .map(testCase -> scenario.run(testCase, context))
        .toList();

    assertThat(results).extracting(IcebergValidationResult::caseId)
        .contains(
            "hdfs-replication-1-actual",
            "hdfs-replication-2-baseline",
            "ec-policy-rs-3-2-1024k",
            "ec-policy-rs-6-3-1024k",
            "ec-policy-rs-10-4-1024k",
            "ec-policy-xor-2-1-1024k"
        );
    assertThat(results).allSatisfy(result -> {
        assertThat(result.metrics()).containsKey("validationPoint");
        assertThat(result.metrics()).doesNotContainKey("ecPolicyCount");
    });
    IcebergValidationResult singleReplica = resultByCaseId(results, "hdfs-replication-1-actual");
    assertThat(singleReplica.metrics()).containsEntry("policyMode", "hdfs-replication-1-actual");
    assertThat(singleReplica.metrics()).containsEntry("storageMetricType", "actual");
    assertThat(singleReplica.metrics()).containsEntry("replication", "1");

    IcebergValidationResult rs104 = resultByCaseId(results, "ec-policy-rs-10-4-1024k");
    assertThat(rs104.metrics()).containsEntry("policy", "RS-10-4-1024k");
    assertThat(rs104.metrics()).containsEntry("physicalEcWritable", "false");
    assertThat(rs104.metrics()).containsEntry("requiredDataNodes", "14");
    assertThat(rs104.metrics()).containsKey("theoreticalEcDiskBytes");
    assertThat(rs104.metrics()).containsKey("theoreticalSavingVsReplication2");
    assertThat(rs104.metrics()).containsEntry("queryPerformanceStatus", "notRepresentative");
    assertThat(rs104.actionCommands()).anyMatch(command -> command.contains("-setErasureCodingPolicy"));
    assertThat(rs104.actionCommands()).anyMatch(command -> command.contains("-getErasureCodingPolicy"));
}
```

Add helper in the test class:

```java
private static IcebergValidationResult resultByCaseId(List<IcebergValidationResult> results, String caseId) {
    return results.stream()
        .filter(result -> caseId.equals(result.caseId()))
        .findFirst()
        .orElseThrow();
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```powershell
mvn "-Dtest=ErasureCodingScenarioTest" test
```

Expected: failure because cases still return only two non-fault aggregate rows and still expose `ecPolicyCount`.

- [ ] **Step 3: Expand `cases` into baseline and policy rows**

In `ErasureCodingScenario.cases`, replace non-fault aggregate cases with:

```java
List<IcebergValidationCase> cases = new ArrayList<>();
cases.add(testCase("hdfs-replication-1-actual", "Measure HDFS single-replica actual disk and query baseline.", Map.of("replication", "1")));
cases.add(testCase("hdfs-replication-2-baseline", "Measure target replication=2 baseline and under-replication status on single DataNode.", Map.of("replication", "2")));
for (String policy : config.hdfs().ecPolicies()) {
    cases.add(testCase("ec-policy-" + policy.toLowerCase(Locale.ROOT).replace('_', '-'), "Evaluate EC policy storage and query behavior on single DataNode.", Map.of("policy", policy)));
}
cases.add(testCase("ec-rs-10-4-failure-tolerance", "Validate RS-10-4 tolerated DataNode failure readability.", Map.of("policy", "RS-10-4-1024k")));
cases.add(testCase("ec-policy-matrix-failure", "Validate tolerated failure matrix for configured EC policies.", Map.of("policies", String.join(",", config.hdfs().ecPolicies()))));
return cases;
```

Add imports:

```java
import com.example.databenchmark.iceberg.hdfs.EcPolicySpec;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Locale;
```

- [ ] **Step 4: Implement baseline rows**

Add method:

```java
private IcebergValidationResult plannedReplicationBaseline(
    IcebergValidationCase testCase,
    IcebergValidationContext context,
    String table,
    String location,
    int replication
) {
    long targetRows = Math.min(context.config().scale().rows(), 1000);
    Map<String, String> metrics = new LinkedHashMap<>();
    metrics.put("validationPoint", replication == 1
        ? "在 1 个 DataNode 下真实测量 HDFS 单副本文件数、磁盘占用和查询耗时基线。"
        : "在 1 个 DataNode 下记录 replication=2 目标基线，并显式标记副本不足风险。");
    metrics.put("policyMode", replication == 1 ? "hdfs-replication-1-actual" : "hdfs-replication-2-target-baseline");
    metrics.put("replication", Integer.toString(replication));
    metrics.put("rowCount", Long.toString(targetRows));
    metrics.put("logicalBytesStatus", "plannedActual");
    metrics.put("fileCountStatus", "plannedActual");
    metrics.put("hdfsDiskBytesStatus", "plannedActual");
    metrics.put("querySecondsStatus", "plannedActual");
    metrics.put("storageMetricType", "actual");
    metrics.put("underReplicated", replication > 1 ? "true" : "false");
    metrics.put("liveDataNodes", "1");
    metrics.put("requiredDataNodes", "1");
    return plannedEcResult(testCase, context, table, location, metrics, replicationBaselineActions(context, location, table, replication),
        replication == 1
            ? IcebergConclusion.FunctionStatus.PASS
            : IcebergConclusion.FunctionStatus.DEGRADED,
        "HDFS replication=" + replication + " baseline exposes actual metric collection commands for the single DataNode environment.");
}
```

The helper `replicationBaselineActions` should return:

```java
return List.of(
    "hdfs dfs -fs " + context.config().hdfs().defaultFs() + " -setrep -w " + replication + " " + location,
    "hdfs dfs -fs " + context.config().hdfs().defaultFs() + " -du -s " + location,
    "hdfs dfs -fs " + context.config().hdfs().defaultFs() + " -count " + location,
    "SELECT COUNT(*) FROM " + table
);
```

- [ ] **Step 5: Implement EC policy rows**

Add method:

```java
private IcebergValidationResult plannedEcPolicy(
    IcebergValidationCase testCase,
    IcebergValidationContext context,
    String table,
    String location,
    String policy
) {
    long targetRows = Math.min(context.config().scale().rows(), 1000);
    long logicalBytesEstimate = targetRows * 128L;
    EcPolicySpec spec = EcPolicySpec.parse(policy);
    boolean physicalWritable = 1 >= spec.requiredDataNodes();
    Map<String, String> metrics = new LinkedHashMap<>();
    metrics.put("validationPoint", "在 1 个 DataNode 下验证 " + policy + " policy 设置路径，并以 theoretical 字段估算 EC 存储开销。");
    metrics.put("policyMode", "ec-policy");
    metrics.put("policy", policy);
    metrics.put("setPolicyPath", location + "/ec-target/" + policy.toLowerCase(Locale.ROOT));
    metrics.put("setPolicyStatus", "planned");
    metrics.put("getPolicyStatus", "planned");
    metrics.put("liveDataNodes", "1");
    metrics.put("requiredDataNodes", Integer.toString(spec.requiredDataNodes()));
    metrics.put("physicalEcWritable", Boolean.toString(physicalWritable));
    metrics.put("rowCount", Long.toString(targetRows));
    metrics.put("logicalBytesStatus", "plannedActual");
    metrics.put("logicalBytesEstimate", Long.toString(logicalBytesEstimate));
    metrics.put("fileCountStatus", "notRepresentative");
    metrics.put("hdfsDiskBytesStatus", "notRepresentative");
    metrics.put("theoreticalEcDiskBytes", Long.toString(spec.theoreticalDiskBytes(logicalBytesEstimate)));
    metrics.put("theoreticalSavingVsReplication2", spec.theoreticalSavingVsReplication2(logicalBytesEstimate));
    metrics.put("queryPerformanceStatus", "notRepresentative");
    metrics.put("skipPhysicalReason", policy + " requires " + spec.requiredDataNodes() + " live DataNodes for physical EC block groups");
    return plannedEcResult(testCase, context, table, location, metrics, ecPolicyActions(context, location, table, policy),
        IcebergConclusion.FunctionStatus.DEGRADED,
        policy + " cannot be physically encoded with one DataNode; policy checks and theoretical storage estimates are reported separately from actual HDFS usage.");
}
```

The helper `ecPolicyActions` should return:

```java
String path = location + "/ec-target/" + policy.toLowerCase(Locale.ROOT);
return List.of(
    "hdfs dfs -fs " + context.config().hdfs().defaultFs() + " -mkdir -p " + path,
    "hdfs ec -fs " + context.config().hdfs().defaultFs() + " -setPolicy -policy " + policy + " -path " + path,
    "hdfs ec -fs " + context.config().hdfs().defaultFs() + " -getPolicy -path " + path,
    "hdfs dfs -fs " + context.config().hdfs().defaultFs() + " -du -s " + path,
    "hdfs dfs -fs " + context.config().hdfs().defaultFs() + " -count " + path,
    "SELECT COUNT(*) FROM " + table
);
```

- [ ] **Step 6: Keep fault cases honest**

Update `skippedFaultTolerance` metrics:

```java
metrics.put("validationPoint", "验证 EC policy 在副本失效后的可读性和查询性能影响；当前 DataNode 数不足，不能真实执行故障后查询。");
metrics.put("querySecondsBeforeFailureStatus", "notExecuted");
metrics.put("querySecondsAfterFailureStatus", "notExecuted");
metrics.put("latencyImpactRatioStatus", "notComparable");
metrics.put("checksumMatchedStatus", "notExecuted");
metrics.put("physicalEcWritable", Boolean.toString(preflight.canRunFaultTolerance()));
```

Do not add `querySecondsAfterFailure` unless it is measured from a real executed command.

- [ ] **Step 7: Verify**

Run:

```powershell
mvn "-Dtest=ErasureCodingScenarioTest,EcPolicySpecTest" test
```

Expected: tests pass.

- [ ] **Step 8: Commit**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenario.java src/test/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenarioTest.java
git commit -m "feat: expand Iceberg EC report into single-node matrix"
```

---

### Task 4: HTML Report Columns and Hidden Query Result Details

**Files:**
- Modify: `src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java`
- Modify: `src/test/java/com/example/databenchmark/iceberg/IcebergValidationReportWriterTest.java`

- [ ] **Step 1: Write failing report assertions**

In `IcebergValidationReportWriterTest`, add sample Schema metrics:

```java
Map.of(
    "validationPoint", "验证字段 rename 后历史快照仍按字段 ID 兼容读取。",
    "schemaChangeType", "add/drop/rename",
    "changeCount", "4",
    "baselineSnapshotId", "111",
    "postAlterSnapshotId", "222",
    "historicalQuerySql", "SELECT id FROM t VERSION AS OF 111 LIMIT 20",
    "currentQuerySql", "SELECT id FROM t LIMIT 20",
    "historicalRows", "1000",
    "currentRows", "2000",
    "historicalQuerySeconds", "0.321",
    "currentQuerySeconds", "0.456",
    "schemaHistoryLength", "5"
)
```

Add evidence:

```java
List.of(
    "historicalSampleRows=id\n1\n2",
    "currentSampleRows=id\n2000\n1999"
)
```

Add EC sample result with:

```java
Map.of(
    "validationPoint", "在 1 个 DataNode 下真实测量 HDFS 单副本。",
    "policyMode", "hdfs-replication-1-actual",
    "replication", "1",
    "rowCount", "1000",
    "logicalBytes", "128000",
    "fileCount", "8",
    "hdfsDiskBytes", "128000",
    "querySeconds", "0.512",
    "storageMetricType", "actual"
)
```

Assert the HTML contains:

```java
.contains("<th>验证点说明</th>")
.contains("历史数据查询")
.contains("当前数据查询")
.contains("返回数据集")
.contains("SELECT id FROM t VERSION AS OF 111 LIMIT 20")
.contains("historicalRows=1000")
.contains("currentRows=2000")
.contains("historicalQuerySeconds=0.321")
.contains("hdfs-replication-1-actual")
.contains("storageMetricType=actual")
.contains("querySeconds=0.512")
.doesNotContain("ecPolicyCount=4")
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```powershell
mvn "-Dtest=IcebergValidationReportWriterTest" test
```

Expected: failure because report headers/cells do not include verification point, Schema query detail cells, or EC single-replica fields.

- [ ] **Step 3: Update Schema section headers and cells**

In `scenarioSections()`, change Schema headers to:

```java
List.of("用例", "验证点说明", "Schema 变化", "历史数据查询", "当前数据查询", "元数据/性能指标", "状态", "执行脚本与证据")
```

In `scenarioCells`, change `schemaEvolution` branch to:

```java
case "schemaEvolution" -> List.of(
    code(result.caseId()),
    metricOrEmpty(result, "validationPoint"),
    schemaChangeSummary(result),
    schemaHistoricalQueryCell(result),
    schemaCurrentQueryCell(result),
    schemaMetadataMetrics(result),
    status,
    evidenceDetails(result)
);
```

Add helper methods:

```java
private static String schemaHistoricalQueryCell(IcebergValidationResult result) {
    return mapValues(result.metrics(),
        "baselineSnapshotId",
        "baselineSnapshotIdStatus",
        "historicalRows",
        "historicalRowsStatus",
        "historicalQuerySeconds",
        "historicalQuerySecondsStatus",
        "historicalQuerySql"
    );
}

private static String schemaCurrentQueryCell(IcebergValidationResult result) {
    return mapValues(result.metrics(),
        "postAlterSnapshotId",
        "postAlterSnapshotIdStatus",
        "currentRows",
        "currentRowsStatus",
        "currentQuerySeconds",
        "currentQuerySecondsStatus",
        "currentQuerySql"
    );
}
```

- [ ] **Step 4: Update EC section headers and cells**

Change EC headers to:

```java
List.of("用例", "验证点说明", "Policy/模式", "EC 设置位置", "DataNode 条件", "相同数据量", "文件数量", "磁盘占用", "查询效率", "结论", "状态", "执行脚本与证据")
```

Change `erasureCoding` branch to:

```java
case "erasureCoding" -> List.of(
    code(result.caseId()),
    metricOrEmpty(result, "validationPoint"),
    ecPolicyModeCell(result),
    ecPolicyLocationCell(result),
    ecDataNodeRequirements(result),
    ecDataScaleCell(result),
    ecFileCountMetrics(result),
    ecDiskUsageMetrics(result),
    ecQueryEfficiencyCell(result),
    result.conclusion(),
    status,
    evidenceDetails(result)
);
```

Add helpers:

```java
private static String ecPolicyModeCell(IcebergValidationResult result) {
    return mapValues(result.metrics(), "policyMode", "replication", "policy");
}

private static String ecPolicyLocationCell(IcebergValidationResult result) {
    return mapValues(result.metrics(), "setPolicyPath", "setPolicyStatus", "getPolicyStatus");
}

private static String ecDataScaleCell(IcebergValidationResult result) {
    return mapValues(result.metrics(), "rowCount", "logicalBytes", "logicalBytesStatus", "logicalBytesEstimate", "checksum", "targetChecksum");
}

private static String ecQueryEfficiencyCell(IcebergValidationResult result) {
    return mapValues(result.metrics(), "querySeconds", "querySecondsStatus", "queryPerformanceStatus", "querySecondsBeforeFailureStatus", "querySecondsAfterFailureStatus", "latencyImpactRatioStatus");
}
```

Update `ecFileCountMetrics` and `ecDiskUsageMetrics`:

```java
private static String ecFileCountMetrics(IcebergValidationResult result) {
    return mapValues(result.metrics(), "fileCount", "fileCountStatus", "replicationFileCount", "ecFileCount");
}

private static String ecDiskUsageMetrics(IcebergValidationResult result) {
    return mapValues(result.metrics(), "hdfsDiskBytes", "hdfsDiskBytesStatus", "replicationDiskBytes", "ecDiskBytes", "theoreticalEcDiskBytes", "theoreticalSavingVsReplication2", "storageMetricType");
}
```

- [ ] **Step 5: Add hidden dataset rendering**

In `evidenceDetails`, keep existing execution evidence rendering and append a result dataset group:

```java
private static String datasetEvidence(IcebergValidationResult result) {
    List<String> samples = result.evidence().stream()
        .filter(value -> value.startsWith("historicalSampleRows=") || value.startsWith("currentSampleRows="))
        .toList();
    if (samples.isEmpty()) {
        return "";
    }
    return "<details><summary>返回数据集</summary>" + pre(samples) + "</details>";
}
```

Use it inside both structured and legacy evidence details:

```java
+ datasetEvidence(result)
```

- [ ] **Step 6: Verify**

Run:

```powershell
mvn "-Dtest=IcebergValidationReportWriterTest" test
```

Expected: tests pass.

- [ ] **Step 7: Commit**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/IcebergValidationReportWriter.java src/test/java/com/example/databenchmark/iceberg/IcebergValidationReportWriterTest.java
git commit -m "feat: add Iceberg report validation points and query evidence"
```

---

### Task 5: Real Execution Hook for Schema and EC Smoke Evidence

**Files:**
- Modify: `src/main/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenario.java`
- Modify: `src/main/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenario.java`
- Modify: `src/test/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenarioTest.java`
- Modify: `src/test/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenarioTest.java`

- [ ] **Step 1: Write failing fake-executor tests**

Add package-private constructors:

```java
SchemaEvolutionScenario(SparkSqlExecutor sparkSqlExecutor) {
    this.sparkSqlExecutor = sparkSqlExecutor;
}
```

and:

```java
ErasureCodingScenario(SparkSqlExecutor sparkSqlExecutor, HdfsCliClient hdfsCliClient) {
    this.sparkSqlExecutor = sparkSqlExecutor;
    this.hdfsCliClient = hdfsCliClient;
}
```

Before implementing constructors, write tests that instantiate fake executors and assert `executionResults` contains measured command evidence.

Schema test fake:

```java
private static final class FakeSparkSqlExecutor extends SparkSqlExecutor {
    @Override
    public CommandResult run(IcebergValidationConfig config, String sql) {
        if (sql.contains(".snapshots") && sql.contains("ASC")) {
            return new CommandResult(List.of("spark-sql"), 0, "snapshot_id\n111\n", "", 0.100);
        }
        if (sql.contains(".snapshots") && sql.contains("DESC")) {
            return new CommandResult(List.of("spark-sql"), 0, "snapshot_id\n222\n", "", 0.100);
        }
        if (sql.contains("VERSION AS OF")) {
            return new CommandResult(List.of("spark-sql"), 0, "id\n1\n2\n", "", 0.321);
        }
        if (sql.contains("ORDER BY id DESC")) {
            return new CommandResult(List.of("spark-sql"), 0, "id\n2000\n1999\n", "", 0.456);
        }
        return new CommandResult(List.of("spark-sql"), 0, "OK\n", "", 0.050);
    }
}
```

Assert:

```java
assertThat(result.metrics()).containsEntry("baselineSnapshotId", "111");
assertThat(result.metrics()).containsEntry("postAlterSnapshotId", "222");
assertThat(result.metrics()).containsEntry("historicalRows", "2");
assertThat(result.metrics()).containsEntry("currentRows", "2");
assertThat(result.metrics()).containsEntry("historicalQuerySeconds", "0.321");
assertThat(result.metrics()).containsEntry("currentQuerySeconds", "0.456");
assertThat(result.executionResults()).extracting(IcebergExecutionEvidence::label)
    .contains("historical query", "current query");
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
mvn "-Dtest=SchemaEvolutionScenarioTest,ErasureCodingScenarioTest" test
```

Expected: compilation fails until injectable constructors and execution logic are added.

- [ ] **Step 3: Implement Schema execution path**

Use `SparkSqlExecutor` in `SchemaEvolutionScenario.run`:

```java
List<IcebergExecutionEvidence> executionResults = new ArrayList<>();
executionResults.add(runSpark(context.config(), "setup", "create schema table", script));
for (String action : plan.actions()) {
    executionResults.add(runSpark(context.config(), "action", "schema change", action));
}
executionResults.add(runSpark(context.config(), "action", "post alter insert", postAlterInsert));
IcebergExecutionEvidence baselineSnapshot = runSpark(context.config(), "metric", "baseline snapshot id", baselineSnapshotSql);
IcebergExecutionEvidence postAlterSnapshot = runSpark(context.config(), "metric", "post alter snapshot id", postAlterSnapshotSql);
String baselineSnapshotId = IcebergMetricCollectors.parseSingleString(baselineSnapshot.stdout());
String postAlterSnapshotId = IcebergMetricCollectors.parseSingleString(postAlterSnapshot.stdout());
String historicalQuerySqlWithId = historicalQuerySql.replace("${baselineSnapshotId}", baselineSnapshotId);
IcebergExecutionEvidence historicalQuery = runSpark(context.config(), "assertion", "historical query", historicalQuerySqlWithId);
IcebergExecutionEvidence currentQuery = runSpark(context.config(), "assertion", "current query", currentQuerySql);
```

Add the helper:

```java
private IcebergExecutionEvidence runSpark(
    IcebergValidationConfig config,
    String phase,
    String label,
    String sql
) throws IOException, InterruptedException {
    CommandResult result = sparkSqlExecutor.run(config, sql);
    return new IcebergExecutionEvidence(
        phase,
        label,
        sql,
        result.exitCode(),
        result.durationSeconds(),
        result.stdout(),
        result.stderr()
    );
}
```

Count non-header data rows with:

```java
private static long dataRowCount(String stdout) {
    return stdout.lines()
        .map(String::trim)
        .filter(line -> !line.isBlank())
        .filter(line -> !line.matches("[-+|]+"))
        .skip(1)
        .count();
}
```

If a Spark command throws, return `IcebergScenarioSupport.fail(...)` with accumulated evidence and error message. Do not produce PASS on failed execution.

- [ ] **Step 4: Implement EC execution fallback**

For Task 5, keep EC HDFS/Spark execution conservative:

- HDFS single-replica and replication=2 rows should contain executable commands and `plannedActual` statuses when tests use the default constructor.
- Fake tests can inject command results later, but production should not fabricate actual `hdfsDiskBytes` if a command is not run.
- EC policy rows should keep `theoreticalEcDiskBytes` and `notRepresentative` in 1 DN.

If full HDFS command execution is added in this task, use `HdfsUsageCollector.parse(du.stdout(), count.stdout())` and store:

```java
metrics.put("logicalBytes", Long.toString(usage.logicalBytes()));
metrics.put("hdfsDiskBytes", Long.toString(usage.diskBytes()));
metrics.put("fileCount", Long.toString(usage.fileCount()));
```

- [ ] **Step 5: Verify**

Run:

```powershell
mvn "-Dtest=SchemaEvolutionScenarioTest,ErasureCodingScenarioTest,IcebergMetricCollectorsTest" test
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```powershell
git add src/main/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenario.java src/main/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenario.java src/test/java/com/example/databenchmark/iceberg/scenario/SchemaEvolutionScenarioTest.java src/test/java/com/example/databenchmark/iceberg/scenario/ErasureCodingScenarioTest.java
git commit -m "feat: execute Iceberg schema historical read probes"
```

---

### Task 6: Documentation, Full Verification, Smoke, and Push

**Files:**
- Modify: `docs/iceberg/validation.md`

- [ ] **Step 1: Update user-facing docs**

Append to `docs/iceberg/validation.md`:

```markdown
## 单 DataNode EC 报告模式

当共享 HDFS 只有 1 个 DataNode 时，报告会增加 `hdfs-replication-1-actual` 行，用于展示真实可落盘的 HDFS 单副本文件数、磁盘占用和查询耗时。`replication=2` 行作为目标双副本基线展示，但在 1 个 DataNode 下需要标记 under-replicated。

EC policy 行会展示 set/get policy 命令、DataNode 条件和理论存储估算。`theoreticalEcDiskBytes` 与 `theoreticalSavingVsReplication2` 不是 HDFS `du` 的真实落盘值；当 `physicalEcWritable=false` 时，查询性能会标记为 `notRepresentative`。
```

- [ ] **Step 2: Run full tests**

Run:

```powershell
mvn test
```

Expected: `BUILD SUCCESS`, 0 failures.

- [ ] **Step 3: Build jar**

Run:

```powershell
mvn package
```

Expected: `BUILD SUCCESS`; existing Maven shade overlap warnings are acceptable if build exits 0.

- [ ] **Step 4: Run smoke**

Run:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar iceberg-validate --config configs/iceberg-validation.yml --run-id iceberg-single-dn-ec-smoke-20260729
```

Expected:

- `reports/iceberg-validation/iceberg-single-dn-ec-smoke-20260729/report.html` exists.
- `reports/iceberg-validation/iceberg-single-dn-ec-smoke-20260729/report.json` exists.
- Report status is `SUCCESS` or contains explicit per-case `SKIPPED/DEGRADED` rows with errors.

- [ ] **Step 5: Audit report for required Schema and EC fields**

Run:

```powershell
rg -n "验证点说明|历史数据查询|当前数据查询|返回数据集|historicalQuerySql|currentQuerySql|after_evolution|hdfs-replication-1-actual|replication=2|theoreticalEcDiskBytes|theoreticalSavingVsReplication2|physicalEcWritable=false|queryPerformanceStatus=notRepresentative" reports/iceberg-validation/iceberg-single-dn-ec-smoke-20260729/report.html reports/iceberg-validation/iceberg-single-dn-ec-smoke-20260729/report.json
```

Expected: matches for all listed report concepts.

- [ ] **Step 6: Audit report for forbidden placeholders**

Run:

```powershell
rg -n "scriptedActions|conversionMetrics|mutationMetrics|incrementalMetrics|timeTravelMetrics|caseImplemented|acidEvidence|ecPolicyCount=4" reports/iceberg-validation/iceberg-single-dn-ec-smoke-20260729/report.html reports/iceberg-validation/iceberg-single-dn-ec-smoke-20260729/report.json
```

Expected: no matches.

- [ ] **Step 7: Commit docs**

```powershell
git add docs/iceberg/validation.md
git commit -m "docs: document single DataNode Iceberg EC reporting"
```

- [ ] **Step 8: Push**

```powershell
git push
```

Expected: remote `main` includes all commits.

---

## Acceptance Criteria

- Schema 表格每行都有中文“验证点说明”。
- Schema 报告能看到 ALTER 后新数据写入 SQL。
- Schema 报告能看到历史数据查询 SQL、当前数据查询 SQL、查询耗时、返回行数。
- Schema 返回数据集默认隐藏在行内 details 中，点击后可展开查看。
- HDFS 纠删码表格每行都有中文“验证点说明”。
- HDFS 纠删码表格包含 `hdfs-replication-1-actual` 单副本行。
- HDFS 纠删码表格包含 `replication=2` baseline 行。
- EC policy 不再用 `ecPolicyCount=4` 聚合展示，而是按 policy 拆行。
- 每个 EC policy 行展示 set/get policy 位置、DataNode 要求、相同数据量、文件数、磁盘占用、查询效率字段。
- 1 个 DataNode 下 EC 存储节省使用 `theoretical*` 字段，不冒充真实 HDFS `du`。
- EC 故障容忍 DataNode 不足时不展示伪造的故障后查询效率。
- 不修改本仓库 Docker Compose 添加重复 HDFS/Spark 基础设施。

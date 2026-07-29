package com.example.databenchmark.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergValidationReportWriterTest {
    @TempDir
    Path tempDir;

    @Test
    void writesJsonAndHtmlReports() throws Exception {
        IcebergValidationReport report = new IcebergValidationReport(
            "run-1",
            "1.10.1",
            "smoke",
            "SUCCESS",
            "2026-07-28T00:00:00Z",
            "2026-07-28T00:00:01Z",
            List.of(
                resultWithExecution("schemaEvolution", "schema-add-drop-rename", "verify schema compatibility",
                    List.of("ALTER TABLE iceberg_table ADD COLUMN added_text STRING"),
                    List.of("row count matched"),
                    Map.ofEntries(
                        Map.entry("validationPoint", "验证字段 rename 后历史快照仍按字段 ID 兼容读取。"),
                        Map.entry("schemaChangeType", "add/drop/rename"),
                        Map.entry("changeCount", "4"),
                        Map.entry("baselineSnapshotId", "111"),
                        Map.entry("postAlterSnapshotId", "222"),
                        Map.entry("historicalQuerySql", "SELECT id FROM t VERSION AS OF 111 LIMIT 20"),
                        Map.entry("currentQuerySql", "SELECT id FROM t LIMIT 20"),
                        Map.entry("historicalRows", "1000"),
                        Map.entry("historicalRowsStatus", "planned"),
                        Map.entry("currentRows", "2000"),
                        Map.entry("currentRowsStatus", "planned"),
                        Map.entry("historicalQuerySeconds", "0.321"),
                        Map.entry("historicalQuerySecondsStatus", "notExecuted"),
                        Map.entry("currentQuerySeconds", "0.456"),
                        Map.entry("currentQuerySecondsStatus", "notExecuted"),
                        Map.entry("schemaHistoryLength", "5")
                    ),
                    Map.of("queryMs", "10.0"),
                    Map.of("latencyRatio", "1.25"),
                    "历史数据兼容读取通过，查询延迟为基线 1.25 倍。",
                    List.of("historicalSampleRows=id\n1\n2", "currentSampleRows=id\n2000\n1999"),
                    List.of(new IcebergExecutionEvidence(
                        "action",
                        "query current row count",
                        "SELECT COUNT(*) FROM iceberg_table",
                        0,
                        0.42,
                        "count\n1000",
                        ""
                    ))),
                skippedResult("erasureCoding", "ec-rs-10-4-failure-tolerance", "verify EC failure tolerance",
                    List.of("hdfs ec -setPolicy -policy RS-10-4-1024k /target"),
                    List.of("checksum matches"),
                    Map.of(
                        "policy", "RS-10-4-1024k",
                        "policyMode", "ec-policy",
                        "replicationBaseline", "2",
                        "liveDataNodes", "1",
                        "requiredDataNodes", "14",
                        "physicalEcWritable", "false",
                        "theoreticalEcDiskBytes", "179200",
                        "theoreticalSavingVsReplication2", "30.00%",
                        "queryPerformanceStatus", "notRepresentative",
                        "skipReason", "RS-10-4-1024k requires at least 14 live DataNodes"
                    ),
                    Map.of("replicationBaseline", "2"),
                    Map.of(),
                    "DataNode 数不足，跳过 RS-10-4 容错验证。",
                    List.of("policy=RS-10-4-1024k", "requiredDataNodes=14")),
                result("erasureCoding", "hdfs-replication-1-actual", "verify EC disk usage",
                    List.of("hdfs dfs -du -s /replication-baseline", "hdfs dfs -count /ec-target"),
                    List.of("checksum matches"),
                    Map.of(
                        "validationPoint", "在 1 个 DataNode 下真实测量 HDFS 单副本。",
                        "policyMode", "hdfs-replication-1-actual",
                        "replication", "1",
                        "rowCount", "1000",
                        "logicalBytes", "128000",
                        "fileCount", "8",
                        "hdfsDiskBytes", "128000",
                        "querySeconds", "0.512",
                        "storageMetricType", "actual",
                        "ecPolicyCount", "4"
                    ),
                    Map.of("replicationBaseline", "2"),
                    Map.of("scriptedActions", "4", "ecPolicies", "RS-3-2"),
                    "HDFS 用量对比已采集。",
                    List.of("policyMode=hdfs-replication-1-actual")),
                skippedResult("erasureCodingConversion", "replication-to-ec-policy-only", "verify EC conversion",
                    List.of(),
                    List.of("conversion not executed until real HDFS collection is wired"),
                    Map.of(
                        "conversionDirection", "replication->ec",
                        "conversionMode", "policy-only",
                        "targetPolicy", "RS-10-4-1024k",
                        "conversionStatus", "notExecuted",
                        "policyCommandStatus", "notExecuted",
                        "hdfsUsageStatus", "notCollected",
                        "checksumStatus", "notCollected",
                        "skipReason", "real HDFS/Spark physical and policy conversion execution is not wired"
                    ),
                    Map.of(),
                    Map.of(),
                    "Real HDFS/Spark conversion is not wired; physical/policy conversion measurement is pending real execution.",
                    List.of("direction=replication->ec")),
                result("concurrentWrite", "concurrent-append-same-partition", "verify concurrent append",
                    List.of("launch Spark writers: [2, 4, 8]"),
                    List.of("failed conflicts do not leak partial data"),
                    Map.of("writerGroups", "[2, 4, 8]", "commitLatencyMs", "40"),
                    Map.of(),
                    Map.of("successfulCommits", "8"),
                    "并发提交符合预期。",
                    List.of("writers=8")),
                result("rowLevelMutation", "row-merge-upsert-delete", "verify row mutation <b>raw</b>",
                    List.of("MERGE INTO table"),
                    List.of("historical snapshot still sees deleted rows"),
                    Map.of("mutationMetrics", "duration,rewriteFiles,deleteFiles,queryMsAfter"),
                    Map.of(),
                    Map.of(),
                    "行级变更符合预期。<script>alert(1)</script>",
                    List.of("deleteFiles=2")),
                result("acidTransaction", "acid-reader-isolation", "verify ACID isolation",
                    List.of("capture snapshot before", "capture snapshot after"),
                    List.of("no half-visible data"),
                    Map.of("acidEvidence", "snapshotLineage,rowCount,conflictError"),
                    Map.of(),
                    Map.of(),
                    "ACID 断言通过。",
                    List.of("snapshotLineage=ok")),
                result("incrementalPull", "incremental-append-only", "verify incremental pull",
                    List.of("read changes between snapshots"),
                    List.of("incremental rows equal appended batch"),
                    Map.of("incrementalMetrics", "fullScanMs,incrementalMs,savingRatio,snapshotWindow"),
                    Map.of("fullScanMs", "100"),
                    Map.of("incrementalMs", "20"),
                    "增量拉取符合预期。",
                    List.of("snapshotWindow=1..3")),
                result("timeTravel", "time-travel-by-snapshot-id", "verify time travel",
                    List.of("SELECT COUNT(*) FROM table VERSION AS OF ${snapshot_id}"),
                    List.of("historical row count matches expected snapshot"),
                    Map.of("timeTravelMetrics", "currentQueryMs,historicalQueryMs,planningMs"),
                    Map.of(),
                    Map.of(),
                    "时间旅行符合预期。",
                    List.of("snapshotId=123")),
                result("smallFileCompaction", "small-files-data-compaction", "verify compaction",
                    List.of("CALL iceberg_catalog.system.rewrite_data_files"),
                    List.of("file count and manifest count collected before and after"),
                    Map.of("targetSnapshots", "100", "filesPerCommit", "4", "dataFileCountAfter", "20", "caseImplemented", "true"),
                    Map.of("dataFileCountBefore", "400"),
                    Map.of("dataFileCountAfter", "20"),
                    "小文件合并符合预期。",
                    List.of("manifestCountAfter=5"))
            )
        );

        Path root = tempDir.resolve("reports");
        Path index = new IcebergValidationReportWriter().write(report, root);

        assertThat(index).isEqualTo(root.resolve("run-1").resolve("report.html"));
        assertThat(root.resolve("run-1").resolve("report.json")).exists();
        assertThat(root.resolve("run-1").resolve("report.html")).exists();
        assertThat(root.resolve("run-1").resolve("report.md")).doesNotExist();

        String html = Files.readString(root.resolve("run-1").resolve("report.html"));
        assertThat(html)
            .contains("<title>Iceberg Validation Report</title>")
            .contains("<details class=\"evidence-details\"><summary>展开证据</summary>")
            .contains("<h2>Schema 长期演进</h2>")
            .contains("<th>用例</th>")
            .contains("<th>验证点说明</th>")
            .contains("<th>Schema 变化</th>")
            .contains("<th>历史数据查询</th>")
            .contains("<th>当前数据查询</th>")
            .contains("<th>元数据/性能指标</th>")
            .contains("<th>状态</th>")
            .contains("<th>执行脚本与证据</th>")
            .contains("验证字段 rename 后历史快照仍按字段 ID 兼容读取。")
            .contains("SELECT id FROM t VERSION AS OF 111 LIMIT 20")
            .contains("SELECT id FROM t LIMIT 20")
            .contains("historicalRows=1000")
            .contains("historicalRowsStatus=planned")
            .contains("currentRows=2000")
            .contains("currentRowsStatus=planned")
            .contains("historicalQuerySeconds=0.321")
            .contains("historicalQuerySecondsStatus=notExecuted")
            .contains("currentQuerySeconds=0.456")
            .contains("currentQuerySecondsStatus=notExecuted")
            .contains("schemaHistoryLength=5")
            .contains("返回数据集")
            .contains("historicalSampleRows=id")
            .contains("currentSampleRows=id")
            .contains("<h2>HDFS 纠删码</h2>")
            .contains("<th>Policy/模式</th>")
            .contains("<th>EC 设置位置</th>")
            .contains("<th>DataNode 条件</th>")
            .contains("<th>相同数据量</th>")
            .contains("<th>文件数量</th>")
            .contains("<th>磁盘占用</th>")
            .contains("<th>查询效率</th>")
            .contains("<th>结论</th>")
            .contains("hdfs-replication-1-actual")
            .contains("policyMode=ec-policy")
            .contains("policy=RS-10-4-1024k")
            .contains("physicalEcWritable=false")
            .contains("theoreticalEcDiskBytes=179200")
            .contains("theoreticalSavingVsReplication2=30.00%")
            .contains("queryPerformanceStatus=notRepresentative")
            .contains("storageMetricType=actual")
            .contains("querySeconds=0.512")
            .contains("rowCount=1000")
            .contains("logicalBytes=128000")
            .contains("fileCount=8")
            .contains("hdfsDiskBytes=128000")
            .contains("liveDataNodes=1")
            .contains("requiredDataNodes=14")
            .contains("<h2>EC/replication")
            .contains("conversionDirection=replication-&gt;ec")
            .contains("conversionMode=policy-only")
            .contains("targetPolicy=RS-10-4-1024k")
            .contains("hdfsUsageStatus=notCollected")
            .contains("conversionStatus=notExecuted")
            .contains("policyCommandStatus=notExecuted")
            .contains("checksumStatus=notCollected")
            .contains("skipReason=real HDFS/Spark physical and policy conversion execution is not wired")
            .contains("status-SKIPPED")
            .contains("NOT_COMPARABLE")
            .contains("&lt;b&gt;raw&lt;/b&gt;")
            .contains("&lt;script&gt;alert(1)&lt;/script&gt;")
            .doesNotContain("<b>raw</b>")
            .doesNotContain("<script>alert(1)</script>")
            .doesNotContain("<td>scriptedActions=4</td>")
            .doesNotContain("scriptedActions=")
            .doesNotContain("ecPolicies=RS-3-2")
            .doesNotContain("ecPolicyCount=4")
            .doesNotContain("conversionMetrics=seconds,throughputMbPerSecond")
            .doesNotContain("conversionSeconds=")
            .doesNotContain("throughputMbPerSecond=")
            .doesNotContain("fileCountBefore=")
            .doesNotContain("fileCountAfter=")
            .doesNotContain("diskBytesBefore=")
            .doesNotContain("diskBytesAfter=")
            .doesNotContain("querySecondsBefore=")
            .doesNotContain("querySecondsAfter=")
            .doesNotContain("checksumMatched=")
            .doesNotContain("hdfs ec -setPolicy -path /target")
            .doesNotContain("INSERT INTO")
            .doesNotContain("_source")
            .doesNotContain("_target")
            .doesNotContain("mutationMetrics=duration,rewriteFiles,deleteFiles,queryMsAfter")
            .doesNotContain("acidEvidence=snapshotLineage,rowCount,conflictError")
            .doesNotContain("incrementalMetrics=fullScanMs,incrementalMs,savingRatio,snapshotWindow")
            .doesNotContain("timeTravelMetrics=currentQueryMs,historicalQueryMs,planningMs")
            .doesNotContain("caseImplemented=")
            .doesNotContain("Schema 变化类型: primitive,numeric,nested,complex")
            .doesNotContain("schemaChangeTypes");
    }

    private static IcebergValidationResult result(
        String scenario,
        String caseId,
        String purpose,
        List<String> actions,
        List<String> assertions,
        Map<String, String> metrics,
        Map<String, String> baseline,
        Map<String, String> comparison,
        String conclusion,
        List<String> evidence
    ) {
        return resultWithExecution(
            scenario,
            caseId,
            purpose,
            actions,
            assertions,
            metrics,
            baseline,
            comparison,
            conclusion,
            evidence,
            List.of()
        );
    }

    private static IcebergValidationResult skippedResult(
        String scenario,
        String caseId,
        String purpose,
        List<String> actions,
        List<String> assertions,
        Map<String, String> metrics,
        Map<String, String> baseline,
        Map<String, String> comparison,
        String conclusion,
        List<String> evidence
    ) {
        return new IcebergValidationResult(
            scenario,
            caseId,
            purpose,
            Map.of("rows", "100000", "profile", "smoke", "smallFileCommits", "100", "filesPerCommit", "4"),
            List.of("CREATE TABLE iceberg_table"),
            actions,
            assertions,
            metrics,
            baseline,
            comparison,
            IcebergConclusion.FunctionStatus.SKIPPED,
            IcebergConclusion.PerformanceStatus.NOT_COMPARABLE,
            conclusion,
            evidence,
            List.of(),
            List.of()
        );
    }

    private static IcebergValidationResult resultWithExecution(
        String scenario,
        String caseId,
        String purpose,
        List<String> actions,
        List<String> assertions,
        Map<String, String> metrics,
        Map<String, String> baseline,
        Map<String, String> comparison,
        String conclusion,
        List<String> evidence,
        List<IcebergExecutionEvidence> executionResults
    ) {
        return new IcebergValidationResult(
            scenario,
            caseId,
            purpose,
            Map.of("rows", "100000", "profile", "smoke", "smallFileCommits", "100", "filesPerCommit", "4"),
            List.of("CREATE TABLE iceberg_table"),
            actions,
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
}

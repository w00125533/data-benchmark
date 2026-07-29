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
                    Map.of(
                        "schemaChangeType", "add/drop/rename",
                        "changeCount", "3",
                        "baselineRows", "1000",
                        "currentRows", "1000",
                        "snapshotCount", "4",
                        "schemaHistoryLength", "4"
                    ),
                    Map.of("queryMs", "10.0"),
                    Map.of("latencyRatio", "1.25"),
                    "历史数据兼容读取通过，查询延迟为基线 1.25 倍。",
                    List.of("snapshotId=123"),
                    List.of(new IcebergExecutionEvidence(
                        "action",
                        "query current row count",
                        "SELECT COUNT(*) FROM iceberg_table",
                        0,
                        0.42,
                        "count\n1000",
                        ""
                    ))),
                result("erasureCoding", "ec-rs-10-4-failure-tolerance", "verify EC failure tolerance",
                    List.of("hdfs ec -setPolicy -policy RS-10-4-1024k /target"),
                    List.of("checksum matches"),
                    Map.of("fileCount", "128", "hdfsDiskBytes", "2048", "replicationBaseline", "2"),
                    Map.of("replicationBaseline", "2"),
                    Map.of("diskSavingRatio", "0.42"),
                    "DataNode 数不足，跳过 RS-10-4 容错验证。",
                    List.of("policy=RS-10-4-1024k", "requiredDataNodes=14")),
                result("erasureCodingConversion", "replication-to-ec-rewrite", "verify EC conversion",
                    List.of("INSERT INTO target SELECT * FROM source"),
                    List.of("checksum matches after conversion"),
                    Map.of("conversionSeconds", "12", "throughputMbPerSecond", "30", "conversionMetrics", "seconds,throughputMbPerSecond"),
                    Map.of("fileCountBefore", "128"),
                    Map.of("fileCountAfter", "64", "scriptedActions", "5"),
                    "转换效率符合预期。",
                    List.of("direction=replication->ec")),
                result("concurrentWrite", "concurrent-append-same-partition", "verify concurrent append",
                    List.of("launch Spark writers: [2, 4, 8]"),
                    List.of("failed conflicts do not leak partial data"),
                    Map.of("writerGroups", "[2, 4, 8]", "commitLatencyMs", "40"),
                    Map.of(),
                    Map.of("successfulCommits", "8"),
                    "并发提交符合预期。",
                    List.of("writers=8")),
                result("rowLevelMutation", "row-merge-upsert-delete", "verify row mutation",
                    List.of("MERGE INTO table"),
                    List.of("historical snapshot still sees deleted rows"),
                    Map.of("mutationMetrics", "duration,rewriteFiles,deleteFiles,queryMsAfter"),
                    Map.of(),
                    Map.of(),
                    "行级变更符合预期。",
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
        assertThat(Files.readString(root.resolve("run-1").resolve("report.html")))
            .contains("<title>Iceberg Validation Report</title>")
            .contains("<div class=\"scenario-method\"><strong>验证策略和方法</strong>")
            .contains("<th>执行脚本与证据</th>")
            .contains("<details class=\"evidence-details\"><summary>展开证据</summary>")
            .doesNotContain("验证项总览")
            .doesNotContain("需求要素矩阵")
            .doesNotContain("<h2>执行脚本与证据</h2>")
            .contains("<h2>Schema 长期演进</h2>")
            .contains("<th>Schema 变化</th>")
            .contains("<h2>HDFS 纠删码</h2>")
            .contains("<th>EC Policy</th>")
            .contains("<th>HDFS 磁盘占用</th>")
            .contains("<h2>EC/replication 转换</h2>")
            .contains("<th>转换方向</th>")
            .contains("<h2>多进程并发写入</h2>")
            .contains("<th>Writer 数</th>")
            .contains("<h2>行级更新删除</h2>")
            .contains("<th>操作类型</th>")
            .contains("<h2>ACID 事务保证</h2>")
            .contains("<th>快照原子性断言</th>")
            .contains("<h2>增量拉取</h2>")
            .contains("<th>Snapshot Window</th>")
            .contains("<h2>时间旅行</h2>")
            .contains("<th>访问方式</th>")
            .contains("<h2>小文件 Compaction</h2>")
            .contains("<th>Compaction 类型</th>")
            .contains("Schema 长期演进")
            .contains("通过多阶段 Schema 变更")
            .contains("<td>schemaChangeType=add/drop/rename<br>changeCount=3<br>ALTER TABLE iceberg_table ADD COLUMN added_text STRING</td>")
            .contains("<td>baselineRows=1000<br>currentRows=1000<br>snapshotCount=4<br>row count matched</td>")
            .contains("<td>snapshotCount=4<br>schemaHistoryLength=4</td>")
            .contains("add/drop/rename")
            .contains("changeCount=3")
            .contains("baselineRows=1000")
            .contains("currentRows=1000")
            .contains("schemaHistoryLength=4")
            .contains("schema-add-drop-rename")
            .contains("<strong>执行脚本</strong>")
            .contains("<strong>执行结果</strong>")
            .contains("SELECT COUNT(*) FROM iceberg_table")
            .contains("exitCode=0")
            .contains("durationSeconds=0.420")
            .contains("count")
            .contains("1000")
            .contains("row count matched")
            .contains("历史数据兼容读取通过")
            .contains("RS-10-4-1024k")
            .contains("replicationBaseline=2")
            .contains("性能指标")
            .contains("fileCount")
            .contains("hdfsDiskBytes")
            .doesNotContain("scriptedActions=")
            .doesNotContain("conversionMetrics=seconds,throughputMbPerSecond")
            .doesNotContain("mutationMetrics=duration,rewriteFiles,deleteFiles,queryMsAfter")
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

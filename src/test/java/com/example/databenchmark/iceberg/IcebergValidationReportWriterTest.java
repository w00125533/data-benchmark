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
                new IcebergValidationResult(
                    "schemaEvolution",
                    "schema-add-drop-rename",
                    "verify schema compatibility",
                    Map.of("rows", "100000"),
                    List.of("CREATE TABLE iceberg_table"),
                    List.of("ALTER TABLE iceberg_table ADD COLUMN added_text STRING"),
                    List.of("row count matched"),
                    Map.of("snapshotCount", "3", "queryMs", "12.5"),
                    Map.of("queryMs", "10.0"),
                    Map.of("latencyRatio", "1.25"),
                    IcebergConclusion.FunctionStatus.PASS,
                    IcebergConclusion.PerformanceStatus.ACCEPTABLE,
                    "历史数据兼容读取通过，查询延迟为基线 1.25 倍。",
                    List.of("snapshotId=123"),
                    List.of()
                ),
                new IcebergValidationResult(
                    "erasureCoding",
                    "ec-rs-10-4-failure-tolerance",
                    "verify EC failure tolerance",
                    Map.of("rows", "100000"),
                    List.of("hdfs dfs -setrep -w 2 /baseline"),
                    List.of("hdfs ec -setPolicy -policy RS-10-4-1024k /target"),
                    List.of("checksum matches"),
                    Map.of("fileCount", "128", "hdfsDiskBytes", "2048"),
                    Map.of("replicationBaseline", "2"),
                    Map.of("diskSavingRatio", "0.42"),
                    IcebergConclusion.FunctionStatus.SKIPPED,
                    IcebergConclusion.PerformanceStatus.NOT_COMPARABLE,
                    "DataNode 数不足，跳过 RS-10-4 容错验证。",
                    List.of("policy=RS-10-4-1024k", "requiredDataNodes=14"),
                    List.of()
                )
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
            .contains("验证项总览")
            .contains("需求要素矩阵")
            .contains("执行脚本与证据")
            .contains("Schema 长期演进")
            .contains("Schema 变化类型")
            .contains("schema-add-drop-rename")
            .contains("历史数据兼容读取通过")
            .contains("RS-10-4-1024k")
            .contains("replicationBaseline=2")
            .contains("性能指标")
            .contains("fileCount")
            .contains("hdfsDiskBytes");
    }
}

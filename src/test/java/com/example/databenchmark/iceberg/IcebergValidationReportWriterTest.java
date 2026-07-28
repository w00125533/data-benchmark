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
}

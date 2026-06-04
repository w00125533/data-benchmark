package com.example.databenchmark.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WebReportWriterTest {
    @TempDir
    Path tempDir;

    @Test
    void writesStandaloneWebReportPackage() throws Exception {
        Path output = new WebReportWriter().write(BenchmarkReport.sample("run-web"), tempDir);

        assertThat(output).isEqualTo(tempDir.resolve("run-web").resolve("index.html"));
        assertThat(output).exists();
        assertThat(tempDir.resolve("run-web").resolve("report.json")).exists();
        assertThat(tempDir.resolve("run-web").resolve("assets").resolve("report-ui.js")).exists();
        assertThat(tempDir.resolve("run-web").resolve("assets").resolve("report-ui.css")).exists();

        String html = Files.readString(output);
        assertThat(html).contains("window.__BENCHMARK_REPORT__");
        assertThat(html).contains("report-ui.js");

        WebBenchmarkReport json = new ObjectMapper()
            .readValue(tempDir.resolve("run-web").resolve("report.json").toFile(), WebBenchmarkReport.class);
        assertThat(json.schemaVersion()).isEqualTo(1);
        assertThat(json.run().runId()).isEqualTo("run-web");
    }

    @Test
    void rejectsUnsafeRunId() {
        BenchmarkReport report = reportWithRunId("../outside");

        assertThatThrownBy(() -> new WebReportWriter().write(report, tempDir))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("runId");
    }

    @Test
    void escapesEmbeddedJsonWithoutCorruptingReportJson() throws Exception {
        BenchmarkReport report = new BenchmarkReport(
            "run-xss",
            "smoke \"quoted\"",
            "kpi",
            "smoke",
            "2026-06-04T00:00:00Z",
            "2026-06-04T00:00:01Z",
            1,
            1,
            1,
            1,
            1,
            List.of(new BenchmarkReport.LoadSummary(
                "local",
                "</script><script>alert(1)</script>",
                "generate",
                1,
                1,
                1.0,
                true,
                "line\u2028separator"
            )),
            List.of(new BenchmarkReport.QuerySummary("local", "generated_parquet", "query", 1, 1, 1, 1, 0, true, "")),
            false
        );

        Path index = new WebReportWriter().write(report, tempDir);
        String html = Files.readString(index);
        WebBenchmarkReport json = new ObjectMapper()
            .readValue(tempDir.resolve("run-xss").resolve("report.json").toFile(), WebBenchmarkReport.class);

        assertThat(html).doesNotContain("</script><script>alert(1)</script>");
        assertThat(html).contains("<\\/script>");
        assertThat(html).contains("\\u2028");
        assertThat(html).contains("smoke \\\"quoted\\\"");
        assertThat(json.loads().get(0).tableShape()).isEqualTo("</script><script>alert(1)</script>");
    }

    @Test
    void failsClearlyWhenInjectionMarkerIsMissing() throws Exception {
        Path resourceRoot = tempDir.resolve("resource-root");
        Files.createDirectories(resourceRoot.resolve("assets"));
        Files.writeString(
            resourceRoot.resolve("index.html"),
            "<!doctype html><script type=\"module\" src=\"./assets/report-ui.js\"></script>",
            StandardCharsets.UTF_8
        );
        Files.writeString(resourceRoot.resolve("assets").resolve("report-ui.js"), "console.log('ok');", StandardCharsets.UTF_8);

        assertThatThrownBy(() -> new WebReportWriter(resourceRoot).write(BenchmarkReport.sample("run-web"), tempDir))
            .isInstanceOf(java.io.IOException.class)
            .hasMessageContaining("BENCHMARK_REPORT_DATA");
    }

    private BenchmarkReport reportWithRunId(String runId) {
        return new BenchmarkReport(
            runId,
            "smoke",
            "kpi",
            "smoke",
            "2026-06-04T00:00:00Z",
            "2026-06-04T00:00:01Z",
            1,
            1,
            1,
            1,
            1,
            List.of(new BenchmarkReport.LoadSummary("local", "generated_parquet", "generate", 1, 1, 1.0, true, "")),
            List.of(new BenchmarkReport.QuerySummary("local", "generated_parquet", "query", 1, 1, 1, 1, 0, true, "")),
            false
        );
    }
}

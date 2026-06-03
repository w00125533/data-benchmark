package com.example.databenchmark.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.databenchmark.metrics.BenchmarkMetrics;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HtmlReportWriterTest {
    @TempDir
    Path tempDir;

    @Test
    void reportContainsRequiredSections() throws Exception {
        BenchmarkReport report = BenchmarkReport.sample("run-test");

        Path output = new HtmlReportWriter().write(report, tempDir);

        String html = Files.readString(output);
        assertThat(output).isEqualTo(tempDir.resolve("run-test").resolve("index.html"));
        assertThat(html).contains("Run Metadata");
        assertThat(html).contains("Dataset Summary");
        assertThat(html).contains("Load Summary");
        assertThat(html).contains("Query Summary");
        assertThat(html).contains("Grafana dashboard for this run");
        assertThat(html).contains("run-test");
        assertThat(html).contains("not a 4.032B row full-profile validation");
    }

    @Test
    void unsafeRunIdIsRejected() {
        BenchmarkReport report = reportWith("../outside", "http://localhost:3000/d/benchmark", false);

        assertThatThrownBy(() -> new HtmlReportWriter().write(report, tempDir))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("runId");
    }

    @Test
    void unsafeGrafanaUrlIsRejected() {
        BenchmarkReport report = reportWith("run-test", "javascript:alert(1)", false);

        assertThatThrownBy(() -> new HtmlReportWriter().write(report, tempDir))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("grafanaUrl");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "http:example.com",
        "http:/example.com",
        "https://exa mple.com/path"
    })
    void malformedGrafanaUrlsAreRejected(String grafanaUrl) {
        BenchmarkReport report = reportWith("run-test", grafanaUrl, false);

        assertThatThrownBy(() -> new HtmlReportWriter().write(report, tempDir))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("grafanaUrl");
    }

    @Test
    void reportFieldsAreEscaped() throws Exception {
        BenchmarkReport report = new BenchmarkReport(
            "run-xss",
            "<script>alert(1)</script>",
            "2026-06-02T00:00:00Z",
            "2026-06-02T00:01:00Z",
            10,
            1,
            100,
            50,
            2_048,
            List.of(new BenchmarkReport.LoadSummary(
                "<script>engine</script>",
                "<script>shape</script>",
                "<script>stage</script>",
                100,
                2_048,
                1.25,
                true,
                ""
            )),
            List.of(new BenchmarkReport.QuerySummary(
                "spark_iceberg",
                "iceberg_db.cell_kpi_1min",
                "<script>query</script>",
                10.0,
                12.0,
                15.0,
                100,
                0,
                true,
                ""
            )),
            "http://localhost:3000/d/benchmark?var-run_id=run-xss",
            false
        );

        String html = Files.readString(new HtmlReportWriter().write(report, tempDir));

        assertThat(html).doesNotContain("<script>");
        assertThat(html).contains("&lt;script&gt;alert(1)&lt;/script&gt;");
        assertThat(html).contains("&lt;script&gt;engine&lt;/script&gt;");
        assertThat(html).contains("&lt;script&gt;shape&lt;/script&gt;");
        assertThat(html).contains("&lt;script&gt;stage&lt;/script&gt;");
        assertThat(html).contains("&lt;script&gt;query&lt;/script&gt;");
    }

    @Test
    void degradedReportRendersStatusAndErrors() throws Exception {
        BenchmarkReport report = new BenchmarkReport(
            "run-degraded",
            "smoke",
            "2026-06-02T00:00:00Z",
            "2026-06-02T00:01:00Z",
            10,
            1,
            14_400,
            50,
            2_048,
            List.of(
                new BenchmarkReport.LoadSummary(
                    "spark_iceberg",
                    "iceberg_db.cell_kpi_1min",
                    "spark_iceberg_load",
                    14_400,
                    2_048,
                    1.2,
                    true,
                    ""
                ),
                new BenchmarkReport.LoadSummary(
                    "starrocks_external_iceberg",
                    "iceberg_db.cell_kpi_1min",
                    "starrocks_external_refresh",
                    0,
                    0,
                    0.4,
                    false,
                    "catalog refresh failed"
                )
            ),
            List.of(
                new BenchmarkReport.QuerySummary(
                    "spark_iceberg",
                    "iceberg_db.cell_kpi_1min",
                    "topn_high_load_cells",
                    10.0,
                    12.0,
                    15.0,
                    42,
                    0,
                    true,
                    ""
                ),
                new BenchmarkReport.QuerySummary(
                    "starrocks_external_iceberg",
                    "iceberg_db.cell_kpi_1min",
                    "topn_high_load_cells",
                    0.0,
                    0.0,
                    0.0,
                    0,
                    1,
                    false,
                    "query failed"
                )
            ),
            "http://localhost:3000/d/benchmark?var-run_id=run-degraded",
            false
        );

        String html = Files.readString(new HtmlReportWriter().write(report, tempDir));

        assertThat(html).contains("Status");
        assertThat(html).contains("Error");
        assertThat(html).contains("DEGRADED");
        assertThat(html).contains("starrocks_external_iceberg");
        assertThat(html).contains("catalog refresh failed");
    }

    @Test
    void representativeLoadAndQueryValuesRender() throws Exception {
        BenchmarkReport report = reportWith("run-values", "https://grafana.example/d/benchmark", false);

        String html = Files.readString(new HtmlReportWriter().write(report, tempDir));

        assertThat(html).contains("generate");
        assertThat(html).contains("14400");
        assertThat(html).contains("2048");
        assertThat(html).contains("1.2");
        assertThat(html).contains("spark_iceberg");
        assertThat(html).contains("iceberg_db.cell_kpi_1min");
        assertThat(html).contains("topn_high_load_cells");
        assertThat(html).contains("SUCCESS");
        assertThat(html).contains("10");
        assertThat(html).contains("12");
        assertThat(html).contains("15");
    }

    @Test
    void fullProfileSuppressesValidationNotice() throws Exception {
        BenchmarkReport report = reportWith("run-full", "http://localhost:3000/d/benchmark", true);

        String html = Files.readString(new HtmlReportWriter().write(report, tempDir));

        assertThat(html).doesNotContain("not a 4.032B row full-profile validation");
    }

    @Test
    void sampleUrlEncodesRunIdForGrafanaQueryParameter() {
        BenchmarkReport report = BenchmarkReport.sample("run test");

        assertThat(report.grafanaUrl()).contains("var-run_id=run+test");
    }

    @Test
    void metricsExposeRequiredNamesAndLabels() {
        assertThat(Modifier.isFinal(BenchmarkMetrics.class.getModifiers())).isTrue();
        assertThat(BenchmarkMetrics.LABELS)
            .isEqualTo(List.of("run_id", "profile", "engine", "table_shape", "stage", "query_name"));
        assertThat(BenchmarkMetrics.LOAD_DURATION_SECONDS).isEqualTo("benchmark_load_duration_seconds");
        assertThat(BenchmarkMetrics.LOAD_ROWS_TOTAL).isEqualTo("benchmark_load_rows_total");
        assertThat(BenchmarkMetrics.LOAD_BYTES_TOTAL).isEqualTo("benchmark_load_bytes_total");
        assertThat(BenchmarkMetrics.QUERY_DURATION_SECONDS).isEqualTo("benchmark_query_duration_seconds");
        assertThat(BenchmarkMetrics.QUERY_ROWS_TOTAL).isEqualTo("benchmark_query_rows_total");
        assertThat(BenchmarkMetrics.QUERY_FAILURES_TOTAL).isEqualTo("benchmark_query_failures_total");
    }

    private BenchmarkReport reportWith(String runId, String grafanaUrl, boolean fullProfile) {
        return new BenchmarkReport(
            runId,
            "smoke",
            "2026-06-02T00:00:00Z",
            "2026-06-02T00:01:00Z",
            10,
            1,
            14_400,
            50,
            2_048,
            List.of(new BenchmarkReport.LoadSummary("local", "generated_parquet", "generate", 14_400, 2_048, 1.2, true, "")),
            List.of(new BenchmarkReport.QuerySummary(
                "spark_iceberg",
                "iceberg_db.cell_kpi_1min",
                "topn_high_load_cells",
                10.0,
                12.0,
                15.0,
                100,
                0,
                true,
                ""
            )),
            grafanaUrl,
            fullProfile
        );
    }
}

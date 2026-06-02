package com.example.databenchmark.report;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.metrics.BenchmarkMetrics;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
}

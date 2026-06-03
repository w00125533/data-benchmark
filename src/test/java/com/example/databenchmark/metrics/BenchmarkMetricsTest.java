package com.example.databenchmark.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.junit.jupiter.api.Test;

class BenchmarkMetricsTest {
    @Test
    void recordsLoadMetricsWithRequiredNamesAndLabels() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        BenchmarkMetrics.recordLoad(
            registry,
            "run-1",
            "smoke",
            "spark_iceberg",
            "iceberg_db.cell_kpi_1min",
            "spark_iceberg_load",
            14_400,
            2_048,
            1.5
        );

        Timer duration = registry.find(BenchmarkMetrics.LOAD_DURATION_SECONDS)
            .tags(requiredTags("", "spark_iceberg_load"))
            .timer();
        Counter rows = registry.find(BenchmarkMetrics.LOAD_ROWS_TOTAL)
            .tags(requiredTags("", "spark_iceberg_load"))
            .counter();
        Counter bytes = registry.find(BenchmarkMetrics.LOAD_BYTES_TOTAL)
            .tags(requiredTags("", "spark_iceberg_load"))
            .counter();

        assertThat(duration).isNotNull();
        assertThat(duration.totalTime(java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(1.5);
        assertThat(rows).isNotNull();
        assertThat(rows.count()).isEqualTo(14_400.0);
        assertThat(bytes).isNotNull();
        assertThat(bytes.count()).isEqualTo(2_048.0);
    }

    @Test
    void recordsQueryMetricsWithRequiredNamesAndLabels() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        BenchmarkMetrics.recordQuery(
            registry,
            "run-1",
            "smoke",
            "starrocks_external_iceberg",
            "iceberg_db.cell_kpi_1min",
            "query",
            "topn_high_load_cells",
            42,
            1,
            0.25
        );

        Timer duration = registry.find(BenchmarkMetrics.QUERY_DURATION_SECONDS)
            .tags(requiredTags("topn_high_load_cells", "query"))
            .timer();
        Counter rows = registry.find(BenchmarkMetrics.QUERY_ROWS_TOTAL)
            .tags(requiredTags("topn_high_load_cells", "query"))
            .counter();
        Counter failures = registry.find(BenchmarkMetrics.QUERY_FAILURES_TOTAL)
            .tags(requiredTags("topn_high_load_cells", "query"))
            .counter();

        assertThat(duration).isNotNull();
        assertThat(duration.totalTime(java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(0.25);
        assertThat(rows).isNotNull();
        assertThat(rows.count()).isEqualTo(42.0);
        assertThat(failures).isNotNull();
        assertThat(failures.count()).isEqualTo(1.0);
    }

    @Test
    void exposesRequiredMetricNamesAndLabels() {
        assertThat(BenchmarkMetrics.LABELS)
            .isEqualTo(List.of("run_id", "profile", "engine", "table_shape", "stage", "query_name"));
        assertThat(BenchmarkMetrics.LOAD_DURATION_SECONDS).isEqualTo("benchmark_load_duration_seconds");
        assertThat(BenchmarkMetrics.LOAD_ROWS_TOTAL).isEqualTo("benchmark_load_rows_total");
        assertThat(BenchmarkMetrics.LOAD_BYTES_TOTAL).isEqualTo("benchmark_load_bytes_total");
        assertThat(BenchmarkMetrics.QUERY_DURATION_SECONDS).isEqualTo("benchmark_query_duration_seconds");
        assertThat(BenchmarkMetrics.QUERY_ROWS_TOTAL).isEqualTo("benchmark_query_rows_total");
        assertThat(BenchmarkMetrics.QUERY_FAILURES_TOTAL).isEqualTo("benchmark_query_failures_total");
    }

    @Test
    void negativeLoadValuesThrowWithFieldNames() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        assertThatThrownBy(() -> recordLoad(registry, -1, 2_048, 1.5))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("rows");
        assertThatThrownBy(() -> recordLoad(registry, 14_400, -1, 1.5))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("bytes");
        assertThatThrownBy(() -> recordLoad(registry, 14_400, 2_048, -0.1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("durationSeconds");
    }

    @Test
    void negativeQueryValuesThrowWithFieldNames() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        assertThatThrownBy(() -> recordQuery(registry, -1, 0, 0.25))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("rows");
        assertThatThrownBy(() -> recordQuery(registry, 42, -1, 0.25))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("failures");
        assertThatThrownBy(() -> recordQuery(registry, 42, 0, -0.1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("durationSeconds");
    }

    private String[] requiredTags(String queryName, String stage) {
        return new String[] {
            "run_id", "run-1",
            "profile", "smoke",
            "engine", queryName.isEmpty() ? "spark_iceberg" : "starrocks_external_iceberg",
            "table_shape", "iceberg_db.cell_kpi_1min",
            "stage", stage,
            "query_name", queryName
        };
    }

    private void recordLoad(SimpleMeterRegistry registry, long rows, long bytes, double durationSeconds) {
        BenchmarkMetrics.recordLoad(
            registry,
            "run-1",
            "smoke",
            "spark_iceberg",
            "iceberg_db.cell_kpi_1min",
            "spark_iceberg_load",
            rows,
            bytes,
            durationSeconds
        );
    }

    private void recordQuery(SimpleMeterRegistry registry, long rows, int failures, double durationSeconds) {
        BenchmarkMetrics.recordQuery(
            registry,
            "run-1",
            "smoke",
            "starrocks_external_iceberg",
            "iceberg_db.cell_kpi_1min",
            "query",
            "topn_high_load_cells",
            rows,
            failures,
            durationSeconds
        );
    }
}

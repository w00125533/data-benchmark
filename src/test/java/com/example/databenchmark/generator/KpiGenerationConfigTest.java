package com.example.databenchmark.generator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.databenchmark.config.BenchmarkConfig;
import org.junit.jupiter.api.Test;

class KpiGenerationConfigTest {
    @Test
    void capsTargetRowsByRowCap() {
        KpiGenerationConfig config = KpiGenerationConfig.from(BenchmarkConfig.defaultSmoke());

        assertThat(config.targetRows()).isEqualTo(10_000L);
        assertThat(config.partitions()).isGreaterThanOrEqualTo(1);
        assertThat(config.master()).isEqualTo("local[*]");
        assertThat(config.outputMode()).isEqualTo("overwrite");
    }

    @Test
    void derivesLargePartitionCountFromRowsPerPartition() {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(10_000, 100, null, "data/generated", 1_000_000_000L);

        KpiGenerationConfig generation = KpiGenerationConfig.from(config);

        assertThat(generation.targetRows()).isEqualTo(1_000_000_000L);
        assertThat(generation.partitions()).isEqualTo(1000);
    }

    @Test
    void hdfsUriOutputDoesNotThrowAndKeepsSafeMetadataPath() {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(10, 1, null, "hdfs://hdfs-namenode:8020/benchmark/kpi-1b/generated", 100L);

        KpiGenerationConfig generation = KpiGenerationConfig.from(config);

        assertThat(generation.output()).isEqualTo("hdfs://hdfs-namenode:8020/benchmark/kpi-1b/generated");
        assertThat(generation.outputPath().toString().replace('\\', '/')).isEqualTo("/benchmark/kpi-1b/generated");
    }

    @Test
    void rejectsInvalidCells() {
        BenchmarkConfig invalid = new BenchmarkConfig(
            "bad",
            1L,
            BenchmarkConfig.SuiteConfig.defaultSuite(),
            new BenchmarkConfig.DatasetConfig(0, 1, 50, "2026-01-01T00:00:00", "data/generated", 10L, null),
            new BenchmarkConfig.QueryConfig(1, 1, 1, null),
            new BenchmarkConfig.ReportConfig("html", "reports/runs")
        );

        assertThatThrownBy(() -> KpiGenerationConfig.from(invalid))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cells must be positive");
    }
}

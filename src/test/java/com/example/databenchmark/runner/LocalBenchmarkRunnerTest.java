package com.example.databenchmark.runner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.databenchmark.config.BenchmarkConfig;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalBenchmarkRunnerTest {
    @TempDir
    Path tempDir;

    @Test
    void localSmokeGeneratesDataAndReport() throws Exception {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(2, 1, 99L, tempDir.resolve("data").toString(), 8L);

        LocalBenchmarkRunner.LocalRunResult result = new LocalBenchmarkRunner()
            .run(config, tempDir.resolve("reports"), "run-local-test");

        assertThat(result.dataset().rows()).isEqualTo(8L);
        assertThat(result.reportPath()).isEqualTo(
            tempDir.resolve("reports").resolve("run-local-test").resolve("index.html")
        );
        assertThat(result.reportPath()).exists();
    }

    @Test
    void localSmokeReportIncludesEncodedGrafanaUrlAndPlaceholderQuery() throws Exception {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(2, 1, 99L, tempDir.resolve("data").toString(), 8L);

        LocalBenchmarkRunner.LocalRunResult result = new LocalBenchmarkRunner()
            .run(config, tempDir.resolve("reports"), "run-local-test");

        String html = Files.readString(result.reportPath());
        assertThat(html)
            .contains("http://localhost:3000/d/benchmark?var-run_id=run-local-test&amp;var-suite=kpi&amp;var-query_set=smoke");
        assertThat(html).contains("local");
        assertThat(html).contains("generated_parquet");
        assertThat(html).contains("catalog_render_check");
        assertThat(html).contains("not a 4.032B row full-profile validation");
    }

    @Test
    void generatedRunIdIsSafeForReportWriterValidation() throws Exception {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(1, 1, 99L, tempDir.resolve("data").toString(), 1L);

        LocalBenchmarkRunner.LocalRunResult result = new LocalBenchmarkRunner()
            .run(config, tempDir.resolve("reports"), null);

        assertThat(result.reportPath()).exists();
        assertThat(result.reportPath().getParent().getFileName().toString()).matches("[A-Za-z0-9._-]+");
    }

    @Test
    void localRunnerRejectsTpchSuiteBeforeGeneratingDataOrReport() {
        Path dataDir = tempDir.resolve("tpch-data");
        Path reportDir = tempDir.resolve("tpch-reports");
        BenchmarkConfig config = new BenchmarkConfig(
            "tpch-smoke",
            99L,
            new BenchmarkConfig.SuiteConfig("tpch", new BigDecimal("0.01"), "smoke"),
            new BenchmarkConfig.DatasetConfig(2, 1, 50, "2026-01-01T00:00:00", dataDir.toString(), 8L),
            new BenchmarkConfig.QueryConfig(1, 1, 1),
            new BenchmarkConfig.ReportConfig("html", reportDir.toString()),
            new BenchmarkConfig.MonitoringConfig(true, true)
        );

        assertThatThrownBy(() -> new LocalBenchmarkRunner().run(config, reportDir, "run-tpch-local"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("local mode")
            .hasMessageContaining("kpi")
            .hasMessageContaining("compose");

        assertThat(dataDir).doesNotExist();
        assertThat(reportDir).doesNotExist();
    }
}

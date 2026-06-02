package com.example.databenchmark.runner;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
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
        assertThat(html).contains("http://localhost:3000/d/benchmark?var-run_id=run-local-test");
        assertThat(html).contains("local");
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
}

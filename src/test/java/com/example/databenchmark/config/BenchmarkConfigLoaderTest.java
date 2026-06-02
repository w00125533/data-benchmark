package com.example.databenchmark.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BenchmarkConfigLoaderTest {
    @TempDir
    Path tempDir;

    @Test
    void loadsSmokeDefaults() throws Exception {
        BenchmarkConfig config = new BenchmarkConfigLoader().load(Path.of("configs/benchmark-smoke.yml"));

        assertThat(config.profile()).isEqualTo("smoke");
        assertThat(config.seed()).isEqualTo(20260602L);
        assertThat(config.dataset().cells()).isEqualTo(10_000);
        assertThat(config.dataset().days()).isEqualTo(1);
        assertThat(config.dataset().columns()).isEqualTo(50);
        assertThat(config.dataset().startTime()).isEqualTo("2026-01-01T00:00:00");
        assertThat(config.dataset().output()).isEqualTo("data/generated");
        assertThat(config.dataset().rowCap()).isEqualTo(10_000L);
        assertThat(config.query().coldRuns()).isEqualTo(1);
        assertThat(config.query().warmRuns()).isEqualTo(3);
        assertThat(config.query().concurrency()).isEqualTo(1);
        assertThat(config.report().format()).isEqualTo("html");
        assertThat(config.report().output()).isEqualTo("reports/runs");
        assertThat(config.monitoring().prometheus()).isTrue();
        assertThat(config.monitoring().grafana()).isTrue();
    }

    @Test
    void overridesKeepProfileNameAndChangeValues() {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(25, 2, 7L, "target/generated-test-data", 100L);

        assertThat(config.profile()).isEqualTo("smoke");
        assertThat(config.seed()).isEqualTo(7L);
        assertThat(config.dataset().cells()).isEqualTo(25);
        assertThat(config.dataset().days()).isEqualTo(2);
        assertThat(config.dataset().columns()).isEqualTo(50);
        assertThat(config.dataset().startTime()).isEqualTo("2026-01-01T00:00:00");
        assertThat(config.dataset().output()).isEqualTo("target/generated-test-data");
        assertThat(config.dataset().rowCap()).isEqualTo(100L);
        assertThat(config.query().coldRuns()).isEqualTo(1);
        assertThat(config.query().warmRuns()).isEqualTo(3);
        assertThat(config.query().concurrency()).isEqualTo(1);
        assertThat(config.report().format()).isEqualTo("html");
        assertThat(config.report().output()).isEqualTo("reports/runs");
        assertThat(config.monitoring().prometheus()).isTrue();
        assertThat(config.monitoring().grafana()).isTrue();
    }

    @Test
    void nullOverridesPreserveOriginalValues() {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(null, null, null, null, null);

        assertThat(config).isEqualTo(BenchmarkConfig.defaultSmoke());
    }

    @Test
    void withoutRowCapClearsDefaultSmokeCap() {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke().withoutRowCap();

        assertThat(config.dataset().rowCap()).isNull();
    }

    @Test
    void withRowCapSetsPositiveCap() {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke().withoutRowCap().withRowCap(500L);

        assertThat(config.dataset().rowCap()).isEqualTo(500L);
    }

    @Test
    void loadsYamlWithoutRowCapAsNull() throws Exception {
        Path configPath = writeConfig("""
            profile: smoke
            seed: 20260602
            dataset:
              cells: 10000
              days: 1
              columns: 50
              startTime: "2026-01-01T00:00:00"
              output: "data/generated"
            query:
              coldRuns: 1
              warmRuns: 3
              concurrency: 1
            report:
              format: html
              output: "reports/runs"
            monitoring:
              prometheus: true
              grafana: true
            """);

        BenchmarkConfig config = new BenchmarkConfigLoader().load(configPath);

        assertThat(config.dataset().rowCap()).isNull();
    }

    @Test
    void loadingYamlMissingDatasetThrowsClearValidationError() throws Exception {
        Path configPath = writeConfig("""
            profile: smoke
            seed: 20260602
            query:
              coldRuns: 1
              warmRuns: 3
              concurrency: 1
            report:
              format: html
              output: "reports/runs"
            monitoring:
              prometheus: true
              grafana: true
            """);

        assertThatThrownBy(() -> new BenchmarkConfigLoader().load(configPath))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("dataset");
    }

    @Test
    void loadingYamlWithZeroRowCapThrowsClearValidationError() throws Exception {
        Path configPath = writeConfig("""
            profile: smoke
            seed: 20260602
            dataset:
              cells: 10000
              days: 1
              columns: 50
              startTime: "2026-01-01T00:00:00"
              output: "data/generated"
              rowCap: 0
            query:
              coldRuns: 1
              warmRuns: 3
              concurrency: 1
            report:
              format: html
              output: "reports/runs"
            monitoring:
              prometheus: true
              grafana: true
            """);

        assertThatThrownBy(() -> new BenchmarkConfigLoader().load(configPath))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("rowCap");
    }

    private Path writeConfig(String yaml) throws Exception {
        Path configPath = tempDir.resolve("benchmark.yml");
        Files.writeString(configPath, yaml);
        return configPath;
    }
}

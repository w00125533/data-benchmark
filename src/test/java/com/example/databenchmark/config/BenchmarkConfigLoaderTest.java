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
    void defaultSmokeUsesKpiSuite() {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke();

        assertThat(config.suite().name()).isEqualTo("kpi");
        assertThat(config.suite().scaleFactor()).isEqualByComparingTo("0.01");
        assertThat(config.suite().querySet()).isEqualTo("smoke");
    }

    @Test
    void loadsTpchSmokeSuite() throws Exception {
        BenchmarkConfig config = new BenchmarkConfigLoader().load(Path.of("configs/benchmark-tpch-smoke.yml"));

        assertThat(config.suite().name()).isEqualTo("tpch");
        assertThat(config.suite().scaleFactor()).isEqualByComparingTo("0.01");
        assertThat(config.suite().querySet()).isEqualTo("smoke");
    }

    @Test
    void composeSmokeLimitsKpiQueriesForFastValidation() throws Exception {
        BenchmarkConfig config = new BenchmarkConfigLoader().load(Path.of("configs/benchmark-compose-smoke.yml"));

        assertThat(config.suite().name()).isEqualTo("kpi");
        assertThat(config.suite().querySet()).isEqualTo("smoke");
        assertThat(config.dataset().output())
            .isEqualTo("hdfs://hdfs-namenode:8020/services/data-benchmark/generated/kpi/compose-smoke");
        assertThat(config.query().names()).containsExactly("date_partition_pruning");
    }

    @Test
    void largeKpiConfigUsesServicesHdfsNamespace() throws Exception {
        BenchmarkConfig config = new BenchmarkConfigLoader().load(Path.of("configs/benchmark-kpi-1b.yml"));

        assertThat(config.profile()).isEqualTo("kpi-1b");
        assertThat(config.dataset().output())
            .isEqualTo("hdfs://hdfs-namenode:8020/services/data-benchmark/generated/kpi/kpi-1b");
    }

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
        assertThat(config.query().names()).isNull();
        assertThat(config.report().format()).isEqualTo("html");
        assertThat(config.report().output()).isEqualTo("reports/runs");
    }

    @Test
    void loadsOptionalQueryNames() throws Exception {
        Path configPath = writeConfig("""
            profile: smoke-fast
            seed: 20260602
            dataset:
              cells: 10000
              days: 1
              columns: 50
              startTime: "2026-01-01T00:00:00"
              output: "data/generated"
              rowCap: 10000
            query:
              coldRuns: 1
              warmRuns: 3
              concurrency: 1
              names:
                - single_cell_day_trend
                - date_partition_pruning
            report:
              format: html
              output: "reports/runs"
            """);

        BenchmarkConfig config = new BenchmarkConfigLoader().load(configPath);

        assertThat(config.query().names())
            .containsExactly("single_cell_day_trend", "date_partition_pruning");
    }

    @Test
    void loadsDatasetSparkConfigWhenPresent() throws Exception {
        Path configFile = tempDir.resolve("spark-generation.yml");
        Files.writeString(configFile, """
            profile: spark-test
            seed: 20260602
            suite:
              name: kpi
              scaleFactor: 0.01
              querySet: smoke
            dataset:
              cells: 100
              days: 1
              columns: 50
              startTime: "2026-01-01T00:00:00"
              output: "data/generated"
              rowCap: 10000
              spark:
                master: "local[2]"
                partitions: 8
                rowsPerPartition: 1250
                outputMode: "overwrite"
            query:
              coldRuns: 1
              warmRuns: 1
              concurrency: 1
            report:
              format: html
              output: reports/runs
            """);

        BenchmarkConfig config = new BenchmarkConfigLoader().load(configFile);

        assertThat(config.dataset().spark()).isNotNull();
        assertThat(config.dataset().spark().master()).isEqualTo("local[2]");
        assertThat(config.dataset().spark().partitions()).isEqualTo(8);
        assertThat(config.dataset().spark().rowsPerPartition()).isEqualTo(1250L);
        assertThat(config.dataset().spark().outputMode()).isEqualTo("overwrite");
    }

    @Test
    void suiteBlockMissingNameDefaultsToKpi() throws Exception {
        Path configPath = writeConfig("""
            profile: smoke
            seed: 20260602
            suite:
              scaleFactor: 0.01
              querySet: smoke
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
            """);

        BenchmarkConfig config = new BenchmarkConfigLoader().load(configPath);

        assertThat(config.suite().name()).isEqualTo("kpi");
    }

    @Test
    void suiteNameInvalidThrowsClearValidationError() throws Exception {
        Path configPath = writeConfig("""
            profile: smoke
            seed: 20260602
            suite:
              name: invalid
              scaleFactor: 0.01
              querySet: smoke
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
            """);

        assertThatThrownBy(() -> new BenchmarkConfigLoader().load(configPath))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("suite.name");
    }

    @Test
    void suiteScaleFactorZeroThrowsClearValidationError() throws Exception {
        Path configPath = writeConfig("""
            profile: smoke
            seed: 20260602
            suite:
              name: kpi
              scaleFactor: 0
              querySet: smoke
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
            """);

        assertThatThrownBy(() -> new BenchmarkConfigLoader().load(configPath))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("suite.scaleFactor");
    }

    @Test
    void suiteQuerySetInvalidThrowsClearValidationError() throws Exception {
        Path configPath = writeConfig("""
            profile: smoke
            seed: 20260602
            suite:
              name: kpi
              scaleFactor: 0.01
              querySet: invalid
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
            """);

        assertThatThrownBy(() -> new BenchmarkConfigLoader().load(configPath))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("suite.querySet");
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
    }

    @Test
    void nullOverridesPreserveOriginalValues() {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(null, null, null, null, null);

        assertThat(config).isEqualTo(BenchmarkConfig.defaultSmoke());
    }

    @Test
    void overridesRejectZeroRowCap() {
        assertThatThrownBy(() -> BenchmarkConfig.defaultSmoke()
            .withOverrides(null, null, null, null, 0L))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("rowCap");
    }

    @Test
    void overridesRejectNegativeRowCap() {
        assertThatThrownBy(() -> BenchmarkConfig.defaultSmoke()
            .withOverrides(null, null, null, null, -1L))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("rowCap");
    }

    @Test
    void overridesRejectNonPositiveCells() {
        assertThatThrownBy(() -> BenchmarkConfig.defaultSmoke()
            .withOverrides(0, null, null, null, null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cells");
    }

    @Test
    void overridesRejectBlankOutput() {
        assertThatThrownBy(() -> BenchmarkConfig.defaultSmoke()
            .withOverrides(null, null, null, " ", null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("output");
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

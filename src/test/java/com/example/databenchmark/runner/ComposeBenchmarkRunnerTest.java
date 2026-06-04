package com.example.databenchmark.runner;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.engine.EngineRunResult;
import com.example.databenchmark.engine.EngineStage;
import com.example.databenchmark.generator.DatasetResult;
import com.example.databenchmark.query.QueryCatalog;
import com.example.databenchmark.report.BenchmarkReport;
import com.example.databenchmark.tpch.TestTpchFixtures;
import com.example.databenchmark.tpch.TpchDatasetResult;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ComposeBenchmarkRunnerTest {
    @TempDir
    Path tempDir;

    @Test
    void composeRunnerRunsColdWarmHotForFourRoutes() throws Exception {
        List<String> calls = new ArrayList<>();
        String queryName = QueryCatalog.queries().get(0).name();
        DatasetResult dataset = new DatasetResult(tempDir.resolve("data"), List.of(tempDir.resolve("part.parquet")), 5L, 123L);
        CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/compose-test/index.html"));

        ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
            config -> dataset,
            (generatedDataset, outputDir) -> outputDir.resolve("cell_kpi_1min.csv"),
            failingTpchGenerator(),
            failingTpchCsvExport(),
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, true),
            new FakeHiveClient(calls),
            new FakeServiceController(calls),
            reportWriter,
            new CapturingMetricsRecorder(calls, "smoke", "kpi", "smoke")
        );

        ComposeBenchmarkRunner.ComposeRunResult result = runner.run(
            BenchmarkConfig.defaultSmoke(),
            tempDir.resolve("reports"),
            "compose-test"
        );

        assertThat(result.success()).isTrue();
        assertThat(calls).containsSubsequence(
            "restart SPARK_ICEBERG",
            "ready SPARK_ICEBERG",
            "Spark query " + queryName + " COLD",
            "Spark query " + queryName + " WARM",
            "Spark query " + queryName + " HOT",
            "restart STARROCKS_INTERNAL",
            "ready STARROCKS_INTERNAL",
            "StarRocks starrocks_internal " + queryName + " COLD",
            "StarRocks starrocks_internal " + queryName + " WARM",
            "StarRocks starrocks_internal " + queryName + " HOT",
            "restart STARROCKS_EXTERNAL_ICEBERG",
            "ready STARROCKS_EXTERNAL_ICEBERG",
            "StarRocks starrocks_external_iceberg " + queryName + " COLD",
            "StarRocks starrocks_external_iceberg " + queryName + " WARM",
            "StarRocks starrocks_external_iceberg " + queryName + " HOT",
            "restart HIVE_HDFS_PARQUET",
            "ready HIVE_HDFS_PARQUET",
            "Hive query " + queryName + " COLD",
            "Hive query " + queryName + " WARM",
            "Hive query " + queryName + " HOT"
        );
        assertThat(reportWriter.report.querySummaries())
            .filteredOn(summary -> summary.queryName().equals(queryName))
            .extracting(BenchmarkReport.QuerySummary::tableShape, BenchmarkReport.QuerySummary::phase)
            .containsSequence(
                org.assertj.core.groups.Tuple.tuple("spark_iceberg", RoutePhase.COLD.name()),
                org.assertj.core.groups.Tuple.tuple("spark_iceberg", RoutePhase.WARM.name()),
                org.assertj.core.groups.Tuple.tuple("spark_iceberg", RoutePhase.HOT.name()),
                org.assertj.core.groups.Tuple.tuple("starrocks_internal", RoutePhase.COLD.name()),
                org.assertj.core.groups.Tuple.tuple("starrocks_internal", RoutePhase.WARM.name()),
                org.assertj.core.groups.Tuple.tuple("starrocks_internal", RoutePhase.HOT.name()),
                org.assertj.core.groups.Tuple.tuple("starrocks_external_iceberg", RoutePhase.COLD.name()),
                org.assertj.core.groups.Tuple.tuple("starrocks_external_iceberg", RoutePhase.WARM.name()),
                org.assertj.core.groups.Tuple.tuple("starrocks_external_iceberg", RoutePhase.HOT.name()),
                org.assertj.core.groups.Tuple.tuple("hive_hdfs_parquet", RoutePhase.COLD.name()),
                org.assertj.core.groups.Tuple.tuple("hive_hdfs_parquet", RoutePhase.WARM.name()),
                org.assertj.core.groups.Tuple.tuple("hive_hdfs_parquet", RoutePhase.HOT.name())
            );
    }

    @Test
    void composeRunnerRecordsRouteFailuresWhenServiceRestartFailsAndContinues() throws Exception {
        List<String> calls = new ArrayList<>();
        String queryName = QueryCatalog.queries().get(0).name();
        DatasetResult dataset = new DatasetResult(tempDir.resolve("data"), List.of(tempDir.resolve("part.parquet")), 5L, 123L);
        CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/compose-test/index.html"));

        ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
            config -> dataset,
            (generatedDataset, outputDir) -> outputDir.resolve("cell_kpi_1min.csv"),
            failingTpchGenerator(),
            failingTpchCsvExport(),
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, true),
            new FakeHiveClient(calls),
            new FakeServiceController(calls, BenchmarkRoute.STARROCKS_INTERNAL),
            reportWriter,
            new CapturingMetricsRecorder(calls, "smoke", "kpi", "smoke")
        );

        ComposeBenchmarkRunner.ComposeRunResult result = runner.run(
            BenchmarkConfig.defaultSmoke(),
            tempDir.resolve("reports"),
            "compose-test"
        );

        assertThat(result.success()).isFalse();
        assertThat(reportWriter.report.status()).isEqualTo("DEGRADED");
        assertThat(reportWriter.report.querySummaries())
            .filteredOn(summary -> summary.queryName().equals(queryName))
            .filteredOn(summary -> summary.tableShape().equals("starrocks_internal"))
            .hasSize(3)
            .allSatisfy(summary -> {
                assertThat(summary.success()).isFalse();
                assertThat(summary.error()).contains("restart failed for STARROCKS_INTERNAL");
            });
        assertThat(calls).containsSubsequence(
            "restart STARROCKS_INTERNAL",
            "restart STARROCKS_EXTERNAL_ICEBERG",
            "ready STARROCKS_EXTERNAL_ICEBERG",
            "StarRocks starrocks_external_iceberg " + queryName + " COLD"
        );
    }

    @Test
    void composeRunnerOrchestratesEnginesAndWritesReportInOrder() throws Exception {
        List<String> calls = new ArrayList<>();
        DatasetResult dataset = new DatasetResult(tempDir.resolve("data"), List.of(tempDir.resolve("part.parquet")), 5L, 123L);
        CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/compose-test/index.html"));
        CapturingMetricsRecorder metricsRecorder = new CapturingMetricsRecorder(calls, "smoke", "kpi", "smoke");

        ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
            config -> {
                calls.add("generate dataset");
                return dataset;
            },
            (generatedDataset, outputDir) -> {
                calls.add("export CSV");
                assertThat(generatedDataset).isSameAs(dataset);
                return outputDir.resolve("cell_kpi_1min.csv");
            },
            failingTpchGenerator(),
            failingTpchCsvExport(),
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, true),
            new FakeHiveClient(calls),
            new FakeServiceController(calls),
            reportWriter,
            metricsRecorder
        );

        ComposeBenchmarkRunner.ComposeRunResult result = runner.run(
            BenchmarkConfig.defaultSmoke(),
            tempDir.resolve("reports"),
            "compose-test"
        );

        assertThat(result.success()).isTrue();
        assertThat(result.dataset()).isSameAs(dataset);
        assertThat(result.rows()).isEqualTo(dataset.rows());
        assertThat(result.bytesWritten()).isEqualTo(dataset.bytesWritten());
        assertThat(result.csvPath().getFileName().toString()).isEqualTo("cell_kpi_1min.csv");
        assertThat(result.reportPath()).isEqualTo(tempDir.resolve("reports/compose-test/index.html"));
        assertThat(calls).containsSubsequence(
            "start metrics",
            "generate dataset",
            "export CSV",
            "Spark load",
            "StarRocks internal load",
            "StarRocks external refresh",
            "Hive external table /data/generated",
            "restart SPARK_ICEBERG",
            "ready SPARK_ICEBERG",
            "record load:GENERATE",
            "record load:SPARK_ICEBERG_LOAD",
            "record load:STARROCKS_INTERNAL_LOAD",
            "record load:STARROCKS_EXTERNAL_REFRESH",
            "write web report"
        );
        assertThat(metricsRecorder.closed).isTrue();
        assertThat(reportWriter.report.runId()).isEqualTo("compose-test");
        assertThat(reportWriter.report.suite()).isEqualTo("kpi");
        assertThat(reportWriter.report.querySet()).isEqualTo("smoke");
        assertThat(reportWriter.report.loadSummaries())
            .extracting(BenchmarkReport.LoadSummary::stage)
            .containsExactly(
                EngineStage.GENERATE.name(),
                EngineStage.SPARK_ICEBERG_LOAD.name(),
                EngineStage.STARROCKS_INTERNAL_LOAD.name(),
                EngineStage.STARROCKS_EXTERNAL_REFRESH.name(),
                "HIVE_HDFS_PARQUET_LOAD"
            );
        assertThat(reportWriter.report.querySummaries())
            .hasSize(QueryCatalog.queries().size() * BenchmarkRoute.values().length * RoutePhase.values().length);
    }

    @Test
    void composeRunnerWritesReportWhenExternalRefreshFails() throws Exception {
        List<String> calls = new ArrayList<>();
        DatasetResult dataset = new DatasetResult(tempDir.resolve("data"), List.of(tempDir.resolve("part.parquet")), 5L, 123L);
        CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/compose-test/index.html"));
        CapturingMetricsRecorder metricsRecorder = new CapturingMetricsRecorder(calls, "smoke", "kpi", "smoke");

        ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
            config -> {
                calls.add("generate dataset");
                return dataset;
            },
            (generatedDataset, outputDir) -> {
                calls.add("export CSV");
                return outputDir.resolve("cell_kpi_1min.csv");
            },
            failingTpchGenerator(),
            failingTpchCsvExport(),
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, false),
            new FakeHiveClient(calls),
            new FakeServiceController(calls),
            reportWriter,
            metricsRecorder
        );

        ComposeBenchmarkRunner.ComposeRunResult result = runner.run(
            BenchmarkConfig.defaultSmoke(),
            tempDir.resolve("reports"),
            "compose-test"
        );

        assertThat(result.success()).isFalse();
        assertThat(result.rows()).isEqualTo(dataset.rows());
        assertThat(result.bytesWritten()).isEqualTo(dataset.bytesWritten());
        assertThat(result.reportPath()).isEqualTo(tempDir.resolve("reports/compose-test/index.html"));
        assertThat(calls).containsSubsequence(
            "start metrics",
            "generate dataset",
            "export CSV",
            "Spark load",
            "StarRocks internal load",
            "StarRocks external refresh",
            "Hive external table /data/generated",
            "restart SPARK_ICEBERG",
            "ready SPARK_ICEBERG",
            "record load:GENERATE",
            "record load:SPARK_ICEBERG_LOAD",
            "record load:STARROCKS_INTERNAL_LOAD",
            "record load:STARROCKS_EXTERNAL_REFRESH",
            "write web report"
        );
        assertThat(metricsRecorder.closed).isTrue();
        assertThat(reportWriter.report.status()).isEqualTo("DEGRADED");
        assertThat(reportWriter.report.loadSummaries())
            .anySatisfy(summary -> assertThat(summary.error()).contains("catalog refresh failed"));
    }

    @Test
    void composeRunnerRunsTpchSuiteWithoutTouchingKpiFlow() throws Exception {
        List<String> calls = new ArrayList<>();
        TpchDatasetResult tpchDataset = TestTpchFixtures.dataset(tempDir.resolve("data/tpch/tpch-unit"));
        Map<String, Path> csvFiles = new LinkedHashMap<>(TestTpchFixtures.csvFiles(tpchDataset));
        CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/tpch-compose/index.html"));
        CapturingMetricsRecorder metricsRecorder = new CapturingMetricsRecorder(calls, "tpch-smoke", "tpch", "smoke");
        BenchmarkConfig config = new BenchmarkConfig(
            "tpch-smoke",
            20260602L,
            new BenchmarkConfig.SuiteConfig("tpch", new BigDecimal("0.01"), "smoke"),
            BenchmarkConfig.defaultSmoke().dataset(),
            BenchmarkConfig.defaultSmoke().query(),
            BenchmarkConfig.defaultSmoke().report()
        );

        ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
            failingDatasetGenerator(),
            failingCsvExporter(),
            (configArg, runId) -> {
                calls.add("generate TPC-H dataset");
                assertThat(configArg).isSameAs(config);
                assertThat(runId).isEqualTo("compose-test");
                return tpchDataset;
            },
            datasetArg -> {
                calls.add("export TPC-H CSV");
                assertThat(datasetArg).isSameAs(tpchDataset);
                return csvFiles;
            },
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, true),
            reportWriter,
            metricsRecorder
        );

        ComposeBenchmarkRunner.ComposeRunResult result = runner.run(
            config,
            tempDir.resolve("reports"),
            "compose-test"
        );

        assertThat(result.success()).isTrue();
        assertThat(result.dataset()).isNull();
        assertThat(result.rows()).isEqualTo(tpchDataset.rows());
        assertThat(result.bytesWritten()).isEqualTo(tpchDataset.bytesWritten());
        assertThat(result.csvPath()).isEqualTo(csvFiles.values().iterator().next().getParent());
        assertThat(result.reportPath()).isEqualTo(tempDir.resolve("reports/tpch-compose/index.html"));
        assertThat(calls).containsExactly(
            "start metrics",
            "generate TPC-H dataset",
            "export TPC-H CSV",
            "Spark TPC-H load",
            "StarRocks TPC-H internal load",
            "StarRocks TPC-H external refresh",
            "Spark TPC-H queries",
            "StarRocks TPC-H queries",
            "record load:GENERATE",
            "record load:SPARK_ICEBERG_LOAD",
            "record load:STARROCKS_INTERNAL_LOAD",
            "record load:STARROCKS_EXTERNAL_REFRESH",
            "record query:q01_pricing_summary_report",
            "record query:q01_pricing_summary_report",
            "record query:q01_pricing_summary_report",
            "write web report"
        );
        assertThat(metricsRecorder.closed).isTrue();
        assertThat(reportWriter.report.runId()).isEqualTo("compose-test");
        assertThat(reportWriter.report.suite()).isEqualTo("tpch");
        assertThat(reportWriter.report.querySet()).isEqualTo("smoke");
        assertThat(reportWriter.report.loadSummaries())
            .extracting(BenchmarkReport.LoadSummary::tableShape)
            .containsExactly(
                "tpch_generated_parquet",
                "tpch_iceberg",
                "tpch_internal",
                "tpch_external_iceberg"
            );
        assertThat(reportWriter.report.querySummaries())
            .extracting(BenchmarkReport.QuerySummary::queryName)
            .containsOnly("q01_pricing_summary_report");
        assertThat(reportWriter.report.querySummaries())
            .extracting(BenchmarkReport.QuerySummary::phase)
            .containsExactly(RoutePhase.COLD.name(), RoutePhase.WARM.name(), RoutePhase.HOT.name());
    }

    @Test
    void composeRunnerWritesTpchReportAndClosesMetricsWhenGenerationFails() throws Exception {
        List<String> calls = new ArrayList<>();
        CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/tpch-compose/index.html"));
        CapturingMetricsRecorder metricsRecorder = new CapturingMetricsRecorder(calls, "tpch-smoke", "tpch", "smoke");
        BenchmarkConfig config = new BenchmarkConfig(
            "tpch-smoke",
            20260602L,
            new BenchmarkConfig.SuiteConfig("tpch", new BigDecimal("0.01"), "smoke"),
            BenchmarkConfig.defaultSmoke().dataset(),
            BenchmarkConfig.defaultSmoke().query(),
            BenchmarkConfig.defaultSmoke().report()
        );

        ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
            failingDatasetGenerator(),
            failingCsvExporter(),
            (configArg, runId) -> {
                calls.add("generate TPC-H dataset");
                throw new IllegalStateException("tpch generation failed");
            },
            failingTpchCsvExport(),
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, true),
            reportWriter,
            metricsRecorder
        );

        ComposeBenchmarkRunner.ComposeRunResult result = runner.run(
            config,
            tempDir.resolve("reports"),
            "compose-test"
        );

        assertThat(result.success()).isFalse();
        assertThat(result.dataset()).isNull();
        assertThat(result.rows()).isZero();
        assertThat(result.bytesWritten()).isZero();
        assertThat(result.csvPath()).isNull();
        assertThat(result.reportPath()).isEqualTo(tempDir.resolve("reports/tpch-compose/index.html"));
        assertThat(calls).containsExactly(
            "start metrics",
            "generate TPC-H dataset",
            "record load:GENERATE",
            "write web report"
        );
        assertThat(metricsRecorder.closed).isTrue();
        assertThat(reportWriter.report.status()).isEqualTo("DEGRADED");
        assertThat(reportWriter.report.loadSummaries())
            .singleElement()
            .satisfies(summary -> {
                assertThat(summary.tableShape()).isEqualTo("tpch_generated_parquet");
                assertThat(summary.success()).isFalse();
                assertThat(summary.error()).contains("tpch generation failed");
            });
    }

    @Test
    void composeRunnerWritesTpchReportAndClosesMetricsWhenCsvExportFails() throws Exception {
        List<String> calls = new ArrayList<>();
        TpchDatasetResult tpchDataset = TestTpchFixtures.dataset(tempDir.resolve("data/tpch/tpch-unit"));
        CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/tpch-compose/index.html"));
        CapturingMetricsRecorder metricsRecorder = new CapturingMetricsRecorder(calls, "tpch-smoke", "tpch", "smoke");
        BenchmarkConfig config = new BenchmarkConfig(
            "tpch-smoke",
            20260602L,
            new BenchmarkConfig.SuiteConfig("tpch", new BigDecimal("0.01"), "smoke"),
            BenchmarkConfig.defaultSmoke().dataset(),
            BenchmarkConfig.defaultSmoke().query(),
            BenchmarkConfig.defaultSmoke().report()
        );

        ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
            failingDatasetGenerator(),
            failingCsvExporter(),
            (configArg, runId) -> {
                calls.add("generate TPC-H dataset");
                return tpchDataset;
            },
            datasetArg -> {
                calls.add("export TPC-H CSV");
                throw new IllegalStateException("tpch csv export failed");
            },
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, true),
            reportWriter,
            metricsRecorder
        );

        ComposeBenchmarkRunner.ComposeRunResult result = runner.run(
            config,
            tempDir.resolve("reports"),
            "compose-test"
        );

        assertThat(result.success()).isFalse();
        assertThat(result.dataset()).isNull();
        assertThat(result.rows()).isEqualTo(tpchDataset.rows());
        assertThat(result.bytesWritten()).isEqualTo(tpchDataset.bytesWritten());
        assertThat(result.csvPath()).isNull();
        assertThat(result.reportPath()).isEqualTo(tempDir.resolve("reports/tpch-compose/index.html"));
        assertThat(calls).containsExactly(
            "start metrics",
            "generate TPC-H dataset",
            "export TPC-H CSV",
            "record load:GENERATE",
            "record load:compose_prepare",
            "write web report"
        );
        assertThat(metricsRecorder.closed).isTrue();
        assertThat(reportWriter.report.status()).isEqualTo("DEGRADED");
        assertThat(reportWriter.report.loadSummaries())
            .extracting(BenchmarkReport.LoadSummary::tableShape)
            .containsExactly("tpch_generated_parquet", "tpch_prepare");
        assertThat(reportWriter.report.loadSummaries())
            .anySatisfy(summary -> {
                assertThat(summary.tableShape()).isEqualTo("tpch_prepare");
                assertThat(summary.success()).isFalse();
                assertThat(summary.error()).contains("tpch csv export failed");
            });
    }

    private static ComposeBenchmarkRunner.DatasetGenerator failingDatasetGenerator() {
        return config -> {
            throw new AssertionError("KPI dataset generator should not be called for TPC-H");
        };
    }

    private static ComposeBenchmarkRunner.CsvExporter failingCsvExporter() {
        return (dataset, outputDir) -> {
            throw new AssertionError("KPI CSV exporter should not be called for TPC-H");
        };
    }

    private static ComposeBenchmarkRunner.TpchGenerator failingTpchGenerator() {
        return (config, runId) -> {
            throw new AssertionError("TPC-H generator should not be called");
        };
    }

    private static ComposeBenchmarkRunner.TpchCsvExport failingTpchCsvExport() {
        return dataset -> {
            throw new AssertionError("TPC-H CSV exporter should not be called");
        };
    }

    private static final class FakeSparkClient implements ComposeBenchmarkRunner.SparkClient {
        private final List<String> calls;

        private FakeSparkClient(List<String> calls) {
            this.calls = calls;
        }

        @Override
        public EngineRunResult load(DatasetResult dataset, String runId, String profile) {
            calls.add("Spark load");
            return new EngineRunResult("spark", "spark_iceberg", EngineStage.SPARK_ICEBERG_LOAD.name(),
                null, dataset.rows(), dataset.bytesWritten(), 1.0, true, "");
        }

        @Override
        public List<EngineRunResult> runQueries(String runId, String profile) {
            calls.add("Spark queries");
            return List.of(new EngineRunResult("spark", "spark_iceberg", EngineStage.QUERY.name(),
                "spark_query", 3L, 0L, 0.2, true, ""));
        }

        @Override
        public EngineRunResult runQuery(String queryName, RoutePhase phase) {
            calls.add("Spark query " + queryName + " " + phase.name());
            return new EngineRunResult("spark", "spark_iceberg", EngineStage.QUERY.name(),
                queryName, phase.name(), 3L, 0L, 0.2, true, "");
        }

        @Override
        public EngineRunResult loadTpch(TpchDatasetResult dataset, String runId, String profile) {
            calls.add("Spark TPC-H load");
            return new EngineRunResult("spark", "tpch_iceberg", EngineStage.SPARK_ICEBERG_LOAD.name(),
                null, dataset.rows(), dataset.bytesWritten(), 1.0, true, "");
        }

        @Override
        public List<EngineRunResult> runTpchQueries(String runId, String profile, String querySet) {
            calls.add("Spark TPC-H queries");
            return List.of(new EngineRunResult("spark", "tpch_iceberg", EngineStage.QUERY.name(),
                "q01_pricing_summary_report", RoutePhase.COLD.name(), 3L, 0L, 0.2, true, ""));
        }
    }

    private static final class FakeStarRocksClient implements ComposeBenchmarkRunner.StarRocksClientFacade {
        private final List<String> calls;
        private final boolean externalRefreshSucceeds;

        private FakeStarRocksClient(List<String> calls, boolean externalRefreshSucceeds) {
            this.calls = calls;
            this.externalRefreshSucceeds = externalRefreshSucceeds;
        }

        @Override
        public EngineRunResult loadInternal(Path csv, String runId, String profile) {
            calls.add("StarRocks internal load");
            return new EngineRunResult("starrocks", "starrocks_internal",
                EngineStage.STARROCKS_INTERNAL_LOAD.name(), null, 5L, 0L, 1.5, true, "");
        }

        @Override
        public EngineRunResult refreshExternalCatalog(String runId, String profile) {
            calls.add("StarRocks external refresh");
            return new EngineRunResult("starrocks", "starrocks_external_iceberg",
                EngineStage.STARROCKS_EXTERNAL_REFRESH.name(), null, 0L, 0L, 0.4,
                externalRefreshSucceeds, externalRefreshSucceeds ? "" : "catalog refresh failed");
        }

        @Override
        public List<EngineRunResult> runQueries(String runId, String profile) {
            calls.add("StarRocks queries");
            return List.of(new EngineRunResult("starrocks", "starrocks_internal", EngineStage.QUERY.name(),
                "starrocks_query", 4L, 0L, 0.3, true, ""));
        }

        @Override
        public EngineRunResult runQueryFor(String tableShape, String queryName, RoutePhase phase) {
            calls.add("StarRocks " + tableShape + " " + queryName + " " + phase.name());
            return new EngineRunResult("starrocks", tableShape, EngineStage.QUERY.name(),
                queryName, phase.name(), 4L, 0L, 0.3, true, "");
        }

        @Override
        public EngineRunResult loadTpchInternal(Map<String, Path> csvFiles, TpchDatasetResult dataset, String runId, String profile) {
            calls.add("StarRocks TPC-H internal load");
            return new EngineRunResult("starrocks", "tpch_internal",
                EngineStage.STARROCKS_INTERNAL_LOAD.name(), null, dataset.rows(), dataset.bytesWritten(), 1.5, true, "");
        }

        @Override
        public EngineRunResult refreshTpchExternalCatalog(String runId, String profile) {
            calls.add("StarRocks TPC-H external refresh");
            return new EngineRunResult("starrocks", "tpch_external_iceberg",
                EngineStage.STARROCKS_EXTERNAL_REFRESH.name(), null, 0L, 0L, 0.4,
                externalRefreshSucceeds, externalRefreshSucceeds ? "" : "catalog refresh failed");
        }

        @Override
        public List<EngineRunResult> runTpchQueries(String runId, String profile, String querySet) {
            calls.add("StarRocks TPC-H queries");
            return List.of(
                new EngineRunResult("starrocks", "tpch_internal", EngineStage.QUERY.name(),
                    "q01_pricing_summary_report", RoutePhase.WARM.name(), 4L, 0L, 0.3, true, ""),
                new EngineRunResult("starrocks", "tpch_external_iceberg", EngineStage.QUERY.name(),
                    "q01_pricing_summary_report", RoutePhase.HOT.name(), 4L, 0L, 0.3, true, "")
            );
        }
    }

    private static final class FakeHiveClient implements ComposeBenchmarkRunner.HiveClientFacade {
        private final List<String> calls;

        private FakeHiveClient(List<String> calls) {
            this.calls = calls;
        }

        @Override
        public EngineRunResult createExternalTable(Path parquetRoot) {
            calls.add("Hive external table " + parquetRoot.toString().replace('\\', '/'));
            return new EngineRunResult("hive", "hive_hdfs_parquet", "HIVE_HDFS_PARQUET_LOAD",
                null, 0L, 0L, 0.5, true, "");
        }

        @Override
        public EngineRunResult runQuery(String queryName, RoutePhase phase) {
            calls.add("Hive query " + queryName + " " + phase.name());
            return new EngineRunResult("hive", "hive_hdfs_parquet", EngineStage.QUERY.name(),
                queryName, phase.name(), 2L, 0L, 0.4, true, "");
        }
    }

    private static final class FakeServiceController implements ComposeBenchmarkRunner.ServiceController {
        private final List<String> calls;
        private final BenchmarkRoute failingRoute;

        private FakeServiceController(List<String> calls) {
            this(calls, null);
        }

        private FakeServiceController(List<String> calls, BenchmarkRoute failingRoute) {
            this.calls = calls;
            this.failingRoute = failingRoute;
        }

        @Override
        public void restart(BenchmarkRoute route) {
            calls.add("restart " + route.name());
            if (route == failingRoute) {
                throw new IllegalStateException("restart failed for " + route.name());
            }
        }

        @Override
        public void waitUntilReady(BenchmarkRoute route) {
            calls.add("ready " + route.name());
        }
    }

    private static final class CapturingReportWriter implements ComposeBenchmarkRunner.ReportWriter {
        private final List<String> calls;
        private final Path reportPath;
        private BenchmarkReport report;

        private CapturingReportWriter(List<String> calls, Path reportPath) {
            this.calls = calls;
            this.reportPath = reportPath;
        }

        @Override
        public Path write(BenchmarkReport report, Path outputRoot) {
            calls.add("write web report");
            this.report = report;
            return reportPath;
        }
    }

    private static final class CapturingMetricsRecorder implements ComposeBenchmarkRunner.MetricsRecorder {
        private final List<String> calls;
        private final String expectedProfile;
        private final String expectedSuite;
        private final String expectedQuerySet;
        private boolean closed;

        private CapturingMetricsRecorder(List<String> calls, String expectedProfile, String expectedSuite, String expectedQuerySet) {
            this.calls = calls;
            this.expectedProfile = expectedProfile;
            this.expectedSuite = expectedSuite;
            this.expectedQuerySet = expectedQuerySet;
        }

        @Override
        public void start() {
            calls.add("start metrics");
        }

        @Override
        public void recordLoad(String runId, String profile, String suite, String querySet, EngineRunResult result) {
            calls.add("record load:" + result.stage());
            assertThat(runId).isEqualTo("compose-test");
            assertThat(profile).isEqualTo(expectedProfile);
            assertThat(suite).isEqualTo(expectedSuite);
            assertThat(querySet).isEqualTo(expectedQuerySet);
        }

        @Override
        public void recordQuery(String runId, String profile, String suite, String querySet, EngineRunResult result) {
            calls.add("record query:" + result.queryName());
            assertThat(runId).isEqualTo("compose-test");
            assertThat(profile).isEqualTo(expectedProfile);
            assertThat(suite).isEqualTo(expectedSuite);
            assertThat(querySet).isEqualTo(expectedQuerySet);
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}

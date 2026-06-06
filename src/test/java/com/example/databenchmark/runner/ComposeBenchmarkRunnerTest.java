package com.example.databenchmark.runner;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
import com.example.databenchmark.engine.EngineRunResult;
import com.example.databenchmark.engine.EngineStage;
import com.example.databenchmark.generator.DatasetResult;
import com.example.databenchmark.query.QueryCatalog;
import com.example.databenchmark.report.BenchmarkReport;
import com.example.databenchmark.tpch.TestTpchFixtures;
import com.example.databenchmark.tpch.TpchDatasetResult;
import java.math.BigDecimal;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.time.Duration;
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
    void composeRunnerRunsColdWarmHotForFiveRoutes() throws Exception {
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
            new FakeHdfsDatasetPublisher(calls, true),
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
            "restart SPARK_NATIVE_PARQUET",
            "ready SPARK_NATIVE_PARQUET",
            "Spark native query " + queryName + " COLD",
            "Spark native query " + queryName + " WARM",
            "Spark native query " + queryName + " HOT",
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
        assertThat(calls).containsSubsequence(
            "Hive HDFS publish /data/generated",
            "Hive external table /data/generated"
        );
        assertThat(reportWriter.report.querySummaries())
            .filteredOn(summary -> summary.queryName().equals(queryName))
            .extracting(BenchmarkReport.QuerySummary::tableShape, BenchmarkReport.QuerySummary::phase)
            .containsSequence(
                org.assertj.core.groups.Tuple.tuple("spark_native_parquet", RoutePhase.COLD.name()),
                org.assertj.core.groups.Tuple.tuple("spark_native_parquet", RoutePhase.WARM.name()),
                org.assertj.core.groups.Tuple.tuple("spark_native_parquet", RoutePhase.HOT.name()),
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
    void composeRunnerCanLimitKpiQueriesFromConfig() throws Exception {
        List<String> calls = new ArrayList<>();
        String selectedQuery = QueryCatalog.queries().get(0).name();
        String skippedQuery = QueryCatalog.queries().get(1).name();
        DatasetResult dataset = new DatasetResult(tempDir.resolve("data"), List.of(tempDir.resolve("part.parquet")), 5L, 123L);
        CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/compose-test/index.html"));
        BenchmarkConfig base = BenchmarkConfig.defaultSmoke();
        BenchmarkConfig config = new BenchmarkConfig(
            base.profile(),
            base.seed(),
            base.suite(),
            base.dataset(),
            new BenchmarkConfig.QueryConfig(1, 3, 1, List.of(selectedQuery)),
            base.report()
        );

        ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
            generatedConfig -> dataset,
            (generatedDataset, outputDir) -> outputDir.resolve("cell_kpi_1min.csv"),
            failingTpchGenerator(),
            failingTpchCsvExport(),
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, true),
            new FakeHdfsDatasetPublisher(calls, true),
            new FakeHiveClient(calls),
            new FakeServiceController(calls),
            reportWriter,
            new CapturingMetricsRecorder(calls, "smoke", "kpi", "smoke")
        );

        runner.run(config, tempDir.resolve("reports"), "compose-test");

        assertThat(reportWriter.report.querySummaries())
            .extracting(BenchmarkReport.QuerySummary::queryName)
            .containsOnly(selectedQuery);
        assertThat(calls)
            .anySatisfy(call -> assertThat(call).contains(selectedQuery))
            .noneSatisfy(call -> assertThat(call).contains(skippedQuery));
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
            new FakeHdfsDatasetPublisher(calls, true),
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
            failingTpchGenerator(),
            failingTpchCsvExport(),
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, true),
            new FakeHdfsDatasetPublisher(calls, true),
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
        assertThat(result.csvPath()).isNull();
        assertThat(result.reportPath()).isEqualTo(tempDir.resolve("reports/compose-test/index.html"));
        assertThat(calls).containsSubsequence(
            "start metrics",
            "generate dataset",
            "Spark native load",
            "Spark load",
            "Hive HDFS publish /data/generated",
            "ready STARROCKS_INTERNAL",
            "StarRocks internal load from parquet /data/generated rows=5 bytes=123",
            "ready STARROCKS_EXTERNAL_ICEBERG",
            "StarRocks external refresh",
            "ready HIVE_HDFS_PARQUET",
            "Hive external table /data/generated",
            "Spark validate spark_native_parquet rows=5",
            "Spark validate spark_iceberg rows=5",
            "StarRocks validate starrocks_internal rows=5",
            "StarRocks validate starrocks_external_iceberg rows=5",
            "Hive validate rows=5",
            "restart SPARK_NATIVE_PARQUET",
            "ready SPARK_NATIVE_PARQUET",
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
                EngineStage.SPARK_NATIVE_PARQUET_LOAD.name(),
                EngineStage.SPARK_ICEBERG_LOAD.name(),
                "HIVE_HDFS_PARQUET_PUBLISH",
                EngineStage.STARROCKS_INTERNAL_LOAD.name(),
                EngineStage.STARROCKS_EXTERNAL_REFRESH.name(),
                "HIVE_HDFS_PARQUET_LOAD",
                EngineStage.SPARK_NATIVE_PARQUET_VALIDATE.name(),
                EngineStage.SPARK_ICEBERG_VALIDATE.name(),
                EngineStage.STARROCKS_INTERNAL_VALIDATE.name(),
                EngineStage.STARROCKS_EXTERNAL_VALIDATE.name(),
                EngineStage.HIVE_HDFS_PARQUET_VALIDATE.name()
            );
        assertThat(reportWriter.report.querySummaries())
            .hasSize(QueryCatalog.queries().size() * BenchmarkRoute.values().length * RoutePhase.values().length);
        assertThat(calls).doesNotContain("export CSV");
    }

    @Test
    void composeRunnerSkipsOnlyRouteWhoseValidationFails() throws Exception {
        List<String> calls = new ArrayList<>();
        String queryName = QueryCatalog.queries().get(0).name();
        DatasetResult dataset = new DatasetResult(tempDir.resolve("data"), List.of(tempDir.resolve("part.parquet")), 5L, 123L);
        CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/compose-test/index.html"));

        ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
            config -> dataset,
            failingCsvExporter(),
            failingTpchGenerator(),
            failingTpchCsvExport(),
            new FakeSparkClient(calls, "spark_native_parquet"),
            new FakeStarRocksClient(calls, true),
            new FakeHdfsDatasetPublisher(calls, true),
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

        assertThat(result.success()).isFalse();
        assertThat(calls).doesNotContain(
            "restart SPARK_NATIVE_PARQUET",
            "Spark native query " + queryName + " COLD",
            "Spark native query " + queryName + " WARM",
            "Spark native query " + queryName + " HOT"
        );
        assertThat(calls).contains(
            "restart SPARK_ICEBERG",
            "Spark query " + queryName + " COLD",
            "StarRocks starrocks_internal " + queryName + " COLD",
            "Hive query " + queryName + " COLD"
        );
        assertThat(reportWriter.report.loadSummaries())
            .filteredOn(summary -> summary.stage().equals(EngineStage.SPARK_NATIVE_PARQUET_VALIDATE.name()))
            .singleElement()
            .satisfies(summary -> {
                assertThat(summary.success()).isFalse();
                assertThat(summary.error()).contains("row count mismatch");
            });
        assertThat(reportWriter.report.querySummaries())
            .filteredOn(summary -> summary.tableShape().equals("spark_native_parquet"))
            .filteredOn(summary -> summary.queryName().equals(queryName))
            .hasSize(3)
            .allSatisfy(summary -> {
                assertThat(summary.success()).isFalse();
                assertThat(summary.error()).contains("row count mismatch");
            });
    }

    @Test
    void composeRunnerPublishesRelativeLocalOutputBeforeStarRocksInternalLoad() throws Exception {
        List<String> calls = new ArrayList<>();
        DatasetResult dataset = new DatasetResult(tempDir.resolve("data/generated"), List.of(tempDir.resolve("part.parquet")), 5L, 123L);
        CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/compose-test/index.html"));

        ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
            config -> dataset,
            failingCsvExporter(),
            failingTpchGenerator(),
            failingTpchCsvExport(),
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, true),
            new FakeHdfsDatasetPublisher(calls, true),
            new FakeHiveClient(calls),
            new FakeServiceController(calls),
            reportWriter,
            new CapturingMetricsRecorder(calls, "smoke", "kpi", "smoke")
        );

        runner.run(BenchmarkConfig.defaultSmoke(), tempDir.resolve("reports"), "compose-test");

        assertThat(calls)
            .containsSubsequence(
                "Hive HDFS publish /data/generated",
                "StarRocks internal load from parquet /data/generated rows=5 bytes=123"
            )
            .contains("StarRocks internal load from parquet /data/generated rows=5 bytes=123")
            .noneMatch(call -> call.contains("StarRocks internal load from parquet ")
                && call.contains(dataset.outputPath().toString().replace('\\', '/')))
            .noneMatch(call -> call.contains("StarRocks internal load from parquet ")
                && call.contains("csv/cell_kpi_1min.csv"));
    }

    @Test
    void composeRunnerPublishesAbsoluteLocalOutputBeforeStarRocksInternalLoad() throws Exception {
        List<String> calls = new ArrayList<>();
        DatasetResult dataset = new DatasetResult(
            tempDir.resolve("benchmark/kpi-smoke/generated"),
            List.of(tempDir.resolve("part.parquet")),
            5L,
            123L
        );
        CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/compose-test/index.html"));

        ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
            config -> dataset,
            failingCsvExporter(),
            failingTpchGenerator(),
            failingTpchCsvExport(),
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, true),
            new FakeHdfsDatasetPublisher(calls, true),
            new FakeHiveClient(calls),
            new FakeServiceController(calls),
            reportWriter,
            new CapturingMetricsRecorder(calls, "smoke", "kpi", "smoke")
        );

        runner.run(
            BenchmarkConfig.defaultSmoke().withOverrides(null, null, null, "/benchmark/kpi-smoke/generated", null),
            tempDir.resolve("reports"),
            "compose-test"
        );

        assertThat(calls).containsSubsequence(
            "Hive HDFS publish /benchmark/kpi-smoke/generated",
            "StarRocks internal load from parquet /benchmark/kpi-smoke/generated rows=5 bytes=123"
        );
    }

    @Test
    void composeRunnerRecordsLoadFailureWhenPreLoadReadinessFailsAndContinues() throws Exception {
        List<String> calls = new ArrayList<>();
        DatasetResult dataset = new DatasetResult(tempDir.resolve("data"), List.of(tempDir.resolve("part.parquet")), 5L, 123L);
        CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/compose-test/index.html"));

        ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
            config -> dataset,
            (generatedDataset, outputDir) -> outputDir.resolve("cell_kpi_1min.csv"),
            failingTpchGenerator(),
            failingTpchCsvExport(),
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, true),
            new FakeHdfsDatasetPublisher(calls, true),
            new FakeHiveClient(calls),
            new FakeServiceController(calls, null, BenchmarkRoute.STARROCKS_INTERNAL),
            reportWriter,
            new CapturingMetricsRecorder(calls, "smoke", "kpi", "smoke")
        );

        ComposeBenchmarkRunner.ComposeRunResult result = runner.run(
            BenchmarkConfig.defaultSmoke(),
            tempDir.resolve("reports"),
            "compose-test"
        );

        assertThat(result.success()).isFalse();
        assertThat(calls).containsSubsequence(
            "Spark load",
            "Hive HDFS publish /data/generated",
            "ready STARROCKS_INTERNAL",
            "ready STARROCKS_EXTERNAL_ICEBERG",
            "StarRocks external refresh",
            "ready HIVE_HDFS_PARQUET",
            "Hive external table /data/generated"
        );
        assertThat(calls).doesNotContain("StarRocks internal load");
        assertThat(reportWriter.report.status()).isEqualTo("DEGRADED");
        assertThat(reportWriter.report.loadSummaries())
            .filteredOn(summary -> summary.stage().equals(EngineStage.STARROCKS_INTERNAL_LOAD.name()))
            .singleElement()
            .satisfies(summary -> {
                assertThat(summary.success()).isFalse();
                assertThat(summary.error()).contains("readiness failed for STARROCKS_INTERNAL");
            });
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
            failingCsvExporter(),
            failingTpchGenerator(),
            failingTpchCsvExport(),
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, false),
            new FakeHdfsDatasetPublisher(calls, true),
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
            "Spark load",
            "Hive HDFS publish /data/generated",
            "StarRocks internal load from parquet /data/generated rows=5 bytes=123",
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
        assertThat(calls).doesNotContain("export CSV");
    }

    @Test
    void composeRunnerUsesHdfsDatasetOutputDirectlyForHiveExternalTable() throws Exception {
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
            new FakeHdfsDatasetPublisher(calls, true),
            new FakeHiveClient(calls),
            new FakeServiceController(calls),
            reportWriter,
            new CapturingMetricsRecorder(calls, "smoke", "kpi", "smoke")
        );

        ComposeBenchmarkRunner.ComposeRunResult result = runner.run(
            BenchmarkConfig.defaultSmoke().withOverrides(null, null, null, "hdfs://hdfs-namenode:8020/benchmark/kpi-smoke/generated", null),
            tempDir.resolve("reports"),
            "compose-test"
        );

        assertThat(result.success()).isTrue();
        assertThat(calls).noneMatch(call -> call.startsWith("Hive HDFS publish "));
        assertThat(calls).contains(
            "StarRocks internal load from parquet /benchmark/kpi-smoke/generated rows=5 bytes=123",
            "Hive external table /benchmark/kpi-smoke/generated",
            "restart STARROCKS_INTERNAL",
            "ready STARROCKS_INTERNAL",
            "StarRocks starrocks_internal " + queryName + " COLD",
            "StarRocks starrocks_internal " + queryName + " WARM",
            "StarRocks starrocks_internal " + queryName + " HOT"
        );
    }

    @Test
    void composeRunnerRecordsHiveHdfsPublishFailureAndSkipsHiveRouteQueries() throws Exception {
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
            new FakeHdfsDatasetPublisher(calls, false),
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

        assertThat(result.success()).isFalse();
        assertThat(reportWriter.report.status()).isEqualTo("DEGRADED");
        assertThat(calls).containsSubsequence(
            "Hive HDFS publish /data/generated",
            "Hive external table /data/generated"
        );
        assertThat(calls).contains(
            "restart SPARK_ICEBERG",
            "ready SPARK_ICEBERG",
            "Spark query " + queryName + " COLD"
        );
        assertThat(calls).doesNotContain(
            "restart HIVE_HDFS_PARQUET",
            "Hive query " + queryName + " COLD",
            "Hive query " + queryName + " WARM",
            "Hive query " + queryName + " HOT"
        );
        assertThat(reportWriter.report.loadSummaries())
            .anySatisfy(summary -> {
                assertThat(summary.engine()).isEqualTo("hive");
                assertThat(summary.tableShape()).isEqualTo("hive_hdfs_parquet");
                assertThat(summary.stage()).isEqualTo("HIVE_HDFS_PARQUET_PUBLISH");
                assertThat(summary.success()).isFalse();
                assertThat(summary.error()).contains("hdfs publish failed");
            });
        assertThat(reportWriter.report.querySummaries())
            .filteredOn(summary -> summary.tableShape().equals("hive_hdfs_parquet"))
            .filteredOn(summary -> summary.queryName().equals(queryName))
            .hasSize(3)
            .allSatisfy(summary -> {
                assertThat(summary.success()).isFalse();
                assertThat(summary.error()).contains("hdfs publish failed");
            });
    }

    @Test
    void composeRunnerRecordsStarRocksInternalLoadFailureWhenHdfsPublishFailsAndSkipsInternalQueries() throws Exception {
        List<String> calls = new ArrayList<>();
        String queryName = QueryCatalog.queries().get(0).name();
        DatasetResult dataset = new DatasetResult(tempDir.resolve("data"), List.of(tempDir.resolve("part.parquet")), 5L, 123L);
        CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/compose-test/index.html"));

        ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
            config -> dataset,
            failingCsvExporter(),
            failingTpchGenerator(),
            failingTpchCsvExport(),
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, true),
            new FakeHdfsDatasetPublisher(calls, false),
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

        assertThat(result.success()).isFalse();
        assertThat(calls)
            .contains("Hive HDFS publish /data/generated")
            .noneMatch(call -> call.startsWith("StarRocks internal load from parquet "))
            .doesNotContain(
                "restart STARROCKS_INTERNAL",
                "StarRocks starrocks_internal " + queryName + " COLD",
                "StarRocks starrocks_internal " + queryName + " WARM",
                "StarRocks starrocks_internal " + queryName + " HOT"
            );
        assertThat(reportWriter.report.loadSummaries())
            .filteredOn(summary -> summary.tableShape().equals("starrocks_internal"))
            .filteredOn(summary -> summary.stage().equals(EngineStage.STARROCKS_INTERNAL_LOAD.name()))
            .singleElement()
            .satisfies(summary -> {
                assertThat(summary.success()).isFalse();
                assertThat(summary.error()).contains("HDFS publish failed");
                assertThat(summary.error()).contains("hdfs publish failed");
            });
        assertThat(reportWriter.report.querySummaries())
            .filteredOn(summary -> summary.tableShape().equals("starrocks_internal"))
            .filteredOn(summary -> summary.queryName().equals(queryName))
            .hasSize(3)
            .allSatisfy(summary -> {
                assertThat(summary.success()).isFalse();
                assertThat(summary.error()).contains("HDFS publish failed");
            });
    }

    @Test
    void composeRunnerRecordsHiveCreateFailureAndSkipsHiveRouteQueries() throws Exception {
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
            new FakeHdfsDatasetPublisher(calls, true),
            new FakeHiveClient(calls, false),
            new FakeServiceController(calls),
            reportWriter,
            new CapturingMetricsRecorder(calls, "smoke", "kpi", "smoke")
        );

        ComposeBenchmarkRunner.ComposeRunResult result = runner.run(
            BenchmarkConfig.defaultSmoke(),
            tempDir.resolve("reports"),
            "compose-test"
        );

        assertThat(result.success()).isFalse();
        assertThat(calls).doesNotContain(
            "restart HIVE_HDFS_PARQUET",
            "Hive query " + queryName + " COLD",
            "Hive query " + queryName + " WARM",
            "Hive query " + queryName + " HOT"
        );
        assertThat(reportWriter.report.querySummaries())
            .filteredOn(summary -> summary.tableShape().equals("hive_hdfs_parquet"))
            .filteredOn(summary -> summary.queryName().equals(queryName))
            .hasSize(3)
            .allSatisfy(summary -> {
                assertThat(summary.success()).isFalse();
                assertThat(summary.error()).contains("hive create failed");
            });
    }

    @Test
    void defaultHdfsPublisherUsesHadoopDockerCliFromHost() throws Exception {
        CapturingCommandRunner commandRunner = new CapturingCommandRunner();
        ComposeBenchmarkRunner.HdfsDatasetPublisher publisher = defaultHdfsPublisher(commandRunner, tempDir, false);
        DatasetResult dataset = new DatasetResult(
            tempDir.resolve("data/generated"),
            List.of(
                tempDir.resolve("data/generated/event_date=2026-01-01/part-00000.parquet"),
                tempDir.resolve("data/generated/event_date=2026-01-02/part-00000.parquet")
            ),
            5L,
            123L
        );

        EngineRunResult result = publisher.publish(dataset, "/data/generated");

        assertThat(result.success()).isTrue();
        assertThat(commandRunner.commands()).hasSize(6);
        assertThat(commandRunner.commands().get(0)).containsExactly(
            "docker", "run", "--rm", "--network", "shared-data-infra",
            "-v", tempDir.toAbsolutePath().normalize() + ":/workspace",
            "-w", "/workspace",
            "apache/hadoop:3.3.6",
            "hdfs", "dfs", "-fs", "hdfs://hdfs-namenode:8020",
            "-rm", "-r", "-f", "/data/generated"
        );
        assertThat(commandRunner.commands().get(1)).containsExactly(
            "docker", "run", "--rm", "--network", "shared-data-infra",
            "-v", tempDir.toAbsolutePath().normalize() + ":/workspace",
            "-w", "/workspace",
            "apache/hadoop:3.3.6",
            "hdfs", "dfs", "-fs", "hdfs://hdfs-namenode:8020",
            "-mkdir", "-p", "/data/generated"
        );
        assertThat(commandRunner.commands().get(2)).containsExactly(
            "docker", "run", "--rm", "--network", "shared-data-infra",
            "-v", tempDir.toAbsolutePath().normalize() + ":/workspace",
            "-w", "/workspace",
            "apache/hadoop:3.3.6",
            "hdfs", "dfs", "-fs", "hdfs://hdfs-namenode:8020",
            "-mkdir", "-p", "/data/generated/event_date=2026-01-01"
        );
        assertThat(commandRunner.commands().get(3)).containsExactly(
            "docker", "run", "--rm", "--network", "shared-data-infra",
            "-v", tempDir.toAbsolutePath().normalize() + ":/workspace",
            "-w", "/workspace",
            "apache/hadoop:3.3.6",
            "hdfs", "dfs", "-fs", "hdfs://hdfs-namenode:8020",
            "-put", "-f", "/workspace/data/generated/event_date=2026-01-01/part-00000.parquet", "/data/generated/event_date=2026-01-01"
        );
        assertThat(commandRunner.commands().get(4)).containsExactly(
            "docker", "run", "--rm", "--network", "shared-data-infra",
            "-v", tempDir.toAbsolutePath().normalize() + ":/workspace",
            "-w", "/workspace",
            "apache/hadoop:3.3.6",
            "hdfs", "dfs", "-fs", "hdfs://hdfs-namenode:8020",
            "-mkdir", "-p", "/data/generated/event_date=2026-01-02"
        );
        assertThat(commandRunner.commands().get(5)).containsExactly(
            "docker", "run", "--rm", "--network", "shared-data-infra",
            "-v", tempDir.toAbsolutePath().normalize() + ":/workspace",
            "-w", "/workspace",
            "apache/hadoop:3.3.6",
            "hdfs", "dfs", "-fs", "hdfs://hdfs-namenode:8020",
            "-put", "-f", "/workspace/data/generated/event_date=2026-01-02/part-00000.parquet", "/data/generated/event_date=2026-01-02"
        );
        assertThat(commandRunner.commands()).noneSatisfy(command ->
            assertThat(command).containsSequence("-put", "-f", "/workspace/data/generated", "/data/generated"));
    }

    @Test
    void defaultHdfsPublisherNetworkCanBeConfiguredFromEnvironment() throws Exception {
        String network = hdfsPublisherNetwork(Map.of("BENCHMARK_INFRA_NETWORK", " benchmark-shared "));

        assertThat(network).isEqualTo("benchmark-shared");
    }

    @Test
    void defaultHdfsPublisherUsesLocalHadoopCliInContainer() throws Exception {
        CapturingCommandRunner commandRunner = new CapturingCommandRunner();
        ComposeBenchmarkRunner.HdfsDatasetPublisher publisher = defaultHdfsPublisher(commandRunner, tempDir, true);
        DatasetResult dataset = new DatasetResult(
            tempDir.resolve("data/generated"),
            List.of(tempDir.resolve("data/generated/event_date=2026-01-01/part-00000.parquet")),
            5L,
            123L
        );

        EngineRunResult result = publisher.publish(dataset, "/data/generated");

        assertThat(result.success()).isTrue();
        assertThat(commandRunner.commands()).hasSize(4);
        assertThat(commandRunner.commands().get(0)).containsExactly(
            "hdfs", "dfs", "-fs", "hdfs://hdfs-namenode:8020",
            "-rm", "-r", "-f", "/data/generated"
        );
        assertThat(commandRunner.commands().get(1)).containsExactly(
            "hdfs", "dfs", "-fs", "hdfs://hdfs-namenode:8020",
            "-mkdir", "-p", "/data/generated"
        );
        assertThat(commandRunner.commands().get(2)).containsExactly(
            "hdfs", "dfs", "-fs", "hdfs://hdfs-namenode:8020",
            "-mkdir", "-p", "/data/generated/event_date=2026-01-01"
        );
        assertThat(commandRunner.commands().get(3)).containsExactly(
            "hdfs", "dfs", "-fs", "hdfs://hdfs-namenode:8020",
            "-put", "-f", "/workspace/data/generated/event_date=2026-01-01/part-00000.parquet", "/data/generated/event_date=2026-01-01"
        );
        assertThat(commandRunner.commands()).noneSatisfy(command ->
            assertThat(command).containsSequence("-put", "-f", "/workspace/data/generated", "/data/generated"));
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

    private static ComposeBenchmarkRunner.HdfsDatasetPublisher defaultHdfsPublisher(
        CommandRunner commandRunner,
        Path workingDirectory,
        boolean inContainer
    ) throws Exception {
        Class<?> type = Class.forName("com.example.databenchmark.runner.ComposeBenchmarkRunner$DefaultHdfsDatasetPublisher");
        Constructor<?> constructor = type.getDeclaredConstructor(CommandRunner.class, Path.class, Duration.class, boolean.class);
        constructor.setAccessible(true);
        return (ComposeBenchmarkRunner.HdfsDatasetPublisher) constructor.newInstance(
            commandRunner,
            workingDirectory,
            Duration.ofMinutes(1),
            inContainer
        );
    }

    private static String hdfsPublisherNetwork(Map<String, String> environment) throws Exception {
        Class<?> type = Class.forName("com.example.databenchmark.runner.ComposeBenchmarkRunner$DefaultHdfsDatasetPublisher");
        var method = type.getDeclaredMethod("dockerNetworkFromEnvironment", Map.class);
        method.setAccessible(true);
        return (String) method.invoke(null, environment);
    }

    private static final class FakeSparkClient implements ComposeBenchmarkRunner.SparkClient {
        private final List<String> calls;
        private final String failingValidationTableShape;

        private FakeSparkClient(List<String> calls) {
            this(calls, null);
        }

        private FakeSparkClient(List<String> calls, String failingValidationTableShape) {
            this.calls = calls;
            this.failingValidationTableShape = failingValidationTableShape;
        }

        @Override
        public EngineRunResult loadNativeParquet(DatasetResult dataset, String runId, String profile) {
            calls.add("Spark native load");
            return new EngineRunResult("spark", "spark_native_parquet", EngineStage.SPARK_NATIVE_PARQUET_LOAD.name(),
                null, dataset.rows(), dataset.bytesWritten(), 0.8, true, "");
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
        public EngineRunResult validateCount(String tableShape, long expectedRows) {
            calls.add("Spark validate " + tableShape + " rows=" + expectedRows);
            boolean success = !tableShape.equals(failingValidationTableShape);
            return new EngineRunResult("spark", tableShape, validateStage(tableShape), null,
                success ? expectedRows : expectedRows - 1, 0L, 0.1, success,
                success ? "" : "row count mismatch for " + tableShape);
        }

        @Override
        public EngineRunResult runQuery(String queryName, RoutePhase phase) {
            calls.add("Spark query " + queryName + " " + phase.name());
            return new EngineRunResult("spark", "spark_iceberg", EngineStage.QUERY.name(),
                queryName, phase.name(), 3L, 0L, 0.2, true, "");
        }

        @Override
        public EngineRunResult runNativeQuery(String queryName, RoutePhase phase) {
            calls.add("Spark native query " + queryName + " " + phase.name());
            return new EngineRunResult("spark", "spark_native_parquet", EngineStage.QUERY.name(),
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

        private String validateStage(String tableShape) {
            return tableShape.equals("spark_native_parquet")
                ? EngineStage.SPARK_NATIVE_PARQUET_VALIDATE.name()
                : EngineStage.SPARK_ICEBERG_VALIDATE.name();
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
        public EngineRunResult loadInternal(Path parquetRoot, String runId, String profile, long expectedRows, long bytesWritten) {
            calls.add("StarRocks internal load from parquet " + parquetRoot.toString().replace('\\', '/')
                + " rows=" + expectedRows + " bytes=" + bytesWritten);
            return new EngineRunResult("starrocks", "starrocks_internal",
                EngineStage.STARROCKS_INTERNAL_LOAD.name(), null, expectedRows, bytesWritten, 1.5, true, "");
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
        public EngineRunResult validateCount(String tableShape, long expectedRows) {
            calls.add("StarRocks validate " + tableShape + " rows=" + expectedRows);
            String stage = tableShape.equals("starrocks_internal")
                ? EngineStage.STARROCKS_INTERNAL_VALIDATE.name()
                : EngineStage.STARROCKS_EXTERNAL_VALIDATE.name();
            return new EngineRunResult("starrocks", tableShape, stage, null, expectedRows, 0L, 0.2, true, "");
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

    private static final class FakeHdfsDatasetPublisher implements ComposeBenchmarkRunner.HdfsDatasetPublisher {
        private final List<String> calls;
        private final boolean succeeds;

        private FakeHdfsDatasetPublisher(List<String> calls, boolean succeeds) {
            this.calls = calls;
            this.succeeds = succeeds;
        }

        @Override
        public EngineRunResult publish(DatasetResult dataset, String hdfsRoot) {
            calls.add("Hive HDFS publish " + hdfsRoot.replace('\\', '/'));
            return new EngineRunResult("hive", "hive_hdfs_parquet", "HIVE_HDFS_PARQUET_PUBLISH",
                null, dataset.rows(), dataset.bytesWritten(), 0.6, succeeds, succeeds ? "" : "hdfs publish failed");
        }
    }

    private static final class CapturingCommandRunner extends CommandRunner {
        private final List<List<String>> commands = new ArrayList<>();

        @Override
        public CommandResult run(List<String> command, Path workingDirectory, Duration timeout) {
            commands.add(command);
            return new CommandResult(command, 0, "ok", "", 0.1);
        }

        private List<List<String>> commands() {
            return commands;
        }
    }

    private static final class FakeHiveClient implements ComposeBenchmarkRunner.HiveClientFacade {
        private final List<String> calls;
        private final boolean createSucceeds;

        private FakeHiveClient(List<String> calls) {
            this(calls, true);
        }

        private FakeHiveClient(List<String> calls, boolean createSucceeds) {
            this.calls = calls;
            this.createSucceeds = createSucceeds;
        }

        @Override
        public EngineRunResult createExternalTable(String parquetRoot) {
            calls.add("Hive external table " + parquetRoot.replace('\\', '/'));
            return new EngineRunResult("hive", "hive_hdfs_parquet", "HIVE_HDFS_PARQUET_LOAD",
                null, 0L, 0L, 0.5, createSucceeds, createSucceeds ? "" : "hive create failed");
        }

        @Override
        public EngineRunResult runQuery(String queryName, RoutePhase phase) {
            calls.add("Hive query " + queryName + " " + phase.name());
            return new EngineRunResult("hive", "hive_hdfs_parquet", EngineStage.QUERY.name(),
                queryName, phase.name(), 2L, 0L, 0.4, true, "");
        }

        @Override
        public EngineRunResult validateCount(long expectedRows) {
            calls.add("Hive validate rows=" + expectedRows);
            return new EngineRunResult("hive", "hive_hdfs_parquet", EngineStage.HIVE_HDFS_PARQUET_VALIDATE.name(),
                null, expectedRows, 0L, 0.2, true, "");
        }
    }

    private static final class FakeServiceController implements ComposeBenchmarkRunner.ServiceController {
        private final List<String> calls;
        private final BenchmarkRoute failingRoute;
        private final BenchmarkRoute failingReadyRoute;

        private FakeServiceController(List<String> calls) {
            this(calls, null, null);
        }

        private FakeServiceController(List<String> calls, BenchmarkRoute failingRoute) {
            this(calls, failingRoute, null);
        }

        private FakeServiceController(List<String> calls, BenchmarkRoute failingRoute, BenchmarkRoute failingReadyRoute) {
            this.calls = calls;
            this.failingRoute = failingRoute;
            this.failingReadyRoute = failingReadyRoute;
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
            if (route == failingReadyRoute) {
                throw new IllegalStateException("readiness failed for " + route.name());
            }
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

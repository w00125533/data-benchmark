package com.example.databenchmark.runner;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.engine.EngineRunResult;
import com.example.databenchmark.engine.EngineStage;
import com.example.databenchmark.generator.DatasetResult;
import com.example.databenchmark.report.BenchmarkReport;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ComposeBenchmarkRunnerTest {
    @TempDir
    Path tempDir;

    @Test
    void composeRunnerOrchestratesEnginesAndWritesReportInOrder() throws Exception {
        List<String> calls = new ArrayList<>();
        DatasetResult dataset = new DatasetResult(tempDir.resolve("data"), List.of(tempDir.resolve("part.parquet")), 5L, 123L);
        CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/compose-test/index.html"));

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
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, true),
            reportWriter
        );

        ComposeBenchmarkRunner.ComposeRunResult result = runner.run(
            BenchmarkConfig.defaultSmoke(),
            tempDir.resolve("reports"),
            "compose-test"
        );

        assertThat(result.success()).isTrue();
        assertThat(result.dataset()).isSameAs(dataset);
        assertThat(result.csvPath().getFileName().toString()).isEqualTo("cell_kpi_1min.csv");
        assertThat(result.reportPath()).isEqualTo(tempDir.resolve("reports/compose-test/index.html"));
        assertThat(calls).containsExactly(
            "generate dataset",
            "export CSV",
            "Spark load",
            "StarRocks internal load",
            "StarRocks external refresh",
            "Spark queries",
            "StarRocks queries",
            "write HTML report"
        );
        assertThat(reportWriter.report.loadSummaries())
            .extracting(BenchmarkReport.LoadSummary::stage)
            .containsExactly(
                EngineStage.GENERATE.name(),
                EngineStage.SPARK_ICEBERG_LOAD.name(),
                EngineStage.STARROCKS_INTERNAL_LOAD.name(),
                EngineStage.STARROCKS_EXTERNAL_REFRESH.name()
            );
        assertThat(reportWriter.report.querySummaries())
            .extracting(BenchmarkReport.QuerySummary::queryName)
            .containsExactly("spark_query", "starrocks_query");
    }

    @Test
    void composeRunnerWritesReportWhenExternalRefreshFails() throws Exception {
        List<String> calls = new ArrayList<>();
        DatasetResult dataset = new DatasetResult(tempDir.resolve("data"), List.of(tempDir.resolve("part.parquet")), 5L, 123L);
        CapturingReportWriter reportWriter = new CapturingReportWriter(calls, tempDir.resolve("reports/compose-test/index.html"));

        ComposeBenchmarkRunner runner = new ComposeBenchmarkRunner(
            config -> {
                calls.add("generate dataset");
                return dataset;
            },
            (generatedDataset, outputDir) -> {
                calls.add("export CSV");
                return outputDir.resolve("cell_kpi_1min.csv");
            },
            new FakeSparkClient(calls),
            new FakeStarRocksClient(calls, false),
            reportWriter
        );

        ComposeBenchmarkRunner.ComposeRunResult result = runner.run(
            BenchmarkConfig.defaultSmoke(),
            tempDir.resolve("reports"),
            "compose-test"
        );

        assertThat(result.success()).isFalse();
        assertThat(result.reportPath()).isEqualTo(tempDir.resolve("reports/compose-test/index.html"));
        assertThat(calls).containsExactly(
            "generate dataset",
            "export CSV",
            "Spark load",
            "StarRocks internal load",
            "StarRocks external refresh",
            "Spark queries",
            "StarRocks queries",
            "write HTML report"
        );
        assertThat(reportWriter.report.status()).isEqualTo("DEGRADED");
        assertThat(reportWriter.report.loadSummaries())
            .anySatisfy(summary -> assertThat(summary.error()).contains("catalog refresh failed"));
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
            calls.add("write HTML report");
            this.report = report;
            return reportPath;
        }
    }
}

package com.example.databenchmark.runner;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.engine.EngineRunResult;
import com.example.databenchmark.engine.EngineStage;
import com.example.databenchmark.engine.SparkIcebergClient;
import com.example.databenchmark.engine.StarRocksClient;
import com.example.databenchmark.engine.StarRocksCsvExporter;
import com.example.databenchmark.generator.DatasetResult;
import com.example.databenchmark.generator.KpiDataGenerator;
import com.example.databenchmark.report.BenchmarkReport;
import com.example.databenchmark.report.WebReportWriter;
import com.example.databenchmark.tpch.TpchCsvExporter;
import com.example.databenchmark.tpch.TpchDataGenerator;
import com.example.databenchmark.tpch.TpchDatasetResult;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ComposeBenchmarkRunner {
    private final DatasetGenerator generator;
    private final CsvExporter csvExporter;
    private final TpchGenerator tpchGenerator;
    private final TpchCsvExport tpchCsvExport;
    private final SparkClient sparkClient;
    private final StarRocksClientFacade starRocksClient;
    private final ReportWriter reportWriter;
    private final MetricsRecorder metricsRecorder;

    public ComposeBenchmarkRunner() {
        this(
            new KpiDataGenerator()::generate,
            new StarRocksCsvExporter()::export,
            new TpchDataGenerator()::generate,
            new TpchCsvExporter()::export,
            new SparkClientAdapter(new SparkIcebergClient()),
            new StarRocksClientAdapter(new StarRocksClient()),
            new WebReportWriter()::write,
            MetricsRecorder.noop()
        );
    }

    ComposeBenchmarkRunner(
        DatasetGenerator generator,
        CsvExporter csvExporter,
        TpchGenerator tpchGenerator,
        TpchCsvExport tpchCsvExport,
        SparkClient sparkClient,
        StarRocksClientFacade starRocksClient,
        ReportWriter reportWriter
    ) {
        this(generator, csvExporter, tpchGenerator, tpchCsvExport, sparkClient, starRocksClient, reportWriter, MetricsRecorder.noop());
    }

    ComposeBenchmarkRunner(
        DatasetGenerator generator,
        CsvExporter csvExporter,
        TpchGenerator tpchGenerator,
        TpchCsvExport tpchCsvExport,
        SparkClient sparkClient,
        StarRocksClientFacade starRocksClient,
        ReportWriter reportWriter,
        MetricsRecorder metricsRecorder
    ) {
        this.generator = generator;
        this.csvExporter = csvExporter;
        this.tpchGenerator = tpchGenerator;
        this.tpchCsvExport = tpchCsvExport;
        this.sparkClient = sparkClient;
        this.starRocksClient = starRocksClient;
        this.reportWriter = reportWriter;
        this.metricsRecorder = metricsRecorder;
    }

    public ComposeRunResult run(BenchmarkConfig config, Path reportRoot, String runId) throws Exception {
        String actualRunId = runId == null ? generatedRunId() : runId;
        Instant started = Instant.now();
        List<EngineRunResult> loadResults = new ArrayList<>();
        List<EngineRunResult> queryResults = new ArrayList<>();
        DatasetResult dataset = null;
        Path csvPath = null;

        metricsRecorder.start();
        try {
            if ("tpch".equals(config.suite().name())) {
                return runTpch(config, reportRoot, actualRunId, started, loadResults, queryResults);
            }

            long generateStarted = System.nanoTime();
            try {
                dataset = generator.generate(config);
                loadResults.add(new EngineRunResult(
                    "local",
                    "generated_parquet",
                    EngineStage.GENERATE.name(),
                    null,
                    dataset.rows(),
                    dataset.bytesWritten(),
                    elapsedSeconds(generateStarted),
                    true,
                    ""
                ));
            } catch (Exception exception) {
                loadResults.add(failed("local", "generated_parquet", EngineStage.GENERATE.name(), exception));
            }

            if (dataset != null) {
                try {
                    csvPath = csvExporter.export(dataset, Path.of(config.dataset().output()).resolve("starrocks-csv"));
                    loadResults.add(sparkClient.load(dataset, actualRunId, config.profile()));
                    loadResults.add(starRocksClient.loadInternal(csvPath, actualRunId, config.profile()));
                    loadResults.add(starRocksClient.refreshExternalCatalog(actualRunId, config.profile()));
                    queryResults.addAll(sparkClient.runQueries(actualRunId, config.profile()));
                    queryResults.addAll(starRocksClient.runQueries(actualRunId, config.profile()));
                } catch (Exception exception) {
                    loadResults.add(failed("compose", "prepare", "compose_prepare", exception));
                }
            }

            recordMetrics(
                actualRunId,
                config.profile(),
                config.suite().name(),
                config.suite().querySet(),
                loadResults,
                queryResults
            );
            BenchmarkReport report = buildReport(
                config,
                actualRunId,
                started,
                Instant.now(),
                dataset == null ? 0L : dataset.rows(),
                dataset == null ? 0L : dataset.bytesWritten(),
                loadResults,
                queryResults
            );
            Path reportPath = reportWriter.write(report, reportRoot);
            return new ComposeRunResult(
                dataset,
                csvPath,
                reportPath,
                report.status().equals("SUCCESS"),
                dataset == null ? 0L : dataset.rows(),
                dataset == null ? 0L : dataset.bytesWritten()
            );
        } finally {
            metricsRecorder.close();
        }
    }

    private ComposeRunResult runTpch(
        BenchmarkConfig config,
        Path reportRoot,
        String runId,
        Instant started,
        List<EngineRunResult> loadResults,
        List<EngineRunResult> queryResults
    ) throws Exception {
        TpchDatasetResult dataset = null;
        Path csvPath = null;

        long generateStarted = System.nanoTime();
        try {
            dataset = tpchGenerator.generate(config, runId);
            loadResults.add(new EngineRunResult(
                "local",
                "tpch_generated_parquet",
                EngineStage.GENERATE.name(),
                null,
                dataset.rows(),
                dataset.bytesWritten(),
                elapsedSeconds(generateStarted),
                true,
                ""
            ));
        } catch (Exception exception) {
            loadResults.add(failed("local", "tpch_generated_parquet", EngineStage.GENERATE.name(), exception));
        }

        if (dataset != null) {
            try {
                Map<String, Path> csvFiles = tpchCsvExport.export(dataset);
                Path firstCsvFile = csvFiles.values().stream().findFirst().orElse(null);
                csvPath = firstCsvFile != null ? firstCsvFile.getParent() : dataset.outputPath().resolve("csv");
                loadResults.add(sparkClient.loadTpch(dataset, runId, config.profile()));
                loadResults.add(starRocksClient.loadTpchInternal(csvFiles, dataset, runId, config.profile()));
                loadResults.add(starRocksClient.refreshTpchExternalCatalog(runId, config.profile()));
                queryResults.addAll(sparkClient.runTpchQueries(runId, config.profile(), config.suite().querySet()));
                queryResults.addAll(starRocksClient.runTpchQueries(runId, config.profile(), config.suite().querySet()));
            } catch (Exception exception) {
                loadResults.add(failed("compose", "tpch_prepare", "compose_prepare", exception));
            }
        }

        recordMetrics(
            runId,
            config.profile(),
            config.suite().name(),
            config.suite().querySet(),
            loadResults,
            queryResults
        );
        BenchmarkReport report = buildReport(
            config,
            runId,
            started,
            Instant.now(),
            dataset == null ? 0L : dataset.rows(),
            dataset == null ? 0L : dataset.bytesWritten(),
            loadResults,
            queryResults
        );
        Path reportPath = reportWriter.write(report, reportRoot);
        return new ComposeRunResult(
            null,
            csvPath,
            reportPath,
            report.status().equals("SUCCESS"),
            dataset == null ? 0L : dataset.rows(),
            dataset == null ? 0L : dataset.bytesWritten()
        );
    }

    private BenchmarkReport buildReport(
        BenchmarkConfig config,
        String runId,
        Instant started,
        Instant ended,
        long rows,
        long bytes,
        List<EngineRunResult> loadResults,
        List<EngineRunResult> queryResults
    ) {
        return new BenchmarkReport(
            runId,
            config.profile(),
            config.suite().name(),
            config.suite().querySet(),
            started.toString(),
            ended.toString(),
            config.dataset().cells(),
            config.dataset().days(),
            rows,
            config.dataset().columns(),
            bytes,
            loadResults.stream().map(this::toLoadSummary).toList(),
            queryResults.stream().map(this::toQuerySummary).toList(),
            "full".equals(config.profile())
        );
    }

    private BenchmarkReport.LoadSummary toLoadSummary(EngineRunResult result) {
        return new BenchmarkReport.LoadSummary(
            result.engine(),
            result.tableShape(),
            result.stage(),
            result.rows(),
            result.bytes(),
            roundSeconds(result.durationSeconds()),
            result.success(),
            result.error()
        );
    }

    private BenchmarkReport.QuerySummary toQuerySummary(EngineRunResult result) {
        double millis = roundMillis(result.durationSeconds() * 1000.0);
        return new BenchmarkReport.QuerySummary(
            result.engine(),
            result.tableShape(),
            result.queryName(),
            result.phase(),
            millis,
            millis,
            millis,
            result.rows(),
            result.success() ? 0 : 1,
            result.success(),
            result.error()
        );
    }

    private EngineRunResult failed(String engine, String tableShape, String stage, Exception exception) {
        return new EngineRunResult(engine, tableShape, stage, null, 0L, 0L, 0.0, false, exception.getMessage());
    }

    private void recordMetrics(
        String runId,
        String profile,
        String suite,
        String querySet,
        List<EngineRunResult> loadResults,
        List<EngineRunResult> queryResults
    ) {
        loadResults.forEach(result -> metricsRecorder.recordLoad(runId, profile, suite, querySet, result));
        queryResults.forEach(result -> metricsRecorder.recordQuery(runId, profile, suite, querySet, result));
    }

    private String generatedRunId() {
        return "compose-" + Instant.now().toEpochMilli();
    }

    private static double elapsedSeconds(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000_000.0;
    }

    private static double roundSeconds(double seconds) {
        return Math.round(seconds * 1000.0) / 1000.0;
    }

    private static double roundMillis(double millis) {
        return Math.round(millis * 1000.0) / 1000.0;
    }

    interface DatasetGenerator {
        DatasetResult generate(BenchmarkConfig config) throws Exception;
    }

    interface CsvExporter {
        Path export(DatasetResult dataset, Path outputDir) throws Exception;
    }

    interface TpchGenerator {
        TpchDatasetResult generate(BenchmarkConfig config, String runId) throws Exception;
    }

    interface TpchCsvExport {
        Map<String, Path> export(TpchDatasetResult dataset) throws Exception;
    }

    interface SparkClient {
        EngineRunResult load(DatasetResult dataset, String runId, String profile);

        List<EngineRunResult> runQueries(String runId, String profile);

        default EngineRunResult loadTpch(TpchDatasetResult dataset, String runId, String profile) {
            throw new UnsupportedOperationException("TPC-H load not implemented");
        }

        default List<EngineRunResult> runTpchQueries(String runId, String profile, String querySet) {
            throw new UnsupportedOperationException("TPC-H queries not implemented");
        }
    }

    interface StarRocksClientFacade {
        EngineRunResult loadInternal(Path csv, String runId, String profile);

        EngineRunResult refreshExternalCatalog(String runId, String profile);

        List<EngineRunResult> runQueries(String runId, String profile);

        default EngineRunResult loadTpchInternal(
            Map<String, Path> csvFiles,
            TpchDatasetResult dataset,
            String runId,
            String profile
        ) {
            throw new UnsupportedOperationException("TPC-H internal load not implemented");
        }

        default EngineRunResult refreshTpchExternalCatalog(String runId, String profile) {
            throw new UnsupportedOperationException("TPC-H external refresh not implemented");
        }

        default List<EngineRunResult> runTpchQueries(String runId, String profile, String querySet) {
            throw new UnsupportedOperationException("TPC-H queries not implemented");
        }
    }

    interface ReportWriter {
        Path write(BenchmarkReport report, Path outputRoot) throws Exception;
    }

    interface MetricsRecorder extends AutoCloseable {
        void start();

        void recordLoad(String runId, String profile, String suite, String querySet, EngineRunResult result);

        void recordQuery(String runId, String profile, String suite, String querySet, EngineRunResult result);

        @Override
        void close();

        static MetricsRecorder noop() {
            return new MetricsRecorder() {
                @Override
                public void start() {}

                @Override
                public void recordLoad(String runId, String profile, String suite, String querySet, EngineRunResult result) {}

                @Override
                public void recordQuery(String runId, String profile, String suite, String querySet, EngineRunResult result) {}

                @Override
                public void close() {}
            };
        }
    }

    private record SparkClientAdapter(SparkIcebergClient delegate) implements SparkClient {
        @Override
        public EngineRunResult load(DatasetResult dataset, String runId, String profile) {
            return delegate.load(dataset, runId, profile);
        }

        @Override
        public List<EngineRunResult> runQueries(String runId, String profile) {
            return delegate.runQueries(runId, profile);
        }

        @Override
        public EngineRunResult loadTpch(TpchDatasetResult dataset, String runId, String profile) {
            return delegate.loadTpch(dataset, runId, profile);
        }

        @Override
        public List<EngineRunResult> runTpchQueries(String runId, String profile, String querySet) {
            return delegate.runTpchQueries(runId, profile, querySet);
        }
    }

    private record StarRocksClientAdapter(StarRocksClient delegate) implements StarRocksClientFacade {
        @Override
        public EngineRunResult loadInternal(Path csv, String runId, String profile) {
            return delegate.loadInternal(csv, runId, profile);
        }

        @Override
        public EngineRunResult refreshExternalCatalog(String runId, String profile) {
            return delegate.refreshExternalCatalog(runId, profile);
        }

        @Override
        public List<EngineRunResult> runQueries(String runId, String profile) {
            return delegate.runQueries(runId, profile);
        }

        @Override
        public EngineRunResult loadTpchInternal(
            Map<String, Path> csvFiles,
            TpchDatasetResult dataset,
            String runId,
            String profile
        ) {
            return delegate.loadTpchInternal(csvFiles, dataset, runId, profile);
        }

        @Override
        public EngineRunResult refreshTpchExternalCatalog(String runId, String profile) {
            return delegate.refreshTpchExternalCatalog(runId, profile);
        }

        @Override
        public List<EngineRunResult> runTpchQueries(String runId, String profile, String querySet) {
            return delegate.runTpchQueries(runId, profile, querySet);
        }
    }

    public record ComposeRunResult(
        DatasetResult dataset,
        Path csvPath,
        Path reportPath,
        boolean success,
        long rows,
        long bytesWritten
    ) {}
}

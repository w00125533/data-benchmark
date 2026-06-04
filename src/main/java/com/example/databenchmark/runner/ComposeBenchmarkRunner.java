package com.example.databenchmark.runner;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.engine.EngineRunResult;
import com.example.databenchmark.engine.EngineStage;
import com.example.databenchmark.engine.HiveClient;
import com.example.databenchmark.engine.SparkIcebergClient;
import com.example.databenchmark.engine.StarRocksClient;
import com.example.databenchmark.engine.StarRocksCsvExporter;
import com.example.databenchmark.generator.DatasetResult;
import com.example.databenchmark.generator.KpiDataGenerator;
import com.example.databenchmark.query.QueryCatalog;
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
    private final HiveClientFacade hiveClient;
    private final ServiceController serviceController;
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
            new HiveClientAdapter(new HiveClient()),
            new ServiceControllerAdapter(new ComposeServiceController()),
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
        this(
            generator,
            csvExporter,
            tpchGenerator,
            tpchCsvExport,
            sparkClient,
            starRocksClient,
            new HiveClientAdapter(new HiveClient()),
            new ServiceControllerAdapter(new ComposeServiceController()),
            reportWriter,
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
        ReportWriter reportWriter,
        MetricsRecorder metricsRecorder
    ) {
        this(
            generator,
            csvExporter,
            tpchGenerator,
            tpchCsvExport,
            sparkClient,
            starRocksClient,
            new HiveClientAdapter(new HiveClient()),
            new ServiceControllerAdapter(new ComposeServiceController()),
            reportWriter,
            metricsRecorder
        );
    }

    ComposeBenchmarkRunner(
        DatasetGenerator generator,
        CsvExporter csvExporter,
        TpchGenerator tpchGenerator,
        TpchCsvExport tpchCsvExport,
        SparkClient sparkClient,
        StarRocksClientFacade starRocksClient,
        HiveClientFacade hiveClient,
        ServiceController serviceController,
        ReportWriter reportWriter,
        MetricsRecorder metricsRecorder
    ) {
        this.generator = generator;
        this.csvExporter = csvExporter;
        this.tpchGenerator = tpchGenerator;
        this.tpchCsvExport = tpchCsvExport;
        this.sparkClient = sparkClient;
        this.starRocksClient = starRocksClient;
        this.hiveClient = hiveClient;
        this.serviceController = serviceController;
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
                } catch (Exception exception) {
                    loadResults.add(failed("compose", "starrocks_csv", "compose_prepare", exception));
                }
                loadResults.add(loadSpark(dataset, actualRunId, config.profile()));
                loadResults.add(loadStarRocksInternal(csvPath, actualRunId, config.profile()));
                loadResults.add(refreshStarRocksExternal(actualRunId, config.profile()));
                loadResults.add(createHiveExternalTable(config, dataset));
                queryResults.addAll(runKpiRouteQueries());
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

    private EngineRunResult failedQuery(BenchmarkRoute route, String queryName, RoutePhase phase, Exception exception) {
        return new EngineRunResult(
            routeEngine(route),
            routeTableShape(route),
            EngineStage.QUERY.name(),
            queryName,
            phase.name(),
            0L,
            0L,
            0.0,
            false,
            exception.getMessage()
        );
    }

    private EngineRunResult failedLoad(String engine, String tableShape, String stage, String error) {
        return new EngineRunResult(engine, tableShape, stage, null, 0L, 0L, 0.0, false, error);
    }

    private EngineRunResult loadSpark(DatasetResult dataset, String runId, String profile) {
        try {
            return sparkClient.load(dataset, runId, profile);
        } catch (Exception exception) {
            return failed("spark", "spark_iceberg", EngineStage.SPARK_ICEBERG_LOAD.name(), exception);
        }
    }

    private EngineRunResult loadStarRocksInternal(Path csvPath, String runId, String profile) {
        if (csvPath == null) {
            return failedLoad(
                "starrocks",
                "starrocks_internal",
                EngineStage.STARROCKS_INTERNAL_LOAD.name(),
                "StarRocks CSV export failed before internal load"
            );
        }
        try {
            return starRocksClient.loadInternal(csvPath, runId, profile);
        } catch (Exception exception) {
            return failed("starrocks", "starrocks_internal", EngineStage.STARROCKS_INTERNAL_LOAD.name(), exception);
        }
    }

    private EngineRunResult refreshStarRocksExternal(String runId, String profile) {
        try {
            return starRocksClient.refreshExternalCatalog(runId, profile);
        } catch (Exception exception) {
            return failed("starrocks", "starrocks_external_iceberg", EngineStage.STARROCKS_EXTERNAL_REFRESH.name(), exception);
        }
    }

    private EngineRunResult createHiveExternalTable(BenchmarkConfig config, DatasetResult dataset) {
        try {
            return hiveClient.createExternalTable(hiveParquetRoot(config, dataset));
        } catch (Exception exception) {
            return failed("hive", "hive_hdfs_parquet", "HIVE_HDFS_PARQUET_LOAD", exception);
        }
    }

    private List<EngineRunResult> runKpiRouteQueries() {
        List<EngineRunResult> results = new ArrayList<>();
        for (var query : QueryCatalog.queries()) {
            String queryName = query.name();
            for (BenchmarkRoute route : BenchmarkRoute.values()) {
                results.addAll(runKpiRoutePhases(queryName, route));
            }
        }
        return results;
    }

    private List<EngineRunResult> runKpiRoutePhases(String queryName, BenchmarkRoute route) {
        List<EngineRunResult> results = new ArrayList<>();
        try {
            serviceController.restart(route);
            serviceController.waitUntilReady(route);
        } catch (Exception exception) {
            for (RoutePhase phase : RoutePhase.values()) {
                results.add(failedQuery(route, queryName, phase, exception));
            }
            return results;
        }

        for (RoutePhase phase : RoutePhase.values()) {
            try {
                results.add(runKpiRouteQuery(route, queryName, phase));
            } catch (Exception exception) {
                results.add(failedQuery(route, queryName, phase, exception));
            }
        }
        return results;
    }

    private EngineRunResult runKpiRouteQuery(BenchmarkRoute route, String queryName, RoutePhase phase) {
        return switch (route) {
            case SPARK_ICEBERG -> sparkClient.runQuery(queryName, phase);
            case STARROCKS_INTERNAL -> starRocksClient.runQueryFor("starrocks_internal", queryName, phase);
            case STARROCKS_EXTERNAL_ICEBERG -> starRocksClient.runQueryFor("starrocks_external_iceberg", queryName, phase);
            case HIVE_HDFS_PARQUET -> hiveClient.runQuery(queryName, phase);
        };
    }

    private String routeEngine(BenchmarkRoute route) {
        return switch (route) {
            case SPARK_ICEBERG -> "spark";
            case STARROCKS_INTERNAL, STARROCKS_EXTERNAL_ICEBERG -> "starrocks";
            case HIVE_HDFS_PARQUET -> "hive";
        };
    }

    private String routeTableShape(BenchmarkRoute route) {
        return switch (route) {
            case SPARK_ICEBERG -> "spark_iceberg";
            case STARROCKS_INTERNAL -> "starrocks_internal";
            case STARROCKS_EXTERNAL_ICEBERG -> "starrocks_external_iceberg";
            case HIVE_HDFS_PARQUET -> "hive_hdfs_parquet";
        };
    }

    private Path hiveParquetRoot(BenchmarkConfig config, DatasetResult dataset) {
        String configuredOutput = config.dataset().output().replace('\\', '/');
        if (configuredOutput.startsWith("hdfs://")) {
            return Path.of(configuredOutput);
        }
        if (configuredOutput.startsWith("/")) {
            return Path.of(configuredOutput);
        }
        if (configuredOutput.matches("^[A-Za-z]:/.*")) {
            return dataset.outputPath();
        }
        return Path.of("/").resolve(configuredOutput).normalize();
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

        EngineRunResult runQuery(String queryName, RoutePhase phase);

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

        EngineRunResult runQueryFor(String tableShape, String queryName, RoutePhase phase);

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

    interface HiveClientFacade {
        EngineRunResult createExternalTable(Path parquetRoot);

        EngineRunResult runQuery(String queryName, RoutePhase phase);
    }

    interface ServiceController {
        void restart(BenchmarkRoute route);

        void waitUntilReady(BenchmarkRoute route);
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
        public EngineRunResult runQuery(String queryName, RoutePhase phase) {
            return delegate.runQuery(queryName, phase);
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
        public EngineRunResult runQueryFor(String tableShape, String queryName, RoutePhase phase) {
            return delegate.runQueryFor(tableShape, queryName, phase);
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

    private record HiveClientAdapter(HiveClient delegate) implements HiveClientFacade {
        @Override
        public EngineRunResult createExternalTable(Path parquetRoot) {
            return delegate.createExternalTable(parquetRoot);
        }

        @Override
        public EngineRunResult runQuery(String queryName, RoutePhase phase) {
            return delegate.runQuery(queryName, phase);
        }
    }

    private record ServiceControllerAdapter(ComposeServiceController delegate) implements ServiceController {
        @Override
        public void restart(BenchmarkRoute route) {
            delegate.restart(route);
        }

        @Override
        public void waitUntilReady(BenchmarkRoute route) {
            delegate.waitUntilReady(route);
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

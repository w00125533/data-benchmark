package com.example.databenchmark.runner;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
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
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ComposeBenchmarkRunner {
    private static final String DEFAULT_HIVE_HDFS_ROOT = "/data/generated";

    private final DatasetGenerator generator;
    private final CsvExporter csvExporter;
    private final TpchGenerator tpchGenerator;
    private final TpchCsvExport tpchCsvExport;
    private final SparkClient sparkClient;
    private final StarRocksClientFacade starRocksClient;
    private final HdfsDatasetPublisher hdfsDatasetPublisher;
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
            new DefaultHdfsDatasetPublisher(new CommandRunner()),
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
            new DefaultHdfsDatasetPublisher(new CommandRunner()),
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
            new DefaultHdfsDatasetPublisher(new CommandRunner()),
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
        HdfsDatasetPublisher hdfsDatasetPublisher,
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
        this.hdfsDatasetPublisher = hdfsDatasetPublisher;
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
                    csvPath = csvExporter.export(dataset, starRocksCsvOutput(config, dataset));
                } catch (Exception exception) {
                    loadResults.add(failed("compose", "starrocks_csv", "compose_prepare", exception));
                }
                loadResults.add(loadSpark(dataset, actualRunId, config.profile()));
                loadResults.add(loadStarRocksInternal(csvPath, actualRunId, config.profile()));
                loadResults.add(refreshStarRocksExternal(actualRunId, config.profile()));
                String hiveRoot = hiveParquetRoot(config);
                EngineRunResult hivePublish = publishHiveDataset(dataset, hiveRoot);
                loadResults.add(hivePublish);
                EngineRunResult hiveLoad = createHiveExternalTable(hiveRoot);
                loadResults.add(hiveLoad);
                queryResults.addAll(runKpiRouteQueries(hiveRouteFailure(hivePublish, hiveLoad)));
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
            serviceController.waitUntilReady(BenchmarkRoute.STARROCKS_INTERNAL);
            return starRocksClient.loadInternal(csvPath, runId, profile);
        } catch (Exception exception) {
            return failed("starrocks", "starrocks_internal", EngineStage.STARROCKS_INTERNAL_LOAD.name(), exception);
        }
    }

    private EngineRunResult refreshStarRocksExternal(String runId, String profile) {
        try {
            serviceController.waitUntilReady(BenchmarkRoute.STARROCKS_EXTERNAL_ICEBERG);
            return starRocksClient.refreshExternalCatalog(runId, profile);
        } catch (Exception exception) {
            return failed("starrocks", "starrocks_external_iceberg", EngineStage.STARROCKS_EXTERNAL_REFRESH.name(), exception);
        }
    }

    private EngineRunResult publishHiveDataset(DatasetResult dataset, String hdfsRoot) {
        try {
            return hdfsDatasetPublisher.publish(dataset, hdfsRoot);
        } catch (Exception exception) {
            return failed("hive", "hive_hdfs_parquet", "HIVE_HDFS_PARQUET_PUBLISH", exception);
        }
    }

    private EngineRunResult createHiveExternalTable(String hdfsRoot) {
        try {
            serviceController.waitUntilReady(BenchmarkRoute.HIVE_HDFS_PARQUET);
            return hiveClient.createExternalTable(hdfsRoot);
        } catch (Exception exception) {
            return failed("hive", "hive_hdfs_parquet", "HIVE_HDFS_PARQUET_LOAD", exception);
        }
    }

    private List<EngineRunResult> runKpiRouteQueries(String hiveRouteFailure) {
        List<EngineRunResult> results = new ArrayList<>();
        for (var query : QueryCatalog.queries()) {
            String queryName = query.name();
            for (BenchmarkRoute route : BenchmarkRoute.values()) {
                if (route == BenchmarkRoute.HIVE_HDFS_PARQUET && hiveRouteFailure != null) {
                    results.addAll(failedRoutePhases(route, queryName, hiveRouteFailure));
                    continue;
                }
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
            results.addAll(failedRoutePhases(route, queryName, exception.getMessage()));
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

    private List<EngineRunResult> failedRoutePhases(BenchmarkRoute route, String queryName, String error) {
        List<EngineRunResult> results = new ArrayList<>();
        for (RoutePhase phase : RoutePhase.values()) {
            results.add(failedQuery(route, queryName, phase, error));
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

    private EngineRunResult failedQuery(BenchmarkRoute route, String queryName, RoutePhase phase, String error) {
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
            error
        );
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

    private String hiveParquetRoot(BenchmarkConfig config) {
        String configuredOutput = config.dataset().output().replace('\\', '/');
        if (configuredOutput.startsWith("hdfs://")) {
            return hdfsUriPath(configuredOutput);
        }
        if (configuredOutput.startsWith("/")) {
            return normalizeHdfsPath(configuredOutput);
        }
        if (configuredOutput.matches("^[A-Za-z]:/.*")) {
            return DEFAULT_HIVE_HDFS_ROOT;
        }
        return normalizeHdfsPath("/" + configuredOutput);
    }

    private Path starRocksCsvOutput(BenchmarkConfig config, DatasetResult dataset) {
        String configuredOutput = config.dataset().output().replace('\\', '/');
        if (configuredOutput.startsWith("hdfs://")) {
            return dataset.outputPath().resolve("starrocks-csv");
        }
        return Path.of(config.dataset().output()).resolve("starrocks-csv");
    }

    private String hiveRouteFailure(EngineRunResult hivePublish, EngineRunResult hiveLoad) {
        if (!hivePublish.success()) {
            return hivePublish.error();
        }
        if (!hiveLoad.success()) {
            return hiveLoad.error();
        }
        return null;
    }

    private static String hdfsUriPath(String value) {
        String path = URI.create(value).getPath();
        if (path == null || path.isBlank() || "/".equals(path)) {
            return DEFAULT_HIVE_HDFS_ROOT;
        }
        return normalizeHdfsPath(path);
    }

    private static String normalizeHdfsPath(String value) {
        String normalized = Path.of(value).normalize().toString().replace('\\', '/');
        return normalized.startsWith("/") ? normalized : "/" + normalized;
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

    interface HdfsDatasetPublisher {
        EngineRunResult publish(DatasetResult dataset, String hdfsRoot);
    }

    interface HiveClientFacade {
        EngineRunResult createExternalTable(String parquetRoot);

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

    private static final class DefaultHdfsDatasetPublisher implements HdfsDatasetPublisher {
        private static final String IN_CONTAINER_ENV = "BENCHMARK_COMPOSE_IN_CONTAINER";
        private static final String HDFS_URI = "hdfs://hdfs-namenode:8020";
        private static final String HADOOP_IMAGE = "apache/hadoop:3.3.6";
        private static final String DOCKER_NETWORK = "databenchmark";
        private static final String STAGE = "HIVE_HDFS_PARQUET_PUBLISH";
        private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(10);

        private final CommandRunner commandRunner;
        private final Path workingDirectory;
        private final Duration timeout;
        private final boolean inContainer;

        private DefaultHdfsDatasetPublisher(CommandRunner commandRunner) {
            this(commandRunner, Path.of("."), DEFAULT_TIMEOUT, defaultInContainer());
        }

        private DefaultHdfsDatasetPublisher(
            CommandRunner commandRunner,
            Path workingDirectory,
            Duration timeout,
            boolean inContainer
        ) {
            this.commandRunner = commandRunner;
            this.workingDirectory = workingDirectory;
            this.timeout = timeout;
            this.inContainer = inContainer;
        }

        @Override
        public EngineRunResult publish(DatasetResult dataset, String hdfsRoot) {
            long started = System.nanoTime();
            String hdfsRootPath = hdfsPath(hdfsRoot);
            List<List<String>> commands = new ArrayList<>();
            commands.add(hdfsCommand("-rm", "-r", "-f", hdfsRootPath));
            commands.add(hdfsCommand("-mkdir", "-p", hdfsRootPath));
            try {
                if (dataset.files().isEmpty()) {
                    return failedPublish(0.0, "Dataset has no parquet files to publish");
                }
                for (Path file : dataset.files()) {
                    String workspacePath = toWorkspacePath(file);
                    String hdfsPartitionPath = hdfsFileParentPath(dataset, file, hdfsRootPath);
                    commands.add(hdfsCommand("-mkdir", "-p", hdfsPartitionPath));
                    commands.add(hdfsCommand("-put", "-f", workspacePath, hdfsPartitionPath));
                }
            } catch (IllegalArgumentException exception) {
                return failedPublish(0.0, exception.getMessage());
            }

            for (List<String> command : commands) {
                CommandResult result;
                try {
                    result = commandRunner.run(command, workingDirectory, timeout);
                } catch (Exception exception) {
                    return failedPublish(elapsedSeconds(started), exception.getMessage());
                }
                if (result.exitCode() != 0) {
                    return failedPublish(result.durationSeconds(), commandError(result));
                }
            }

            return new EngineRunResult(
                "hive",
                "hive_hdfs_parquet",
                STAGE,
                null,
                dataset.rows(),
                dataset.bytesWritten(),
                elapsedSeconds(started),
                true,
                ""
            );
        }

        private List<String> hdfsCommand(String... hdfsArgs) {
            List<String> command = new ArrayList<>();
            if (!inContainer) {
                String workspaceMount = workingDirectory.toAbsolutePath().normalize() + ":/workspace";
                command.addAll(List.of(
                    "docker", "run", "--rm", "--network", DOCKER_NETWORK,
                    "-v", workspaceMount,
                    "-w", "/workspace",
                    HADOOP_IMAGE
                ));
            }
            command.addAll(List.of("hdfs", "dfs", "-fs", HDFS_URI));
            command.addAll(List.of(hdfsArgs));
            return command;
        }

        private String toWorkspacePath(Path path) {
            Path workspace = workingDirectory.toAbsolutePath().normalize();
            Path output = path.toAbsolutePath().normalize();
            if (!output.startsWith(workspace)) {
                throw new IllegalArgumentException("Dataset output path is outside workspace: " + output);
            }
            String relative = workspace.relativize(output).toString().replace('\\', '/');
            return relative.isEmpty() ? "/workspace" : "/workspace/" + relative;
        }

        private String hdfsFileParentPath(DatasetResult dataset, Path file, String hdfsRootPath) {
            Path datasetRoot = dataset.outputPath().toAbsolutePath().normalize();
            Path fileParent = file.toAbsolutePath().normalize().getParent();
            if (fileParent == null || !fileParent.startsWith(datasetRoot)) {
                throw new IllegalArgumentException("Dataset file is outside dataset output path: " + file);
            }
            String relativeParent = datasetRoot.relativize(fileParent).toString().replace('\\', '/');
            if (relativeParent.isBlank()) {
                return hdfsRootPath;
            }
            return hdfsRootPath + "/" + relativeParent;
        }

        private static EngineRunResult failedPublish(double durationSeconds, String error) {
            return new EngineRunResult(
                "hive",
                "hive_hdfs_parquet",
                STAGE,
                null,
                0L,
                0L,
                durationSeconds,
                false,
                error
            );
        }

        private static String hdfsPath(String path) {
            String normalized = path.replace('\\', '/');
            return normalized.startsWith("/") ? normalized : "/" + normalized;
        }

        private static String commandError(CommandResult command) {
            return command.stderr().isBlank() ? command.stdout() : command.stderr();
        }

        private static boolean defaultInContainer() {
            return Boolean.parseBoolean(System.getenv().getOrDefault(IN_CONTAINER_ENV, "false"));
        }
    }

    private record HiveClientAdapter(HiveClient delegate) implements HiveClientFacade {
        @Override
        public EngineRunResult createExternalTable(String parquetRoot) {
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

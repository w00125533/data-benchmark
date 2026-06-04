package com.example.databenchmark.runner;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.generator.DatasetResult;
import com.example.databenchmark.generator.KpiDataGenerator;
import com.example.databenchmark.report.BenchmarkReport;
import com.example.databenchmark.report.WebReportWriter;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public class LocalBenchmarkRunner {
    public LocalRunResult run(BenchmarkConfig config, Path reportRoot, String runId) throws Exception {
        requireKpiSuite(config);

        String actualRunId = runId == null ? generatedRunId() : runId;
        Instant started = Instant.now();
        long startedNanos = System.nanoTime();

        DatasetResult dataset = new KpiDataGenerator().generate(config);

        double durationSeconds = (System.nanoTime() - startedNanos) / 1_000_000_000.0;
        Instant ended = Instant.now();
        BenchmarkReport report = new BenchmarkReport(
            actualRunId,
            config.profile(),
            config.suite().name(),
            config.suite().querySet(),
            started.toString(),
            ended.toString(),
            config.dataset().cells(),
            config.dataset().days(),
            dataset.rows(),
            config.dataset().columns(),
            dataset.bytesWritten(),
            List.of(new BenchmarkReport.LoadSummary(
                "local",
                "generated_parquet",
                "generate",
                dataset.rows(),
                dataset.bytesWritten(),
                Math.round(durationSeconds * 1000.0) / 1000.0,
                true,
                ""
            )),
            List.of(new BenchmarkReport.QuerySummary(
                "local",
                "generated_parquet",
                "catalog_render_check",
                0.0,
                0.0,
                0.0,
                0,
                0,
                true,
                ""
            )),
            "full".equals(config.profile())
        );

        Path reportPath = new WebReportWriter().write(report, reportRoot);
        return new LocalRunResult(dataset, reportPath);
    }

    private static void requireKpiSuite(BenchmarkConfig config) {
        String suiteName = config.suite().name();
        if (!"kpi".equals(suiteName)) {
            throw new IllegalArgumentException(
                "local mode supports only the kpi suite; use compose mode for tpch suite configs"
            );
        }
    }

    private String generatedRunId() {
        return "run-" + Instant.now().toEpochMilli();
    }

    public record LocalRunResult(DatasetResult dataset, Path reportPath) {}
}

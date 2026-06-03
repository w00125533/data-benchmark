package com.example.databenchmark.runner;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.generator.DatasetResult;
import com.example.databenchmark.generator.KpiDataGenerator;
import com.example.databenchmark.report.BenchmarkReport;
import com.example.databenchmark.report.HtmlReportWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public class LocalBenchmarkRunner {
    public LocalRunResult run(BenchmarkConfig config, Path reportRoot, String runId) throws Exception {
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
            grafanaUrl(actualRunId, config),
            "full".equals(config.profile())
        );

        Path reportPath = new HtmlReportWriter().write(report, reportRoot);
        return new LocalRunResult(dataset, reportPath);
    }

    private String generatedRunId() {
        return "run-" + Instant.now().toEpochMilli();
    }

    private String grafanaUrl(String runId, BenchmarkConfig config) {
        return "http://localhost:3000/d/benchmark?var-run_id="
            + URLEncoder.encode(runId, StandardCharsets.UTF_8)
            + "&var-suite="
            + URLEncoder.encode(config.suite().name(), StandardCharsets.UTF_8)
            + "&var-query_set="
            + URLEncoder.encode(config.suite().querySet(), StandardCharsets.UTF_8);
    }

    public record LocalRunResult(DatasetResult dataset, Path reportPath) {}
}

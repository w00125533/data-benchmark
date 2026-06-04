package com.example.databenchmark.report;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class WebBenchmarkReportMapper {
    private static final int SCHEMA_VERSION = 1;

    public WebBenchmarkReport map(BenchmarkReport report) {
        return new WebBenchmarkReport(
            SCHEMA_VERSION,
            new WebBenchmarkReport.RunInfo(
                report.runId(),
                report.profile(),
                report.suite(),
                report.querySet(),
                report.status(),
                report.startedAt(),
                report.endedAt(),
                report.durationSeconds(),
                report.fullProfile()
            ),
            new WebBenchmarkReport.DatasetInfo(
                report.cells(),
                report.days(),
                report.rows(),
                report.columns(),
                report.bytesWritten()
            ),
            report.loadSummaries(),
            report.querySummaries(),
            charts(report),
            notices(report)
        );
    }

    private WebBenchmarkReport.ChartData charts(BenchmarkReport report) {
        return new WebBenchmarkReport.ChartData(
            report.loadSummaries().stream()
                .map(load -> new WebBenchmarkReport.LoadDurationPoint(
                    load.engine(),
                    load.tableShape(),
                    load.stage(),
                    load.durationSeconds(),
                    load.success()
                ))
                .toList(),
            queryLatency(report),
            report.querySummaries().stream()
                .map(query -> new WebBenchmarkReport.QueryRowsPoint(
                    query.engine(),
                    query.queryName(),
                    query.rows(),
                    query.success() && query.failures() == 0
                ))
                .toList(),
            failureSummary(report)
        );
    }

    private List<WebBenchmarkReport.QueryLatencyPoint> queryLatency(BenchmarkReport report) {
        List<WebBenchmarkReport.QueryLatencyPoint> points = new ArrayList<>();
        for (BenchmarkReport.QuerySummary query : report.querySummaries()) {
            boolean success = query.success() && query.failures() == 0;
            points.add(new WebBenchmarkReport.QueryLatencyPoint(
                query.engine(), query.tableShape(), query.queryName(), "p50", query.p50Ms(), success));
            points.add(new WebBenchmarkReport.QueryLatencyPoint(
                query.engine(), query.tableShape(), query.queryName(), "p95", query.p95Ms(), success));
            points.add(new WebBenchmarkReport.QueryLatencyPoint(
                query.engine(), query.tableShape(), query.queryName(), "p99", query.p99Ms(), success));
        }
        return points;
    }

    private List<WebBenchmarkReport.FailureSummaryPoint> failureSummary(BenchmarkReport report) {
        Map<String, Integer> failures = new LinkedHashMap<>();
        for (BenchmarkReport.LoadSummary load : report.loadSummaries()) {
            addFailure(failures, load.stage(), load.engine(), load.success() ? 0 : 1);
        }
        for (BenchmarkReport.QuerySummary query : report.querySummaries()) {
            int failureCount = query.success() && query.failures() == 0 ? 0 : Math.max(1, query.failures());
            addFailure(failures, query.queryName(), query.engine(), failureCount);
        }
        return failures.entrySet().stream()
            .map(entry -> {
                String[] parts = entry.getKey().split("\\|", 2);
                return new WebBenchmarkReport.FailureSummaryPoint(parts[0], parts[1], entry.getValue());
            })
            .toList();
    }

    private void addFailure(Map<String, Integer> failures, String stage, String engine, int count) {
        failures.merge(stage + "|" + engine, count, Integer::sum);
    }

    private List<String> notices(BenchmarkReport report) {
        List<String> notices = new ArrayList<>();
        if (!report.fullProfile()) {
            notices.add("This run is not a 4.032B row full-profile validation.");
        }
        if ("tpch".equals(report.suite())) {
            notices.add("TPC-H smoke data is compatible test data, not an official TPC-H benchmark result.");
        }
        return notices;
    }
}

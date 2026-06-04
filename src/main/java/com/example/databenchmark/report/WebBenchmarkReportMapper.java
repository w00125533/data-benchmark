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
            loads(report),
            queries(report),
            charts(report),
            notices(report)
        );
    }

    private List<WebBenchmarkReport.LoadSummary> loads(BenchmarkReport report) {
        return report.loadSummaries().stream()
            .map(load -> new WebBenchmarkReport.LoadSummary(
                load.engine(),
                load.tableShape(),
                load.stage(),
                load.rows(),
                load.bytes(),
                load.durationSeconds(),
                load.success(),
                load.error()
            ))
            .toList();
    }

    private List<WebBenchmarkReport.QuerySummary> queries(BenchmarkReport report) {
        return report.querySummaries().stream()
            .map(query -> new WebBenchmarkReport.QuerySummary(
                query.engine(),
                query.tableShape(),
                query.queryName(),
                query.p50Ms(),
                query.p95Ms(),
                query.p99Ms(),
                query.rows(),
                query.failures(),
                query.success(),
                query.error()
            ))
            .toList();
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
        Map<FailureKey, Integer> failures = new LinkedHashMap<>();
        for (BenchmarkReport.LoadSummary load : report.loadSummaries()) {
            addFailure(failures, load.stage(), load.engine(), load.success() ? 0 : 1);
        }
        for (BenchmarkReport.QuerySummary query : report.querySummaries()) {
            int failureCount = query.success() ? query.failures() : Math.max(1, query.failures());
            addFailure(failures, query.queryName(), query.engine(), failureCount);
        }
        return failures.entrySet().stream()
            .map(entry -> new WebBenchmarkReport.FailureSummaryPoint(
                entry.getKey().stage(),
                entry.getKey().engine(),
                entry.getValue()
            ))
            .toList();
    }

    private void addFailure(Map<FailureKey, Integer> failures, String stage, String engine, int count) {
        if (count <= 0) {
            return;
        }
        failures.merge(new FailureKey(stage, engine), count, Integer::sum);
    }

    private record FailureKey(String stage, String engine) {}

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

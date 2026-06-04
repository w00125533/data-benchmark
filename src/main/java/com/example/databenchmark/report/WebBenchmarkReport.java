package com.example.databenchmark.report;

import java.util.List;

public record WebBenchmarkReport(
    int schemaVersion,
    RunInfo run,
    DatasetInfo dataset,
    List<BenchmarkReport.LoadSummary> loads,
    List<BenchmarkReport.QuerySummary> queries,
    ChartData charts,
    List<String> notices
) {
    public record RunInfo(
        String runId,
        String profile,
        String suite,
        String querySet,
        String status,
        String startedAt,
        String endedAt,
        double durationSeconds,
        boolean fullProfile
    ) {}

    public record DatasetInfo(
        int cells,
        int days,
        long rows,
        int columns,
        long bytesWritten
    ) {}

    public record ChartData(
        List<LoadDurationPoint> loadDurationByEngine,
        List<QueryLatencyPoint> queryLatencyByEngine,
        List<QueryRowsPoint> queryRowsByEngine,
        List<FailureSummaryPoint> failureSummary
    ) {}

    public record LoadDurationPoint(
        String engine,
        String tableShape,
        String stage,
        double durationSeconds,
        boolean success
    ) {}

    public record QueryLatencyPoint(
        String engine,
        String tableShape,
        String queryName,
        String metric,
        double latencyMs,
        boolean success
    ) {}

    public record QueryRowsPoint(
        String engine,
        String queryName,
        long rows,
        boolean success
    ) {}

    public record FailureSummaryPoint(
        String stage,
        String engine,
        int failures
    ) {}
}

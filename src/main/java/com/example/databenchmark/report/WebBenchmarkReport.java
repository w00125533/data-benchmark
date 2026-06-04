package com.example.databenchmark.report;

import java.util.List;
import java.util.Map;

public record WebBenchmarkReport(
    int schemaVersion,
    RunInfo run,
    DatasetInfo dataset,
    List<LoadSummary> loads,
    List<QuerySummary> queries,
    List<PerformanceMatrixRow> performanceMatrix,
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

    public record LoadSummary(
        String engine,
        String tableShape,
        String stage,
        long rows,
        long bytes,
        double durationSeconds,
        boolean success,
        String error
    ) {}

    public record QuerySummary(
        String datasetId,
        String datasetName,
        String querySet,
        String engine,
        String tableShape,
        String queryName,
        double p50Ms,
        double p95Ms,
        double p99Ms,
        long rows,
        String status,
        String error
    ) {}

    public record PerformanceMatrixRow(
        String datasetId,
        String datasetName,
        String querySet,
        String queryName,
        Map<String, RouteResult> routes,
        String bestRoute,
        double bestRouteHotMs
    ) {}

    public record RouteResult(
        String status,
        double coldMs,
        double warmMs,
        double hotMs,
        String coldStatus,
        String warmStatus,
        String hotStatus,
        long rows,
        String error
    ) {}
}

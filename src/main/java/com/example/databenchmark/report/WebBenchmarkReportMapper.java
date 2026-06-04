package com.example.databenchmark.report;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class WebBenchmarkReportMapper {
    private static final int SCHEMA_VERSION = 2;
    private static final List<String> ROUTES = List.of(
        "spark_iceberg",
        "starrocks_internal",
        "starrocks_external_iceberg"
    );

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
            performanceMatrix(report),
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
        String datasetId = datasetId(report);
        String datasetName = datasetName(report);
        return report.querySummaries().stream()
            .map(query -> new WebBenchmarkReport.QuerySummary(
                datasetId,
                datasetName,
                report.querySet(),
                normalizeRoute(query.engine()),
                query.tableShape(),
                query.queryName(),
                query.p50Ms(),
                query.p95Ms(),
                query.p99Ms(),
                query.rows(),
                status(query),
                query.error()
            ))
            .toList();
    }

    private List<WebBenchmarkReport.PerformanceMatrixRow> performanceMatrix(BenchmarkReport report) {
        Map<MatrixKey, Map<String, WebBenchmarkReport.RouteResult>> grouped = new LinkedHashMap<>();
        String datasetId = datasetId(report);
        String datasetName = datasetName(report);
        for (BenchmarkReport.QuerySummary query : report.querySummaries()) {
            String route = normalizeRoute(query.engine());
            if (!ROUTES.contains(route)) {
                continue;
            }
            MatrixKey key = new MatrixKey(datasetId, datasetName, report.querySet(), query.queryName());
            Map<String, WebBenchmarkReport.RouteResult> routes = grouped.computeIfAbsent(key, ignored -> skippedRoutes());
            routes.put(route, new WebBenchmarkReport.RouteResult(
                status(query),
                query.p50Ms(),
                query.p95Ms(),
                query.p99Ms(),
                query.rows(),
                query.error()
            ));
        }
        return grouped.entrySet().stream()
            .map(entry -> matrixRow(entry.getKey(), entry.getValue()))
            .toList();
    }

    private Map<String, WebBenchmarkReport.RouteResult> skippedRoutes() {
        Map<String, WebBenchmarkReport.RouteResult> routes = new LinkedHashMap<>();
        for (String route : ROUTES) {
            routes.put(route, new WebBenchmarkReport.RouteResult("SKIPPED", 0, 0, 0, 0, ""));
        }
        return routes;
    }

    private WebBenchmarkReport.PerformanceMatrixRow matrixRow(
        MatrixKey key,
        Map<String, WebBenchmarkReport.RouteResult> routes
    ) {
        String bestRoute = "";
        double bestP95 = 0;
        for (Map.Entry<String, WebBenchmarkReport.RouteResult> entry : routes.entrySet()) {
            WebBenchmarkReport.RouteResult result = entry.getValue();
            if (!"SUCCESS".equals(result.status())) {
                continue;
            }
            if (bestRoute.isEmpty() || result.p95Ms() < bestP95) {
                bestRoute = entry.getKey();
                bestP95 = result.p95Ms();
            }
        }
        return new WebBenchmarkReport.PerformanceMatrixRow(
            key.datasetId(),
            key.datasetName(),
            key.querySet(),
            key.queryName(),
            routes,
            bestRoute,
            bestP95
        );
    }

    private String status(BenchmarkReport.QuerySummary query) {
        return query.success() && query.failures() == 0 ? "SUCCESS" : "FAILED";
    }

    private String normalizeRoute(String engine) {
        if (engine == null) {
            return "";
        }
        String normalized = engine.toLowerCase(Locale.ROOT);
        if (normalized.contains("spark") && normalized.contains("iceberg")) {
            return "spark_iceberg";
        }
        if (normalized.contains("external") && normalized.contains("iceberg")) {
            return "starrocks_external_iceberg";
        }
        if (normalized.contains("starrocks") && normalized.contains("internal")) {
            return "starrocks_internal";
        }
        return normalized;
    }

    private String datasetId(BenchmarkReport report) {
        return report.suite() == null || report.suite().isBlank() ? "default" : report.suite();
    }

    private String datasetName(BenchmarkReport report) {
        String datasetId = datasetId(report);
        if ("default".equals(datasetId)) {
            return "Default Dataset";
        }
        if ("tpch".equals(datasetId)) {
            return "TPC-H SF 0.01";
        }
        String profile = report.profile() == null || report.profile().isBlank() ? "" : report.profile();
        if ("kpi".equals(datasetId)) {
            return profile.isEmpty() ? "KPI" : "KPI " + profile;
        }
        return profile.isEmpty() ? datasetId : datasetId + " " + profile;
    }

    private record MatrixKey(String datasetId, String datasetName, String querySet, String queryName) {}

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

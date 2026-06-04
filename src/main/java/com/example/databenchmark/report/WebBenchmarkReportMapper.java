package com.example.databenchmark.report;

import com.example.databenchmark.runner.RoutePhase;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class WebBenchmarkReportMapper {
    private static final int SCHEMA_VERSION = 3;
    private static final List<String> ROUTES = List.of(
        "spark_iceberg",
        "starrocks_internal",
        "starrocks_external_iceberg",
        "hive_hdfs_parquet"
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
                normalizeRouteOrEngine(query.engine(), query.tableShape()),
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
        Map<MatrixKey, Map<String, List<BenchmarkReport.QuerySummary>>> grouped = new LinkedHashMap<>();
        String datasetId = datasetId(report);
        String datasetName = datasetName(report);
        for (BenchmarkReport.QuerySummary query : report.querySummaries()) {
            String route = normalizeRoute(query.engine(), query.tableShape());
            if (!ROUTES.contains(route)) {
                continue;
            }
            MatrixKey key = new MatrixKey(datasetId, datasetName, report.querySet(), query.queryName());
            grouped.computeIfAbsent(key, ignored -> new LinkedHashMap<>())
                .computeIfAbsent(route, ignored -> new ArrayList<>())
                .add(query);
        }
        return grouped.entrySet().stream()
            .map(entry -> matrixRow(entry.getKey(), aggregateRoutes(entry.getValue())))
            .toList();
    }

    private Map<String, WebBenchmarkReport.RouteResult> aggregateRoutes(
        Map<String, List<BenchmarkReport.QuerySummary>> grouped
    ) {
        Map<String, WebBenchmarkReport.RouteResult> routes = new LinkedHashMap<>();
        for (String route : ROUTES) {
            routes.put(route, routeResult(grouped.getOrDefault(route, List.of())));
        }
        return routes;
    }

    private WebBenchmarkReport.RouteResult routeResult(List<BenchmarkReport.QuerySummary> samples) {
        if (samples.isEmpty()) {
            return skippedRouteResult();
        }

        Map<RoutePhase, BenchmarkReport.QuerySummary> byPhase = new EnumMap<>(RoutePhase.class);
        for (BenchmarkReport.QuerySummary sample : samples) {
            RoutePhase phase = phase(sample.phase());
            if (phase != null) {
                byPhase.put(phase, sample);
            }
        }
        if (byPhase.isEmpty()) {
            return skippedRouteResult();
        }

        String coldStatus = phaseStatus(byPhase.get(RoutePhase.COLD));
        String warmStatus = phaseStatus(byPhase.get(RoutePhase.WARM));
        String hotStatus = phaseStatus(byPhase.get(RoutePhase.HOT));
        boolean hasFailure = byPhase.values().stream().anyMatch(sample -> !"SUCCESS".equals(status(sample)));
        String routeStatus = hasFailure ? "FAILED" : "SUCCESS";
        return new WebBenchmarkReport.RouteResult(
            routeStatus,
            phaseMillis(byPhase.get(RoutePhase.COLD)),
            phaseMillis(byPhase.get(RoutePhase.WARM)),
            phaseMillis(byPhase.get(RoutePhase.HOT)),
            coldStatus,
            warmStatus,
            hotStatus,
            rows(byPhase),
            error(byPhase)
        );
    }

    private WebBenchmarkReport.RouteResult skippedRouteResult() {
        return new WebBenchmarkReport.RouteResult("SKIPPED", 0, 0, 0, "SKIPPED", "SKIPPED", "SKIPPED", 0, "");
    }

    private WebBenchmarkReport.PerformanceMatrixRow matrixRow(
        MatrixKey key,
        Map<String, WebBenchmarkReport.RouteResult> routes
    ) {
        String bestRoute = "";
        double bestHotMs = 0;
        for (Map.Entry<String, WebBenchmarkReport.RouteResult> entry : routes.entrySet()) {
            WebBenchmarkReport.RouteResult result = entry.getValue();
            if (!"SUCCESS".equals(result.status()) || !"SUCCESS".equals(result.hotStatus())) {
                continue;
            }
            if (bestRoute.isEmpty() || result.hotMs() < bestHotMs) {
                bestRoute = entry.getKey();
                bestHotMs = result.hotMs();
            }
        }
        return new WebBenchmarkReport.PerformanceMatrixRow(
            key.datasetId(),
            key.datasetName(),
            key.querySet(),
            key.queryName(),
            routes,
            bestRoute,
            bestHotMs
        );
    }

    private String status(BenchmarkReport.QuerySummary query) {
        return query.success() && query.failures() == 0 ? "SUCCESS" : "FAILED";
    }

    private String phaseStatus(BenchmarkReport.QuerySummary query) {
        return query == null ? "SKIPPED" : status(query);
    }

    private double phaseMillis(BenchmarkReport.QuerySummary query) {
        return query == null ? 0 : query.p95Ms();
    }

    private long rows(Map<RoutePhase, BenchmarkReport.QuerySummary> byPhase) {
        BenchmarkReport.QuerySummary hot = byPhase.get(RoutePhase.HOT);
        if (hot != null) {
            return hot.rows();
        }
        return byPhase.values().stream()
            .mapToLong(BenchmarkReport.QuerySummary::rows)
            .filter(rows -> rows > 0)
            .findFirst()
            .orElse(0);
    }

    private String error(Map<RoutePhase, BenchmarkReport.QuerySummary> byPhase) {
        return byPhase.values().stream()
            .map(BenchmarkReport.QuerySummary::error)
            .filter(error -> error != null && !error.isBlank())
            .findFirst()
            .orElse("");
    }

    private RoutePhase phase(String phase) {
        if (phase == null || phase.isBlank()) {
            return RoutePhase.HOT;
        }
        try {
            return RoutePhase.valueOf(phase.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    private String normalizeRouteOrEngine(String engine, String tableShape) {
        String route = normalizeRoute(engine, tableShape);
        if (!route.isEmpty()) {
            return route;
        }
        return engine == null ? "" : engine.toLowerCase(Locale.ROOT);
    }

    private String normalizeRoute(String engine) {
        String route = normalizeRouteValue(engine);
        if (!route.isEmpty()) {
            return route;
        }
        return engine == null ? "" : engine.toLowerCase(Locale.ROOT);
    }

    private String normalizeRoute(String engine, String tableShape) {
        String route = normalizeRouteValue(tableShape);
        if (!route.isEmpty()) {
            return route;
        }
        return normalizeRouteValue(engine);
    }

    private String normalizeRouteValue(String value) {
        if (value == null) {
            return "";
        }
        String normalized = value.toLowerCase(Locale.ROOT);
        if (ROUTES.contains(normalized)) {
            return normalized;
        }
        if (normalized.contains("spark") && normalized.contains("iceberg")) {
            return "spark_iceberg";
        }
        if (normalized.contains("external") && normalized.contains("iceberg")) {
            return "starrocks_external_iceberg";
        }
        if (normalized.contains("starrocks") && normalized.contains("internal")) {
            return "starrocks_internal";
        }
        if (normalized.contains("hive") && (normalized.contains("parquet") || normalized.contains("hdfs"))) {
            return "hive_hdfs_parquet";
        }
        return "";
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

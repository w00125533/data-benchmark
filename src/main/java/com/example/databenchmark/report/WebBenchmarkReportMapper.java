package com.example.databenchmark.report;

import com.example.databenchmark.engine.SqlRenderer;
import com.example.databenchmark.runner.RoutePhase;
import com.example.databenchmark.schema.KpiTableNaming;
import com.example.databenchmark.tpch.TpchQueryCatalog;
import com.example.databenchmark.tpch.TpchSchema;
import com.example.databenchmark.tpch.TpchSqlRenderer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class WebBenchmarkReportMapper {
    private static final int SCHEMA_VERSION = 3;
    private static final List<String> ROUTES = List.of(
        "spark_native_parquet",
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
            tableRuntimeInfos(report),
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
            .map(query -> {
                String route = normalizeRouteOrEngine(query.engine(), query.tableShape());
                return new WebBenchmarkReport.QuerySummary(
                    datasetId,
                    datasetName,
                    report.querySet(),
                    route,
                    query.tableShape(),
                    query.queryName(),
                    query.phase(),
                    query.p50Ms(),
                    query.p95Ms(),
                    query.p99Ms(),
                    query.p95Ms(),
                    query.rows(),
                    status(query),
                    query.error()
                );
            })
            .toList();
    }

    private List<WebBenchmarkReport.TableRuntimeInfo> tableRuntimeInfos(BenchmarkReport report) {
        List<WebBenchmarkReport.TableRuntimeInfo> infos = new ArrayList<>();
        if (report.tableRuntimeInfos() == null) {
            return infos;
        }
        for (BenchmarkReport.TableRuntimeInfo info : report.tableRuntimeInfos()) {
            String route = normalizeRoute(info.route(), info.tableShape());
            if (!route.isEmpty()) {
                infos.add(toWebTableRuntimeInfo(route, info));
            }
        }
        return infos;
    }

    private WebBenchmarkReport.TableRuntimeInfo toWebTableRuntimeInfo(String route, BenchmarkReport.TableRuntimeInfo info) {
        return new WebBenchmarkReport.TableRuntimeInfo(
            route,
            info.displayName(),
            info.tableShape(),
            info.tableIdentifier(),
            info.storageType(),
            info.location(),
            info.format(),
            info.columns(),
            info.partitioning(),
            info.bucketingOrDistribution(),
            info.indexes(),
            info.snapshotOrVersion(),
            info.fileCount(),
            info.tabletCount(),
            info.rowsetCount(),
            info.segmentCount(),
            info.totalBytes(),
            info.rawDetails(),
            info.success(),
            info.error()
        );
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
            .map(entry -> matrixRow(report.profile(), entry.getKey(), aggregateRoutes(entry.getValue())))
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
        String profile,
        MatrixKey key,
        Map<String, WebBenchmarkReport.RouteResult> routes
    ) {
        routes = markRowCountMismatches(routes);
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
            sqlByRoute(profile, key.datasetId(), key.queryName()),
            routes,
            bestRoute,
            bestHotMs
        );
    }

    private Map<String, WebBenchmarkReport.RouteResult> markRowCountMismatches(
        Map<String, WebBenchmarkReport.RouteResult> routes
    ) {
        Map<Long, Long> rowFrequencies = new LinkedHashMap<>();
        for (WebBenchmarkReport.RouteResult result : routes.values()) {
            if ("SUCCESS".equals(result.status()) && result.rows() > 0) {
                rowFrequencies.merge(result.rows(), 1L, Long::sum);
            }
        }
        if (rowFrequencies.size() <= 1) {
            return routes;
        }

        long expectedRows = rowFrequencies.entrySet().stream()
            .max(Map.Entry.<Long, Long>comparingByValue().thenComparing(Map.Entry.comparingByKey()))
            .map(Map.Entry::getKey)
            .orElse(0L);
        Map<String, WebBenchmarkReport.RouteResult> checked = new LinkedHashMap<>();
        for (Map.Entry<String, WebBenchmarkReport.RouteResult> entry : routes.entrySet()) {
            WebBenchmarkReport.RouteResult result = entry.getValue();
            if (!"SUCCESS".equals(result.status()) || result.rows() == expectedRows || result.rows() <= 0) {
                checked.put(entry.getKey(), result);
                continue;
            }
            String error = firstNonBlank(
                result.error(),
                "row count differs from comparable routes: expected=" + expectedRows + " actual=" + result.rows()
            );
            checked.put(entry.getKey(), new WebBenchmarkReport.RouteResult(
                "FAILED",
                result.coldMs(),
                result.warmMs(),
                result.hotMs(),
                "FAILED",
                "FAILED",
                "FAILED",
                result.rows(),
                error
            ));
        }
        return checked;
    }

    private Map<String, String> sqlByRoute(String profile, String datasetId, String queryName) {
        Map<String, String> sqlByRoute = new LinkedHashMap<>();
        for (String route : ROUTES) {
            sqlByRoute.put(route, renderSql(profile, datasetId, queryName, route));
        }
        return sqlByRoute;
    }

    private String renderSql(String profile, String datasetId, String queryName, String route) {
        try {
            if ("tpch".equals(datasetId)) {
                return TpchSqlRenderer.render(queryName, route);
            }
            if ("kpi".equals(datasetId)) {
                return SqlRenderer.render(queryName, route, KpiTableNaming.tableShapesForProfile(profile));
            }
        } catch (IllegalArgumentException exception) {
            return "";
        }
        return "";
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
        if ("tpch_iceberg".equals(normalized)) {
            return "spark_iceberg";
        }
        if ("tpch_internal".equals(normalized)) {
            return "starrocks_internal";
        }
        if ("tpch_external_iceberg".equals(normalized)) {
            return "starrocks_external_iceberg";
        }
        if (normalized.contains("spark") && normalized.contains("native") && normalized.contains("parquet")) {
            return "spark_native_parquet";
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
        if (!report.fullProfile() && "kpi".equals(datasetId(report))) {
            notices.add(reducedKpiDatasetNotice(report));
        }
        if ("tpch".equals(report.suite())) {
            notices.add("TPC-H smoke data is compatible test data, not an official TPC-H benchmark result.");
        }
        return notices;
    }

    private String reducedKpiDatasetNotice(BenchmarkReport report) {
        long theoreticalRows = report.cells() > 0 && report.days() > 0
            ? (long) report.cells() * report.days() * 24L * 60L
            : 0L;
        if (theoreticalRows <= 0) {
            return "Reduced KPI validation dataset: this run generated " + formatRows(report.rows()) + " rows.";
        }
        return "Reduced KPI validation dataset: "
            + formatRows(report.cells()) + " cells * "
            + formatRows(report.days()) + " " + (report.days() == 1 ? "day" : "days")
            + " = " + formatRows(theoreticalRows) + " theoretical rows; this run generated "
            + formatRows(report.rows()) + " rows.";
    }

    private static String formatRows(long value) {
        return String.format(Locale.ROOT, "%,d", value);
    }

    private static String firstNonBlank(String first, String fallback) {
        return first == null || first.isBlank() ? fallback : first;
    }
}

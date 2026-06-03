package com.example.databenchmark.report;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public record BenchmarkReport(
    String runId,
    String profile,
    String startedAt,
    String endedAt,
    int cells,
    int days,
    long rows,
    int columns,
    long bytesWritten,
    List<LoadSummary> loadSummaries,
    List<QuerySummary> querySummaries,
    String grafanaUrl,
    boolean fullProfile
) {
    public static BenchmarkReport sample(String runId) {
        return new BenchmarkReport(
            runId,
            "smoke",
            "2026-06-02T00:00:00Z",
            "2026-06-02T00:01:00Z",
            10,
            1,
            14_400L,
            50,
            2_048L,
            List.of(new LoadSummary("local", "generated_parquet", "generate", 14_400L, 2_048L, 1.2, true, "")),
            List.of(new QuerySummary(
                "spark_iceberg",
                "iceberg_db.cell_kpi_1min",
                "topn_high_load_cells",
                10.0,
                12.0,
                15.0,
                100L,
                0,
                true,
                ""
            )),
            "http://localhost:3000/d/benchmark?var-run_id=" + URLEncoder.encode(runId, StandardCharsets.UTF_8),
            false
        );
    }

    public String status() {
        boolean allLoadsSuccessful = loadSummaries.stream().allMatch(LoadSummary::success);
        boolean allQueriesSuccessful = querySummaries.stream().allMatch(QuerySummary::success);
        return allLoadsSuccessful && allQueriesSuccessful ? "SUCCESS" : "DEGRADED";
    }

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
        String engine,
        String tableShape,
        String queryName,
        double p50Ms,
        double p95Ms,
        double p99Ms,
        long rows,
        int failures,
        boolean success,
        String error
    ) {}
}

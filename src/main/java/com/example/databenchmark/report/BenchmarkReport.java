package com.example.databenchmark.report;

import com.example.databenchmark.runner.RoutePhase;
import java.time.Duration;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.List;

public record BenchmarkReport(
    String runId,
    String profile,
    String suite,
    String querySet,
    String startedAt,
    String endedAt,
    int cells,
    int days,
    long rows,
    int columns,
    long bytesWritten,
    List<LoadSummary> loadSummaries,
    List<QuerySummary> querySummaries,
    List<TableRuntimeInfo> tableRuntimeInfos,
    boolean fullProfile
) {
    public BenchmarkReport(
        String runId,
        String profile,
        String suite,
        String querySet,
        String startedAt,
        String endedAt,
        int cells,
        int days,
        long rows,
        int columns,
        long bytesWritten,
        List<LoadSummary> loadSummaries,
        List<QuerySummary> querySummaries,
        boolean fullProfile
    ) {
        this(
            runId,
            profile,
            suite,
            querySet,
            startedAt,
            endedAt,
            cells,
            days,
            rows,
            columns,
            bytesWritten,
            loadSummaries,
            querySummaries,
            List.of(),
            fullProfile
        );
    }

    public static BenchmarkReport sample(String runId) {
        return new BenchmarkReport(
            runId,
            "smoke",
            "kpi",
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
                RoutePhase.HOT.name(),
                10.0,
                12.0,
                15.0,
                100L,
                0,
                true,
                ""
            )),
            List.of(),
            false
        );
    }

    public String status() {
        if (loadSummaries.isEmpty() || querySummaries.isEmpty()) {
            return "DEGRADED";
        }
        boolean allLoadsSuccessful = loadSummaries.stream().allMatch(LoadSummary::success);
        boolean allQueriesSuccessful = querySummaries.stream()
            .allMatch(query -> query.success() && query.failures() == 0);
        return allLoadsSuccessful && allQueriesSuccessful ? "SUCCESS" : "DEGRADED";
    }

    public double durationSeconds() {
        if (startedAt == null || endedAt == null) {
            return 0.0;
        }
        try {
            long millis = Duration.between(Instant.parse(startedAt), Instant.parse(endedAt)).toMillis();
            return millis < 0 ? 0.0 : millis / 1000.0;
        } catch (DateTimeException exception) {
            return 0.0;
        }
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

    public record TableRuntimeInfo(
        String route,
        String displayName,
        String tableShape,
        String tableIdentifier,
        String storageType,
        String location,
        String format,
        int columns,
        String partitioning,
        String bucketingOrDistribution,
        String indexes,
        String snapshotOrVersion,
        long fileCount,
        long tabletCount,
        long rowsetCount,
        long segmentCount,
        long totalBytes,
        String rawDetails,
        boolean success,
        String error
    ) {
        public TableRuntimeInfo(
            String route,
            String displayName,
            String tableShape,
            String tableIdentifier,
            String storageType,
            String location,
            String format,
            int columns,
            String partitioning,
            String bucketingOrDistribution,
            String indexes,
            String snapshotOrVersion,
            long fileCount,
            long totalBytes,
            String rawDetails,
            boolean success,
            String error
        ) {
            this(
                route,
                displayName,
                tableShape,
                tableIdentifier,
                storageType,
                location,
                format,
                columns,
                partitioning,
                bucketingOrDistribution,
                indexes,
                snapshotOrVersion,
                fileCount,
                0L,
                0L,
                0L,
                totalBytes,
                rawDetails,
                success,
                error
            );
        }
    }

    public record QuerySummary(
        String engine,
        String tableShape,
        String queryName,
        String phase,
        double p50Ms,
        double p95Ms,
        double p99Ms,
        long rows,
        int failures,
        boolean success,
        String error
    ) {
        public QuerySummary {
            phase = phase == null || phase.isBlank() ? RoutePhase.HOT.name() : phase;
        }

        public QuerySummary(
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
        ) {
            this(
                engine,
                tableShape,
                queryName,
                RoutePhase.HOT.name(),
                p50Ms,
                p95Ms,
                p99Ms,
                rows,
                failures,
                success,
                error
            );
        }
    }
}

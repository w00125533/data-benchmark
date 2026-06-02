package com.example.databenchmark.metrics;

import java.util.List;

public final class BenchmarkMetrics {
    public static final String LOAD_DURATION_SECONDS = "benchmark_load_duration_seconds";
    public static final String LOAD_ROWS_TOTAL = "benchmark_load_rows_total";
    public static final String LOAD_BYTES_TOTAL = "benchmark_load_bytes_total";
    public static final String QUERY_DURATION_SECONDS = "benchmark_query_duration_seconds";
    public static final String QUERY_ROWS_TOTAL = "benchmark_query_rows_total";
    public static final String QUERY_FAILURES_TOTAL = "benchmark_query_failures_total";

    public static final List<String> LABELS = List.of(
        "run_id",
        "profile",
        "engine",
        "table_shape",
        "stage",
        "query_name"
    );

    private BenchmarkMetrics() {}
}

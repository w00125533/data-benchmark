package com.example.databenchmark.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
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
        "suite",
        "query_set",
        "engine",
        "table_shape",
        "stage",
        "query_name"
    );

    private BenchmarkMetrics() {}

    public static void recordLoad(
        MeterRegistry registry,
        String runId,
        String profile,
        String suite,
        String querySet,
        String engine,
        String tableShape,
        String stage,
        long rows,
        long bytes,
        double durationSeconds
    ) {
        requireNonNegative("rows", rows);
        requireNonNegative("bytes", bytes);
        requireNonNegative("durationSeconds", durationSeconds);

        Tags tags = tags(runId, profile, suite, querySet, engine, tableShape, stage, "");
        registry.timer(LOAD_DURATION_SECONDS, tags).record(duration(durationSeconds));
        registry.counter(LOAD_ROWS_TOTAL, tags).increment(rows);
        registry.counter(LOAD_BYTES_TOTAL, tags).increment(bytes);
    }

    public static void recordQuery(
        MeterRegistry registry,
        String runId,
        String profile,
        String suite,
        String querySet,
        String engine,
        String tableShape,
        String stage,
        String queryName,
        long rows,
        int failures,
        double durationSeconds
    ) {
        requireNonNegative("rows", rows);
        requireNonNegative("failures", failures);
        requireNonNegative("durationSeconds", durationSeconds);

        Tags tags = tags(runId, profile, suite, querySet, engine, tableShape, stage, queryName);
        registry.timer(QUERY_DURATION_SECONDS, tags).record(duration(durationSeconds));
        registry.counter(QUERY_ROWS_TOTAL, tags).increment(rows);
        registry.counter(QUERY_FAILURES_TOTAL, tags).increment(failures);
    }

    private static Tags tags(
        String runId,
        String profile,
        String suite,
        String querySet,
        String engine,
        String tableShape,
        String stage,
        String queryName
    ) {
        return Tags.of(
            "run_id", value(runId),
            "profile", value(profile),
            "suite", value(suite),
            "query_set", value(querySet),
            "engine", value(engine),
            "table_shape", value(tableShape),
            "stage", value(stage),
            "query_name", value(queryName)
        );
    }

    private static Duration duration(double durationSeconds) {
        return Duration.ofNanos(Math.round(durationSeconds * 1_000_000_000.0));
    }

    private static void requireNonNegative(String field, long value) {
        if (value < 0) {
            throw new IllegalArgumentException(field + " must be non-negative");
        }
    }

    private static void requireNonNegative(String field, double value) {
        if (value < 0) {
            throw new IllegalArgumentException(field + " must be non-negative");
        }
    }

    private static String value(String value) {
        return value == null ? "" : value;
    }
}

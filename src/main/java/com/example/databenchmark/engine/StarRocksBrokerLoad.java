package com.example.databenchmark.engine;

import java.time.Duration;

public final class StarRocksBrokerLoad {
    public static final Duration DEFAULT_TIMEOUT = Duration.ofHours(6);

    private StarRocksBrokerLoad() {}

    public static String label(String runId) {
        String sanitized = runId == null ? "run" : runId.replaceAll("[^A-Za-z0-9_]", "_");
        return "kpi_" + (sanitized.isBlank() ? "run" : sanitized) + "_" + System.currentTimeMillis();
    }

    public static String normalizeHdfsParquetGlob(String outputPath) {
        String normalized = outputPath.replace('\\', '/');
        if (normalized.startsWith("hdfs://")) {
            return appendParquetGlob(normalized);
        }
        if (normalized.startsWith("/")) {
            return appendParquetGlob("hdfs://hdfs-namenode:8020" + normalized);
        }
        return appendParquetGlob("hdfs://hdfs-namenode:8020/" + normalized);
    }

    private static String appendParquetGlob(String path) {
        return path.endsWith("/") ? path + "*.parquet" : path + "/*.parquet";
    }

    public record LoadState(String state, long sinkRows, String errorMessage) {
        public boolean finished() {
            return "FINISHED".equalsIgnoreCase(state);
        }

        public boolean cancelled() {
            return "CANCELLED".equalsIgnoreCase(state);
        }
    }
}

package com.example.databenchmark.engine;

public final class HdfsReplication {
    private static final int DEFAULT_REPLICATION = 1;
    private static final int MAX_REPLICATION = 2;
    private static final String PROPERTY = "benchmark.hdfs.replication";
    private static final String ENVIRONMENT = "BENCHMARK_HDFS_REPLICATION";

    private HdfsReplication() {}

    public static int configured() {
        String propertyValue = System.getProperty(PROPERTY);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return configured(propertyValue);
        }
        return configured(System.getenv(ENVIRONMENT));
    }

    public static String sparkConf() {
        return "spark.hadoop.dfs.replication=" + configured();
    }

    public static String hadoopGenericOption() {
        return "-Ddfs.replication=" + configured();
    }

    static int configured(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT_REPLICATION;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            return Math.min(Math.max(parsed, DEFAULT_REPLICATION), MAX_REPLICATION);
        } catch (NumberFormatException exception) {
            return DEFAULT_REPLICATION;
        }
    }
}

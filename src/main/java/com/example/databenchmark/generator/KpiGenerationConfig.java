package com.example.databenchmark.generator;

import com.example.databenchmark.config.BenchmarkConfig;
import java.net.URI;
import java.nio.file.Path;
import java.time.LocalDateTime;

public record KpiGenerationConfig(
    BenchmarkConfig benchmarkConfig,
    long targetRows,
    String master,
    int partitions,
    long rowsPerPartition,
    String outputMode
) {
    private static final long DEFAULT_ROWS_PER_PARTITION = 1_000_000L;

    public static KpiGenerationConfig from(BenchmarkConfig config) {
        int cells = config.dataset().cells();
        int days = config.dataset().days();
        if (cells <= 0) {
            throw new IllegalArgumentException("cells must be positive");
        }
        if (days <= 0) {
            throw new IllegalArgumentException("days must be positive");
        }

        long fullRows = (long) cells * days * 24L * 60L;
        Long rowCap = config.dataset().rowCap();
        long targetRows = rowCap == null ? fullRows : Math.min(fullRows, rowCap);
        if (targetRows <= 0) {
            throw new IllegalArgumentException("targetRows must be positive");
        }

        BenchmarkConfig.DatasetSparkConfig spark = config.dataset().spark();
        String master = spark != null && spark.master() != null && !spark.master().isBlank()
            ? spark.master()
            : "local[*]";
        long rowsPerPartition = spark != null && spark.rowsPerPartition() != null
            ? spark.rowsPerPartition()
            : DEFAULT_ROWS_PER_PARTITION;
        if (rowsPerPartition <= 0) {
            throw new IllegalArgumentException("rowsPerPartition must be positive");
        }

        int partitions = spark != null && spark.partitions() != null
            ? spark.partitions()
            : (int) Math.max(1L, (targetRows + rowsPerPartition - 1L) / rowsPerPartition);
        if (partitions <= 0) {
            throw new IllegalArgumentException("partitions must be positive");
        }

        String outputMode = spark != null && spark.outputMode() != null && !spark.outputMode().isBlank()
            ? spark.outputMode()
            : "overwrite";

        return new KpiGenerationConfig(config, targetRows, master, partitions, rowsPerPartition, outputMode);
    }

    public int cells() {
        return benchmarkConfig.dataset().cells();
    }

    public long seed() {
        return benchmarkConfig.seed();
    }

    public LocalDateTime startTime() {
        return LocalDateTime.parse(benchmarkConfig.dataset().startTime());
    }

    public Path outputPath() {
        String output = output();
        if (isHdfsUri(output)) {
            return Path.of(URI.create(output).getPath());
        }
        return Path.of(output);
    }

    public String output() {
        return benchmarkConfig.dataset().output();
    }

    private static boolean isHdfsUri(String output) {
        return output.replace('\\', '/').startsWith("hdfs://");
    }
}

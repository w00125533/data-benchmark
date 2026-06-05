package com.example.databenchmark.config;

import java.util.List;

public record BenchmarkConfig(
    String profile,
    long seed,
    SuiteConfig suite,
    DatasetConfig dataset,
    QueryConfig query,
    ReportConfig report
) {
    public static BenchmarkConfig defaultSmoke() {
        return new BenchmarkConfig(
            "smoke",
            20260602L,
            SuiteConfig.defaultSuite(),
            new DatasetConfig(10_000, 1, 50, "2026-01-01T00:00:00", "data/generated", 10_000L, null),
            new QueryConfig(1, 3, 1, null),
            new ReportConfig("html", "reports/runs")
        );
    }

    public BenchmarkConfig withOverrides(Integer cells, Integer days, Long seed, String output, Long rowCap) {
        requirePositive(cells, "cells");
        requirePositive(days, "days");
        requirePositive(seed, "seed");
        requireNonBlank(output, "output");
        requirePositive(rowCap, "rowCap");

        DatasetConfig current = dataset;
        DatasetConfig updatedDataset = new DatasetConfig(
            cells == null ? current.cells() : cells,
            days == null ? current.days() : days,
            current.columns(),
            current.startTime(),
            output == null ? current.output() : output,
            rowCap == null ? current.rowCap() : rowCap,
            current.spark()
        );

        return new BenchmarkConfig(
            profile,
            seed == null ? this.seed : seed,
            suite,
            updatedDataset,
            query,
            report
        );
    }

    public BenchmarkConfig withRowCap(long rowCap) {
        if (rowCap <= 0) {
            throw new IllegalArgumentException("rowCap must be positive");
        }
        return withDataset(dataset.withRowCap(rowCap));
    }

    public BenchmarkConfig withoutRowCap() {
        return withDataset(dataset.withRowCap(null));
    }

    private BenchmarkConfig withDataset(DatasetConfig dataset) {
        return new BenchmarkConfig(profile, seed, suite, dataset, query, report);
    }

    public record SuiteConfig(String name, java.math.BigDecimal scaleFactor, String querySet) {
        public static SuiteConfig defaultSuite() {
            return new SuiteConfig("kpi", new java.math.BigDecimal("0.01"), "smoke");
        }
    }

    private static void requirePositive(Number value, String field) {
        if (value != null && value.longValue() <= 0) {
            throw new IllegalArgumentException(field + " must be positive");
        }
    }

    private static void requireNonBlank(String value, String field) {
        if (value != null && value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }

    public record DatasetConfig(
        int cells,
        int days,
        int columns,
        String startTime,
        String output,
        Long rowCap,
        DatasetSparkConfig spark
    ) {
        private DatasetConfig withRowCap(Long rowCap) {
            return new DatasetConfig(cells, days, columns, startTime, output, rowCap, spark);
        }
    }

    public record DatasetSparkConfig(
        String master,
        Integer partitions,
        Long rowsPerPartition,
        String outputMode
    ) {}

    public record QueryConfig(int coldRuns, int warmRuns, int concurrency, List<String> names) {}

    public record ReportConfig(String format, String output) {}
}

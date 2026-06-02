package com.example.databenchmark.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;

public class BenchmarkConfigLoader {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory()).findAndRegisterModules();

    public BenchmarkConfig load(Path path) throws IOException {
        BenchmarkConfig config = mapper.readValue(path.toFile(), BenchmarkConfig.class);
        validate(config);
        return config;
    }

    private static void validate(BenchmarkConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        requireNonBlank(config.profile(), "profile");
        requirePositive(config.seed(), "seed");
        validateDataset(config.dataset());
        validateQuery(config.query());
        validateReport(config.report());
        if (config.monitoring() == null) {
            throw new IllegalArgumentException("monitoring must not be null");
        }
    }

    private static void validateDataset(BenchmarkConfig.DatasetConfig dataset) {
        if (dataset == null) {
            throw new IllegalArgumentException("dataset must not be null");
        }
        requirePositive(dataset.cells(), "dataset.cells");
        requirePositive(dataset.days(), "dataset.days");
        requirePositive(dataset.columns(), "dataset.columns");
        requireNonBlank(dataset.startTime(), "dataset.startTime");
        requireNonBlank(dataset.output(), "dataset.output");
        if (dataset.rowCap() != null && dataset.rowCap() <= 0) {
            throw new IllegalArgumentException("rowCap must be positive when set");
        }
    }

    private static void validateQuery(BenchmarkConfig.QueryConfig query) {
        if (query == null) {
            throw new IllegalArgumentException("query must not be null");
        }
        requireNonNegative(query.coldRuns(), "query.coldRuns");
        requireNonNegative(query.warmRuns(), "query.warmRuns");
        requirePositive(query.concurrency(), "query.concurrency");
    }

    private static void validateReport(BenchmarkConfig.ReportConfig report) {
        if (report == null) {
            throw new IllegalArgumentException("report must not be null");
        }
        requireNonBlank(report.format(), "report.format");
        requireNonBlank(report.output(), "report.output");
    }

    private static void requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }

    private static void requirePositive(long value, String field) {
        if (value <= 0) {
            throw new IllegalArgumentException(field + " must be positive");
        }
    }

    private static void requireNonNegative(int value, String field) {
        if (value < 0) {
            throw new IllegalArgumentException(field + " must be non-negative");
        }
    }
}

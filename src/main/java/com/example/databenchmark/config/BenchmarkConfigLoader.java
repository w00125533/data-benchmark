package com.example.databenchmark.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;

public class BenchmarkConfigLoader {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory()).findAndRegisterModules();

    public BenchmarkConfig load(Path path) throws IOException {
        BenchmarkConfig config = mapper.readValue(path.toFile(), BenchmarkConfig.class);
        BenchmarkConfig normalized = normalizeSuite(config);
        validate(normalized);
        return normalized;
    }

    private static BenchmarkConfig normalizeSuite(BenchmarkConfig config) {
        if (config.suite() == null) {
            return new BenchmarkConfig(
                config.profile(),
                config.seed(),
                BenchmarkConfig.SuiteConfig.defaultSuite(),
                config.dataset(),
                config.query(),
                config.report(),
                config.monitoring()
            );
        }

        BenchmarkConfig.SuiteConfig suite = config.suite();
        BenchmarkConfig.SuiteConfig defaultSuite = BenchmarkConfig.SuiteConfig.defaultSuite();
        String normalizedName = isBlank(suite.name()) ? defaultSuite.name() : suite.name();
        java.math.BigDecimal normalizedScaleFactor = suite.scaleFactor() == null ? defaultSuite.scaleFactor() : suite.scaleFactor();
        String normalizedQuerySet = isBlank(suite.querySet()) ? defaultSuite.querySet() : suite.querySet();

        return new BenchmarkConfig(
            config.profile(),
            config.seed(),
            new BenchmarkConfig.SuiteConfig(normalizedName, normalizedScaleFactor, normalizedQuerySet),
            config.dataset(),
            config.query(),
            config.report(),
            config.monitoring()
        );
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static void validate(BenchmarkConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        requireNonBlank(config.profile(), "profile");
        requirePositive(config.seed(), "seed");
        validateSuite(config.suite());
        validateDataset(config.dataset());
        validateQuery(config.query());
        validateReport(config.report());
        if (config.monitoring() == null) {
            throw new IllegalArgumentException("monitoring must not be null");
        }
    }

    private static void validateSuite(BenchmarkConfig.SuiteConfig suite) {
        if (suite == null) {
            throw new IllegalArgumentException("suite must not be null");
        }
        requireNonBlank(suite.name(), "suite.name");
        requireNonBlank(suite.querySet(), "suite.querySet");
        if (!suite.name().equals("kpi") && !suite.name().equals("tpch")) {
            throw new IllegalArgumentException("suite.name must be kpi or tpch");
        }
        if (suite.scaleFactor() == null || suite.scaleFactor().signum() <= 0) {
            throw new IllegalArgumentException("suite.scaleFactor must be positive");
        }
        if (!suite.querySet().equals("smoke") && !suite.querySet().equals("all")) {
            throw new IllegalArgumentException("suite.querySet must be smoke or all");
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

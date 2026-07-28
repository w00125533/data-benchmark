package com.example.databenchmark.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;

public class IcebergValidationConfigLoader {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory()).findAndRegisterModules();

    public IcebergValidationConfig load(Path path) throws IOException {
        IcebergValidationConfig config = mapper.readValue(path.toFile(), IcebergValidationConfig.class);
        validate(config);
        return config;
    }

    private static void validate(IcebergValidationConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (config.iceberg() == null) {
            throw new IllegalArgumentException("iceberg must not be null");
        }
        requireEquals(config.iceberg().version(), "1.10.1", "iceberg.version");
        requireNonBlank(config.iceberg().catalog(), "iceberg.catalog");
        requireNonBlank(config.iceberg().namespace(), "iceberg.namespace");
        requireNonBlank(config.iceberg().warehouse(), "iceberg.warehouse");
        if (config.iceberg().formatVersion() != 2) {
            throw new IllegalArgumentException("iceberg.formatVersion must be 2");
        }
        if (config.spark() == null) {
            throw new IllegalArgumentException("spark must not be null");
        }
        requireNonBlank(config.spark().service(), "spark.service");
        requirePositive(config.spark().timeoutSeconds(), "spark.timeoutSeconds");
        requireNotEmpty(config.spark().packages(), "spark.packages");
        requireNotEmpty(config.spark().extensions(), "spark.extensions");
        if (config.spark().packages().stream().noneMatch(value -> value.contains(":1.10.1"))) {
            throw new IllegalArgumentException("spark.packages must include Iceberg 1.10.1");
        }
        if (config.hdfs() == null) {
            throw new IllegalArgumentException("hdfs must not be null");
        }
        requireNonBlank(config.hdfs().defaultFs(), "hdfs.defaultFs");
        if (config.hdfs().replicationBaseline() != 2) {
            throw new IllegalArgumentException("hdfs.replicationBaseline must be 2");
        }
        requireNotEmpty(config.hdfs().ecPolicies(), "hdfs.ecPolicies");
        if (config.scale() == null) {
            throw new IllegalArgumentException("scale must not be null");
        }
        requireNonBlank(config.scale().profile(), "scale.profile");
        requirePositive(config.scale().rows(), "scale.rows");
        requirePositive(config.scale().partitions(), "scale.partitions");
        requirePositive(config.scale().smallFileCommits(), "scale.smallFileCommits");
        requirePositive(config.scale().filesPerCommit(), "scale.filesPerCommit");
        requireNotEmpty(config.scale().concurrentWriters(), "scale.concurrentWriters");
        if (config.scenarios() == null || config.scenarios().isEmpty()) {
            throw new IllegalArgumentException("scenarios must not be empty");
        }
        if (config.report() == null) {
            throw new IllegalArgumentException("report must not be null");
        }
        requireNonBlank(config.report().output(), "report.output");
        requireNotEmpty(config.report().formats(), "report.formats");
    }

    private static void requireEquals(String actual, String expected, String field) {
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException(field + " must be " + expected);
        }
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

    private static void requireNotEmpty(Collection<?> values, String field) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be empty");
        }
    }
}

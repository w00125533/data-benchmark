package com.example.databenchmark.generator;

import com.example.databenchmark.config.BenchmarkConfig;

public interface KpiDatasetGenerator {
    DatasetResult generate(BenchmarkConfig config) throws Exception;
}

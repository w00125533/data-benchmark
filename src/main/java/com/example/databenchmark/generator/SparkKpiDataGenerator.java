package com.example.databenchmark.generator;

import com.example.databenchmark.config.BenchmarkConfig;

public class SparkKpiDataGenerator implements KpiDatasetGenerator {
    private final SparkKpiGenerationJob job;

    public SparkKpiDataGenerator() {
        this(new SparkKpiGenerationJob());
    }

    SparkKpiDataGenerator(SparkKpiGenerationJob job) {
        this.job = job;
    }

    @Override
    public DatasetResult generate(BenchmarkConfig config) throws Exception {
        return job.run(KpiGenerationConfig.from(config));
    }
}

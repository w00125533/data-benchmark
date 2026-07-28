package com.example.databenchmark.iceberg;

import java.util.List;
import java.util.Map;

public record IcebergValidationConfig(
    IcebergConfig iceberg,
    SparkConfig spark,
    HdfsConfig hdfs,
    ScaleConfig scale,
    Map<String, ScenarioConfig> scenarios,
    ReportConfig report
) {
    public record IcebergConfig(String version, String catalog, String namespace, String warehouse, int formatVersion) {}

    public record SparkConfig(String service, int timeoutSeconds, List<String> packages, List<String> extensions) {}

    public record HdfsConfig(String defaultFs, int replicationBaseline, List<String> ecPolicies) {}

    public record ScaleConfig(
        String profile,
        long rows,
        int partitions,
        int smallFileCommits,
        int filesPerCommit,
        List<Integer> concurrentWriters
    ) {}

    public record ScenarioConfig(boolean enabled) {}

    public record ReportConfig(String output, List<String> formats) {}
}

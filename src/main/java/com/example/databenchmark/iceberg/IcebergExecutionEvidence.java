package com.example.databenchmark.iceberg;

public record IcebergExecutionEvidence(
    String phase,
    String label,
    String script,
    int exitCode,
    double durationSeconds,
    String stdout,
    String stderr
) {}

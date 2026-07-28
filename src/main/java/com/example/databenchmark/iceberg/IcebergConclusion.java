package com.example.databenchmark.iceberg;

public final class IcebergConclusion {
    private IcebergConclusion() {}

    public enum FunctionStatus {
        PASS,
        FAIL,
        SKIPPED,
        DEGRADED
    }

    public enum PerformanceStatus {
        GOOD,
        ACCEPTABLE,
        DEGRADED,
        NOT_COMPARABLE
    }

    public enum ErrorType {
        INFRA_UNAVAILABLE,
        UNSUPPORTED_FEATURE,
        ASSERTION_FAILED,
        PERFORMANCE_DEGRADED,
        COMMAND_FAILED,
        CLEANUP_FAILED
    }
}

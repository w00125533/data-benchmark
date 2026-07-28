package com.example.databenchmark.iceberg;

import java.util.List;
import java.util.Map;

public record IcebergValidationResult(
    String scenario,
    String caseId,
    String purpose,
    Map<String, String> dataScale,
    List<String> setupCommands,
    List<String> actionCommands,
    List<String> assertions,
    Map<String, String> metrics,
    Map<String, String> baseline,
    Map<String, String> comparison,
    IcebergConclusion.FunctionStatus functionStatus,
    IcebergConclusion.PerformanceStatus performanceStatus,
    String conclusion,
    List<String> evidence,
    List<String> errors
) {
    public boolean successful() {
        return functionStatus == IcebergConclusion.FunctionStatus.PASS
            || functionStatus == IcebergConclusion.FunctionStatus.DEGRADED
            || functionStatus == IcebergConclusion.FunctionStatus.SKIPPED;
    }
}

package com.example.databenchmark.iceberg;

import java.util.List;
import java.util.Map;

public final class IcebergScenarioSupport {
    private IcebergScenarioSupport() {}

    public static String tableName(IcebergValidationContext context, String scenario, String caseId) {
        return context.config().iceberg().catalog()
            + "."
            + context.config().iceberg().namespace()
            + "."
            + sanitize(scenario)
            + "_"
            + sanitize(caseId)
            + "_"
            + sanitize(context.runId());
    }

    public static String tableLocation(IcebergValidationContext context, String scenario, String caseId) {
        return context.config().iceberg().warehouse()
            + "/"
            + context.config().iceberg().namespace()
            + "/"
            + scenario
            + "/"
            + caseId
            + "/"
            + sanitize(context.runId());
    }

    public static Map<String, String> dataScale(IcebergValidationConfig config) {
        return Map.of(
            "profile", config.scale().profile(),
            "rows", Long.toString(config.scale().rows()),
            "partitions", Integer.toString(config.scale().partitions()),
            "smallFileCommits", Integer.toString(config.scale().smallFileCommits()),
            "filesPerCommit", Integer.toString(config.scale().filesPerCommit())
        );
    }

    public static IcebergValidationResult pass(
        IcebergValidationCase testCase,
        IcebergValidationContext context,
        List<String> setupCommands,
        List<String> actionCommands,
        List<String> assertions,
        Map<String, String> metrics,
        Map<String, String> baseline,
        Map<String, String> comparison,
        String conclusion,
        List<String> evidence
    ) {
        return pass(
            testCase,
            context,
            setupCommands,
            actionCommands,
            assertions,
            metrics,
            baseline,
            comparison,
            conclusion,
            evidence,
            List.of()
        );
    }

    public static IcebergValidationResult pass(
        IcebergValidationCase testCase,
        IcebergValidationContext context,
        List<String> setupCommands,
        List<String> actionCommands,
        List<String> assertions,
        Map<String, String> metrics,
        Map<String, String> baseline,
        Map<String, String> comparison,
        String conclusion,
        List<String> evidence,
        List<IcebergExecutionEvidence> executionResults
    ) {
        return new IcebergValidationResult(
            testCase.scenario(),
            testCase.caseId(),
            testCase.purpose(),
            dataScale(context.config()),
            setupCommands,
            actionCommands,
            assertions,
            metrics,
            baseline,
            comparison,
            IcebergConclusion.FunctionStatus.PASS,
            IcebergConclusion.PerformanceStatus.ACCEPTABLE,
            conclusion,
            evidence,
            List.of(),
            executionResults
        );
    }

    public static IcebergValidationResult skipped(
        IcebergValidationCase testCase,
        IcebergValidationContext context,
        String reason,
        List<String> evidence
    ) {
        return new IcebergValidationResult(
            testCase.scenario(),
            testCase.caseId(),
            testCase.purpose(),
            dataScale(context.config()),
            List.of(),
            List.of(),
            List.of("Skipped: " + reason),
            Map.of(),
            Map.of(),
            Map.of(),
            IcebergConclusion.FunctionStatus.SKIPPED,
            IcebergConclusion.PerformanceStatus.NOT_COMPARABLE,
            reason,
            evidence,
            List.of(),
            List.of()
        );
    }

    public static IcebergValidationResult fail(
        IcebergValidationCase testCase,
        IcebergValidationContext context,
        String conclusion,
        List<String> evidence,
        List<String> errors
    ) {
        return new IcebergValidationResult(
            testCase.scenario(),
            testCase.caseId(),
            testCase.purpose(),
            dataScale(context.config()),
            List.of(),
            List.of(),
            List.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            IcebergConclusion.FunctionStatus.FAIL,
            IcebergConclusion.PerformanceStatus.NOT_COMPARABLE,
            conclusion,
            evidence,
            errors,
            List.of()
        );
    }

    public static String sanitize(String value) {
        return value.replaceAll("[^a-zA-Z0-9_]", "_");
    }
}

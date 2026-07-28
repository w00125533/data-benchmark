package com.example.databenchmark.iceberg;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public class IcebergValidationRunner {
    private final List<IcebergValidationScenario> scenarios;
    private final IcebergValidationReportWriter reportWriter;

    public IcebergValidationRunner() {
        this(List.of(), new IcebergValidationReportWriter());
    }

    public IcebergValidationRunner(List<IcebergValidationScenario> scenarios, IcebergValidationReportWriter reportWriter) {
        this.scenarios = List.copyOf(scenarios);
        this.reportWriter = reportWriter;
    }

    public IcebergValidationReport run(
        IcebergValidationConfig config,
        String runId,
        List<String> scenarioFilter,
        List<String> caseFilter,
        boolean keepArtifacts
    ) throws Exception {
        String actualRunId = runId == null || runId.isBlank()
            ? "iceberg-validation-" + Instant.now().toEpochMilli()
            : runId;
        Instant started = Instant.now();
        IcebergValidationContext context = new IcebergValidationContext(
            config,
            actualRunId,
            Path.of(".").toAbsolutePath().normalize(),
            Path.of(config.report().output()),
            keepArtifacts
        );
        List<IcebergValidationResult> results = scenarios.stream()
            .filter(scenario -> scenarioFilter == null || scenarioFilter.isEmpty() || scenarioFilter.contains(scenario.name()))
            .flatMap(scenario -> scenario.cases(config).stream()
                .filter(testCase -> caseFilter == null || caseFilter.isEmpty() || caseFilter.contains(testCase.caseId()))
                .map(testCase -> runCase(scenario, testCase, context)))
            .toList();
        String status = results.stream().allMatch(IcebergValidationResult::successful) ? "SUCCESS" : "DEGRADED";
        IcebergValidationReport report = new IcebergValidationReport(
            actualRunId,
            config.iceberg().version(),
            config.scale().profile(),
            status,
            started.toString(),
            Instant.now().toString(),
            results
        );
        reportWriter.write(report, Path.of(config.report().output()));
        return report;
    }

    private static IcebergValidationResult runCase(
        IcebergValidationScenario scenario,
        IcebergValidationCase testCase,
        IcebergValidationContext context
    ) {
        try {
            return scenario.run(testCase, context);
        } catch (Exception exception) {
            return new IcebergValidationResult(
                scenario.name(),
                testCase.caseId(),
                testCase.purpose(),
                testCase.parameters(),
                List.of(),
                List.of(),
                List.of(),
                java.util.Map.of(),
                java.util.Map.of(),
                java.util.Map.of(),
                IcebergConclusion.FunctionStatus.FAIL,
                IcebergConclusion.PerformanceStatus.NOT_COMPARABLE,
                "用例执行失败。",
                List.of(),
                List.of(exception.getMessage())
            );
        }
    }
}

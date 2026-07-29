package com.example.databenchmark.iceberg.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.iceberg.IcebergConclusion;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationConfigLoader;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationReport;
import com.example.databenchmark.iceberg.IcebergValidationReportWriter;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import com.example.databenchmark.iceberg.IcebergValidationScenario;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RemainingScenarioMetricsTest {
    private static final List<String> FORBIDDEN_KEYS = List.of(
        "mutationMetrics",
        "incrementalMetrics",
        "timeTravelMetrics",
        "scriptedActions",
        "caseImplemented",
        "acidEvidence"
    );

    @TempDir
    Path tempDir;

    @Test
    void remainingScenariosExposeAuditableSkippedMetricPlansWithoutPlaceholders() throws Exception {
        IcebergValidationConfig config = config();
        IcebergValidationContext context = context(config);
        Map<String, IcebergValidationScenario> scenarios = Map.of(
            "concurrentWrite", new ConcurrentWriteScenario(),
            "rowLevelMutation", new RowLevelMutationScenario(),
            "acidTransaction", new AcidTransactionScenario(),
            "incrementalPull", new IncrementalPullScenario(),
            "timeTravel", new TimeTravelScenario(),
            "smallFileCompaction", new SmallFileCompactionScenario()
        );

        for (Map.Entry<String, IcebergValidationScenario> entry : scenarios.entrySet()) {
            List<IcebergValidationResult> results = entry.getValue().cases(config).stream()
                .map(testCase -> run(entry.getValue(), testCase, context))
                .toList();

            assertThat(results).isNotEmpty();
            assertThat(results).allSatisfy(result -> {
                assertThat(result.functionStatus()).isEqualTo(IcebergConclusion.FunctionStatus.SKIPPED);
                assertThat(result.performanceStatus()).isEqualTo(IcebergConclusion.PerformanceStatus.NOT_COMPARABLE);
                assertThat(result.metrics()).containsEntry("executed", "false");
                assertThat(result.metrics()).containsEntry("metricCollectionStatus", "notExecuted");
                assertThat(result.metrics()).containsKeys("notExecutedReason", "expectedMetricFields", "plannedActionCount");
                assertThat(result.executionResults()).isNotEmpty();
                assertThat(result.executionResults()).allSatisfy(evidence -> {
                    assertThat(evidence.phase()).isEqualTo("plan");
                    assertThat(evidence.exitCode()).isEqualTo(-1);
                    assertThat(evidence.stdout()).contains("notExecuted=true");
                });
                assertThat(result.metrics()).doesNotContainKeys(FORBIDDEN_KEYS.toArray(String[]::new));
                assertThat(result.baseline()).doesNotContainKeys(FORBIDDEN_KEYS.toArray(String[]::new));
                assertThat(result.comparison()).doesNotContainKeys(FORBIDDEN_KEYS.toArray(String[]::new));
            });
        }
    }

    @Test
    void remainingScenarioExpectedMetricFieldsAreScenarioSpecific() throws Exception {
        IcebergValidationConfig config = config();
        IcebergValidationContext context = context(config);

        assertExpectedFields(new ConcurrentWriteScenario(), config, context,
            "writerCount", "successfulCommits", "commitLatencyP95Seconds", "finalSnapshotId");
        assertExpectedFields(new RowLevelMutationScenario(), config, context,
            "operation", "affectedRows", "deleteFilesAfter", "historicalSnapshotRows");
        assertExpectedFields(new AcidTransactionScenario(), config, context,
            "snapshotBefore", "halfVisibleData", "conflictError", "postCommitSnapshotId");
        assertExpectedFields(new IncrementalPullScenario(), config, context,
            "baseSnapshotId", "incrementalRows", "savingRatio", "retentionFailureReason");
        assertExpectedFields(new TimeTravelScenario(), config, context,
            "selector", "targetSnapshotId", "historicalQuerySeconds", "expiredSnapshotUnavailable");
        assertExpectedFields(new SmallFileCompactionScenario(), config, context,
            "snapshotCountBefore", "dataFileCountBefore", "manifestCountAfter", "hdfsDiskBytesAfter",
            "planningSecondsAfter", "querySecondsAfter");
    }

    @Test
    void reportHtmlAndJsonDoNotContainRemainingPlaceholderKeys() throws Exception {
        IcebergValidationConfig config = config();
        IcebergValidationContext context = context(config);
        List<IcebergValidationResult> results = List.of(
            runFirst(new ConcurrentWriteScenario(), config, context),
            runFirst(new RowLevelMutationScenario(), config, context),
            runFirst(new AcidTransactionScenario(), config, context),
            runFirst(new IncrementalPullScenario(), config, context),
            runFirst(new TimeTravelScenario(), config, context),
            runFirst(new SmallFileCompactionScenario(), config, context)
        );
        IcebergValidationReport report = new IcebergValidationReport(
            "remaining-scenarios",
            "1.10.1",
            "smoke",
            "SUCCESS",
            "2026-07-29T00:00:00Z",
            "2026-07-29T00:00:01Z",
            results
        );

        Path htmlPath = new IcebergValidationReportWriter().write(report, tempDir);
        String html = Files.readString(htmlPath);
        String json = Files.readString(htmlPath.getParent().resolve("report.json"));

        assertThat(html).contains("expectedMetricFields").contains("SKIPPED").contains("NOT_COMPARABLE");
        assertThat(json).contains("expectedMetricFields").contains("SKIPPED").contains("NOT_COMPARABLE");
        for (String forbiddenKey : FORBIDDEN_KEYS) {
            assertThat(html).doesNotContain(forbiddenKey);
            assertThat(json).doesNotContain(forbiddenKey);
        }
    }

    private static void assertExpectedFields(
        IcebergValidationScenario scenario,
        IcebergValidationConfig config,
        IcebergValidationContext context,
        String... fields
    ) {
        List<IcebergValidationResult> results = scenario.cases(config).stream()
            .map(testCase -> run(scenario, testCase, context))
            .toList();
        assertThat(results).allSatisfy(result ->
            assertThat(result.metrics().get("expectedMetricFields")).contains(fields));
    }

    private static IcebergValidationResult runFirst(
        IcebergValidationScenario scenario,
        IcebergValidationConfig config,
        IcebergValidationContext context
    ) {
        return run(scenario, scenario.cases(config).get(0), context);
    }

    private static IcebergValidationResult run(
        IcebergValidationScenario scenario,
        com.example.databenchmark.iceberg.IcebergValidationCase testCase,
        IcebergValidationContext context
    ) {
        try {
            return scenario.run(testCase, context);
        } catch (Exception exception) {
            throw new AssertionError(exception);
        }
    }

    private static IcebergValidationContext context(IcebergValidationConfig config) {
        return new IcebergValidationContext(config, "remaining-test", Path.of("."), Path.of("reports"), false);
    }

    private static IcebergValidationConfig config() throws Exception {
        return new IcebergValidationConfigLoader().load(Path.of("configs", "iceberg-validation.yml"));
    }
}

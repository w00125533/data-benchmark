package com.example.databenchmark.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergValidationRunnerTest {
    @TempDir
    Path tempDir;

    @Test
    void appliesScenarioCaseAndEnabledFilters() throws Exception {
        RecordingScenario enabled = new RecordingScenario("enabledScenario", "case-a", "case-b");
        RecordingScenario disabled = new RecordingScenario("disabledScenario", "case-c");
        IcebergValidationConfig config = config(Map.of(
            "enabledScenario", new IcebergValidationConfig.ScenarioConfig(true),
            "disabledScenario", new IcebergValidationConfig.ScenarioConfig(false)
        ));

        IcebergValidationReport report = new IcebergValidationRunner(
            List.of(enabled, disabled),
            new IcebergValidationReportWriter()
        ).run(config, "run-1", List.of("enabledScenario", "disabledScenario"), List.of("case-b"), false);

        assertThat(report.results()).extracting(IcebergValidationResult::caseId).containsExactly("case-b");
        assertThat(enabled.executed).isEqualTo(1);
        assertThat(disabled.executed).isZero();
        assertThat(tempDir.resolve("run-1").resolve("report.html")).exists();
    }

    @Test
    void isolatesCaseFailureAndMarksSuiteDegraded() throws Exception {
        IcebergValidationConfig config = config(Map.of("boom", new IcebergValidationConfig.ScenarioConfig(true)));

        IcebergValidationReport report = new IcebergValidationRunner(
            List.of(new FailingScenario()),
            new IcebergValidationReportWriter()
        ).run(config, "run-2", List.of(), List.of(), false);

        assertThat(report.status()).isEqualTo("DEGRADED");
        assertThat(report.results()).hasSize(1);
        assertThat(report.results().get(0).functionStatus()).isEqualTo(IcebergConclusion.FunctionStatus.FAIL);
        assertThat(report.results().get(0).errors()).containsExactly("boom");
    }

    private IcebergValidationConfig config(Map<String, IcebergValidationConfig.ScenarioConfig> scenarios) throws Exception {
        IcebergValidationConfig base = new IcebergValidationConfigLoader().load(Path.of("configs", "iceberg-validation.yml"));
        return new IcebergValidationConfig(base.iceberg(), base.spark(), base.hdfs(), base.scale(), scenarios,
            new IcebergValidationConfig.ReportConfig(tempDir.toString(), base.report().formats()));
    }

    private static final class RecordingScenario implements IcebergValidationScenario {
        private final String name;
        private final List<String> caseIds;
        private int executed;

        private RecordingScenario(String name, String... caseIds) {
            this.name = name;
            this.caseIds = List.of(caseIds);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public List<IcebergValidationCase> cases(IcebergValidationConfig config) {
            return caseIds.stream()
                .map(caseId -> new IcebergValidationCase(name, caseId, "purpose", Map.of(), true))
                .toList();
        }

        @Override
        public IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) {
            executed++;
            return IcebergScenarioSupport.pass(
                testCase,
                context,
                List.of(),
                List.of(),
                List.of("ok"),
                Map.of(),
                Map.of(),
                Map.of(),
                "ok",
                List.of()
            );
        }
    }

    private static final class FailingScenario implements IcebergValidationScenario {
        @Override
        public String name() {
            return "boom";
        }

        @Override
        public List<IcebergValidationCase> cases(IcebergValidationConfig config) {
            return List.of(new IcebergValidationCase(name(), "case-fail", "purpose", Map.of(), true));
        }

        @Override
        public IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) {
            throw new IllegalStateException("boom");
        }
    }
}

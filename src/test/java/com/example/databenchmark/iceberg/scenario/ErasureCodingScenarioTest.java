package com.example.databenchmark.iceberg.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.iceberg.IcebergConclusion;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationConfigLoader;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class ErasureCodingScenarioTest {
    @Test
    void nonFaultCasesExposeConcretePlannedMetricsWithoutFakeHdfsUsage() throws Exception {
        ErasureCodingScenario scenario = new ErasureCodingScenario();
        IcebergValidationConfig config = new IcebergValidationConfigLoader().load(Path.of("configs/iceberg-validation.yml"));
        IcebergValidationContext context = new IcebergValidationContext(
            config,
            "ec-test",
            Path.of("work"),
            Path.of("reports"),
            false
        );

        List<IcebergValidationResult> results = scenario.cases(config).stream()
            .filter(testCase -> !testCase.caseId().contains("failure"))
            .map(testCase -> scenario.run(testCase, context))
            .toList();

        assertThat(results).hasSize(2);
        assertThat(results).allSatisfy(result -> {
            assertThat(result.metrics()).containsEntry("replicationBaseline", "2");
            assertThat(result.metrics()).containsEntry("ecPolicyCount", Integer.toString(config.hdfs().ecPolicies().size()));
            assertThat(result.metrics()).containsEntry("targetRowCount", "1000");
            assertThat(result.metrics()).containsEntry("targetChecksum", "planned");
            assertThat(result.metrics()).containsEntry("hdfsUsageStatus", "notCollected");
            assertThat(result.metrics()).doesNotContainKey("ecPolicies");
            assertThat(result.comparison()).doesNotContainKey("scriptedActions");
            assertThat(result.metrics()).doesNotContainKeys(
                "replicationFileCount",
                "ecFileCount",
                "replicationDiskBytes",
                "ecDiskBytes",
                "diskSavingRatio"
            );
        });
    }

    @Test
    void faultCasesExposeSkipDataNodeFactsAsMetrics() throws Exception {
        ErasureCodingScenario scenario = new ErasureCodingScenario();
        IcebergValidationConfig config = new IcebergValidationConfigLoader().load(Path.of("configs/iceberg-validation.yml"));
        IcebergValidationContext context = new IcebergValidationContext(
            config,
            "ec-test",
            Path.of("work"),
            Path.of("reports"),
            false
        );
        IcebergValidationCase testCase = scenario.cases(config).stream()
            .filter(candidate -> candidate.caseId().equals("ec-rs-10-4-failure-tolerance"))
            .findFirst()
            .orElseThrow();

        IcebergValidationResult result = scenario.run(testCase, context);

        assertThat(result.functionStatus()).isEqualTo(IcebergConclusion.FunctionStatus.SKIPPED);
        assertThat(result.metrics()).containsEntry("policy", "RS-10-4-1024k");
        assertThat(result.metrics()).containsEntry("liveDataNodes", "1");
        assertThat(result.metrics()).containsEntry("requiredDataNodes", "14");
        assertThat(result.metrics()).containsEntry(
            "skipReason",
            "RS-10-4-1024k requires at least 14 live DataNodes for full policy tolerance validation"
        );
    }
}

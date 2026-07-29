package com.example.databenchmark.iceberg.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.iceberg.IcebergConclusion;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationConfigLoader;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class ErasureCodingConversionScenarioTest {
    @Test
    void conversionCasesExposeHonestUnexecutedStatusInsteadOfPlaceholderMetrics() throws Exception {
        ErasureCodingConversionScenario scenario = new ErasureCodingConversionScenario();
        IcebergValidationConfig config = new IcebergValidationConfigLoader().load(Path.of("configs/iceberg-validation.yml"));
        IcebergValidationContext context = new IcebergValidationContext(
            config,
            "conversion-test",
            Path.of("work"),
            Path.of("reports"),
            false
        );

        List<IcebergValidationResult> results = scenario.cases(config).stream()
            .map(testCase -> scenario.run(testCase, context))
            .toList();

        assertThat(results).hasSize(4);
        assertThat(results).allSatisfy(result -> {
            assertThat(result.metrics()).containsEntry("conversionStatus", "notExecuted");
            assertThat(result.metrics()).containsEntry("hdfsUsageStatus", "notCollected");
            assertThat(result.metrics()).containsEntry("checksumStatus", "notCollected");
            assertThat(result.metrics()).containsKeys("conversionDirection", "conversionMode");
            assertThat(result.performanceStatus()).isEqualTo(IcebergConclusion.PerformanceStatus.NOT_COMPARABLE);
            assertThat(result.metrics()).doesNotContainKeys(
                "conversionMetrics",
                "conversionSeconds",
                "throughputMbPerSecond",
                "fileCountBefore",
                "fileCountAfter",
                "diskBytesBefore",
                "diskBytesAfter",
                "querySecondsBefore",
                "querySecondsAfter",
                "checksumMatched"
            );
            assertThat(result.comparison()).doesNotContainKey("scriptedActions");
        });

        IcebergValidationResult replicationToEcPolicyOnly = resultByCaseId(results, "replication-to-ec-policy-only");
        assertThat(replicationToEcPolicyOnly.metrics())
            .containsEntry("conversionDirection", "replication->ec")
            .containsEntry("conversionMode", "policy-only")
            .containsEntry("targetPolicy", "RS-10-4-1024k")
            .containsEntry("policyCommandStatus", "notExecuted");

        IcebergValidationResult replicationToEcRewrite = resultByCaseId(results, "replication-to-ec-rewrite");
        assertThat(replicationToEcRewrite.metrics())
            .containsEntry("conversionDirection", "replication->ec")
            .containsEntry("conversionMode", "physical rewrite")
            .containsEntry("targetPolicy", "RS-10-4-1024k");
        assertThat(replicationToEcRewrite.metrics()).doesNotContainKey("policyCommandStatus");

        IcebergValidationResult ecToReplicationPolicyOnly = resultByCaseId(results, "ec-to-replication-policy-only");
        assertThat(ecToReplicationPolicyOnly.metrics())
            .containsEntry("conversionDirection", "ec->replication")
            .containsEntry("conversionMode", "policy-only")
            .containsEntry("targetReplication", Integer.toString(config.hdfs().replicationBaseline()))
            .containsEntry("policyCommandStatus", "notExecuted");

        IcebergValidationResult ecToReplicationRewrite = resultByCaseId(results, "ec-to-replication-rewrite");
        assertThat(ecToReplicationRewrite.metrics())
            .containsEntry("conversionDirection", "ec->replication")
            .containsEntry("conversionMode", "physical rewrite")
            .containsEntry("targetReplication", Integer.toString(config.hdfs().replicationBaseline()));
        assertThat(ecToReplicationRewrite.metrics()).doesNotContainKey("policyCommandStatus");
    }

    private static IcebergValidationResult resultByCaseId(List<IcebergValidationResult> results, String caseId) {
        return results.stream()
            .filter(result -> caseId.equals(result.caseId()))
            .findFirst()
            .orElseThrow();
    }
}

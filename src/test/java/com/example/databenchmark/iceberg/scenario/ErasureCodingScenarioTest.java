package com.example.databenchmark.iceberg.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.iceberg.IcebergConclusion;
import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationConfigLoader;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import com.example.databenchmark.iceberg.hdfs.EcPolicySpec;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class ErasureCodingScenarioTest {
    @Test
    void ecCasesIncludeSingleReplicaBaselineAndPolicyRowsWithoutEcPolicyCount() throws Exception {
        ErasureCodingScenario scenario = new ErasureCodingScenario();
        IcebergValidationConfig config = new IcebergValidationConfigLoader().load(Path.of("configs/iceberg-validation.yml"));
        IcebergValidationContext context = new IcebergValidationContext(
            config,
            "ec-single-dn-test",
            Path.of("work"),
            Path.of("reports"),
            false
        );

        List<IcebergValidationResult> results = scenario.cases(config).stream()
            .filter(testCase -> !testCase.caseId().contains("failure"))
            .map(testCase -> scenario.run(testCase, context))
            .toList();

        assertThat(results).extracting(IcebergValidationResult::caseId)
            .containsExactly(
                "hdfs-replication-1-actual",
                "hdfs-replication-2-baseline",
                "ec-policy-rs-3-2-1024k",
                "ec-policy-rs-6-3-1024k",
                "ec-policy-rs-10-4-1024k",
                "ec-policy-xor-2-1-1024k"
            );
        assertThat(results).allSatisfy(result -> {
            assertThat(result.metrics()).containsKey("validationPoint");
            assertThat(result.metrics()).doesNotContainKey("ecPolicyCount");
        });
        IcebergValidationResult singleReplica = resultByCaseId(results, "hdfs-replication-1-actual");
        assertThat(singleReplica.metrics()).containsEntry("policyMode", "hdfs-replication-1-actual");
        assertThat(singleReplica.metrics()).containsEntry("storageMetricType", "actual");
        assertThat(singleReplica.metrics()).containsEntry("replication", "1");
        assertThat(singleReplica.metrics()).containsEntry("underReplicated", "false");
        assertThat(singleReplica.metrics()).containsEntry("liveDataNodes", "1");
        assertThat(singleReplica.metrics()).containsEntry("requiredDataNodes", "1");
        assertThat(singleReplica.metrics()).containsEntry("rowCount", "1000");
        assertThat(singleReplica.metrics()).containsEntry("logicalBytesStatus", "plannedActual");
        assertThat(singleReplica.metrics()).containsEntry("fileCountStatus", "plannedActual");
        assertThat(singleReplica.metrics()).containsEntry("hdfsDiskBytesStatus", "plannedActual");
        assertThat(singleReplica.metrics()).containsEntry("querySecondsStatus", "plannedActual");
        String singleReplicaLocation = IcebergScenarioSupport.tableLocation(
            context,
            scenario.name(),
            singleReplica.caseId()
        );
        assertThat(singleReplica.actionCommands())
            .noneMatch(command -> command.contains("replication-1-baseline"))
            .anySatisfy(command -> assertThat(command).contains(" -setrep -w 1 " + singleReplicaLocation))
            .anySatisfy(command -> assertThat(command).contains(" -du -s " + singleReplicaLocation))
            .anySatisfy(command -> assertThat(command).contains(" -count " + singleReplicaLocation));

        IcebergValidationResult replication2 = resultByCaseId(results, "hdfs-replication-2-baseline");
        assertThat(replication2.metrics()).containsEntry("policyMode", "hdfs-replication-2-target-baseline");
        assertThat(replication2.metrics()).containsEntry("replication", "2");
        assertThat(replication2.metrics()).containsEntry("underReplicated", "true");
        assertThat(replication2.metrics()).containsEntry("storageMetricType", "actual");
        String replication2Location = IcebergScenarioSupport.tableLocation(
            context,
            scenario.name(),
            replication2.caseId()
        );
        assertThat(replication2.actionCommands())
            .noneMatch(command -> command.contains(replication2Location + "/replication-2-baseline"))
            .anySatisfy(command -> assertThat(command).contains(" -setrep -w 2 " + replication2Location))
            .anySatisfy(command -> assertThat(command).contains(" -du -s " + replication2Location))
            .anySatisfy(command -> assertThat(command).contains(" -count " + replication2Location));

        IcebergValidationResult rs104 = resultByCaseId(results, "ec-policy-rs-10-4-1024k");
        assertThat(rs104.metrics()).containsEntry("policy", "RS-10-4-1024k");
        assertThat(rs104.metrics()).containsEntry("physicalEcWritable", "false");
        assertThat(rs104.metrics()).containsEntry("requiredDataNodes", "14");
        assertThat(rs104.metrics()).containsKey("theoreticalEcDiskBytes");
        assertThat(rs104.metrics()).containsKey("theoreticalSavingVsReplication2");
        assertThat(rs104.metrics()).containsEntry("queryPerformanceStatus", "notRepresentative");
        assertThat(rs104.actionCommands()).anyMatch(command -> command.contains("-setErasureCodingPolicy"));
        assertThat(rs104.actionCommands()).anyMatch(command -> command.contains("-getErasureCodingPolicy"));
        assertThat(rs104.actionCommands()).anyMatch(command -> command.contains(" -du -s "));
        assertThat(rs104.actionCommands()).anyMatch(command -> command.contains(" -count "));
        assertThat(rs104.actionCommands()).anyMatch(command -> command.contains("SELECT COUNT(*)"));

        for (String policy : config.hdfs().ecPolicies()) {
            IcebergValidationResult policyResult = resultByCaseId(
                results,
                "ec-policy-" + policy.toLowerCase(java.util.Locale.ROOT).replace('_', '-')
            );
            EcPolicySpec spec = EcPolicySpec.parse(policy);
            assertThat(policyResult.metrics()).containsEntry("policyMode", "ec-policy");
            assertThat(policyResult.metrics()).containsEntry("policy", policy);
            assertThat(policyResult.metrics()).containsEntry("setPolicyStatus", "planned");
            assertThat(policyResult.metrics()).containsEntry("getPolicyStatus", "planned");
            assertThat(policyResult.metrics()).containsEntry("liveDataNodes", "1");
            assertThat(policyResult.metrics()).containsEntry("requiredDataNodes", Integer.toString(spec.requiredDataNodes()));
            assertThat(policyResult.metrics()).containsEntry("physicalEcWritable", "false");
            assertThat(policyResult.metrics()).containsEntry("rowCount", "1000");
            assertThat(policyResult.metrics()).containsEntry("logicalBytesStatus", "plannedActual");
            assertThat(policyResult.metrics()).containsKey("logicalBytesEstimate");
            assertThat(policyResult.metrics()).containsEntry("fileCountStatus", "notRepresentative");
            assertThat(policyResult.metrics()).containsEntry("hdfsDiskBytesStatus", "notRepresentative");
            assertThat(policyResult.metrics()).containsKey("theoreticalEcDiskBytes");
            assertThat(policyResult.metrics()).containsKey("theoreticalSavingVsReplication2");
            assertThat(policyResult.metrics()).containsEntry("queryPerformanceStatus", "notRepresentative");
            assertThat(policyResult.metrics()).containsKey("skipPhysicalReason");
            assertThat(policyResult.metrics()).containsKey("setPolicyPath");
        }
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
        assertThat(result.metrics()).containsKey("validationPoint");
        assertThat(result.metrics()).containsEntry("querySecondsBeforeFailureStatus", "notExecuted");
        assertThat(result.metrics()).containsEntry("querySecondsAfterFailureStatus", "notExecuted");
        assertThat(result.metrics()).containsEntry("latencyImpactRatioStatus", "notComparable");
        assertThat(result.metrics()).containsEntry("checksumMatchedStatus", "notExecuted");
        assertThat(result.metrics()).containsEntry("physicalEcWritable", "false");
        assertThat(result.metrics()).doesNotContainKey("querySecondsAfterFailure");
        assertThat(result.metrics()).containsEntry(
            "skipReason",
            "RS-10-4-1024k requires at least 14 live DataNodes for full policy tolerance validation"
        );
    }

    @Test
    void policyMatrixFailureReportsConfiguredPoliciesWithoutSinglePolicyMetrics() throws Exception {
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
            .filter(candidate -> candidate.caseId().equals("ec-policy-matrix-failure"))
            .findFirst()
            .orElseThrow();

        IcebergValidationResult result = scenario.run(testCase, context);

        assertThat(result.functionStatus()).isEqualTo(IcebergConclusion.FunctionStatus.SKIPPED);
        assertThat(result.performanceStatus()).isEqualTo(IcebergConclusion.PerformanceStatus.NOT_COMPARABLE);
        assertThat(result.metrics()).containsEntry(
            "validationPoint",
            "验证配置中的多种 EC policy 在副本失效后的 DataNode 要求；当前单 DataNode 环境不执行真实故障后查询。"
        );
        assertThat(result.metrics()).containsEntry(
            "policyMatrix",
            "RS-3-2-1024k(required=5,live=1);RS-6-3-1024k(required=9,live=1);"
                + "RS-10-4-1024k(required=14,live=1);XOR-2-1-1024k(required=3,live=1)"
        );
        assertThat(result.metrics()).containsEntry("liveDataNodes", "1");
        assertThat(result.metrics()).containsEntry("physicalEcWritable", "false");
        assertThat(result.metrics()).containsEntry("querySecondsBeforeFailureStatus", "notExecuted");
        assertThat(result.metrics()).containsEntry("querySecondsAfterFailureStatus", "notExecuted");
        assertThat(result.metrics()).containsEntry("latencyImpactRatioStatus", "notComparable");
        assertThat(result.metrics()).containsEntry("checksumMatchedStatus", "notExecuted");
        assertThat(result.metrics()).doesNotContainKey("policy");
        assertThat(result.metrics()).doesNotContainKey("querySecondsAfterFailure");
    }

    private static IcebergValidationResult resultByCaseId(List<IcebergValidationResult> results, String caseId) {
        return results.stream()
            .filter(result -> caseId.equals(result.caseId()))
            .findFirst()
            .orElseThrow();
    }
}

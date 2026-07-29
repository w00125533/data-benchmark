package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergConclusion;
import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import com.example.databenchmark.iceberg.exec.SparkSqlExecutor;
import com.example.databenchmark.iceberg.hdfs.EcPolicySpec;
import com.example.databenchmark.iceberg.hdfs.HdfsCliClient;
import com.example.databenchmark.iceberg.hdfs.HdfsEcPolicyClient;
import com.example.databenchmark.iceberg.sql.IcebergSqlTemplates;
import com.example.databenchmark.iceberg.sql.SparkSqlScriptBuilder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ErasureCodingScenario extends AbstractIcebergValidationScenario {
    private final SparkSqlExecutor sparkSqlExecutor;
    private final HdfsCliClient hdfsCliClient;

    public ErasureCodingScenario() {
        this(new SparkSqlExecutor(), new HdfsCliClient());
    }

    ErasureCodingScenario(SparkSqlExecutor sparkSqlExecutor, HdfsCliClient hdfsCliClient) {
        this.sparkSqlExecutor = sparkSqlExecutor;
        this.hdfsCliClient = hdfsCliClient;
    }

    @Override
    public String name() {
        return "erasureCoding";
    }

    @Override
    public List<IcebergValidationCase> cases(IcebergValidationConfig config) {
        List<IcebergValidationCase> cases = new ArrayList<>();
        cases.add(testCase(
            "hdfs-replication-1-actual",
            "Measure HDFS single-replica actual disk and query baseline.",
            Map.of("replication", "1")
        ));
        cases.add(testCase(
            "hdfs-replication-2-baseline",
            "Measure target replication=2 baseline and under-replication status on single DataNode.",
            Map.of("replication", "2")
        ));
        for (String policy : config.hdfs().ecPolicies()) {
            cases.add(testCase(
                "ec-policy-" + policy.toLowerCase(Locale.ROOT).replace('_', '-'),
                "Evaluate EC policy storage and query behavior on single DataNode.",
                Map.of("policy", policy)
            ));
        }
        cases.add(testCase(
            "ec-rs-10-4-failure-tolerance",
            "Validate RS-10-4 tolerated DataNode failure readability.",
            Map.of("policy", "RS-10-4-1024k")
        ));
        cases.add(testCase(
            "ec-policy-matrix-failure",
            "Validate tolerated failure matrix for configured EC policies.",
            Map.of("policies", String.join(",", config.hdfs().ecPolicies()))
        ));
        return cases;
    }

    @Override
    public IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) {
        String table = IcebergScenarioSupport.tableName(context, name(), testCase.caseId());
        String location = IcebergScenarioSupport.tableLocation(context, name(), testCase.caseId());
        if (testCase.caseId().equals("ec-policy-matrix-failure")) {
            return skippedPolicyMatrixFailure(testCase, context);
        }
        if (testCase.caseId().equals("ec-rs-10-4-failure-tolerance")) {
            HdfsEcPolicyClient.EcPreflight preflight = HdfsEcPolicyClient.preflight("RS-10-4-1024k", true, 1);
            return skippedFaultTolerance(testCase, context, preflight);
        }
        if (testCase.caseId().startsWith("hdfs-replication-")) {
            int replication = Integer.parseInt(testCase.parameters().get("replication"));
            return plannedReplicationBaseline(testCase, context, table, location, replication);
        }
        return plannedEcPolicy(testCase, context, table, location, testCase.parameters().get("policy"));
    }

    private IcebergValidationResult plannedReplicationBaseline(
        IcebergValidationCase testCase,
        IcebergValidationContext context,
        String table,
        String location,
        int replication
    ) {
        long targetRows = Math.min(context.config().scale().rows(), 1000);
        Map<String, String> metrics = new LinkedHashMap<>();
        metrics.put(
            "validationPoint",
            replication == 1
                ? "Measure the real single-replica HDFS file count, disk usage, and query latency baseline on one DataNode."
                : "Record the target replication=2 baseline on one DataNode and explicitly mark under-replication risk."
        );
        metrics.put("policyMode", replication == 1 ? "hdfs-replication-1-actual" : "hdfs-replication-2-target-baseline");
        metrics.put("replication", Integer.toString(replication));
        metrics.put("rowCount", Long.toString(targetRows));
        metrics.put("logicalBytesStatus", "plannedActual");
        metrics.put("fileCountStatus", "plannedActual");
        metrics.put("hdfsDiskBytesStatus", "plannedActual");
        metrics.put("querySecondsStatus", "plannedActual");
        metrics.put("storageMetricType", "actual");
        metrics.put("underReplicated", Boolean.toString(replication > 1));
        metrics.put("liveDataNodes", "1");
        metrics.put("requiredDataNodes", "1");

        List<String> actions = List.of(
            "hdfs dfs -fs " + context.config().hdfs().defaultFs() + " -setrep -w "
                + replication + " " + location,
            "hdfs dfs -fs " + context.config().hdfs().defaultFs() + " -du -s " + location,
            "hdfs dfs -fs " + context.config().hdfs().defaultFs() + " -count " + location,
            "SELECT COUNT(*) FROM " + table
        );

        return plannedEcResult(
            testCase,
            context,
            table,
            location,
            metrics,
            actions,
            replication == 1 ? IcebergConclusion.FunctionStatus.PASS : IcebergConclusion.FunctionStatus.DEGRADED,
            replication == 1
                ? "Single-replica HDFS baseline is planned for actual collection on the available DataNode."
                : "Replication=2 baseline is auditable, but it is under-replicated with only one live DataNode."
        );
    }

    private IcebergValidationResult plannedEcPolicy(
        IcebergValidationCase testCase,
        IcebergValidationContext context,
        String table,
        String location,
        String policy
    ) {
        long targetRows = Math.min(context.config().scale().rows(), 1000);
        long logicalBytesEstimate = targetRows * 128L;
        EcPolicySpec spec = EcPolicySpec.parse(policy);
        Map<String, String> metrics = new LinkedHashMap<>();
        metrics.put("validationPoint", "Report EC policy commands, DataNode requirements, and theoretical storage on one DataNode.");
        metrics.put("policyMode", "ec-policy");
        metrics.put("policy", policy);
        metrics.put("setPolicyPath", ecPolicyPath(location, policy));
        metrics.put("setPolicyStatus", "planned");
        metrics.put("getPolicyStatus", "planned");
        metrics.put("liveDataNodes", "1");
        metrics.put("requiredDataNodes", Integer.toString(spec.requiredDataNodes()));
        metrics.put("physicalEcWritable", "false");
        metrics.put("rowCount", Long.toString(targetRows));
        metrics.put("logicalBytesStatus", "plannedActual");
        metrics.put("logicalBytesEstimate", Long.toString(logicalBytesEstimate));
        metrics.put("fileCountStatus", "notRepresentative");
        metrics.put("hdfsDiskBytesStatus", "notRepresentative");
        metrics.put("theoreticalEcDiskBytes", Long.toString(spec.theoreticalDiskBytes(logicalBytesEstimate)));
        metrics.put("theoreticalSavingVsReplication2", spec.theoreticalSavingVsReplication2(logicalBytesEstimate));
        metrics.put("queryPerformanceStatus", "notRepresentative");
        metrics.put(
            "skipPhysicalReason",
            policy + " requires " + spec.requiredDataNodes() + " live DataNodes for physical EC block groups"
        );

        return plannedEcResult(
            testCase,
            context,
            table,
            location,
            metrics,
            ecPolicyActions(context, location, table, policy),
            IcebergConclusion.FunctionStatus.DEGRADED,
            policy + " cannot be physically encoded with one DataNode; theoretical storage estimates are reported separately from actual HDFS usage."
        );
    }

    private IcebergValidationResult plannedEcResult(
        IcebergValidationCase testCase,
        IcebergValidationContext context,
        String table,
        String location,
        Map<String, String> metrics,
        List<String> actions,
        IcebergConclusion.FunctionStatus functionStatus,
        String conclusion
    ) {
        long targetRows = Math.min(context.config().scale().rows(), 1000);
        List<String> setup = setupCommands(context, table, location, targetRows);
        String setupScript = setupScript(setup);
        return new IcebergValidationResult(
            testCase.scenario(),
            testCase.caseId(),
            testCase.purpose(),
            IcebergScenarioSupport.dataScale(context.config()),
            setup,
            actions,
            List.of("row count target is planned", "HDFS du/count and query commands are auditable but not executed in this planner path"),
            metrics,
            Map.of("baselineRows", Long.toString(targetRows)),
            Map.of(),
            functionStatus,
            IcebergConclusion.PerformanceStatus.NOT_COMPARABLE,
            conclusion,
            List.of("table=" + table, "location=" + location, "setupScript=" + setupScript.strip()),
            List.of(),
            List.of()
        );
    }

    private List<String> ecPolicyActions(
        IcebergValidationContext context,
        String location,
        String table,
        String policy
    ) {
        String path = ecPolicyPath(location, policy);
        String defaultFs = context.config().hdfs().defaultFs();
        return List.of(
            "hdfs dfs -fs " + defaultFs + " -mkdir -p " + path,
            "hdfs dfs -fs " + defaultFs + " -setErasureCodingPolicy -path " + path + " -policy " + policy,
            "hdfs dfs -fs " + defaultFs + " -getErasureCodingPolicy -path " + path,
            "hdfs dfs -fs " + defaultFs + " -du -s " + path,
            "hdfs dfs -fs " + defaultFs + " -count " + path,
            "SELECT COUNT(*) FROM " + table
        );
    }

    private static String ecPolicyPath(String location, String policy) {
        return location + "/ec-target/" + policy.toLowerCase(Locale.ROOT);
    }

    private List<String> setupCommands(
        IcebergValidationContext context,
        String table,
        String location,
        long targetRows
    ) {
        return List.of(
            IcebergSqlTemplates.createNamespace(context.config().iceberg().catalog(), context.config().iceberg().namespace()),
            IcebergSqlTemplates.dropTable(table),
            IcebergSqlTemplates.createBaseTable(table, location),
            IcebergSqlTemplates.insertRange(table, 0, targetRows, "baseline")
        );
    }

    private static String setupScript(List<String> setup) {
        SparkSqlScriptBuilder builder = new SparkSqlScriptBuilder();
        setup.forEach(builder::add);
        return builder.build();
    }

    private IcebergValidationResult skippedFaultTolerance(
        IcebergValidationCase testCase,
        IcebergValidationContext context,
        HdfsEcPolicyClient.EcPreflight preflight
    ) {
        int requiredDataNodes = HdfsEcPolicyClient.requiredDataNodes(preflight.policy());
        Map<String, String> metrics = new LinkedHashMap<>();
        metrics.put("validationPoint", "Validate EC policy readability and query performance impact after replica failure; current DataNode count is insufficient for execution.");
        metrics.put("policy", preflight.policy());
        metrics.put("liveDataNodes", Integer.toString(preflight.liveDataNodes()));
        metrics.put("requiredDataNodes", Integer.toString(requiredDataNodes));
        metrics.put("skipReason", preflight.reason());
        metrics.put("querySecondsBeforeFailureStatus", "notExecuted");
        metrics.put("querySecondsAfterFailureStatus", "notExecuted");
        metrics.put("latencyImpactRatioStatus", "notComparable");
        metrics.put("checksumMatchedStatus", "notExecuted");
        metrics.put("physicalEcWritable", Boolean.toString(preflight.canRunFaultTolerance()));
        return new IcebergValidationResult(
            testCase.scenario(),
            testCase.caseId(),
            testCase.purpose(),
            IcebergScenarioSupport.dataScale(context.config()),
            List.of(),
            List.of(),
            List.of("Skipped: " + preflight.reason()),
            metrics,
            Map.of(),
            Map.of(),
            IcebergConclusion.FunctionStatus.SKIPPED,
            IcebergConclusion.PerformanceStatus.NOT_COMPARABLE,
            preflight.reason(),
            List.of(
                "policy=" + preflight.policy(),
                "liveDataNodes=" + preflight.liveDataNodes(),
                "requiredDataNodes=" + requiredDataNodes
            ),
            List.of(),
            List.of()
        );
    }

    private IcebergValidationResult skippedPolicyMatrixFailure(
        IcebergValidationCase testCase,
        IcebergValidationContext context
    ) {
        int liveDataNodes = 1;
        Map<String, String> metrics = new LinkedHashMap<>();
        metrics.put(
            "validationPoint",
            "验证配置中的多种 EC policy 在副本失效后的 DataNode 要求；当前单 DataNode 环境不执行真实故障后查询。"
        );
        metrics.put("policyMatrix", policyMatrix(context.config().hdfs().ecPolicies(), liveDataNodes));
        metrics.put("liveDataNodes", Integer.toString(liveDataNodes));
        metrics.put("physicalEcWritable", "false");
        metrics.put("querySecondsBeforeFailureStatus", "notExecuted");
        metrics.put("querySecondsAfterFailureStatus", "notExecuted");
        metrics.put("latencyImpactRatioStatus", "notComparable");
        metrics.put("checksumMatchedStatus", "notExecuted");

        String reason = "Single DataNode environment does not execute real post-failure EC matrix queries.";
        return new IcebergValidationResult(
            testCase.scenario(),
            testCase.caseId(),
            testCase.purpose(),
            IcebergScenarioSupport.dataScale(context.config()),
            List.of(),
            List.of(),
            List.of("Skipped: " + reason),
            metrics,
            Map.of(),
            Map.of(),
            IcebergConclusion.FunctionStatus.SKIPPED,
            IcebergConclusion.PerformanceStatus.NOT_COMPARABLE,
            reason,
            List.of(
                "liveDataNodes=" + liveDataNodes,
                "policyMatrix=" + metrics.get("policyMatrix")
            ),
            List.of(),
            List.of()
        );
    }

    private static String policyMatrix(List<String> policies, int liveDataNodes) {
        List<String> entries = new ArrayList<>();
        for (String policy : policies) {
            EcPolicySpec spec = EcPolicySpec.parse(policy);
            entries.add(policy + "(required=" + spec.requiredDataNodes() + ",live=" + liveDataNodes + ")");
        }
        return String.join(";", entries);
    }
}

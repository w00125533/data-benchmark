package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergConclusion;
import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import com.example.databenchmark.iceberg.hdfs.HdfsEcPolicyClient;
import com.example.databenchmark.iceberg.sql.IcebergSqlTemplates;
import com.example.databenchmark.iceberg.sql.SparkSqlScriptBuilder;
import java.util.List;
import java.util.Map;

public class ErasureCodingScenario extends AbstractIcebergValidationScenario {
    @Override
    public String name() {
        return "erasureCoding";
    }

    @Override
    public List<IcebergValidationCase> cases(IcebergValidationConfig config) {
        return List.of(
            testCase("ec-policy-write-read", "Compare replication=2 with configured EC policies.", Map.of("baseline", "replication=2")),
            testCase("ec-rs-10-4-failure-tolerance", "Validate RS-10-4 tolerated DataNode failure readability.", Map.of("policy", "RS-10-4-1024k")),
            testCase("ec-policy-matrix-failure", "Validate tolerated failure matrix for configured EC policies.", Map.of("policies", String.join(",", config.hdfs().ecPolicies()))),
            testCase("ec-file-count-and-disk-usage", "Compare file count and HDFS disk usage across EC policies.", Map.of("baseline", "replication=2"))
        );
    }

    @Override
    public IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) {
        String table = IcebergScenarioSupport.tableName(context, name(), testCase.caseId());
        String location = IcebergScenarioSupport.tableLocation(context, name(), testCase.caseId());
        if (testCase.caseId().contains("failure")) {
            HdfsEcPolicyClient.EcPreflight preflight = HdfsEcPolicyClient.preflight("RS-10-4-1024k", true, 1);
            return skippedFaultTolerance(testCase, context, preflight);
        }
        return plannedEcComparison(testCase, context, table, location);
    }

    private IcebergValidationResult plannedEcComparison(
        IcebergValidationCase testCase,
        IcebergValidationContext context,
        String table,
        String location
    ) {
        long targetRows = Math.min(context.config().scale().rows(), 1000);
        List<String> setup = List.of(
            IcebergSqlTemplates.createNamespace(context.config().iceberg().catalog(), context.config().iceberg().namespace()),
            IcebergSqlTemplates.dropTable(table),
            IcebergSqlTemplates.createBaseTable(table, location),
            IcebergSqlTemplates.insertRange(table, 0, targetRows, "baseline")
        );
        String setupScript = new SparkSqlScriptBuilder()
            .add(setup.get(0))
            .add(setup.get(1))
            .add(setup.get(2))
            .add(setup.get(3))
            .build();
        List<String> actions = List.of(
            "hdfs dfs -fs " + context.config().hdfs().defaultFs() + " -setrep -w "
                + context.config().hdfs().replicationBaseline() + " " + location + "/replication-baseline",
            "hdfs ec -fs " + context.config().hdfs().defaultFs() + " -setPolicy -policy RS-10-4-1024k -path "
                + location + "/ec-target",
            "SELECT COUNT(*) FROM " + table,
            "SELECT COUNT(*), SUM(file_size_in_bytes) FROM " + table + ".files",
            "hdfs dfs -fs " + context.config().hdfs().defaultFs() + " -du -s " + location + "/replication-baseline",
            "hdfs dfs -fs " + context.config().hdfs().defaultFs() + " -count " + location + "/ec-target"
        );
        Map<String, String> metrics = Map.of(
            "replicationBaseline", Integer.toString(context.config().hdfs().replicationBaseline()),
            "ecPolicyCount", Integer.toString(context.config().hdfs().ecPolicies().size()),
            "targetRowCount", Long.toString(targetRows),
            "targetChecksum", "planned",
            "hdfsUsageStatus", "notCollected"
        );
        return new IcebergValidationResult(
            testCase.scenario(),
            testCase.caseId(),
            testCase.purpose(),
            IcebergScenarioSupport.dataScale(context.config()),
            setup,
            actions,
            List.of("row count target defined for replication and EC", "HDFS usage collection not executed in this planner path"),
            metrics,
            Map.of("baselineRows", Long.toString(targetRows)),
            Map.of(),
            IcebergConclusion.FunctionStatus.PASS,
            IcebergConclusion.PerformanceStatus.NOT_COMPARABLE,
            "EC comparison exposes replication baseline, EC policy count, row/checksum targets; HDFS du/count is not collected yet.",
            List.of("table=" + table, "location=" + location, "setupScript=" + setupScript.strip()),
            List.of(),
            List.of()
        );
    }

    private IcebergValidationResult skippedFaultTolerance(
        IcebergValidationCase testCase,
        IcebergValidationContext context,
        HdfsEcPolicyClient.EcPreflight preflight
    ) {
        int requiredDataNodes = HdfsEcPolicyClient.requiredDataNodes(preflight.policy());
        Map<String, String> metrics = Map.of(
            "policy", preflight.policy(),
            "liveDataNodes", Integer.toString(preflight.liveDataNodes()),
            "requiredDataNodes", Integer.toString(requiredDataNodes),
            "skipReason", preflight.reason()
        );
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
}

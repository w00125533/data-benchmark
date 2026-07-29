package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergConclusion;
import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import com.example.databenchmark.iceberg.sql.IcebergSqlTemplates;
import com.example.databenchmark.iceberg.sql.SparkSqlScriptBuilder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ErasureCodingConversionScenario extends AbstractIcebergValidationScenario {
    @Override
    public String name() {
        return "erasureCodingConversion";
    }

    @Override
    public List<IcebergValidationCase> cases(IcebergValidationConfig config) {
        return List.of(
            testCase("replication-to-ec-policy-only", "Measure policy-only replication to EC transition.", Map.of("direction", "replication->ec")),
            testCase("replication-to-ec-rewrite", "Measure physical rewrite from replication to EC.", Map.of("direction", "replication->ec")),
            testCase("ec-to-replication-policy-only", "Measure policy-only EC to replication transition.", Map.of("direction", "ec->replication")),
            testCase("ec-to-replication-rewrite", "Measure physical rewrite from EC to replication.", Map.of("direction", "ec->replication"))
        );
    }

    @Override
    public IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) {
        String sourceTable = IcebergScenarioSupport.tableName(context, name(), testCase.caseId()) + "_source";
        String targetTable = IcebergScenarioSupport.tableName(context, name(), testCase.caseId()) + "_target";
        String baseTable = IcebergScenarioSupport.tableName(context, name(), testCase.caseId());
        String location = IcebergScenarioSupport.tableLocation(context, name(), testCase.caseId());
        List<String> setup = List.of(
            IcebergSqlTemplates.createNamespace(context.config().iceberg().catalog(), context.config().iceberg().namespace()),
            IcebergSqlTemplates.dropTable(baseTable),
            IcebergSqlTemplates.createBaseTable(baseTable, location),
            IcebergSqlTemplates.insertRange(baseTable, 0, Math.min(context.config().scale().rows(), 1000), "baseline")
        );
        String setupScript = new SparkSqlScriptBuilder()
            .add(setup.get(0))
            .add(setup.get(1))
            .add(setup.get(2))
            .add(setup.get(3))
            .build();
        return new IcebergValidationResult(
            testCase.scenario(),
            testCase.caseId(),
            testCase.purpose(),
            IcebergScenarioSupport.dataScale(context.config()),
            setup,
            conversionActions(testCase, context, sourceTable, targetTable, location),
            List.of(
                "real HDFS/Spark conversion execution is not wired",
                "file, disk, timing, query, and checksum metrics are not collected"
            ),
            conversionMetrics(testCase, context.config()),
            Map.of(),
            Map.of(),
            IcebergConclusion.FunctionStatus.PASS,
            IcebergConclusion.PerformanceStatus.NOT_COMPARABLE,
            "Real HDFS/Spark conversion is not wired; performance is not comparable until execution metrics are collected.",
            List.of(
                "sourceTable=" + sourceTable,
                "targetTable=" + targetTable,
                "location=" + location,
                "setupScript=" + setupScript.strip()
            ),
            List.of(),
            List.of()
        );
    }

    private static List<String> conversionActions(
        IcebergValidationCase testCase,
        IcebergValidationContext context,
        String sourceTable,
        String targetTable,
        String location
    ) {
        String defaultFs = context.config().hdfs().defaultFs();
        if (conversionMode(testCase).equals("policy-only")) {
            if (conversionDirection(testCase).equals("replication->ec")) {
                return List.of(
                    "hdfs ec -setPolicy -path " + location + "/target -policy " + targetPolicy(context.config()),
                    "hdfs dfs -fs " + defaultFs + " -du -s " + location + "/target"
                );
            }
            return List.of(
                "hdfs ec -unsetPolicy -path " + location + "/replication-target",
                "hdfs dfs -fs " + defaultFs + " -setrep -w " + context.config().hdfs().replicationBaseline()
                    + " " + location + "/replication-target"
            );
        }
        return List.of(
            "hdfs dfs -fs " + defaultFs + " -du -s " + location + "/before",
            "INSERT INTO " + targetTable + " SELECT * FROM " + sourceTable,
            "hdfs dfs -fs " + defaultFs + " -du -s " + location + "/after"
        );
    }

    private static Map<String, String> conversionMetrics(IcebergValidationCase testCase, IcebergValidationConfig config) {
        Map<String, String> metrics = new LinkedHashMap<>();
        String direction = conversionDirection(testCase);
        String mode = conversionMode(testCase);
        metrics.put("conversionDirection", direction);
        metrics.put("conversionMode", mode);
        if (direction.equals("replication->ec")) {
            metrics.put("targetPolicy", targetPolicy(config));
        } else {
            metrics.put("targetReplication", Integer.toString(config.hdfs().replicationBaseline()));
        }
        metrics.put("conversionStatus", "notExecuted");
        if (mode.equals("policy-only")) {
            metrics.put("policyCommandStatus", "notExecuted");
        }
        metrics.put("hdfsUsageStatus", "notCollected");
        metrics.put("checksumStatus", "notCollected");
        return metrics;
    }

    private static String conversionDirection(IcebergValidationCase testCase) {
        if (testCase.caseId().startsWith("replication-to-ec")) {
            return "replication->ec";
        }
        return "ec->replication";
    }

    private static String conversionMode(IcebergValidationCase testCase) {
        return testCase.caseId().contains("policy-only") ? "policy-only" : "physical rewrite";
    }

    private static String targetPolicy(IcebergValidationConfig config) {
        return config.hdfs().ecPolicies().stream()
            .filter(policy -> policy.equals("RS-10-4-1024k"))
            .findFirst()
            .orElseGet(() -> config.hdfs().ecPolicies().get(0));
    }
}

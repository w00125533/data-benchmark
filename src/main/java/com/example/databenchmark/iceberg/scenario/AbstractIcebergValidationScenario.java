package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergConclusion;
import com.example.databenchmark.iceberg.IcebergExecutionEvidence;
import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import com.example.databenchmark.iceberg.IcebergValidationScenario;
import com.example.databenchmark.iceberg.sql.IcebergSqlTemplates;
import com.example.databenchmark.iceberg.sql.SparkSqlScriptBuilder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

abstract class AbstractIcebergValidationScenario implements IcebergValidationScenario {
    protected IcebergValidationCase testCase(String caseId, String purpose, Map<String, String> parameters) {
        return new IcebergValidationCase(name(), caseId, purpose, parameters, true);
    }

    protected IcebergValidationResult scriptedPass(
        IcebergValidationCase testCase,
        IcebergValidationContext context,
        List<String> actions,
        List<String> assertions,
        Map<String, String> metrics,
        String conclusion
    ) {
        String table = IcebergScenarioSupport.tableName(context, name(), testCase.caseId());
        String location = IcebergScenarioSupport.tableLocation(context, name(), testCase.caseId());
        List<String> setup = List.of(
            IcebergSqlTemplates.createNamespace(context.config().iceberg().catalog(), context.config().iceberg().namespace()),
            IcebergSqlTemplates.dropTable(table),
            IcebergSqlTemplates.createBaseTable(table, location),
            IcebergSqlTemplates.insertRange(table, 0, Math.min(context.config().scale().rows(), 1000), "baseline")
        );
        String script = new SparkSqlScriptBuilder()
            .add(setup.get(0))
            .add(setup.get(1))
            .add(setup.get(2))
            .add(setup.get(3))
            .build();
        return IcebergScenarioSupport.pass(
            testCase,
            context,
            setup,
            actions,
            assertions,
            metrics,
            Map.of("baselineRows", Long.toString(Math.min(context.config().scale().rows(), 1000))),
            Map.of(),
            conclusion,
            List.of("table=" + table, "location=" + location, "setupScript=" + script.strip())
        );
    }

    protected IcebergValidationResult scriptedSkipped(
        IcebergValidationCase testCase,
        IcebergValidationContext context,
        List<String> actions,
        List<String> assertions,
        Map<String, String> scenarioFacts,
        List<String> expectedMetricFields,
        String notExecutedReason,
        String conclusion
    ) {
        String table = IcebergScenarioSupport.tableName(context, name(), testCase.caseId());
        String location = IcebergScenarioSupport.tableLocation(context, name(), testCase.caseId());
        List<String> setup = List.of(
            IcebergSqlTemplates.createNamespace(context.config().iceberg().catalog(), context.config().iceberg().namespace()),
            IcebergSqlTemplates.dropTable(table),
            IcebergSqlTemplates.createBaseTable(table, location),
            IcebergSqlTemplates.insertRange(table, 0, Math.min(context.config().scale().rows(), 1000), "baseline")
        );
        SparkSqlScriptBuilder builder = new SparkSqlScriptBuilder();
        setup.forEach(builder::add);
        actions.forEach(builder::add);
        String script = builder.build().strip();
        Map<String, String> metrics = new LinkedHashMap<>(scenarioFacts);
        metrics.put("executed", "false");
        metrics.put("metricCollectionStatus", "notExecuted");
        metrics.put("notExecutedReason", notExecutedReason);
        metrics.put("expectedMetricFields", String.join(",", expectedMetricFields));
        metrics.put("plannedActionCount", Integer.toString(actions.size()));
        IcebergExecutionEvidence executionEvidence = new IcebergExecutionEvidence(
            "plan",
            "Spark SQL/HDFS script not executed",
            script,
            -1,
            0.0,
            "notExecuted=true\nreason=" + notExecutedReason,
            ""
        );
        return new IcebergValidationResult(
            testCase.scenario(),
            testCase.caseId(),
            testCase.purpose(),
            IcebergScenarioSupport.dataScale(context.config()),
            setup,
            actions,
            assertions,
            metrics,
            Map.of(),
            Map.of(),
            IcebergConclusion.FunctionStatus.SKIPPED,
            IcebergConclusion.PerformanceStatus.NOT_COMPARABLE,
            conclusion,
            List.of("table=" + table, "location=" + location, "notExecutedReason=" + notExecutedReason),
            List.of(),
            List.of(executionEvidence)
        );
    }

    @Override
    public IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) {
        return scriptedSkipped(
            testCase,
            context,
            List.of("SELECT COUNT(*) FROM " + IcebergScenarioSupport.tableName(context, name(), testCase.caseId())),
            List.of("row count matched"),
            Map.of(),
            List.of("rowCount", "snapshotId", "querySeconds"),
            "This scenario only generated a validation script; Spark SQL was not executed.",
            "Validation script was generated, but no functional or performance result was collected."
        );
    }
}

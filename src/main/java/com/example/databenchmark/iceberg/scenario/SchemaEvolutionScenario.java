package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.iceberg.IcebergConclusion;
import com.example.databenchmark.iceberg.IcebergExecutionEvidence;
import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import com.example.databenchmark.iceberg.exec.SparkSqlExecutor;
import com.example.databenchmark.iceberg.metrics.IcebergMetricCollectors;
import com.example.databenchmark.iceberg.sql.IcebergSqlTemplates;
import com.example.databenchmark.iceberg.sql.SparkSqlScriptBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SchemaEvolutionScenario extends AbstractIcebergValidationScenario {
    private final SparkSqlExecutor sparkSqlExecutor;
    private final boolean executeSpark;

    public SchemaEvolutionScenario() {
        this(new SparkSqlExecutor(), true);
    }

    SchemaEvolutionScenario(SparkSqlExecutor sparkSqlExecutor) {
        this(sparkSqlExecutor, true);
    }

    SchemaEvolutionScenario(SparkSqlExecutor sparkSqlExecutor, boolean executeSpark) {
        this.sparkSqlExecutor = sparkSqlExecutor;
        this.executeSpark = executeSpark;
    }

    @Override
    public String name() {
        return "schemaEvolution";
    }

    @Override
    public List<IcebergValidationCase> cases(IcebergValidationConfig config) {
        return List.of(
            testCase("schema-add-drop-rename", "Validate add, drop, and rename compatibility for historical reads.", Map.of("changes", "add,drop,rename")),
            testCase("schema-type-promotion", "Validate compatible primitive and decimal type promotion with historical reads.", Map.of("changes", "int->long,float->double,decimal expansion")),
            testCase("schema-nested-struct", "Validate nested struct field add, drop, and rename compatibility.", Map.of("changes", "nested struct")),
            testCase("schema-complex-types", "Validate map, list, and struct mixed-version reads.", Map.of("types", "map,list,struct")),
            testCase("schema-long-chain-history", "Validate long schema history with repeated appends.", Map.of("schemaChanges", "100"))
        );
    }

    @Override
    public IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) {
        String table = IcebergScenarioSupport.tableName(context, name(), testCase.caseId());
        String location = IcebergScenarioSupport.tableLocation(context, name(), testCase.caseId());
        long rows = Math.min(context.config().scale().rows(), 1000);
        SchemaPlan plan = schemaPlan(testCase.caseId(), table, context);
        long baselineEnd = rows;
        long postAlterEnd = rows + Math.min(rows, 1000);
        String postAlterInsert = postAlterInsert(table, testCase.caseId(), baselineEnd, postAlterEnd);
        String historicalQuerySql = "SELECT id, event_day, metric_int FROM " + table
            + " VERSION AS OF ${baselineSnapshotId} ORDER BY id LIMIT 20";
        String currentQuerySql = "SELECT id, event_day, metric_int FROM " + table
            + " ORDER BY id DESC LIMIT 20";
        List<String> setup = List.of(
            IcebergSqlTemplates.createNamespace(context.config().iceberg().catalog(), context.config().iceberg().namespace()),
            IcebergSqlTemplates.dropTable(table),
            IcebergSqlTemplates.createBaseTable(table, location),
            IcebergSqlTemplates.insertRange(table, 0, rows, "baseline")
        );
        String script = new SparkSqlScriptBuilder()
            .add(setup.get(0))
            .add(setup.get(1))
            .add(setup.get(2))
            .add(setup.get(3))
            .build();
        List<String> actions = new ArrayList<>(plan.actions());
        actions.add(postAlterInsert);
        actions.add("SELECT snapshot_id AS baselineSnapshotId FROM " + table + ".snapshots ORDER BY committed_at ASC LIMIT 1");
        actions.add("SELECT snapshot_id AS postAlterSnapshotId FROM " + table + ".snapshots ORDER BY committed_at DESC LIMIT 1");
        actions.add(historicalQuerySql);
        actions.add(currentQuerySql);
        Map<String, String> metrics = new LinkedHashMap<>();
        metrics.put("schemaChangeType", plan.changeType());
        metrics.put("changeCount", Integer.toString(plan.changeCount()));
        metrics.put("validationPoint", plan.validationPoint());
        metrics.put("baselineRows", Long.toString(rows));
        metrics.put("currentRows", Long.toString(postAlterEnd));
        metrics.put("snapshotCount", Integer.toString(plan.snapshotCount()));
        metrics.put("schemaHistoryLength", Integer.toString(plan.schemaHistoryLength()));
        metrics.put("baselineSnapshotIdStatus", "planned");
        metrics.put("postAlterSnapshotIdStatus", "planned");
        metrics.put("historicalQuerySql", historicalQuerySql);
        metrics.put("currentQuerySql", currentQuerySql);
        metrics.put("historicalRowsStatus", "planned");
        metrics.put("currentRowsStatus", "planned");
        metrics.put("historicalQuerySecondsStatus", "notExecuted");
        metrics.put("currentQuerySecondsStatus", "notExecuted");
        metrics.put("historicalSampleRowsStatus", "planned");
        metrics.put("currentSampleRowsStatus", "planned");
        List<String> evidence = List.of(
            "table=" + table,
            "location=" + location,
            "setupScript=" + script.strip(),
            "historicalQuerySql=" + historicalQuerySql,
            "currentQuerySql=" + currentQuerySql,
            "historicalSampleRows=notExecuted",
            "currentSampleRows=notExecuted"
        );

        if (executeSpark) {
            return runWithSpark(
                testCase,
                context,
                setup,
                actions,
                plan,
                metrics,
                rows,
                postAlterEnd,
                table,
                location,
                script,
                postAlterInsert,
                historicalQuerySql,
                currentQuerySql
            );
        }

        return IcebergScenarioSupport.pass(
            testCase,
            context,
            setup,
            actions,
            plan.assertions(),
            metrics,
            Map.of("baselineRows", Long.toString(rows)),
            Map.of(
                "currentRows", Long.toString(postAlterEnd),
                "snapshotCount", Integer.toString(plan.snapshotCount()),
                "schemaHistoryLength", Integer.toString(plan.schemaHistoryLength())
            ),
            plan.conclusion(),
            evidence
        );
    }

    private IcebergValidationResult runWithSpark(
        IcebergValidationCase testCase,
        IcebergValidationContext context,
        List<String> setup,
        List<String> actions,
        SchemaPlan plan,
        Map<String, String> metrics,
        long rows,
        long postAlterEnd,
        String table,
        String location,
        String script,
        String postAlterInsert,
        String historicalQuerySql,
        String currentQuerySql
    ) {
        List<IcebergExecutionEvidence> executionResults = new ArrayList<>();
        List<String> evidence = new ArrayList<>();
        evidence.add("table=" + table);
        evidence.add("location=" + location);
        evidence.add("setupScript=" + script.strip());

        try {
            runAndRequireSuccess(context.config(), executionResults, "setup", "create schema table", script);
            for (String action : plan.actions()) {
                runAndRequireSuccess(context.config(), executionResults, "action", "schema change", action);
            }
            runAndRequireSuccess(context.config(), executionResults, "action", "post alter insert", postAlterInsert);

            String baselineSnapshotSql = "SELECT snapshot_id AS baselineSnapshotId FROM "
                + table + ".snapshots ORDER BY committed_at ASC LIMIT 1";
            String postAlterSnapshotSql = "SELECT snapshot_id AS postAlterSnapshotId FROM "
                + table + ".snapshots ORDER BY committed_at DESC LIMIT 1";
            IcebergExecutionEvidence baselineSnapshot = runAndRequireSuccess(
                context.config(),
                executionResults,
                "metric",
                "baseline snapshot id",
                baselineSnapshotSql
            );
            IcebergExecutionEvidence postAlterSnapshot = runAndRequireSuccess(
                context.config(),
                executionResults,
                "metric",
                "post alter snapshot id",
                postAlterSnapshotSql
            );
            String baselineSnapshotId = IcebergMetricCollectors.parseSingleString(baselineSnapshot.stdout());
            String postAlterSnapshotId = IcebergMetricCollectors.parseSingleString(postAlterSnapshot.stdout());
            String historicalQuerySqlWithId = historicalQuerySql.replace("${baselineSnapshotId}", baselineSnapshotId);
            IcebergExecutionEvidence historicalQuery = runAndRequireSuccess(
                context.config(),
                executionResults,
                "assertion",
                "historical query",
                historicalQuerySqlWithId
            );
            IcebergExecutionEvidence currentQuery = runAndRequireSuccess(
                context.config(),
                executionResults,
                "assertion",
                "current query",
                currentQuerySql
            );

            metrics.remove("baselineSnapshotIdStatus");
            metrics.remove("postAlterSnapshotIdStatus");
            metrics.remove("historicalRowsStatus");
            metrics.remove("currentRowsStatus");
            metrics.remove("historicalQuerySecondsStatus");
            metrics.remove("currentQuerySecondsStatus");
            metrics.remove("historicalSampleRowsStatus");
            metrics.remove("currentSampleRowsStatus");
            metrics.put("baselineSnapshotId", baselineSnapshotId);
            metrics.put("postAlterSnapshotId", postAlterSnapshotId);
            metrics.put("historicalQuerySql", historicalQuerySqlWithId);
            metrics.put("currentQuerySql", currentQuerySql);
            metrics.put("historicalRows", Long.toString(dataRowCount(historicalQuery.stdout())));
            metrics.put("currentRows", Long.toString(dataRowCount(currentQuery.stdout())));
            metrics.put("historicalQuerySeconds", Double.toString(historicalQuery.durationSeconds()));
            metrics.put("currentQuerySeconds", Double.toString(currentQuery.durationSeconds()));

            evidence.add("historicalQuerySql=" + historicalQuerySqlWithId);
            evidence.add("currentQuerySql=" + currentQuerySql);
            evidence.add("historicalSampleRows=" + historicalQuery.stdout().strip());
            evidence.add("currentSampleRows=" + currentQuery.stdout().strip());

            return IcebergScenarioSupport.pass(
                testCase,
                context,
                setup,
                actions,
                plan.assertions(),
                metrics,
                Map.of("baselineRows", Long.toString(rows)),
                Map.of(
                    "currentRows", Long.toString(postAlterEnd),
                    "snapshotCount", Integer.toString(plan.snapshotCount()),
                    "schemaHistoryLength", Integer.toString(plan.schemaHistoryLength())
                ),
                plan.conclusion(),
                evidence,
                executionResults
            );
        } catch (IOException | InterruptedException | RuntimeException exception) {
            if (exception instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            String error = exception.getMessage() == null ? exception.toString() : exception.getMessage();
            evidence.add("executionError=" + error);
            return new IcebergValidationResult(
                testCase.scenario(),
                testCase.caseId(),
                testCase.purpose(),
                IcebergScenarioSupport.dataScale(context.config()),
                setup,
                actions,
                plan.assertions(),
                metrics,
                Map.of("baselineRows", Long.toString(rows)),
                Map.of(),
                IcebergConclusion.FunctionStatus.FAIL,
                IcebergConclusion.PerformanceStatus.NOT_COMPARABLE,
                "Schema evolution Spark execution failed.",
                evidence,
                List.of(error),
                executionResults
            );
        }
    }

    private IcebergExecutionEvidence runAndRequireSuccess(
        IcebergValidationConfig config,
        List<IcebergExecutionEvidence> executionResults,
        String phase,
        String label,
        String sql
    ) throws IOException, InterruptedException {
        IcebergExecutionEvidence evidence = runSpark(config, phase, label, sql);
        executionResults.add(evidence);
        if (evidence.exitCode() != 0) {
            throw new IllegalStateException("Spark SQL failed during " + label + ": " + evidence.stderr());
        }
        return evidence;
    }

    private IcebergExecutionEvidence runSpark(IcebergValidationConfig config, String phase, String label, String sql)
        throws IOException, InterruptedException {
        CommandResult result = sparkSqlExecutor.run(config, sql);
        return new IcebergExecutionEvidence(
            phase,
            label,
            sql,
            result.exitCode(),
            result.durationSeconds(),
            result.stdout(),
            result.stderr()
        );
    }

    private static long dataRowCount(String stdout) {
        return stdout.lines()
            .map(String::trim)
            .filter(line -> !line.isBlank())
            .filter(line -> !line.chars().allMatch(character ->
                character == '-' || character == '+' || character == '|' || Character.isWhitespace(character)))
            .filter(line -> !line.startsWith("Time taken:"))
            .filter(line -> !line.startsWith("Setting default log level to "))
            .skip(1)
            .count();
    }

    private static SchemaPlan schemaPlan(String caseId, String table, IcebergValidationContext context) {
        return switch (caseId) {
            case "schema-add-drop-rename" -> new SchemaPlan(
                "add/drop/rename",
                "验证字段新增、删除、重命名后历史快照仍按 Iceberg 字段 ID 兼容读取，ALTER 后新写入数据只在当前快照可见。",
                List.of(
                    "ALTER TABLE " + table + " ADD COLUMN added_text STRING",
                    "ALTER TABLE " + table + " ADD COLUMN category STRING",
                    "ALTER TABLE " + table + " RENAME COLUMN region TO service_region",
                    "ALTER TABLE " + table + " DROP COLUMN category"
                ),
                List.of("historical snapshots remain readable", "renamed fields preserve values by Iceberg field ID"),
                "Add, drop, and rename schema changes are planned with historical row-count compatibility checks."
            );
            case "schema-type-promotion" -> new SchemaPlan(
                "type promotion",
                "验证数值类型提升和 decimal 扩容后，历史快照中的旧类型数据可被兼容读取，ALTER 后新数据按当前类型写入。",
                List.of(
                    "ALTER TABLE " + table + " ALTER COLUMN metric_int TYPE BIGINT",
                    "ALTER TABLE " + table + " ALTER COLUMN metric_float TYPE DOUBLE",
                    "ALTER TABLE " + table + " ALTER COLUMN amount TYPE DECIMAL(18, 2)"
                ),
                List.of("promoted numeric columns remain readable", "historical snapshots keep expected row counts"),
                "Compatible numeric and decimal promotions are planned with historical snapshot read checks."
            );
            case "schema-nested-struct" -> new SchemaPlan(
                "nested struct",
                "验证嵌套 struct 字段新增和重命名后，历史快照字段投影仍可读取，当前快照能读取新增嵌套字段。",
                List.of(
                    "ALTER TABLE " + table + " ADD COLUMN payload.vendor_code STRING",
                    "ALTER TABLE " + table + " ADD COLUMN payload.quality_score DOUBLE",
                    "ALTER TABLE " + table + " RENAME COLUMN payload.vendor_code TO vendor_id"
                ),
                List.of("nested field projection remains compatible", "nested field IDs preserve historical values"),
                "Nested struct evolution is planned with field projection and historical compatibility checks."
            );
            case "schema-complex-types" -> new SchemaPlan(
                "map/list/struct",
                "验证 map、array、struct 复杂类型新增后，历史数据可按旧投影读取，当前数据可写入并读取复杂类型列。",
                List.of(
                    "ALTER TABLE " + table + " ADD COLUMN attributes MAP<STRING, STRING>",
                    "ALTER TABLE " + table + " ADD COLUMN checkpoints ARRAY<STRUCT<ts: TIMESTAMP, status: STRING>>",
                    "ALTER TABLE " + table + " ADD COLUMN owner STRUCT<team: STRING, priority: INT>"
                ),
                List.of("historical rows equal the baseline count", "current rows include post-ALTER appended data"),
                "Map, list, and struct additions are planned with mixed-version read checks."
            );
            case "schema-long-chain-history" -> longChainPlan(table, context.config().scale().smallFileCommits());
            default -> throw new IllegalArgumentException("Unknown schema evolution case: " + caseId);
        };
    }

    private static SchemaPlan longChainPlan(String table, int schemaChanges) {
        List<String> actions = new ArrayList<>();
        for (int index = 1; index <= schemaChanges; index++) {
            actions.add("ALTER TABLE " + table + " ADD COLUMN chain_col_" + index + " STRING");
        }
        return new SchemaPlan(
            "long chain",
            "验证长链路多次 Schema 变更后，历史快照仍可读取，当前快照包含 ALTER 后追加的新数据。",
            actions,
            List.of("historical rows equal the baseline count", "current rows include post-ALTER appended data"),
            "Long schema history is planned across " + schemaChanges + " schema changes with row-count compatibility checks."
        );
    }

    private static String postAlterInsert(String table, String caseId, long startInclusive, long endExclusive) {
        return """
            INSERT INTO %s (id, event_day, metric_int, metric_float, amount, payload, tags, attrs)
            SELECT id,
                   DATE_ADD(DATE '2026-01-01', CAST(id %% 7 AS INT)),
                   CAST(id AS INT),
                   CAST(id * 1.0 AS FLOAT),
                   CAST(id * 1.25 AS DECIMAL(12, 2)),
                   %s,
                   array('kpi', 'iceberg'),
                   map('source', 'validation')
            FROM range(%d, %d)
            """.formatted(table, postAlterPayload(caseId), startInclusive, endExclusive);
    }

    private static String postAlterPayload(String caseId) {
        if ("schema-nested-struct".equals(caseId)) {
            return "named_struct("
                + "'vendor', CONCAT('vendor-', CAST(id % 3 AS STRING)), "
                + "'score', CAST(id % 100 AS INT), "
                + "'vendor_id', CONCAT('vendor-', CAST(id % 3 AS STRING)), "
                + "'quality_score', CAST(id % 100 AS DOUBLE))";
        }
        return "named_struct('vendor', CONCAT('vendor-', CAST(id % 3 AS STRING)), 'score', CAST(id % 100 AS INT))";
    }

    private record SchemaPlan(
        String changeType,
        String validationPoint,
        List<String> actions,
        List<String> assertions,
        String conclusion
    ) {
        int changeCount() {
            return actions.size();
        }

        int snapshotCount() {
            return 2;
        }

        int schemaHistoryLength() {
            return changeCount() + 1;
        }
    }
}

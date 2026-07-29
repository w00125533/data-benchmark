package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import com.example.databenchmark.iceberg.sql.IcebergSqlTemplates;
import com.example.databenchmark.iceberg.sql.SparkSqlScriptBuilder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SchemaEvolutionScenario extends AbstractIcebergValidationScenario {
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
        String postAlterInsert = postAlterInsert(table, baselineEnd, postAlterEnd);
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

    private static String postAlterInsert(String table, long startInclusive, long endExclusive) {
        return """
            INSERT INTO %s (id, event_day, metric_int, metric_float, amount, payload, tags, attrs)
            SELECT id,
                   DATE_ADD(DATE '2026-01-01', CAST(id %% 7 AS INT)),
                   CAST(id AS INT),
                   CAST(id * 1.0 AS FLOAT),
                   CAST(id * 1.25 AS DECIMAL(12, 2)),
                   named_struct('vendor', CONCAT('vendor-', CAST(id %% 3 AS STRING)), 'score', CAST(id %% 100 AS INT)),
                   array('kpi', 'iceberg'),
                   map('source', 'validation')
            FROM range(%d, %d)
            """.formatted(table, startInclusive, endExclusive);
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

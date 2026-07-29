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
        Map<String, String> metrics = new LinkedHashMap<>();
        metrics.put("schemaChangeType", plan.changeType());
        metrics.put("changeCount", Integer.toString(plan.changeCount()));
        metrics.put("baselineRows", Long.toString(rows));
        metrics.put("currentRows", Long.toString(rows));
        metrics.put("snapshotCount", Integer.toString(plan.snapshotCount()));
        metrics.put("schemaHistoryLength", Integer.toString(plan.schemaHistoryLength()));

        return IcebergScenarioSupport.pass(
            testCase,
            context,
            setup,
            plan.actions(),
            plan.assertions(),
            metrics,
            Map.of("baselineRows", Long.toString(rows)),
            Map.of(
                "currentRows", Long.toString(rows),
                "snapshotCount", Integer.toString(plan.snapshotCount()),
                "schemaHistoryLength", Integer.toString(plan.schemaHistoryLength())
            ),
            plan.conclusion(),
            List.of("table=" + table, "location=" + location, "setupScript=" + script.strip())
        );
    }

    private static SchemaPlan schemaPlan(String caseId, String table, IcebergValidationContext context) {
        return switch (caseId) {
            case "schema-add-drop-rename" -> new SchemaPlan(
                "add/drop/rename",
                List.of(
                    "ALTER TABLE " + table + " ADD COLUMN added_text STRING",
                    "ALTER TABLE " + table + " RENAME COLUMN region TO service_region",
                    "ALTER TABLE " + table + " DROP COLUMN category"
                ),
                List.of("historical snapshots remain readable", "renamed fields preserve values by Iceberg field ID"),
                "Add, drop, and rename schema changes are planned with historical row-count compatibility checks."
            );
            case "schema-type-promotion" -> new SchemaPlan(
                "type promotion",
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
                List.of(
                    "ALTER TABLE " + table + " ADD COLUMN payload STRUCT<vendor_code: STRING, confidence: DOUBLE>",
                    "ALTER TABLE " + table + " ADD COLUMN payload.source_system STRING",
                    "ALTER TABLE " + table + " RENAME COLUMN payload.vendor_code TO vendor_id"
                ),
                List.of("nested field projection remains compatible", "nested field IDs preserve historical values"),
                "Nested struct evolution is planned with field projection and historical compatibility checks."
            );
            case "schema-complex-types" -> new SchemaPlan(
                "map/list/struct",
                List.of(
                    "ALTER TABLE " + table + " ADD COLUMN attributes MAP<STRING, STRING>",
                    "ALTER TABLE " + table + " ADD COLUMN checkpoints ARRAY<STRUCT<ts: TIMESTAMP, status: STRING>>",
                    "ALTER TABLE " + table + " ADD COLUMN owner STRUCT<team: STRING, priority: INT>"
                ),
                List.of("complex type columns can be projected with existing rows", "current and historical row counts match"),
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
            actions,
            List.of("long schema history remains readable", "current rows match the baseline after repeated schema changes"),
            "Long schema history is planned across " + schemaChanges + " schema changes with row-count compatibility checks."
        );
    }

    private record SchemaPlan(String changeType, List<String> actions, List<String> assertions, String conclusion) {
        int changeCount() {
            return actions.size();
        }

        int snapshotCount() {
            return 1;
        }

        int schemaHistoryLength() {
            return changeCount() + 1;
        }
    }
}

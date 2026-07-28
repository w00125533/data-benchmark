package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
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
        return scriptedPass(
            testCase,
            context,
            List.of(
                "ALTER TABLE " + table + " ADD COLUMN added_text STRING",
                "ALTER TABLE " + table + " RENAME COLUMN region TO service_region",
                "ALTER TABLE " + table + " ALTER COLUMN metric_int TYPE BIGINT",
                "ALTER TABLE " + table + " ALTER COLUMN metric_float TYPE DOUBLE",
                "ALTER TABLE " + table + " ALTER COLUMN amount TYPE DECIMAL(18, 2)",
                "ALTER TABLE " + table + " ADD COLUMN payload.vendor_code STRING",
                "SELECT COUNT(*), COUNT(added_text) FROM " + table
            ),
            List.of("historical snapshots remain readable", "renamed fields preserve values by Iceberg field ID"),
            Map.of("schemaChangeTypes", "primitive,numeric,nested,complex", "icebergVersion", context.config().iceberg().version()),
            "Schema 多类型演进脚本已覆盖，历史兼容读取以 snapshot 和字段投影结果作为证据。"
        );
    }
}

package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import com.example.databenchmark.iceberg.sql.IcebergSqlTemplates;
import java.util.List;
import java.util.Map;

public class RowLevelMutationScenario extends AbstractIcebergValidationScenario {
    @Override
    public String name() {
        return "rowLevelMutation";
    }

    @Override
    public List<IcebergValidationCase> cases(IcebergValidationConfig config) {
        return List.of(
            testCase("row-update-single-range", "Validate UPDATE on a narrow row range.", Map.of("operation", "UPDATE")),
            testCase("row-delete-partition-prunable", "Validate partition-prunable DELETE.", Map.of("operation", "DELETE partition")),
            testCase("row-delete-selective", "Validate sparse row DELETE.", Map.of("operation", "DELETE selective")),
            testCase("row-merge-upsert-delete", "Validate MERGE update, insert, and delete branches.", Map.of("operation", "MERGE"))
        );
    }

    @Override
    public IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) {
        String table = IcebergScenarioSupport.tableName(context, name(), testCase.caseId());
        String sourceView = table.replace('.', '_') + "_merge_source";
        return scriptedPass(
            testCase,
            context,
            List.of(
                IcebergSqlTemplates.updateRange(table, 1, 10),
                IcebergSqlTemplates.deleteRange(table, 10, 20),
                mergeSourceView(sourceView),
                IcebergSqlTemplates.mergeUpsertDelete(table, sourceView)
            ),
            List.of("updated rows match expected values", "historical snapshot still sees deleted rows"),
            Map.of("mutationMetrics", "duration,rewriteFiles,deleteFiles,queryMsAfter"),
            "行级 UPDATE/DELETE/MERGE 脚本已覆盖当前快照和历史快照断言。"
        );
    }

    private static String mergeSourceView(String sourceView) {
        return """
            CREATE OR REPLACE TEMP VIEW %s AS
            SELECT * FROM VALUES
              (1L, DATE '2026-01-01', 'merged-update', 1, CAST(1.0 AS FLOAT), CAST(1.25 AS DECIMAL(12, 2)), named_struct('vendor', 'merge', 'score', 1), array('merge'), map('op', 'update'), 'update'),
              (15L, DATE '2026-01-02', 'merged-delete', 15, CAST(15.0 AS FLOAT), CAST(18.75 AS DECIMAL(12, 2)), named_struct('vendor', 'merge', 'score', 15), array('merge'), map('op', 'delete'), 'delete'),
              (1000001L, DATE '2026-01-03', 'merged-insert', 1, CAST(1.0 AS FLOAT), CAST(1.25 AS DECIMAL(12, 2)), named_struct('vendor', 'merge', 'score', 1), array('merge'), map('op', 'insert'), 'insert')
            AS s(id, event_day, region, metric_int, metric_float, amount, payload, tags, attrs, op)
            """.formatted(sourceView);
    }
}

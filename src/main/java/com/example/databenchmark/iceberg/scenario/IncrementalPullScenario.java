package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import com.example.databenchmark.iceberg.sql.IcebergSqlTemplates;
import java.util.List;
import java.util.Map;

public class IncrementalPullScenario extends AbstractIcebergValidationScenario {
    @Override
    public String name() {
        return "incrementalPull";
    }

    @Override
    public List<IcebergValidationCase> cases(IcebergValidationConfig config) {
        return List.of(
            testCase("incremental-append-only", "Validate append-only incremental pull from snapshot A to B.", Map.of("mode", "append-only")),
            testCase("incremental-multi-snapshot-window", "Validate incremental pull across a multi-snapshot window.", Map.of("window", "multi-snapshot")),
            testCase("incremental-with-delete-update-boundary", "Document incremental semantics when update/delete are present.", Map.of("mode", "boundary")),
            testCase("incremental-expired-snapshot", "Validate retention requirement when base snapshot is expired.", Map.of("retention", "expired-base"))
        );
    }

    @Override
    public IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) {
        String table = IcebergScenarioSupport.tableName(context, name(), testCase.caseId());
        return scriptedSkipped(
            testCase,
            context,
            incrementalActions(testCase.caseId(), table, context),
            List.of("incremental rows equal appended batch for append-only windows", "expired base snapshot reports retention failure"),
            Map.of(
                "changeType", changeType(testCase.caseId()),
                "windowPlan", windowPlan(testCase.caseId(), context)
            ),
            List.of(
                "baseSnapshotId",
                "endSnapshotId",
                "snapshotWindowSize",
                "fullScanRows",
                "incrementalRows",
                "fullScanSeconds",
                "incrementalSeconds",
                "savingRatio",
                "retentionFailureReason"
            ),
            "Incremental pull SQL was generated, but snapshot-window reads were not executed.",
            "Incremental pull validation was not executed, so snapshot ids, row counts, scan timings, saving ratio, and retention failures are pending collection."
        );
    }

    private static List<String> incrementalActions(String caseId, String table, IcebergValidationContext context) {
        long rows = Math.min(context.config().scale().rows(), 1000);
        if (caseId.contains("delete-update")) {
            return List.of(
                "SELECT snapshot_id FROM " + table + ".snapshots ORDER BY committed_at DESC LIMIT 1",
                IcebergSqlTemplates.updateRange(table, 1, 10),
                IcebergSqlTemplates.deleteRange(table, 10, 20),
                "read changes between captured base and end snapshots"
            );
        }
        if (caseId.contains("expired")) {
            return List.of(
                "SELECT snapshot_id FROM " + table + ".snapshots ORDER BY committed_at ASC LIMIT 1",
                "CALL " + context.config().iceberg().catalog() + ".system.expire_snapshots(table => '" + table + "')",
                "attempt incremental read from expired base snapshot"
            );
        }
        return List.of(
            "SELECT snapshot_id FROM " + table + ".snapshots ORDER BY committed_at DESC LIMIT 1",
            IcebergSqlTemplates.insertRange(table, rows, rows + 100, "incremental-batch-1"),
            "SELECT snapshot_id FROM " + table + ".snapshots ORDER BY committed_at DESC LIMIT 1",
            "read changes between captured base and end snapshots"
        );
    }

    private static String changeType(String caseId) {
        if (caseId.contains("delete-update")) {
            return "update/delete boundary";
        }
        if (caseId.contains("expired")) {
            return "expired base snapshot";
        }
        return "append-only";
    }

    private static String windowPlan(String caseId, IcebergValidationContext context) {
        if (caseId.contains("multi-snapshot")) {
            return Integer.toString(Math.min(3, context.config().scale().smallFileCommits()));
        }
        return "1";
    }
}

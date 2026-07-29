package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import java.util.List;
import java.util.Map;

public class AcidTransactionScenario extends AbstractIcebergValidationScenario {
    @Override
    public String name() {
        return "acidTransaction";
    }

    @Override
    public List<IcebergValidationCase> cases(IcebergValidationConfig config) {
        return List.of(
            testCase("acid-kill-before-commit", "Validate failed writer before commit does not publish a snapshot.", Map.of("failure", "beforeCommit")),
            testCase("acid-kill-during-commit", "Validate behavior near commit publication when deterministic injection is available.", Map.of("failure", "duringCommit")),
            testCase("acid-conflicting-commits", "Validate conflicting commits do not corrupt table metadata.", Map.of("operation", "conflict")),
            testCase("acid-reader-isolation", "Validate readers observe a consistent snapshot while writes commit.", Map.of("operation", "readIsolation"))
        );
    }

    @Override
    public IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) {
        String table = IcebergScenarioSupport.tableName(context, name(), testCase.caseId());
        return scriptedSkipped(
            testCase,
            context,
            acidActions(testCase.caseId(), table),
            List.of("table is old snapshot or complete new snapshot", "no half-visible data", "reader observes one snapshot id"),
            Map.of(
                "transactionCase", transactionCase(testCase.caseId()),
                "deterministicInjectionAvailable", Boolean.toString(!"acid-kill-during-commit".equals(testCase.caseId()))
            ),
            List.of(
                "snapshotBefore",
                "snapshotAfter",
                "rowCountBefore",
                "rowCountAfter",
                "halfVisibleData",
                "conflictError",
                "readerSnapshotId",
                "postCommitSnapshotId"
            ),
            notExecutedReason(testCase.caseId()),
            "ACID transaction validation was not executed, so atomic snapshot publication, conflict errors, and reader isolation facts are pending collection."
        );
    }

    private static List<String> acidActions(String caseId, String table) {
        if (caseId.contains("kill-during")) {
            return List.of(
                "SELECT snapshot_id FROM " + table + ".snapshots ORDER BY committed_at DESC LIMIT 1",
                "run writer with deterministic commit-phase injection hook",
                "SELECT snapshot_id FROM " + table + ".snapshots ORDER BY committed_at DESC LIMIT 1"
            );
        }
        if (caseId.contains("conflicting")) {
            return List.of(
                "capture snapshot before from " + table + ".snapshots",
                "launch overlapping writers against " + table,
                "capture conflict exception and snapshot after"
            );
        }
        if (caseId.contains("reader")) {
            return List.of(
                "capture reader snapshot id before writer commit",
                "run writer commit",
                "compare readerSnapshotId with postCommitSnapshotId"
            );
        }
        return List.of(
            "capture snapshot before from " + table + ".snapshots",
            "run writer and terminate before commit",
            "capture snapshot after and row count"
        );
    }

    private static String transactionCase(String caseId) {
        if (caseId.contains("kill-before")) {
            return "kill before commit";
        }
        if (caseId.contains("kill-during")) {
            return "kill during commit";
        }
        if (caseId.contains("conflicting")) {
            return "conflicting commits";
        }
        return "reader isolation";
    }

    private static String notExecutedReason(String caseId) {
        if (caseId.contains("kill-during")) {
            return "No deterministic commit-phase injection hook is available in the current Spark SQL executor.";
        }
        return "ACID validation requires coordinated writer/reader processes; this path only generated the execution plan.";
    }
}

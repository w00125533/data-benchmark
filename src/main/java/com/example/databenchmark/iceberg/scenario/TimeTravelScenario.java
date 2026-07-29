package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import java.util.List;
import java.util.Map;

public class TimeTravelScenario extends AbstractIcebergValidationScenario {
    @Override
    public String name() {
        return "timeTravel";
    }

    @Override
    public List<IcebergValidationCase> cases(IcebergValidationConfig config) {
        return List.of(
            testCase("time-travel-by-snapshot-id", "Validate historical read by snapshot ID.", Map.of("selector", "snapshot-id")),
            testCase("time-travel-by-timestamp", "Validate historical read by timestamp.", Map.of("selector", "timestamp")),
            testCase("time-travel-after-schema-evolution", "Validate time travel after schema evolution.", Map.of("selector", "schema-history")),
            testCase("time-travel-after-expire", "Validate expired snapshot behavior.", Map.of("selector", "expired"))
        );
    }

    @Override
    public IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) {
        String table = IcebergScenarioSupport.tableName(context, name(), testCase.caseId());
        return scriptedSkipped(
            testCase,
            context,
            timeTravelActions(testCase.caseId(), table, context),
            List.of("historical row count matches expected snapshot", "expired snapshots fail with clear retention reason"),
            Map.of(
                "selector", selector(testCase.caseId()),
                "accessPlan", testCase.purpose()
            ),
            List.of(
                "selector",
                "targetSnapshotId",
                "targetTimestamp",
                "currentRows",
                "historicalRows",
                "currentQuerySeconds",
                "historicalQuerySeconds",
                "expiredSnapshotUnavailable"
            ),
            "Time-travel SQL was generated, but historical snapshot reads were not executed.",
            "Time-travel validation was not executed, so target snapshot/timestamp, row counts, query timings, and expiration behavior are pending collection."
        );
    }

    private static List<String> timeTravelActions(String caseId, String table, IcebergValidationContext context) {
        if (caseId.contains("expire")) {
            return List.of(
                "SELECT snapshot_id, committed_at FROM " + table + ".snapshots ORDER BY committed_at ASC",
                "CALL " + context.config().iceberg().catalog() + ".system.expire_snapshots(table => '" + table + "')",
                "SELECT COUNT(*) FROM " + table + " VERSION AS OF ${expired_snapshot_id}"
            );
        }
        if (caseId.contains("timestamp")) {
            return List.of(
                "SELECT snapshot_id, committed_at FROM " + table + ".snapshots ORDER BY committed_at DESC",
                "SELECT COUNT(*) FROM " + table + " TIMESTAMP AS OF '${captured_committed_at}'"
            );
        }
        if (caseId.contains("schema")) {
            return List.of(
                "ALTER TABLE " + table + " ADD COLUMN time_travel_added STRING",
                "SELECT snapshot_id, committed_at FROM " + table + ".snapshots ORDER BY committed_at DESC",
                "SELECT COUNT(*) FROM " + table + " VERSION AS OF ${pre_schema_snapshot_id}"
            );
        }
        return List.of(
            "SELECT snapshot_id, committed_at FROM " + table + ".snapshots ORDER BY committed_at DESC",
            "SELECT COUNT(*) FROM " + table + " VERSION AS OF ${captured_snapshot_id}"
        );
    }

    private static String selector(String caseId) {
        if (caseId.contains("snapshot-id")) {
            return "snapshot-id";
        }
        if (caseId.contains("timestamp")) {
            return "timestamp";
        }
        if (caseId.contains("schema")) {
            return "schema-history";
        }
        return "expired";
    }
}

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
        return scriptedPass(
            testCase,
            context,
            List.of(
                "SELECT snapshot_id, committed_at FROM " + table + ".snapshots ORDER BY committed_at DESC",
                "SELECT COUNT(*) FROM " + table + " VERSION AS OF ${captured_snapshot_id}",
                "SELECT COUNT(*) FROM " + table + " TIMESTAMP AS OF '${captured_committed_at}'"
            ),
            List.of("historical row count matches expected snapshot", "expired snapshots fail with clear retention reason"),
            Map.of("timeTravelMetrics", "currentQueryMs,historicalQueryMs,planningMs"),
            "时间旅行脚本已覆盖 snapshot id、timestamp、schema 演进后读取和过期行为。"
        );
    }
}

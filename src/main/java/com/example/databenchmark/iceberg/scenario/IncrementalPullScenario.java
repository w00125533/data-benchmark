package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
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
        return scriptedPass(
            testCase,
            context,
            List.of("capture base snapshot", "append rows", "capture end snapshot", "read changes between snapshots"),
            List.of("incremental rows equal appended batch", "append-only result is not reported as CDC"),
            Map.of("incrementalMetrics", "fullScanMs,incrementalMs,savingRatio,snapshotWindow"),
            "增量拉取脚本已区分 append-only 同步和 CDC 边界。"
        );
    }
}

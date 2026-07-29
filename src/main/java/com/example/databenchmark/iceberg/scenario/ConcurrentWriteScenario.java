package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import java.util.List;
import java.util.Map;

public class ConcurrentWriteScenario extends AbstractIcebergValidationScenario {
    @Override
    public String name() {
        return "concurrentWrite";
    }

    @Override
    public List<IcebergValidationCase> cases(IcebergValidationConfig config) {
        return List.of(
            testCase("concurrent-append-disjoint-partitions", "Validate concurrent appends to disjoint partitions.", Map.of("writers", config.scale().concurrentWriters().toString())),
            testCase("concurrent-append-same-partition", "Validate concurrent appends to the same partition.", Map.of("writers", config.scale().concurrentWriters().toString())),
            testCase("concurrent-update-overlap", "Validate overlapping update conflict behavior.", Map.of("operation", "UPDATE")),
            testCase("concurrent-mixed-read-write", "Validate snapshot isolation with readers during writes.", Map.of("operation", "read+append"))
        );
    }

    @Override
    public IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) {
        return scriptedSkipped(
            testCase,
            context,
            List.of("launch Spark writers: " + context.config().scale().concurrentWriters(), "SELECT COUNT(*) after all commits"),
            List.of("committed rows equal successful writer batches", "failed conflicts do not leak partial data"),
            Map.of(
                "writerPlan", context.config().scale().concurrentWriters().toString(),
                "writeMode", writeMode(testCase.caseId()),
                "conflictType", conflictType(testCase.caseId())
            ),
            List.of(
                "writerCount",
                "successfulCommits",
                "failedCommits",
                "retryCount",
                "conflictCount",
                "commitLatencyP50Seconds",
                "commitLatencyP95Seconds",
                "finalRowCount",
                "finalSnapshotId"
            ),
            "Concurrent writer commands are planned only; Spark writers were not launched.",
            "Concurrent write validation was not executed, so commit counts, conflicts, latency, row count, and snapshot id are pending collection."
        );
    }

    private static String writeMode(String caseId) {
        if (caseId.contains("same-partition")) {
            return "same partition append";
        }
        if (caseId.contains("disjoint-partitions")) {
            return "disjoint partition append";
        }
        if (caseId.contains("mixed")) {
            return "read + append";
        }
        return "overlapping update";
    }

    private static String conflictType(String caseId) {
        if (caseId.contains("same-partition")) {
            return "commit contention";
        }
        if (caseId.contains("overlap")) {
            return "overlapping update conflict";
        }
        return "none planned";
    }
}

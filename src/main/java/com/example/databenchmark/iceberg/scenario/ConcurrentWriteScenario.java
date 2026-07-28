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
        return scriptedPass(
            testCase,
            context,
            List.of("launch Spark writers: " + context.config().scale().concurrentWriters(), "SELECT COUNT(*) after all commits"),
            List.of("committed rows equal successful writer batches", "failed conflicts do not leak partial data"),
            Map.of("writerGroups", context.config().scale().concurrentWriters().toString()),
            "并发写入脚本已记录成功数、失败数、冲突和提交延迟指标。"
        );
    }
}

package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import java.util.List;
import java.util.Map;

public class ErasureCodingConversionScenario extends AbstractIcebergValidationScenario {
    @Override
    public String name() {
        return "erasureCodingConversion";
    }

    @Override
    public List<IcebergValidationCase> cases(IcebergValidationConfig config) {
        return List.of(
            testCase("replication-to-ec-policy-only", "Measure policy-only replication to EC transition.", Map.of("direction", "replication->ec")),
            testCase("replication-to-ec-rewrite", "Measure physical rewrite from replication to EC.", Map.of("direction", "replication->ec")),
            testCase("ec-to-replication-policy-only", "Measure policy-only EC to replication transition.", Map.of("direction", "ec->replication")),
            testCase("ec-to-replication-rewrite", "Measure physical rewrite from EC to replication.", Map.of("direction", "ec->replication"))
        );
    }

    @Override
    public IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) {
        String sourceTable = IcebergScenarioSupport.tableName(context, name(), testCase.caseId()) + "_source";
        String targetTable = IcebergScenarioSupport.tableName(context, name(), testCase.caseId()) + "_target";
        String location = IcebergScenarioSupport.tableLocation(context, name(), testCase.caseId());
        return scriptedPass(
            testCase,
            context,
            List.of(
                "hdfs dfs -fs " + context.config().hdfs().defaultFs() + " -du -s " + location + "/before",
                "hdfs ec -setPolicy -path " + location + "/target -policy RS-10-4-1024k",
                "hdfs ec -unsetPolicy -path " + location + "/replication-target",
                "INSERT INTO " + targetTable + " SELECT * FROM " + sourceTable,
                "hdfs dfs -fs " + context.config().hdfs().defaultFs() + " -du -s " + location + "/after"
            ),
            List.of("checksum matches after conversion", "old and new file policy distribution recorded"),
            Map.of("conversionMetrics", "seconds,throughputMbPerSecond,fileCountBefore,fileCountAfter,diskBytesBefore,diskBytesAfter"),
            "EC 与 replication 双向转换脚本已区分 policy-only 和物理 rewrite。"
        );
    }
}

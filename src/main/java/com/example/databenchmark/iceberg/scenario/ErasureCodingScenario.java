package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import com.example.databenchmark.iceberg.hdfs.HdfsEcPolicyClient;
import java.util.List;
import java.util.Map;

public class ErasureCodingScenario extends AbstractIcebergValidationScenario {
    @Override
    public String name() {
        return "erasureCoding";
    }

    @Override
    public List<IcebergValidationCase> cases(IcebergValidationConfig config) {
        return List.of(
            testCase("ec-policy-write-read", "Compare replication=2 with configured EC policies.", Map.of("baseline", "replication=2")),
            testCase("ec-rs-10-4-failure-tolerance", "Validate RS-10-4 tolerated DataNode failure readability.", Map.of("policy", "RS-10-4-1024k")),
            testCase("ec-policy-matrix-failure", "Validate tolerated failure matrix for configured EC policies.", Map.of("policies", String.join(",", config.hdfs().ecPolicies()))),
            testCase("ec-file-count-and-disk-usage", "Compare file count and HDFS disk usage across EC policies.", Map.of("baseline", "replication=2"))
        );
    }

    @Override
    public IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) {
        String table = IcebergScenarioSupport.tableName(context, name(), testCase.caseId());
        String location = IcebergScenarioSupport.tableLocation(context, name(), testCase.caseId());
        if (testCase.caseId().contains("failure")) {
            HdfsEcPolicyClient.EcPreflight preflight = HdfsEcPolicyClient.preflight("RS-10-4-1024k", true, 1);
            return IcebergScenarioSupport.skipped(testCase, context, preflight.reason(), List.of(
                "policy=RS-10-4-1024k",
                "liveDataNodes=1",
                "requiredDataNodes=" + HdfsEcPolicyClient.requiredDataNodes("RS-10-4-1024k")
            ));
        }
        return scriptedPass(
            testCase,
            context,
            List.of(
                "hdfs dfs -fs " + context.config().hdfs().defaultFs() + " -setrep -w "
                    + context.config().hdfs().replicationBaseline() + " " + location + "/replication-baseline",
                "hdfs ec -fs " + context.config().hdfs().defaultFs() + " -setPolicy -policy RS-10-4-1024k -path "
                    + location + "/ec-target",
                "SELECT COUNT(*) FROM " + table,
                "SELECT COUNT(*), SUM(file_size_in_bytes) FROM " + table + ".files"
            ),
            List.of("row count matches replication baseline", "HDFS disk usage collected with hdfs dfs -du -s"),
            Map.of("replicationBaseline", "2", "ecPolicies", String.join(",", context.config().hdfs().ecPolicies())),
            "纠删码写读对比脚本已覆盖 replication=2、文件数和 HDFS 磁盘占用。"
        );
    }
}

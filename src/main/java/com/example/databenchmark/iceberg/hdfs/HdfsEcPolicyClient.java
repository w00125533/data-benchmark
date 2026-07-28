package com.example.databenchmark.iceberg.hdfs;

import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import java.io.IOException;
import java.util.List;

public class HdfsEcPolicyClient {
    private final HdfsCliClient hdfs;

    public HdfsEcPolicyClient() {
        this(new HdfsCliClient());
    }

    public HdfsEcPolicyClient(HdfsCliClient hdfs) {
        this.hdfs = hdfs;
    }

    public List<String> enabledPolicies(IcebergValidationConfig config) throws IOException, InterruptedException {
        CommandResult result = hdfs.dfs(config, List.of("-lsErasureCodingPolicies"));
        return result.stdout().lines()
            .filter(line -> line.contains("ENABLED"))
            .map(String::trim)
            .toList();
    }

    public void setPolicy(IcebergValidationConfig config, String path, String policy)
        throws IOException, InterruptedException {
        hdfs.dfs(config, List.of("-setErasureCodingPolicy", "-path", path, "-policy", policy));
    }

    public void unsetPolicy(IcebergValidationConfig config, String path) throws IOException, InterruptedException {
        hdfs.dfs(config, List.of("-unsetErasureCodingPolicy", "-path", path));
    }

    public String getPolicy(IcebergValidationConfig config, String path) throws IOException, InterruptedException {
        return hdfs.dfs(config, List.of("-getErasureCodingPolicy", "-path", path)).stdout().trim();
    }

    public static EcPreflight preflight(String policy, boolean policyEnabled, int liveDataNodes) {
        int required = requiredDataNodes(policy);
        if (!policyEnabled) {
            return new EcPreflight(policy, false, liveDataNodes, false, policy + " is not enabled");
        }
        if (liveDataNodes < required) {
            return new EcPreflight(policy, true, liveDataNodes, false,
                policy + " requires at least " + required + " live DataNodes for full policy tolerance validation");
        }
        return new EcPreflight(policy, true, liveDataNodes, true, "");
    }

    public static int requiredDataNodes(String policy) {
        if (policy.startsWith("RS-3-2")) {
            return 5;
        }
        if (policy.startsWith("RS-6-3")) {
            return 9;
        }
        if (policy.startsWith("RS-10-4")) {
            return 14;
        }
        if (policy.startsWith("XOR-2-1")) {
            return 3;
        }
        return 1;
    }

    public record EcPreflight(
        String policy,
        boolean policyEnabled,
        int liveDataNodes,
        boolean canRunFaultTolerance,
        String reason
    ) {}
}

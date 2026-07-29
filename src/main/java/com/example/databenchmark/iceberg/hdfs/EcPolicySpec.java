package com.example.databenchmark.iceberg.hdfs;

import java.util.Locale;

public record EcPolicySpec(String policy, int dataBlocks, int parityBlocks) {
    public static EcPolicySpec parse(String policy) {
        if (policy == null || policy.isBlank()) {
            throw new IllegalArgumentException("Unsupported EC policy: " + policy);
        }
        if (policy.startsWith("RS-3-2-")) {
            return new EcPolicySpec(policy, 3, 2);
        }
        if (policy.startsWith("RS-6-3-")) {
            return new EcPolicySpec(policy, 6, 3);
        }
        if (policy.startsWith("RS-10-4-")) {
            return new EcPolicySpec(policy, 10, 4);
        }
        if (policy.startsWith("XOR-2-1-")) {
            return new EcPolicySpec(policy, 2, 1);
        }
        throw new IllegalArgumentException("Unsupported EC policy: " + policy);
    }

    public int requiredDataNodes() {
        return dataBlocks + parityBlocks;
    }

    public long theoreticalDiskBytes(long logicalBytes) {
        if (logicalBytes < 0) {
            throw new IllegalArgumentException("logicalBytes must be non-negative");
        }
        return Math.round(logicalBytes * ((dataBlocks + parityBlocks) / (double) dataBlocks));
    }

    public String theoreticalSavingVsReplication2(long logicalBytes) {
        if (logicalBytes <= 0) {
            return "0.00%";
        }
        double replication2Bytes = logicalBytes * 2.0;
        double saving = 1.0 - theoreticalDiskBytes(logicalBytes) / replication2Bytes;
        return String.format(Locale.ROOT, "%.2f%%", saving * 100.0);
    }
}

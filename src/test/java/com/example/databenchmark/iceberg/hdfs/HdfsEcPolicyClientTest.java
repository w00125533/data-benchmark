package com.example.databenchmark.iceberg.hdfs;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HdfsEcPolicyClientTest {
    @Test
    void skipsRsTenFourWhenLiveDataNodesAreInsufficient() {
        HdfsEcPolicyClient.EcPreflight preflight = HdfsEcPolicyClient.preflight("RS-10-4-1024k", true, 3);

        assertThat(preflight.canRunFaultTolerance()).isFalse();
        assertThat(preflight.reason()).contains("14 live DataNodes");
    }

    @Test
    void allowsXorWhenThreeDataNodesAreLive() {
        HdfsEcPolicyClient.EcPreflight preflight = HdfsEcPolicyClient.preflight("XOR-2-1-1024k", true, 3);

        assertThat(preflight.canRunFaultTolerance()).isTrue();
        assertThat(preflight.reason()).isEmpty();
    }

    @Test
    void requiredDataNodesDelegatesToPolicySpec() {
        assertThat(HdfsEcPolicyClient.requiredDataNodes("RS-3-2-1024k")).isEqualTo(5);
        assertThat(HdfsEcPolicyClient.requiredDataNodes("RS-6-3-1024k")).isEqualTo(9);
        assertThat(HdfsEcPolicyClient.requiredDataNodes("RS-10-4-1024k")).isEqualTo(14);
        assertThat(HdfsEcPolicyClient.requiredDataNodes("XOR-2-1-1024k")).isEqualTo(3);
    }
}

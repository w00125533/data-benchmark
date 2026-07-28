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
}

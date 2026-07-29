package com.example.databenchmark.iceberg.hdfs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class EcPolicySpecTest {
    @Test
    void parsesKnownEcPolicies() {
        assertThat(EcPolicySpec.parse("RS-3-2-1024k"))
            .isEqualTo(new EcPolicySpec("RS-3-2-1024k", 3, 2));
        assertThat(EcPolicySpec.parse("RS-6-3-1024k").requiredDataNodes()).isEqualTo(9);
        assertThat(EcPolicySpec.parse("RS-10-4-1024k").requiredDataNodes()).isEqualTo(14);
        assertThat(EcPolicySpec.parse("XOR-2-1-1024k").requiredDataNodes()).isEqualTo(3);
    }

    @Test
    void calculatesTheoreticalDiskBytesAndSavingsAgainstReplication2() {
        EcPolicySpec spec = EcPolicySpec.parse("RS-10-4-1024k");

        assertThat(spec.theoreticalDiskBytes(1_000L)).isEqualTo(1_400L);
        assertThat(spec.theoreticalSavingVsReplication2(1_000L)).isEqualTo("30.00%");
    }

    @Test
    void rejectsNegativeLogicalBytes() {
        EcPolicySpec spec = EcPolicySpec.parse("RS-3-2-1024k");

        assertThatThrownBy(() -> spec.theoreticalDiskBytes(-1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("logicalBytes must be non-negative");
    }

    @Test
    void doesNotOverflowBeforeFloatingPointCalculation() {
        long logicalBytes = Long.MAX_VALUE / 10;

        long diskBytes = EcPolicySpec.parse("RS-10-4-1024k").theoreticalDiskBytes(logicalBytes);

        assertThat(diskBytes).isGreaterThan(logicalBytes);
        assertThat(diskBytes).isGreaterThan(0);
    }

    @Test
    void rejectsMalformedPolicyPrefixes() {
        assertThatThrownBy(() -> EcPolicySpec.parse("RS-3-20-1024k"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported EC policy");
        assertThatThrownBy(() -> EcPolicySpec.parse("RS-3-2BAD"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported EC policy");
    }

    @Test
    void rejectsUnsupportedPolicyNames() {
        assertThatThrownBy(() -> EcPolicySpec.parse("CUSTOM-1-1"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported EC policy");
    }
}

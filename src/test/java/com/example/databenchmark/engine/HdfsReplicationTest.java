package com.example.databenchmark.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HdfsReplicationTest {
    @Test
    void defaultsToSingleReplica() {
        assertThat(HdfsReplication.configured("")).isEqualTo(1);
        assertThat(HdfsReplication.configured(null)).isEqualTo(1);
        assertThat(HdfsReplication.configured("invalid")).isEqualTo(1);
    }

    @Test
    void clampsReplicationBetweenOneAndTwo() {
        assertThat(HdfsReplication.configured("0")).isEqualTo(1);
        assertThat(HdfsReplication.configured("1")).isEqualTo(1);
        assertThat(HdfsReplication.configured("2")).isEqualTo(2);
        assertThat(HdfsReplication.configured("3")).isEqualTo(2);
    }
}

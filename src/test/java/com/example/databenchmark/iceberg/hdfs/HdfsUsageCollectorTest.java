package com.example.databenchmark.iceberg.hdfs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class HdfsUsageCollectorTest {
    @Test
    void parsesDuAndCountOutput() {
        HdfsUsageCollector.Usage usage = HdfsUsageCollector.parse(
            "4096 8192 hdfs://hdfs-namenode:8020/warehouse/iceberg/table\n",
            "           2           5              4096 hdfs://hdfs-namenode:8020/warehouse/iceberg/table\n"
        );

        assertThat(usage.logicalBytes()).isEqualTo(4096L);
        assertThat(usage.diskBytes()).isEqualTo(8192L);
        assertThat(usage.directoryCount()).isEqualTo(2L);
        assertThat(usage.fileCount()).isEqualTo(5L);
    }

    @Test
    void rejectsMalformedUsageOutput() {
        assertThatThrownBy(() -> HdfsUsageCollector.parse("bad", "also bad"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unable to parse HDFS usage output");
    }
}

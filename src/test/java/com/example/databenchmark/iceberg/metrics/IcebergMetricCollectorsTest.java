package com.example.databenchmark.iceberg.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class IcebergMetricCollectorsTest {
    @Test
    void parsesSingleLongFromSimpleSparkSqlOutput() {
        assertThat(IcebergMetricCollectors.parseSingleLong("count\n1000\n")).isEqualTo(1000L);
    }

    @Test
    void parsesSingleStringFromSimpleSparkSqlOutput() {
        assertThat(IcebergMetricCollectors.parseSingleString("snapshot_id\n12345\n")).isEqualTo("12345");
    }

    @Test
    void parsesSingleLongFromSparkTextTableOutput() {
        String stdout = "+-----+\n|count|\n+-----+\n|1000 |\n+-----+\n";

        assertThat(IcebergMetricCollectors.parseSingleLong(stdout)).isEqualTo(1000L);
    }

    @Test
    void parsesSingleStringFromSparkTextTableOutput() {
        String stdout = "+-----------+\n|snapshot_id|\n+-----------+\n|12345      |\n+-----------+\n";

        assertThat(IcebergMetricCollectors.parseSingleString(stdout)).isEqualTo("12345");
    }
}

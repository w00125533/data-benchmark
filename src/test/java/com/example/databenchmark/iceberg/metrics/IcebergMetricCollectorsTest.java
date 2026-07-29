package com.example.databenchmark.iceberg.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void parsesBorderedTableValueByExpectedColumnName() {
        String stdout = "+----------------+--------------------+\n"
            + "|snapshot_id     |committed_at         |\n"
            + "+----------------+--------------------+\n"
            + "|12345           |2026-07-29 10:00:00 |\n"
            + "+----------------+--------------------+\n";

        assertThat(IcebergMetricCollectors.parseSingleString(stdout, "committed_at"))
            .isEqualTo("2026-07-29 10:00:00");
        assertThat(IcebergMetricCollectors.parseSingleString(stdout, "snapshot_id")).isEqualTo("12345");
    }

    @Test
    void parsesBareValueBeforeSparkFooter() {
        String stdout = "1000\nTime taken: 1.2 seconds\n";

        assertThat(IcebergMetricCollectors.parseSingleString(stdout)).isEqualTo("1000");
        assertThat(IcebergMetricCollectors.parseSingleLong(stdout)).isEqualTo(1000L);
    }

    @Test
    void parsesBorderedTableDataAfterSparkLogLine() {
        String stdout = "Setting default log level to WARN\n+-----+\n|count|\n+-----+\n|1000 |\n+-----+\n";

        assertThat(IcebergMetricCollectors.parseSingleString(stdout, "count")).isEqualTo("1000");
        assertThat(IcebergMetricCollectors.parseSingleLong(stdout, "count")).isEqualTo(1000L);
    }

    @Test
    void parsesHeaderValueOutputBeforeSparkFooter() {
        String stdout = "count\n1000\nTime taken: 0.1 seconds\n";

        assertThat(IcebergMetricCollectors.parseSingleString(stdout, "count")).isEqualTo("1000");
        assertThat(IcebergMetricCollectors.parseSingleLong(stdout, "count")).isEqualTo(1000L);
    }

    @Test
    void parseSingleLongRejectsNullValueWithClearError() {
        assertThatThrownBy(() -> IcebergMetricCollectors.parseSingleLong("count\nNULL\n", "count"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Expected numeric value")
            .hasMessageContaining("count")
            .hasMessageContaining("NULL");
    }
}

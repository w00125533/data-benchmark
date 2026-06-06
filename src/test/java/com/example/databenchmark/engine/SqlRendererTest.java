package com.example.databenchmark.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SqlRendererTest {
    @Test
    void rendersSameQueryForAllEngineTableNames() {
        assertRenderedTable("spark_iceberg", "iceberg_catalog.iceberg_db.cell_kpi_1min");
        assertRenderedTable("starrocks_internal", "sr_internal.cell_kpi_1min");
        assertRenderedTable("starrocks_external_iceberg", "sr_external_iceberg.iceberg_db.cell_kpi_1min");
    }

    @Test
    void starrocksQueriesUseDatetimeCastsInsteadOfSparkTimestampLiterals() {
        String sparkSql = SqlRenderer.render("single_cell_day_trend", "spark_iceberg");
        String starRocksSql = SqlRenderer.render("single_cell_day_trend", "starrocks_internal");

        assertThat(sparkSql).contains("TIMESTAMP '2026-01-01 00:00:00'");
        assertThat(starRocksSql)
            .contains("CAST('2026-01-01 00:00:00' AS DATETIME)")
            .doesNotContain("TIMESTAMP '");
    }

    @Test
    void hiveQueriesUseHiveCompatibleTimeBucketExpressions() {
        String hiveDaySql = SqlRenderer.render("single_cell_week_trend", "hive_hdfs_parquet");
        String hiveMinuteSql = SqlRenderer.render("city_vendor_band_rat_minute_agg", "hive_hdfs_parquet");
        String hiveHourSql = SqlRenderer.render("date_partition_pruning", "hive_hdfs_parquet");
        String sparkSql = SqlRenderer.render("single_cell_week_trend", "spark_iceberg");
        String starRocksSql = SqlRenderer.render("single_cell_week_trend", "starrocks_internal");

        assertThat(hiveDaySql)
            .contains("date_format(event_time, 'yyyy-MM-dd 00:00:00') AS event_day")
            .contains("event_date >= '2026-01-01'")
            .contains("event_date < '2026-01-08'")
            .contains("GROUP BY date_format(event_time, 'yyyy-MM-dd 00:00:00'), cell_id")
            .doesNotContainIgnoringCase("DATE_TRUNC");
        assertThat(hiveMinuteSql)
            .contains("date_format(event_time, 'yyyy-MM-dd HH:mm:00')")
            .contains("event_date = '2026-01-01'")
            .doesNotContainIgnoringCase("DATE_TRUNC");
        assertThat(hiveHourSql)
            .contains("date_format(event_time, 'yyyy-MM-dd HH:00:00')")
            .contains("event_date = '2026-01-01'")
            .doesNotContainIgnoringCase("DATE_TRUNC");
        assertThat(sparkSql).contains("DATE_TRUNC('day', event_time)");
        assertThat(starRocksSql).contains("DATE_TRUNC('day', event_time)");
    }

    @Test
    void hiveAdjacentWindowAddsPartitionPredicateToBothSubqueries() {
        String sql = SqlRenderer.render("adjacent_window_kpi_spike", "hive_hdfs_parquet");

        assertThat(sql.split("event_date = '2026-01-01'", -1)).hasSize(3);
    }

    @Test
    void unknownEngineOrQueryThrowsIllegalArgumentException() {
        assertThatThrownBy(() -> SqlRenderer.render("single_cell_day_trend", "missing_engine"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown engine key: missing_engine");
        assertThatThrownBy(() -> SqlRenderer.render("missing_query", "spark_iceberg"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown query: missing_query");
    }

    private void assertRenderedTable(String engineKey, String tableName) {
        String sql = SqlRenderer.render("single_cell_day_trend", engineKey);

        assertThat(sql).contains(tableName);
        assertThat(sql).doesNotContain("{table}");
    }
}

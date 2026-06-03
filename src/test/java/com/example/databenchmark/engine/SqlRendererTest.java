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

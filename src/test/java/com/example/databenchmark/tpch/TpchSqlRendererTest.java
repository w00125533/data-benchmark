package com.example.databenchmark.tpch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TpchSqlRendererTest {
    @Test
    void rendersSparkTablePlaceholders() {
        String sql = TpchSqlRenderer.render("q03_shipping_priority", "spark_iceberg");

        assertThat(sql).contains("iceberg_catalog.tpch.customer");
        assertThat(sql).contains("iceberg_catalog.tpch.orders");
        assertThat(sql).contains("iceberg_catalog.tpch.lineitem");
        assertThat(sql).doesNotContain("{customer}");
    }

    @Test
    void rendersStarRocksDateLiterals() {
        String sql = TpchSqlRenderer.render("q03_shipping_priority", "starrocks_internal");

        assertThat(sql).contains("sr_internal_tpch.customer");
        assertThat(sql).contains("CAST('1995-03-15' AS DATE)");
        assertThat(sql).doesNotContain("DATE '1995-03-15'");
    }

    @Test
    void unknownEngineThrowsClearError() {
        assertThatThrownBy(() -> TpchSqlRenderer.render("q03_shipping_priority", "bad_engine"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown engine key");
    }

    @Test
    void unresolvedTableTokenThrowsClearError() {
        assertThatThrownBy(() -> TpchSqlRenderer.renderTemplate("SELECT * FROM {cust}", "spark_iceberg"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown TPC-H table placeholder")
            .hasMessageContaining("{cust}");
    }
}

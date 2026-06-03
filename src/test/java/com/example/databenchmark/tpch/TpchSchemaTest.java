package com.example.databenchmark.tpch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TpchSchemaTest {
    @Test
    void containsAllTpchTables() {
        assertThat(TpchSchema.tables())
            .extracting(TpchTable::name)
            .containsExactly("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem");
    }

    @Test
    void lineitemContainsJoinAndMeasureColumns() {
        TpchTable lineitem = TpchSchema.table("lineitem");

        assertThat(lineitem.columns())
            .extracting(TpchColumn::name)
            .contains("l_orderkey", "l_partkey", "l_suppkey", "l_quantity", "l_extendedprice", "l_shipdate");
    }

    @Test
    void tableNamesAreSeparatedByEngineShape() {
        assertThat(TpchSchema.tableName("lineitem", "spark_iceberg")).isEqualTo("iceberg_catalog.tpch.lineitem");
        assertThat(TpchSchema.tableName("lineitem", "starrocks_internal")).isEqualTo("sr_internal_tpch.lineitem");
        assertThat(TpchSchema.tableName("lineitem", "starrocks_external_iceberg")).isEqualTo("sr_external_iceberg.tpch.lineitem");
    }
}

package com.example.databenchmark.tpch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void unknownTableThrowsClearError() {
        assertThatThrownBy(() -> TpchSchema.table("missing"))
            .hasMessageContaining("Unknown TPC-H table");
    }

    @Test
    void unknownTableNameMappingThrowsClearError() {
        assertThatThrownBy(() -> TpchSchema.tableName("missing", "spark_iceberg"))
            .hasMessageContaining("Unknown TPC-H table");
    }

    @Test
    void unknownEngineThrowsClearError() {
        assertThatThrownBy(() -> TpchSchema.tableName("lineitem", "bad_engine"))
            .hasMessageContaining("Unknown engine key");
    }

    @Test
    void tablesAreImmutable() {
        assertThatThrownBy(() -> TpchSchema.tables().add(TpchSchema.table("region")))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void columnsAreImmutable() {
        assertThatThrownBy(() -> TpchSchema.table("lineitem").columns().add(new TpchColumn("x", "string")))
            .isInstanceOf(UnsupportedOperationException.class);
    }
}

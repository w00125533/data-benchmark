package com.example.databenchmark.tpch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TpchSqlTemplatesTest {
    @Test
    void rendersSparkIcebergTableDdl() {
        String ddl = TpchSqlTemplates.sparkCreateTable(TpchSchema.table("orders"));

        assertThat(ddl).contains("CREATE TABLE IF NOT EXISTS iceberg_catalog.tpch.orders");
        assertThat(ddl).contains("o_orderkey BIGINT");
        assertThat(ddl).contains("USING iceberg");
    }

    @Test
    void rendersStarRocksInternalTableDdl() {
        String ddl = TpchSqlTemplates.starRocksCreateInternalTable(TpchSchema.table("lineitem"));

        assertThat(ddl).contains("CREATE TABLE IF NOT EXISTS sr_internal_tpch.lineitem");
        assertThat(ddl).contains("l_orderkey BIGINT");
        assertThat(ddl).contains("DISTRIBUTED BY HASH");
    }

    @Test
    void starRocksStringColumnsUseSafeWidth() {
        String ddl = TpchSqlTemplates.starRocksCreateInternalTable(TpchSchema.table("part"));

        assertThat(ddl).contains("p_comment VARCHAR(512)");
    }

    @Test
    void rendersCompositeDuplicateKeysForPartsuppAndLineitem() {
        assertThat(TpchSqlTemplates.starRocksCreateInternalTable(TpchSchema.table("partsupp")))
            .contains("DUPLICATE KEY(ps_partkey, ps_suppkey)")
            .contains("DISTRIBUTED BY HASH(ps_partkey, ps_suppkey)");

        assertThat(TpchSqlTemplates.starRocksCreateInternalTable(TpchSchema.table("lineitem")))
            .contains("DUPLICATE KEY(l_orderkey, l_linenumber)")
            .contains("DISTRIBUTED BY HASH(l_orderkey, l_linenumber)");
    }

    @Test
    void sparkInsertEscapesSingleQuotesInParquetPath() {
        String sql = TpchSqlTemplates.sparkInsertFromParquet(TpchSchema.table("orders"), "/workspace/data/o'rders.parquet");

        assertThat(sql).contains("OPTIONS (path '/workspace/data/o''rders.parquet')");
    }
}

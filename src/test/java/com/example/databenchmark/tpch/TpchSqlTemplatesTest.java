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
}

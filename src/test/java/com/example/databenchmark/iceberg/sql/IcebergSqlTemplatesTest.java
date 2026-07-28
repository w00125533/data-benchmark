package com.example.databenchmark.iceberg.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class IcebergSqlTemplatesTest {
    @Test
    void createsValidationTableWithFormatVersionTwoAndExplicitLocation() {
        String sql = IcebergSqlTemplates.createBaseTable(
            "iceberg_catalog.iceberg_validation.schema_case_run",
            "hdfs://hdfs-namenode:8020/warehouse/iceberg/iceberg_validation/schema/case/run"
        );

        assertThat(sql).contains("CREATE TABLE iceberg_catalog.iceberg_validation.schema_case_run");
        assertThat(sql).contains("USING iceberg");
        assertThat(sql).contains("'format-version'='2'");
        assertThat(sql).contains("LOCATION 'hdfs://hdfs-namenode:8020/warehouse/iceberg/iceberg_validation/schema/case/run'");
        assertThat(sql).contains("id BIGINT");
        assertThat(sql).contains("payload STRUCT");
    }

    @Test
    void rendersMetadataTableQueries() {
        String table = "iceberg_catalog.iceberg_validation.t";

        assertThat(MetadataTableQueries.snapshotCount(table)).contains(table + ".snapshots");
        assertThat(MetadataTableQueries.dataFileStats(table)).contains(table + ".files");
        assertThat(MetadataTableQueries.manifestCount(table)).contains(table + ".manifests");
        assertThat(MetadataTableQueries.currentSnapshot(table)).contains("ORDER BY committed_at DESC LIMIT 1");
    }
}

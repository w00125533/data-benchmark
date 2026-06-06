package com.example.databenchmark.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.schema.KpiSchema;
import org.junit.jupiter.api.Test;

class SqlTemplatesTest {
    @Test
    void sparkIcebergDdlUsesHiveCatalogAndHdfsWarehouse() {
        String sql = SqlTemplates.sparkCreateIcebergTable();

        assertThat(sql).contains("CREATE TABLE IF NOT EXISTS iceberg_catalog.iceberg_db.cell_kpi_1min");
        assertThat(sql).contains("USING iceberg");
        assertThat(sql).contains("PARTITIONED BY (days(event_time))");
        assertThat(sql).contains("event_time TIMESTAMP");
        assertThat(sql).contains("energy_kwh DOUBLE");
        assertThat(sql).doesNotContain("s3").doesNotContain("minio");
    }

    @Test
    void sparkInsertFromParquetEscapesSingleQuotes() {
        String sql = SqlTemplates.sparkInsertFromParquet("hdfs://host/a'b/part.parquet");

        assertThat(sql).contains("path 'hdfs://host/a''b/part.parquet'");
        assertThat(sql).contains("INSERT INTO iceberg_catalog.iceberg_db.cell_kpi_1min");
        assertThat(sql).contains("SELECT * FROM generated_kpi");
    }

    @Test
    void starrocksInternalDdlContainsAllColumnsInSchemaOrder() {
        String sql = SqlTemplates.starRocksCreateInternalTable();

        assertThat(sql).contains("CREATE DATABASE IF NOT EXISTS sr_internal");
        assertThat(sql).contains("CREATE TABLE IF NOT EXISTS sr_internal.cell_kpi_1min");
        int previousIndex = -1;
        for (String column : KpiSchema.columnNames()) {
            int index = sql.indexOf(column);
            assertThat(index).as(column).isGreaterThan(previousIndex);
            previousIndex = index;
        }
        assertThat(sql).contains("DUPLICATE KEY(event_time, cell_id)");
        assertThat(sql).contains("event_time DATETIME");
        assertThat(sql).contains("DISTRIBUTED BY HASH(cell_id)");
    }

    @Test
    void starRocksBrokerLoadEscapesDoubleQuotedDataInfilePath() {
        String sql = SqlTemplates.starRocksBrokerLoadFromParquet(
            "run-1",
            "hdfs://hdfs-namenode:8020/benchmark/kpi\"smoke\\generated/*/*.parquet"
        );

        assertThat(sql)
            .contains("DATA INFILE(\"hdfs://hdfs-namenode:8020/benchmark/kpi\\\"smoke\\\\generated/*/*.parquet\")");
    }

    @Test
    void externalCatalogUsesHiveMetastoreOnly() {
        String sql = SqlTemplates.starRocksCreateExternalCatalog();

        assertThat(sql).contains("CREATE EXTERNAL CATALOG IF NOT EXISTS sr_external_iceberg");
        assertThat(sql).contains("\"type\" = \"iceberg\"");
        assertThat(sql).contains("\"iceberg.catalog.type\" = \"hive\"");
        assertThat(sql).contains("\"iceberg.catalog.hive.metastore.uris\" = \"thrift://hive-metastore:9083\"");
        assertThat(sql).doesNotContain("aws").doesNotContain("s3").doesNotContain("minio");
    }

    @Test
    void starRocksRefreshExternalCatalogStatement() {
        assertThat(SqlTemplates.starRocksRefreshExternalCatalog())
            .isEqualTo("REFRESH EXTERNAL TABLE sr_external_iceberg.iceberg_db.cell_kpi_1min;");
    }

    @Test
    void hiveExternalParquetDdlDiscoversEventDatePartitions() {
        String sql = SqlTemplates.hiveCreateExternalParquetTable("hdfs://hdfs-namenode:8020/data/generated");

        assertThat(sql).contains("CREATE EXTERNAL TABLE IF NOT EXISTS hive_hdfs_parquet.cell_kpi_1min");
        assertThat(sql).contains("PARTITIONED BY (event_date STRING)");
        assertThat(sql).contains("LOCATION 'hdfs://hdfs-namenode:8020/data/generated'");
        assertThat(sql).contains("MSCK REPAIR TABLE hive_hdfs_parquet.cell_kpi_1min");
    }
}

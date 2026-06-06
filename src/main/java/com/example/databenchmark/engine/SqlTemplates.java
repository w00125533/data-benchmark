package com.example.databenchmark.engine;

import com.example.databenchmark.schema.KpiColumn;
import com.example.databenchmark.schema.KpiSchema;
import java.util.stream.Collectors;

public final class SqlTemplates {
    private static final String ICEBERG_TABLE = "iceberg_catalog.iceberg_db.cell_kpi_1min";
    private static final String HDFS_WAREHOUSE = "hdfs://hdfs-namenode:8020/warehouse/iceberg";

    private SqlTemplates() {}

    public static String sparkCreateIcebergTable() {
        return """
            CREATE DATABASE IF NOT EXISTS iceberg_catalog.iceberg_db
            LOCATION '%s/iceberg_db';

            CREATE TABLE IF NOT EXISTS %s (
            %s
            )
            USING iceberg
            PARTITIONED BY (days(event_time))
            LOCATION '%s/iceberg_db/cell_kpi_1min';
            """.formatted(HDFS_WAREHOUSE, ICEBERG_TABLE, sparkColumns(), HDFS_WAREHOUSE);
    }

    public static String sparkInsertFromParquet(String parquetPath) {
        return """
            CREATE OR REPLACE TEMPORARY VIEW generated_kpi
            USING parquet
            OPTIONS (path '%s');

            INSERT INTO %s
            SELECT * FROM generated_kpi;
            """.formatted(escapeSqlLiteral(parquetPath), ICEBERG_TABLE);
    }

    public static String starRocksCreateInternalTable() {
        return """
            CREATE DATABASE IF NOT EXISTS sr_internal;

            CREATE TABLE IF NOT EXISTS sr_internal.cell_kpi_1min (
            %s
            )
            DUPLICATE KEY(event_time, cell_id)
            DISTRIBUTED BY HASH(cell_id)
            PROPERTIES (
                "replication_num" = "1"
            );
            """.formatted(starRocksColumns());
    }

    public static String starRocksTruncateInternalTable() {
        return "TRUNCATE TABLE sr_internal.cell_kpi_1min;";
    }

    public static String starRocksBrokerLoadFromParquet(String label, String parquetGlob) {
        return """
            LOAD LABEL sr_internal.%s
            (
                DATA INFILE("%s")
                INTO TABLE cell_kpi_1min
                FORMAT AS "parquet"
                (%s)
            )
            WITH BROKER
            (
                "hadoop.security.authentication" = "simple",
                "username" = "root",
                "password" = ""
            )
            PROPERTIES
            (
                "timeout" = "%d",
                "max_filter_ratio" = "0"
            );
            """.formatted(
                escapeSqlIdentifierPart(label),
                escapeSqlLiteral(parquetGlob),
                kpiColumnNames(),
                StarRocksBrokerLoad.DEFAULT_TIMEOUT.toSeconds()
            );
    }

    public static String starRocksCreateExternalCatalog() {
        return """
            CREATE EXTERNAL CATALOG IF NOT EXISTS sr_external_iceberg
            PROPERTIES (
                "type" = "iceberg",
                "iceberg.catalog.type" = "hive",
                "iceberg.catalog.hive.metastore.uris" = "thrift://hive-metastore:9083"
            );
            """;
    }

    public static String starRocksRefreshExternalCatalog() {
        return "REFRESH EXTERNAL TABLE sr_external_iceberg.iceberg_db.cell_kpi_1min;";
    }

    public static String hiveCreateExternalParquetTable(String parquetRoot) {
        return """
            CREATE DATABASE IF NOT EXISTS hive_hdfs_parquet;

            CREATE EXTERNAL TABLE IF NOT EXISTS hive_hdfs_parquet.cell_kpi_1min (
            %s
            )
            PARTITIONED BY (event_date STRING)
            STORED AS PARQUET
            LOCATION '%s';

            MSCK REPAIR TABLE hive_hdfs_parquet.cell_kpi_1min;
            """.formatted(hiveColumns(), escapeSqlLiteral(parquetRoot));
    }

    private static String sparkColumns() {
        return KpiSchema.columns().stream()
            .map(column -> "    " + column.name() + " " + sparkType(column))
            .collect(Collectors.joining(",\n"));
    }

    private static String kpiColumnNames() {
        return KpiSchema.columns().stream()
            .map(KpiColumn::name)
            .collect(Collectors.joining(", "));
    }

    private static String starRocksColumns() {
        return KpiSchema.columns().stream()
            .map(column -> "    " + column.name() + " " + starRocksType(column))
            .collect(Collectors.joining(",\n"));
    }

    private static String hiveColumns() {
        return KpiSchema.columns().stream()
            .map(column -> "    " + column.name() + " " + hiveType(column))
            .collect(Collectors.joining(",\n"));
    }

    private static String sparkType(KpiColumn column) {
        return switch (column.logicalType()) {
            case "timestamp_ms" -> "TIMESTAMP";
            case "string" -> "STRING";
            case "int" -> "INT";
            case "double" -> "DOUBLE";
            default -> throw new IllegalArgumentException("Unknown KPI type: " + column.logicalType());
        };
    }

    private static String starRocksType(KpiColumn column) {
        return switch (column.logicalType()) {
            case "timestamp_ms" -> "DATETIME";
            case "string" -> "VARCHAR(64)";
            case "int" -> "INT";
            case "double" -> "DOUBLE";
            default -> throw new IllegalArgumentException("Unknown KPI type: " + column.logicalType());
        };
    }

    private static String hiveType(KpiColumn column) {
        return switch (column.logicalType()) {
            case "timestamp_ms" -> "TIMESTAMP";
            case "string" -> "STRING";
            case "int" -> "INT";
            case "double" -> "DOUBLE";
            default -> throw new IllegalArgumentException("Unknown KPI type: " + column.logicalType());
        };
    }

    private static String escapeSqlLiteral(String value) {
        return value.replace("'", "''");
    }

    private static String escapeSqlIdentifierPart(String value) {
        return value.replaceAll("[^A-Za-z0-9_]", "_");
    }
}

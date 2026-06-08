package com.example.databenchmark.engine;

import com.example.databenchmark.schema.KpiColumn;
import com.example.databenchmark.schema.KpiSchema;
import java.util.stream.Collectors;

public final class SqlTemplates {
    private static final String ICEBERG_TABLE = "iceberg_catalog.iceberg_db.cell_kpi_1min";
    private static final String HDFS_WAREHOUSE = "hdfs://hdfs-namenode:8020/warehouse/iceberg";

    private SqlTemplates() {}

    public static String sparkCreateIcebergTable() {
        return sparkCreateIcebergTable("default");
    }

    public static String sparkCreateIcebergTable(String runId) {
        return """
            CREATE DATABASE IF NOT EXISTS iceberg_catalog.iceberg_db
            LOCATION '%s/iceberg_db';

            DROP TABLE IF EXISTS %s;

            CREATE TABLE IF NOT EXISTS %s (
            %s
            )
            USING iceberg
            PARTITIONED BY (days(event_time))
            LOCATION '%s/iceberg_db/cell_kpi_1min';
            """.formatted(
                HDFS_WAREHOUSE,
                ICEBERG_TABLE,
                ICEBERG_TABLE,
                sparkColumns(),
                HDFS_WAREHOUSE
            );
    }

    public static String hiveDropIcebergTableRegistration() {
        return """
            CREATE DATABASE IF NOT EXISTS iceberg_db
            LOCATION '%s/iceberg_db';

            DROP TABLE IF EXISTS iceberg_db.cell_kpi_1min;
            """.formatted(HDFS_WAREHOUSE);
    }

    public static String sparkInsertFromParquet(String parquetPath) {
        return """
            CREATE OR REPLACE TEMPORARY VIEW generated_kpi
            USING parquet
            OPTIONS (path '%s');

            INSERT INTO %s
            SELECT %s FROM generated_kpi;
            """.formatted(escapeSqlLiteral(parquetPath), ICEBERG_TABLE, kpiColumnNames());
    }

    public static String sparkCreateNativeParquetTable(String parquetRoot) {
        return """
            CREATE DATABASE IF NOT EXISTS spark_catalog.benchmark_native;

            DROP TABLE IF EXISTS spark_catalog.benchmark_native.cell_kpi_1min;

            CREATE TABLE spark_catalog.benchmark_native.cell_kpi_1min (
            %s
            )
            USING parquet
            LOCATION '%s';
            """.formatted(sparkColumns(), escapeSqlLiteral(parquetRoot));
    }

    public static String starRocksCreateInternalTable() {
        return starRocksCreateInternalTable("cell_kpi_1min");
    }

    public static String starRocksCreateInternalTable(String tableName) {
        String safeTableName = requireSqlIdentifier(tableName);
        return """
            CREATE DATABASE IF NOT EXISTS sr_internal;

            CREATE TABLE IF NOT EXISTS sr_internal.%s (
            %s
            )
            DUPLICATE KEY(event_time, cell_id)
            DISTRIBUTED BY HASH(cell_id)
            PROPERTIES (
                "replication_num" = "1"
            );
            """.formatted(safeTableName, starRocksColumns());
    }

    public static String starRocksTruncateInternalTable() {
        return starRocksTruncateInternalTable("cell_kpi_1min");
    }

    public static String starRocksTruncateInternalTable(String tableName) {
        return "TRUNCATE TABLE sr_internal.%s;".formatted(requireSqlIdentifier(tableName));
    }

    public static String starRocksBrokerLoadFromParquet(String label, String parquetGlob) {
        return starRocksBrokerLoadFromParquet(label, parquetGlob, "cell_kpi_1min");
    }

    public static String starRocksBrokerLoadFromParquet(String label, String parquetGlob, String tableName) {
        return """
            LOAD LABEL sr_internal.%s
            (
                DATA INFILE("%s")
                INTO TABLE %s
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
                escapeDoubleQuotedSqlString(parquetGlob),
                requireSqlIdentifier(tableName),
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

            DROP TABLE IF EXISTS hive_hdfs_parquet.cell_kpi_1min;

            CREATE EXTERNAL TABLE hive_hdfs_parquet.cell_kpi_1min (
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

    private static String escapeDoubleQuotedSqlString(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String escapeSqlIdentifierPart(String value) {
        return value.replaceAll("[^A-Za-z0-9_]", "_");
    }

    private static String requireSqlIdentifier(String value) {
        if (value == null || !value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("Unsafe SQL identifier: " + value);
        }
        return value;
    }

}

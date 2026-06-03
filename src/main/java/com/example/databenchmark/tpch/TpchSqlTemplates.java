package com.example.databenchmark.tpch;

import java.util.stream.Collectors;

public final class TpchSqlTemplates {
    private static final String HDFS_WAREHOUSE = "hdfs://hdfs-namenode:8020/warehouse/iceberg";

    private TpchSqlTemplates() {}

    public static String sparkCreateDatabase() {
        return "CREATE DATABASE IF NOT EXISTS iceberg_catalog.tpch LOCATION '" + HDFS_WAREHOUSE + "/tpch';";
    }

    public static String sparkCreateTable(TpchTable table) {
        return """
            CREATE TABLE IF NOT EXISTS %s (
            %s
            )
            USING iceberg
            LOCATION '%s/tpch/%s';
            """.formatted(
            TpchSchema.tableName(table.name(), "spark_iceberg"),
            columns(table, true),
            HDFS_WAREHOUSE,
            table.name()
        );
    }

    public static String sparkInsertFromParquet(TpchTable table, String parquetPath) {
        String view = "generated_tpch_" + table.name();
        return """
            CREATE OR REPLACE TEMPORARY VIEW %s
            USING parquet
            OPTIONS (path '%s');

            INSERT INTO %s
            SELECT * FROM %s;
            """.formatted(view, parquetPath.replace("'", "''"), TpchSchema.tableName(table.name(), "spark_iceberg"), view);
    }

    public static String starRocksCreateDatabase() {
        return "CREATE DATABASE IF NOT EXISTS sr_internal_tpch;";
    }

    public static String starRocksCreateInternalTable(TpchTable table) {
        String key = duplicateKeyColumns(table);
        return """
            CREATE TABLE IF NOT EXISTS %s (
            %s
            )
            DUPLICATE KEY(%s)
            DISTRIBUTED BY HASH(%s)
            PROPERTIES ("replication_num" = "1");
            """.formatted(TpchSchema.tableName(table.name(), "starrocks_internal"), columns(table, false), key, key);
    }

    public static String starRocksRefreshExternalTable(TpchTable table) {
        return "REFRESH EXTERNAL TABLE " + TpchSchema.tableName(table.name(), "starrocks_external_iceberg") + ";";
    }

    private static String columns(TpchTable table, boolean spark) {
        return table.columns().stream()
            .map(column -> "    " + column.name() + " " + type(column.logicalType(), spark))
            .collect(Collectors.joining(",\n"));
    }

    private static String duplicateKeyColumns(TpchTable table) {
        return switch (table.name()) {
            case "region" -> "r_regionkey";
            case "nation" -> "n_nationkey";
            case "supplier" -> "s_suppkey";
            case "customer" -> "c_custkey";
            case "part" -> "p_partkey";
            case "partsupp" -> "ps_partkey, ps_suppkey";
            case "orders" -> "o_orderkey";
            case "lineitem" -> "l_orderkey, l_linenumber";
            default -> throw new IllegalArgumentException("Unknown TPC-H table: " + table.name());
        };
    }

    private static String type(String logicalType, boolean spark) {
        return switch (logicalType) {
            case "long" -> "BIGINT";
            case "int" -> "INT";
            case "double" -> "DOUBLE";
            case "date" -> spark ? "DATE" : "DATE";
            case "string" -> spark ? "STRING" : "VARCHAR(512)";
            default -> throw new IllegalArgumentException("Unknown TPC-H type: " + logicalType);
        };
    }
}

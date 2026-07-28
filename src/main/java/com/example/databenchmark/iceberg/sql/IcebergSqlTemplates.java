package com.example.databenchmark.iceberg.sql;

public final class IcebergSqlTemplates {
    private IcebergSqlTemplates() {}

    public static String createNamespace(String catalog, String namespace) {
        return "CREATE NAMESPACE IF NOT EXISTS " + catalog + "." + namespace;
    }

    public static String dropTable(String table) {
        return "DROP TABLE IF EXISTS " + table + " PURGE";
    }

    public static String createBaseTable(String table, String location) {
        return """
            CREATE TABLE %s (
              id BIGINT,
              event_day DATE,
              region STRING,
              metric_int INT,
              metric_float FLOAT,
              amount DECIMAL(12, 2),
              payload STRUCT<vendor: STRING, score: INT>,
              tags ARRAY<STRING>,
              attrs MAP<STRING, STRING>
            )
            USING iceberg
            PARTITIONED BY (event_day)
            LOCATION '%s'
            TBLPROPERTIES ('format-version'='2')
            """.formatted(table, location);
    }

    public static String insertRange(String table, long startInclusive, long endExclusive, String region) {
        return """
            INSERT INTO %s
            SELECT id,
                   DATE_ADD(DATE '2026-01-01', CAST(id %% 7 AS INT)),
                   '%s',
                   CAST(id AS INT),
                   CAST(id * 1.0 AS FLOAT),
                   CAST(id * 1.25 AS DECIMAL(12, 2)),
                   named_struct('vendor', CONCAT('vendor-', CAST(id %% 3 AS STRING)), 'score', CAST(id %% 100 AS INT)),
                   array('kpi', 'iceberg'),
                   map('source', 'validation')
            FROM range(%d, %d)
            """.formatted(table, region, startInclusive, endExclusive);
    }

    public static String updateRange(String table, long minIdInclusive, long maxIdExclusive) {
        return "UPDATE " + table + " SET region = 'updated' WHERE id >= "
            + minIdInclusive + " AND id < " + maxIdExclusive;
    }

    public static String deleteRange(String table, long minIdInclusive, long maxIdExclusive) {
        return "DELETE FROM " + table + " WHERE id >= "
            + minIdInclusive + " AND id < " + maxIdExclusive;
    }

    public static String mergeUpsertDelete(String table, String sourceView) {
        return """
            MERGE INTO %s t
            USING %s s
            ON t.id = s.id
            WHEN MATCHED AND s.op = 'delete' THEN DELETE
            WHEN MATCHED AND s.op = 'update' THEN UPDATE SET region = s.region
            WHEN NOT MATCHED THEN INSERT *
            """.formatted(table, sourceView);
    }

    public static String rewriteDataFiles(String catalog, String table) {
        return "CALL " + catalog + ".system.rewrite_data_files(table => '" + table + "')";
    }

    public static String rewriteManifests(String catalog, String table) {
        return "CALL " + catalog + ".system.rewrite_manifests('" + table + "')";
    }

    public static String expireSnapshots(String catalog, String table, String olderThanTimestamp) {
        return "CALL " + catalog + ".system.expire_snapshots(table => '" + table
            + "', older_than => TIMESTAMP '" + olderThanTimestamp + "')";
    }
}

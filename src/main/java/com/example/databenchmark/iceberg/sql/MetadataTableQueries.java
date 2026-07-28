package com.example.databenchmark.iceberg.sql;

public final class MetadataTableQueries {
    private MetadataTableQueries() {}

    public static String snapshotCount(String table) {
        return "SELECT COUNT(*) AS snapshot_count FROM " + table + ".snapshots";
    }

    public static String dataFileStats(String table) {
        return "SELECT COUNT(*) AS file_count, COALESCE(SUM(file_size_in_bytes), 0) AS logical_bytes FROM "
            + table + ".files";
    }

    public static String manifestCount(String table) {
        return "SELECT COUNT(*) AS manifest_count FROM " + table + ".manifests";
    }

    public static String currentSnapshot(String table) {
        return "SELECT snapshot_id FROM " + table + ".snapshots ORDER BY committed_at DESC LIMIT 1";
    }
}

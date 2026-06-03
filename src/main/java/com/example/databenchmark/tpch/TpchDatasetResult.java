package com.example.databenchmark.tpch;

import java.nio.file.Path;
import java.util.Map;

public record TpchDatasetResult(Path outputPath, Map<String, TableResult> tables, long rows, long bytesWritten) {
    public TableResult table(String name) {
        TableResult result = tables.get(name);
        if (result == null) {
            throw new IllegalArgumentException("Unknown generated TPC-H table: " + name);
        }
        return result;
    }

    public record TableResult(String table, Path parquetPath, Path csvPath, long rows, long bytesWritten) {}
}

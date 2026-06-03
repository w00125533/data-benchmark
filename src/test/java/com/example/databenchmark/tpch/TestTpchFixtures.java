package com.example.databenchmark.tpch;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public final class TestTpchFixtures {
    private TestTpchFixtures() {}

    public static TpchDatasetResult dataset(Path root) {
        Map<String, TpchDatasetResult.TableResult> tables = new LinkedHashMap<>();
        long rows = 0;
        long bytes = 0;
        for (TpchTable table : TpchSchema.tables()) {
            Path tableRoot = root.resolve(table.name());
            TpchDatasetResult.TableResult result = new TpchDatasetResult.TableResult(
                table.name(),
                tableRoot.resolve("part-00000.parquet"),
                tableRoot.resolve(table.name() + ".csv"),
                1,
                1
            );
            tables.put(table.name(), result);
            rows += result.rows();
            bytes += result.bytesWritten();
        }
        return new TpchDatasetResult(root, tables, rows, bytes);
    }

    public static Map<String, Path> csvFiles(TpchDatasetResult dataset) {
        Map<String, Path> csvFiles = new LinkedHashMap<>();
        for (TpchTable table : TpchSchema.tables()) {
            csvFiles.put(table.name(), dataset.table(table.name()).csvPath());
        }
        return csvFiles;
    }
}

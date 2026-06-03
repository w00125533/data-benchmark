package com.example.databenchmark.tpch;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;

public final class TpchCsvExporter {
    public Map<String, Path> export(TpchDatasetResult dataset) throws IOException {
        Path csvDir = dataset.outputPath().resolve("csv");
        Files.createDirectories(csvDir);

        Map<String, Long> rowCounts = rowCounts(dataset);
        TpchDataGenerator generator = new TpchDataGenerator();
        LinkedHashMap<String, Path> files = new LinkedHashMap<>();

        for (TpchTable table : TpchSchema.tables()) {
            Path csvPath = csvDir.resolve(table.name() + ".csv");
            writeTable(dataset, table, csvPath, rowCounts, generator);
            files.put(table.name(), csvPath);
        }

        return files;
    }

    private void writeTable(
        TpchDatasetResult dataset,
        TpchTable table,
        Path csvPath,
        Map<String, Long> rowCounts,
        TpchDataGenerator generator
    ) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(csvPath, StandardCharsets.UTF_8)) {
            long rows = dataset.table(table.name()).rows();
            for (long row = 0; row < rows; row++) {
                writeRow(writer, table, row, rowCounts, generator);
            }
        }
    }

    private void writeRow(
        BufferedWriter writer,
        TpchTable table,
        long row,
        Map<String, Long> rowCounts,
        TpchDataGenerator generator
    ) throws IOException {
        boolean first = true;
        for (TpchColumn column : table.columns()) {
            if (!first) {
                writer.write(',');
            }
            Object value = generator.valueFor(table, column, row, rowCounts);
            writer.write(formatValue(column, value));
            first = false;
        }
        writer.write('\n');
    }

    private Map<String, Long> rowCounts(TpchDatasetResult dataset) {
        LinkedHashMap<String, Long> counts = new LinkedHashMap<>();
        for (TpchTable table : TpchSchema.tables()) {
            counts.put(table.name(), dataset.table(table.name()).rows());
        }
        return counts;
    }

    static String formatValue(TpchColumn column, Object value) {
        if (value == null) {
            return "";
        }
        if ("date".equals(column.logicalType()) && value instanceof Number number) {
            return LocalDate.ofEpochDay(number.longValue()).toString();
        }
        return escapeCsv(String.valueOf(value));
    }

    static String escapeCsv(String value) {
        if (!requiresEscaping(value)) {
            return value;
        }
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    private static boolean requiresEscaping(String value) {
        return value.indexOf(',') >= 0
            || value.indexOf('"') >= 0
            || value.indexOf('\r') >= 0
            || value.indexOf('\n') >= 0;
    }
}

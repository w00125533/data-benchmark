package com.example.databenchmark.engine;

import com.example.databenchmark.generator.DatasetResult;
import com.example.databenchmark.schema.KpiSchema;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

public final class StarRocksCsvExporter {
    private static final String OUTPUT_FILE = "cell_kpi_1min.csv";
    private static final DateTimeFormatter EVENT_TIME_FORMATTER = DateTimeFormatter
        .ofPattern("yyyy-MM-dd HH:mm:ss")
        .withZone(ZoneOffset.UTC);

    public Path export(DatasetResult dataset, Path outputDir) throws IOException {
        Files.createDirectories(outputDir);
        Path csv = outputDir.resolve(OUTPUT_FILE);

        try (BufferedWriter writer = Files.newBufferedWriter(csv, StandardCharsets.UTF_8)) {
            for (Path file : dataset.files()) {
                writeParquetFile(file, writer);
            }
        }

        return csv;
    }

    private void writeParquetFile(Path file, BufferedWriter writer) throws IOException {
        HadoopInputFile inputFile = HadoopInputFile.fromPath(
            new org.apache.hadoop.fs.Path(file.toUri()),
            new Configuration()
        );

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                writeRecord(record, writer);
            }
        }
    }

    private void writeRecord(GenericRecord record, BufferedWriter writer) throws IOException {
        boolean first = true;
        for (String column : KpiSchema.columnNames()) {
            if (!first) {
                writer.write(',');
            }
            writer.write(escapeCsv(formatValue(column, record.get(column))));
            first = false;
        }
        writer.newLine();
    }

    private String formatValue(String column, Object value) {
        if (value == null) {
            return "";
        }
        if ("event_time".equals(column)) {
            long epochMillis = value instanceof Number number
                ? number.longValue()
                : Long.parseLong(value.toString());
            return EVENT_TIME_FORMATTER.format(Instant.ofEpochMilli(epochMillis));
        }
        return value.toString();
    }

    private String escapeCsv(String value) {
        if (!requiresEscaping(value)) {
            return value;
        }
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    private boolean requiresEscaping(String value) {
        return value.indexOf(',') >= 0
            || value.indexOf('"') >= 0
            || value.indexOf('\r') >= 0
            || value.indexOf('\n') >= 0;
    }
}

package com.example.databenchmark.tpch;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.hadoop.HadoopLocalConfiguration;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

public class TpchDataGenerator {
    private static final LocalDate BASE_DATE = LocalDate.of(1992, 1, 1);

    public TpchDatasetResult generate(BenchmarkConfig config, String runId) throws IOException {
        Path root = Path.of(config.dataset().output(), "tpch", runId);
        Files.createDirectories(root);

        Map<String, Long> rowCounts = rowCounts(config.suite().scaleFactor());
        Map<String, TpchDatasetResult.TableResult> tables = new LinkedHashMap<>();
        long totalRows = 0L;
        long totalBytes = 0L;

        for (TpchTable table : TpchSchema.tables()) {
            long rows = rowCounts.get(table.name());
            Path tableDir = root.resolve(table.name());
            Files.createDirectories(tableDir);

            Path parquetPath = tableDir.resolve("part-00000.parquet");
            Path csvPath = root.resolve("csv").resolve(table.name() + ".csv");
            writeParquet(table, parquetPath, rows, rowCounts);

            long bytesWritten = Files.size(parquetPath);
            tables.put(table.name(), new TpchDatasetResult.TableResult(table.name(), parquetPath, csvPath, rows, bytesWritten));
            totalRows += rows;
            totalBytes += bytesWritten;
        }

        return new TpchDatasetResult(root, Map.copyOf(tables), totalRows, totalBytes);
    }

    static long scaledRows(TpchTable table, BigDecimal scaleFactor) {
        long scaled = scaleFactor.multiply(BigDecimal.valueOf(table.baseRows())).setScale(0, RoundingMode.CEILING).longValue();
        if (table.name().equals("region")) {
            return 5L;
        }
        if (table.name().equals("nation")) {
            return 25L;
        }
        return Math.max(1L, scaled);
    }

    Object valueFor(TpchTable table, TpchColumn column, long row, Map<String, Long> rowCounts) {
        String name = column.name();
        if (isPrimaryKey(name)) {
            return row + 1;
        }

        switch (name) {
            case "n_regionkey":
                return foreignKey(row, rowCounts.get("region"));
            case "s_nationkey":
            case "c_nationkey":
                return foreignKey(row, rowCounts.get("nation"));
            case "o_custkey":
                return foreignKey(row, rowCounts.get("customer"));
            case "ps_partkey":
            case "l_partkey":
                return foreignKey(row, rowCounts.get("part"));
            case "ps_suppkey":
            case "l_suppkey":
                return foreignKey(row, rowCounts.get("supplier"));
            case "l_orderkey":
                return foreignKey(row, rowCounts.get("orders"));
            case "l_linenumber":
                return (int) ((row % 7) + 1);
            default:
                return scalarValue(table, column, row);
        }
    }

    private Map<String, Long> rowCounts(BigDecimal scaleFactor) {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (TpchTable table : TpchSchema.tables()) {
            counts.put(table.name(), scaledRows(table, scaleFactor));
        }
        return counts;
    }

    private void writeParquet(TpchTable table, Path parquetPath, long rows, Map<String, Long> rowCounts) throws IOException {
        Files.deleteIfExists(parquetPath);

        Schema schema = createSchema(table);
        HadoopOutputFile outputFile = HadoopOutputFile.fromPath(
            new org.apache.hadoop.fs.Path(parquetPath.toUri()),
            HadoopLocalConfiguration.create()
        );

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
            .withSchema(schema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()) {
            for (long row = 0; row < rows; row++) {
                writer.write(createRecord(table, schema, row, rowCounts));
            }
        }
    }

    private GenericRecord createRecord(TpchTable table, Schema schema, long row, Map<String, Long> rowCounts) {
        GenericRecord record = new GenericData.Record(schema);
        for (TpchColumn column : table.columns()) {
            record.put(column.name(), valueFor(table, column, row, rowCounts));
        }
        return record;
    }

    private Schema createSchema(TpchTable table) {
        List<Schema.Field> fields = new ArrayList<>();
        for (TpchColumn column : table.columns()) {
            fields.add(new Schema.Field(column.name(), avroType(column), null, (Object) null));
        }

        Schema schema = Schema.createRecord(
            toRecordName(table.name()),
            "Deterministic TPC-H smoke dataset row.",
            "com.example.databenchmark.tpch.avro",
            false
        );
        schema.setFields(fields);
        return schema;
    }

    private Schema avroType(TpchColumn column) {
        return switch (column.logicalType()) {
            case "string" -> Schema.create(Schema.Type.STRING);
            case "int" -> Schema.create(Schema.Type.INT);
            case "long" -> Schema.create(Schema.Type.LONG);
            case "double" -> Schema.create(Schema.Type.DOUBLE);
            case "date" -> LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            default -> throw new IllegalArgumentException(
                "Unsupported TPC-H logical type '" + column.logicalType() + "' for column '" + column.name() + "'"
            );
        };
    }

    private Object scalarValue(TpchTable table, TpchColumn column, long row) {
        return switch (column.logicalType()) {
            case "int" -> (int) ((row % 100) + 1);
            case "long" -> row + 1;
            case "double" -> ((row % 10_000) + 100) / 10.0;
            case "date" -> (int) BASE_DATE.plusDays(row % 2400).toEpochDay();
            case "string" -> table.name() + "-" + column.name() + "-" + row;
            default -> throw new IllegalArgumentException(
                "Unsupported TPC-H logical type '" + column.logicalType() + "' for column '" + column.name() + "'"
            );
        };
    }

    private long foreignKey(long row, long max) {
        return (row % max) + 1;
    }

    private boolean isPrimaryKey(String name) {
        return switch (name) {
            case "r_regionkey", "n_nationkey", "s_suppkey", "c_custkey", "p_partkey", "o_orderkey" -> true;
            default -> false;
        };
    }

    private String toRecordName(String tableName) {
        return Character.toUpperCase(tableName.charAt(0)) + tableName.substring(1) + "Row";
    }
}

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
    private static final List<String> CUSTOMER_SEGMENTS = List.of("BUILDING", "AUTOMOBILE", "MACHINERY", "HOUSEHOLD", "FURNITURE");
    private static final List<String> LINE_RETURN_FLAGS = List.of("R", "A", "N");
    private static final List<String> LINE_STATUSES = List.of("O", "F");
    private static final List<String> ORDER_STATUSES = List.of("F", "O", "P");
    private static final List<String> ORDER_PRIORITIES = List.of("1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW");
    private static final List<String> SHIP_MODES = List.of("MAIL", "SHIP", "RAIL", "TRUCK", "AIR", "FOB");
    private static final List<String> SHIP_INSTRUCTIONS = List.of("DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN");
    private static final List<String> PART_BRANDS = List.of("Brand#12", "Brand#23", "Brand#34", "Brand#45", "Brand#56");
    private static final List<String> PART_TYPES = List.of(
        "PROMO BURNISHED COPPER",
        "STANDARD POLISHED TIN",
        "PROMO ANODIZED STEEL",
        "ECONOMY BRUSHED BRASS"
    );
    private static final List<LocalDate> ORDER_DATES = List.of(
        LocalDate.of(1993, 10, 15),
        LocalDate.of(1994, 6, 15),
        LocalDate.of(1995, 3, 1),
        LocalDate.of(1996, 2, 1)
    );
    private static final List<LocalDate> SHIP_DATES = List.of(
        LocalDate.of(1994, 6, 20),
        LocalDate.of(1995, 3, 20),
        LocalDate.of(1995, 9, 15),
        LocalDate.of(1996, 2, 15),
        LocalDate.of(1998, 8, 1)
    );

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
                return partsuppPartKey(row, rowCounts);
            case "ps_suppkey":
                return partsuppSuppKey(row, rowCounts);
            case "l_partkey":
                return lineitemPartKey(row, rowCounts);
            case "l_suppkey":
                return lineitemSuppKey(row, rowCounts);
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
        ensurePartsuppKeyCapacity(counts);
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
        if ("string".equals(column.logicalType())) {
            String domainValue = domainStringValue(column.name(), row);
            if (domainValue != null) {
                return domainValue;
            }
        }
        if ("date".equals(column.logicalType())) {
            return logicalDate(dateValue(column.name(), row));
        }

        return switch (column.logicalType()) {
            case "int" -> (int) ((row % 100) + 1);
            case "long" -> row + 1;
            case "double" -> ((row % 10_000) + 100) / 10.0;
            case "string" -> table.name() + "-" + column.name() + "-" + row;
            default -> throw new IllegalArgumentException(
                "Unsupported TPC-H logical type '" + column.logicalType() + "' for column '" + column.name() + "'"
            );
        };
    }

    private long foreignKey(long row, long max) {
        return (row % max) + 1;
    }

    private String domainStringValue(String columnName, long row) {
        return switch (columnName) {
            case "c_mktsegment" -> cycle(CUSTOMER_SEGMENTS, row);
            case "l_returnflag" -> cycle(LINE_RETURN_FLAGS, row);
            case "l_linestatus" -> cycle(LINE_STATUSES, row);
            case "o_orderstatus" -> cycle(ORDER_STATUSES, row);
            case "o_orderpriority" -> cycle(ORDER_PRIORITIES, row);
            case "l_shipmode" -> cycle(SHIP_MODES, row);
            case "l_shipinstruct" -> cycle(SHIP_INSTRUCTIONS, row);
            case "p_brand" -> cycle(PART_BRANDS, row);
            case "p_type" -> cycle(PART_TYPES, row);
            default -> null;
        };
    }

    private LocalDate dateValue(String columnName, long row) {
        return switch (columnName) {
            case "o_orderdate" -> cycleWithFallback(ORDER_DATES, row, 37);
            case "l_shipdate" -> cycleWithFallback(SHIP_DATES, row, 53);
            case "l_commitdate" -> dateValue("l_shipdate", row).plusDays(1);
            case "l_receiptdate" -> dateValue("l_shipdate", row).plusDays(2);
            default -> BASE_DATE.plusDays(row % 2400);
        };
    }

    private int logicalDate(LocalDate date) {
        return (int) date.toEpochDay();
    }

    private void ensurePartsuppKeyCapacity(Map<String, Long> counts) {
        long partRows = counts.get("part");
        long supplierRows = counts.get("supplier");
        long partsuppRows = counts.get("partsupp");
        if ((partRows * supplierRows) >= partsuppRows) {
            return;
        }

        long requiredSuppliers = (long) Math.ceil((double) partsuppRows / partRows);
        counts.put("supplier", Math.max(supplierRows, requiredSuppliers));
    }

    private long partsuppPartKey(long row, Map<String, Long> rowCounts) {
        return (row % rowCounts.get("part")) + 1;
    }

    private long partsuppSuppKey(long row, Map<String, Long> rowCounts) {
        long partRows = rowCounts.get("part");
        return ((row / partRows) % rowCounts.get("supplier")) + 1;
    }

    private long lineitemPartKey(long row, Map<String, Long> rowCounts) {
        return (row % rowCounts.get("part")) + 1;
    }

    private long lineitemSuppKey(long row, Map<String, Long> rowCounts) {
        long partRows = rowCounts.get("part");
        return ((row / partRows) % rowCounts.get("supplier")) + 1;
    }

    private <T> T cycle(List<T> values, long row) {
        return values.get((int) (row % values.size()));
    }

    private LocalDate cycleWithFallback(List<LocalDate> values, long row, long spacingDays) {
        if (row < values.size()) {
            return values.get((int) row);
        }
        return BASE_DATE.plusDays((row - values.size()) * spacingDays);
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

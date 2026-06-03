package com.example.databenchmark.tpch;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TpchDataGeneratorTest {
    @TempDir
    Path tempDir;

    @Test
    void generatesDeterministicReadableParquetForEveryTable() throws Exception {
        BenchmarkConfig config = config();
        TpchDataGenerator generator = new TpchDataGenerator();

        TpchDatasetResult firstRun = generator.generate(config, "tpch-unit");
        List<List<String>> firstLineitemRows = recordSnapshots(readAllRecords(firstRun.table("lineitem").parquetPath(), 5));
        List<List<String>> firstOrdersRows = recordSnapshots(readAllRecords(firstRun.table("orders").parquetPath(), 5));

        TpchDatasetResult secondRun = generator.generate(config, "tpch-unit");

        assertThat(secondRun.outputPath()).isEqualTo(tempDir.resolve(Path.of("tpch", "tpch-unit")));
        assertThat(secondRun.tables()).containsKeys("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem");
        assertThat(secondRun.table("region").rows()).isEqualTo(5L);
        assertThat(secondRun.table("nation").rows()).isEqualTo(25L);
        assertThat(secondRun.table("supplier").rows()).isEqualTo(4L);
        assertThat(secondRun.table("customer").rows()).isEqualTo(15L);
        assertThat(secondRun.table("part").rows()).isEqualTo(20L);
        assertThat(secondRun.table("partsupp").rows()).isEqualTo(80L);
        assertThat(secondRun.table("orders").rows()).isEqualTo(150L);
        assertThat(secondRun.table("lineitem").rows()).isEqualTo(600L);
        assertThat(secondRun.rows()).isEqualTo(899L);
        assertThat(secondRun.bytesWritten()).isPositive();
        assertThat(secondRun.table("lineitem").csvPath()).isEqualTo(tempDir.resolve(Path.of("tpch", "tpch-unit", "csv", "lineitem.csv")));

        for (String tableName : secondRun.tables().keySet()) {
            Path parquetPath = secondRun.table(tableName).parquetPath();
            assertThat(Files.exists(parquetPath)).isTrue();
            assertThat(readFirstRecord(parquetPath)).as(tableName).isNotNull();
        }

        assertThat(recordSnapshots(readAllRecords(secondRun.table("lineitem").parquetPath(), 5))).isEqualTo(firstLineitemRows);
        assertThat(recordSnapshots(readAllRecords(secondRun.table("orders").parquetPath(), 5))).isEqualTo(firstOrdersRows);

        GenericRecord firstLineitem = readFirstRecord(secondRun.table("lineitem").parquetPath());
        assertThat(firstLineitem.getSchema().getFields())
            .extracting(Schema.Field::name)
            .containsExactly(
                "l_orderkey",
                "l_partkey",
                "l_suppkey",
                "l_linenumber",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "l_tax",
                "l_returnflag",
                "l_linestatus",
                "l_shipdate",
                "l_commitdate",
                "l_receiptdate",
                "l_shipinstruct",
                "l_shipmode",
                "l_comment"
            );

        Set<String> dateFields = new LinkedHashSet<>();
        for (Schema.Field field : firstLineitem.getSchema().getFields()) {
            if (field.schema().getLogicalType() != null && "date".equals(field.schema().getLogicalType().getName())) {
                dateFields.add(field.name());
            }
        }
        assertThat(dateFields).containsExactly("l_shipdate", "l_commitdate", "l_receiptdate");
    }

    @Test
    void keepsForeignKeysWithinGeneratedTableRanges() throws Exception {
        TpchDatasetResult result = new TpchDataGenerator().generate(config(), "tpch-fks");

        long regionRows = result.table("region").rows();
        long nationRows = result.table("nation").rows();
        long customerRows = result.table("customer").rows();
        long ordersRows = result.table("orders").rows();
        long partRows = result.table("part").rows();
        long supplierRows = result.table("supplier").rows();

        for (GenericRecord nation : readAllRecords(result.table("nation").parquetPath())) {
            assertThat(asLong(nation.get("n_regionkey"))).isBetween(1L, regionRows);
        }
        for (GenericRecord customer : readAllRecords(result.table("customer").parquetPath())) {
            assertThat(asLong(customer.get("c_nationkey"))).isBetween(1L, nationRows);
        }
        for (GenericRecord supplier : readAllRecords(result.table("supplier").parquetPath())) {
            assertThat(asLong(supplier.get("s_nationkey"))).isBetween(1L, nationRows);
        }
        for (GenericRecord order : readAllRecords(result.table("orders").parquetPath())) {
            assertThat(asLong(order.get("o_custkey"))).isBetween(1L, customerRows);
        }
        for (GenericRecord lineitem : readAllRecords(result.table("lineitem").parquetPath())) {
            assertThat(asLong(lineitem.get("l_orderkey"))).isBetween(1L, ordersRows);
            assertThat(asLong(lineitem.get("l_partkey"))).isBetween(1L, partRows);
            assertThat(asLong(lineitem.get("l_suppkey"))).isBetween(1L, supplierRows);
        }
    }

    @Test
    void generatesUniquePartsuppKeysAndSmokeQueryFriendlyValues() throws Exception {
        TpchDatasetResult result = new TpchDataGenerator().generate(config(), "tpch-smoke-values");

        List<GenericRecord> partsuppRows = readAllRecords(result.table("partsupp").parquetPath());
        Set<String> partSuppKeys = new LinkedHashSet<>();
        for (GenericRecord partsupp : partsuppRows) {
            partSuppKeys.add(asLong(partsupp.get("ps_partkey")) + ":" + asLong(partsupp.get("ps_suppkey")));
        }
        assertThat(partSuppKeys).hasSize(partsuppRows.size());

        assertThat(readAllRecords(result.table("customer").parquetPath()))
            .anySatisfy(record -> assertThat(record.get("c_mktsegment").toString()).isEqualTo("BUILDING"));

        assertThat(readAllRecords(result.table("lineitem").parquetPath()))
            .anySatisfy(record -> assertThat(record.get("l_returnflag").toString()).isEqualTo("R"))
            .anySatisfy(record -> assertThat(asDate(record.get("l_shipdate"))).isAfter(LocalDate.of(1995, 3, 15)))
            .anySatisfy(record -> assertThat(asDate(record.get("l_shipdate"))).isBetween(LocalDate.of(1994, 1, 1), LocalDate.of(1995, 12, 31)));

        assertThat(readAllRecords(result.table("orders").parquetPath()))
            .anySatisfy(record -> assertThat(asDate(record.get("o_orderdate")).getYear()).isEqualTo(1994))
            .anySatisfy(record -> assertThat(asDate(record.get("o_orderdate"))).isBetween(LocalDate.of(1993, 10, 1), LocalDate.of(1993, 10, 31)));
    }

    private BenchmarkConfig config() {
        return new BenchmarkConfig(
            "tpch-smoke",
            20260602L,
            new BenchmarkConfig.SuiteConfig("tpch", new BigDecimal("0.01"), "smoke"),
            new BenchmarkConfig.DatasetConfig(10000, 1, 50, "2026-01-01T00:00:00", tempDir.toString(), 10000L),
            new BenchmarkConfig.QueryConfig(1, 1, 1),
            new BenchmarkConfig.ReportConfig("html", tempDir.resolve("reports").toString()),
            new BenchmarkConfig.MonitoringConfig(true, true)
        );
    }

    private GenericRecord readFirstRecord(Path file) throws Exception {
        List<GenericRecord> records = readAllRecords(file, 1);
        return records.isEmpty() ? null : records.get(0);
    }

    private List<GenericRecord> readAllRecords(Path file) throws Exception {
        return readAllRecords(file, Integer.MAX_VALUE);
    }

    private List<GenericRecord> readAllRecords(Path file, int limit) throws Exception {
        HadoopInputFile inputFile = HadoopInputFile.fromPath(
            new org.apache.hadoop.fs.Path(file.toUri()),
            new org.apache.hadoop.conf.Configuration()
        );
        List<GenericRecord> records = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null && records.size() < limit) {
                records.add(record);
            }
        }
        return records;
    }

    private List<List<String>> recordSnapshots(List<GenericRecord> records) {
        List<List<String>> snapshots = new ArrayList<>();
        for (GenericRecord record : records) {
            List<String> values = new ArrayList<>();
            for (Schema.Field field : record.getSchema().getFields()) {
                values.add(field.name() + "=" + record.get(field.name()));
            }
            snapshots.add(values);
        }
        return snapshots;
    }

    private long asLong(Object value) {
        return ((Number) value).longValue();
    }

    private LocalDate asDate(Object value) {
        return LocalDate.ofEpochDay(((Number) value).intValue());
    }
}

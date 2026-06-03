package com.example.databenchmark.tpch;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.LinkedHashSet;
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
    void generatesAllTablesWithDeterministicSmokeRows() throws Exception {
        BenchmarkConfig config = new BenchmarkConfig(
            "tpch-smoke",
            20260602L,
            new BenchmarkConfig.SuiteConfig("tpch", new BigDecimal("0.01"), "smoke"),
            new BenchmarkConfig.DatasetConfig(10000, 1, 50, "2026-01-01T00:00:00", tempDir.toString(), 10000L),
            new BenchmarkConfig.QueryConfig(1, 1, 1),
            new BenchmarkConfig.ReportConfig("html", tempDir.resolve("reports").toString()),
            new BenchmarkConfig.MonitoringConfig(true, true)
        );

        TpchDatasetResult result = new TpchDataGenerator().generate(config, "tpch-unit");

        assertThat(result.outputPath()).isEqualTo(tempDir.resolve(Path.of("tpch", "tpch-unit")));
        assertThat(result.tables()).containsKeys("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem");
        assertThat(result.table("region").rows()).isEqualTo(5);
        assertThat(result.table("nation").rows()).isEqualTo(25);
        assertThat(result.table("lineitem").rows()).isEqualTo(600);
        assertThat(result.rows()).isEqualTo(896L);
        assertThat(result.bytesWritten()).isPositive();
        assertThat(Files.exists(result.table("lineitem").parquetPath())).isTrue();
        assertThat(result.table("lineitem").csvPath()).isEqualTo(tempDir.resolve(Path.of("tpch", "tpch-unit", "csv", "lineitem.csv")));

        GenericRecord firstRecord = readFirstRecord(result.table("lineitem").parquetPath());
        assertThat(firstRecord).isNotNull();
        assertThat(firstRecord.getSchema().getFields())
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
        assertThat(firstRecord.get("l_orderkey")).isEqualTo(1L);
        assertThat(firstRecord.get("l_partkey")).isEqualTo(1L);
        assertThat(firstRecord.get("l_suppkey")).isEqualTo(1L);
        assertThat(firstRecord.get("l_linenumber")).isEqualTo(1);
        assertThat(firstRecord.get("l_quantity")).isEqualTo(10.0d);
        assertThat(firstRecord.get("l_extendedprice")).isEqualTo(10.0d);
        assertThat(firstRecord.get("l_discount")).isEqualTo(10.0d);
        assertThat(firstRecord.get("l_tax")).isEqualTo(10.0d);
        assertThat(firstRecord.get("l_returnflag").toString()).isEqualTo("lineitem-l_returnflag-0");
        assertThat(firstRecord.get("l_linestatus").toString()).isEqualTo("lineitem-l_linestatus-0");
        assertThat(firstRecord.get("l_shipdate")).isEqualTo((int) LocalDate.of(1992, 1, 1).toEpochDay());
        assertThat(firstRecord.get("l_commitdate")).isEqualTo((int) LocalDate.of(1992, 1, 1).toEpochDay());
        assertThat(firstRecord.get("l_receiptdate")).isEqualTo((int) LocalDate.of(1992, 1, 1).toEpochDay());
        assertThat(firstRecord.get("l_shipinstruct").toString()).isEqualTo("lineitem-l_shipinstruct-0");
        assertThat(firstRecord.get("l_shipmode").toString()).isEqualTo("lineitem-l_shipmode-0");
        assertThat(firstRecord.get("l_comment").toString()).isEqualTo("lineitem-l_comment-0");

        Set<String> dateFields = new LinkedHashSet<>();
        for (Schema.Field field : firstRecord.getSchema().getFields()) {
            if (field.schema().getLogicalType() != null && "date".equals(field.schema().getLogicalType().getName())) {
                dateFields.add(field.name());
            }
        }
        assertThat(dateFields).containsExactly("l_shipdate", "l_commitdate", "l_receiptdate");
    }

    private GenericRecord readFirstRecord(Path file) throws Exception {
        HadoopInputFile inputFile = HadoopInputFile.fromPath(
            new org.apache.hadoop.fs.Path(file.toUri()),
            new org.apache.hadoop.conf.Configuration()
        );
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
            return reader.read();
        }
    }
}

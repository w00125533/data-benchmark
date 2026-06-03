package com.example.databenchmark.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.schema.KpiSchema;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KpiDataGeneratorTest {
    @TempDir
    Path tempDir;

    @Test
    void generatorWritesDeterministicPartitionedParquetOutput() throws Exception {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(3, 1, 123L, tempDir.toString(), 12L);
        KpiDataGenerator generator = new KpiDataGenerator();

        DatasetResult first = generator.generate(config);
        List<GenericRecord> firstRows = readRecords(first.files().get(0));
        DatasetResult second = generator.generate(config);
        List<GenericRecord> secondRows = readRecords(second.files().get(0));

        assertThat(first.rows()).isEqualTo(12L);
        assertThat(first.bytesWritten()).isPositive();
        assertThat(first.files()).isEqualTo(second.files());
        assertThat(first.files()).hasSize(1);
        assertThat(first.files().get(0).toString()).contains("event_date=2026-01-01");
        assertThat(firstRows).hasSize(12);
        assertThat(secondRows).hasSize(12);
        assertThat(firstRows.stream().map(GenericRecord::toString).toList())
            .isEqualTo(secondRows.stream().map(GenericRecord::toString).toList());

        GenericRecord firstRecord = firstRows.get(0);
        assertThat(firstRecord.getSchema().getFields())
            .extracting(org.apache.avro.Schema.Field::name)
            .containsExactlyElementsOf(KpiSchema.columnNames());
        assertThat(firstRecord.getSchema().getFields()).hasSize(50);
        assertThat(firstRecord.get("cell_id").toString()).isEqualTo("CELL-000000");
        assertThat(firstRecord.get("province").toString()).isEqualTo("province-00");
        assertThat(firstRecord.get("city").toString()).isEqualTo("city-000");
        assertThat(firstRecord.get("vendor").toString()).isEqualTo("Huawei");
        assertThat(firstRecord.get("rat").toString()).isEqualTo("4G");
        assertThat(firstRecord.get("band").toString()).isEqualTo("B3");
        assertThat(firstRecord.get("event_time")).isEqualTo(1767225600000L);
        assertThat(firstRecord.get("load_score")).isInstanceOf(Double.class);
    }

    private List<GenericRecord> readRecords(Path file) throws Exception {
        List<GenericRecord> records = new ArrayList<>();
        HadoopInputFile inputFile = HadoopInputFile.fromPath(
            new org.apache.hadoop.fs.Path(file.toUri()),
            new Configuration()
        );
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                records.add(record);
            }
        }
        return records;
    }
}

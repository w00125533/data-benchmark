package com.example.databenchmark.generator;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.hadoop.HadoopLocalConfiguration;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

public class KpiDataGenerator {
    public DatasetResult generate(BenchmarkConfig config) throws IOException {
        long rows = targetRows(config);
        Path outputPath = Path.of(config.dataset().output());
        LocalDateTime start = LocalDateTime.parse(config.dataset().startTime());
        Path partitionPath = outputPath.resolve("event_date=" + start.toLocalDate());
        Files.createDirectories(partitionPath);

        Path file = partitionPath.resolve("part-00000.parquet");
        writeRows(config, rows, start, file);

        return new DatasetResult(outputPath, List.of(file), rows, Files.size(file));
    }

    private long targetRows(BenchmarkConfig config) {
        long fullRows = (long) config.dataset().cells() * config.dataset().days() * 24L * 60L;
        Long rowCap = config.dataset().rowCap();
        return rowCap == null ? fullRows : Math.min(fullRows, rowCap);
    }

    private void writeRows(BenchmarkConfig config, long rows, LocalDateTime start, Path file) throws IOException {
        Files.deleteIfExists(file);
        Schema schema = KpiAvroSchemaFactory.createSchema();
        KpiRecordFactory recordFactory = new KpiRecordFactory(config, schema, start);
        HadoopOutputFile outputFile = HadoopOutputFile.fromPath(
            new org.apache.hadoop.fs.Path(file.toUri()),
            HadoopLocalConfiguration.create()
        );

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
            .withSchema(schema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()) {
            for (long row = 0; row < rows; row++) {
                writer.write(recordFactory.create(row));
            }
        }
    }
}

package com.example.databenchmark.generator;

import com.example.databenchmark.config.BenchmarkConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
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
            hadoopConfiguration()
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

    private static Configuration hadoopConfiguration() throws IOException {
        ensureHadoopHomeForWindows();
        Configuration configuration = new Configuration();
        configuration.setClass("fs.file.impl", NoPermissionRawLocalFileSystem.class, FileSystem.class);
        configuration.setBoolean("fs.file.impl.disable.cache", true);
        return configuration;
    }

    private static void ensureHadoopHomeForWindows() throws IOException {
        if (!System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT).contains("win")
            || System.getProperty("hadoop.home.dir") != null) {
            return;
        }

        Path hadoopHome = Path.of(System.getProperty("java.io.tmpdir"), "data-benchmark-hadoop-home");
        Path bin = hadoopHome.resolve("bin");
        Path winutils = bin.resolve("winutils.exe");
        Files.createDirectories(bin);
        if (Files.notExists(winutils)) {
            Files.createFile(winutils);
        }
        System.setProperty("hadoop.home.dir", hadoopHome.toAbsolutePath().toString());
    }

    public static class NoPermissionRawLocalFileSystem extends RawLocalFileSystem {
        @Override
        public void setPermission(org.apache.hadoop.fs.Path path, FsPermission permission) {
            // Hadoop's Windows local FS shells out to winutils for chmod; benchmark output does not need it.
        }
    }
}

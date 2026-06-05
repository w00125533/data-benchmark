package com.example.databenchmark.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.hadoop.HadoopLocalConfiguration;
import java.nio.file.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SparkKpiDataGeneratorTest {
    @TempDir
    Path tempDir;

    @Test
    void writesPartitionedParquetWithExpectedRows() throws Exception {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(10, 1, null, tempDir.resolve("generated").toString(), 100L);

        DatasetResult result = new SparkKpiDataGenerator().generate(config);

        assertThat(result.rows()).isEqualTo(100L);
        assertThat(result.bytesWritten()).isPositive();
        assertThat(result.files()).isNotEmpty();
        assertThat(result.files()).allMatch(path -> path.toString().endsWith(".parquet"));
        assertThat(result.files()).anyMatch(path -> path.toString().contains("event_date=2026-01-01"));

        long parquetRows = 0L;
        for (Path file : result.files()) {
            try (ParquetFileReader reader = ParquetFileReader.open(
                HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(file.toUri()), HadoopLocalConfiguration.create())
            )) {
                parquetRows += reader.getRecordCount();
            }
        }
        assertThat(parquetRows).isEqualTo(100L);
    }
}

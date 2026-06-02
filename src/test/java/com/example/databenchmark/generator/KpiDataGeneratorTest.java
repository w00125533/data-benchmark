package com.example.databenchmark.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KpiDataGeneratorTest {
    @TempDir
    Path tempDir;

    @Test
    void generatorWritesDeterministicPartitionedOutput() throws Exception {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(3, 1, 123L, tempDir.toString(), 12L);
        KpiDataGenerator generator = new KpiDataGenerator();

        DatasetResult first = generator.generate(config);
        String firstContent = Files.readString(first.files().get(0));
        DatasetResult second = generator.generate(config);

        assertThat(first.rows()).isEqualTo(12L);
        assertThat(first.bytesWritten()).isPositive();
        assertThat(first.files()).isEqualTo(second.files());
        assertThat(first.files()).hasSize(1);
        assertThat(firstContent).isEqualTo(Files.readString(second.files().get(0)));
        assertThat(firstContent).contains("CELL-000000");
        assertThat(first.files().get(0).toString()).contains("event_date=2026-01-01");
    }
}

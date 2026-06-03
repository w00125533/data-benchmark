package com.example.databenchmark.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.generator.DatasetResult;
import com.example.databenchmark.generator.KpiDataGenerator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StarRocksCsvExporterTest {
    @TempDir
    Path tempDir;

    @Test
    void exportsHeaderlessCsvForStarRocksStreamLoad() throws Exception {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(3, 1, 123L, tempDir.resolve("parquet").toString(), 12L);
        DatasetResult dataset = new KpiDataGenerator().generate(config);

        Path csv = new StarRocksCsvExporter().export(dataset, tempDir.resolve("csv"));

        List<String> lines = Files.readAllLines(csv);
        assertThat(csv.getFileName()).hasToString("cell_kpi_1min.csv");
        assertThat(lines).hasSize(12);
        assertThat(lines.get(0))
            .startsWith("2026-01-01 00:00:00,CELL-000000,province-00,city-000");
        assertThat(lines.get(0).split(",", -1)).hasSize(50);
        assertThat(lines).allSatisfy(line -> assertThat(line).doesNotContain("null"));
    }
}

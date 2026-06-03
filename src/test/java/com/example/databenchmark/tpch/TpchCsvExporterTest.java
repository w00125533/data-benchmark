package com.example.databenchmark.tpch;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TpchCsvExporterTest {
    @TempDir
    Path tempDir;

    @Test
    void exportsHeaderlessCsvForAllTpchTables() throws Exception {
        TpchDatasetResult dataset = new TpchDataGenerator().generate(config(), "tpch-csv");

        var files = new TpchCsvExporter().export(dataset);

        assertThat(files.keySet()).containsExactly(
            "region",
            "nation",
            "supplier",
            "customer",
            "part",
            "partsupp",
            "orders",
            "lineitem"
        );
        assertThat(files.values()).allSatisfy(path -> assertThat(Files.exists(path)).isTrue());

        List<String> regionLines = Files.readAllLines(files.get("region"));
        List<String> lineitemLines = Files.readAllLines(files.get("lineitem"));

        assertThat(regionLines).hasSize(5);
        assertThat(lineitemLines).hasSize(600);
        assertThat(lineitemLines.get(0))
            .startsWith("1,1,1,1,10.0,10.0,10.0,10.0,R,O,1994-06-20,1994-06-21,1994-06-22,");
    }

    @Test
    void csvFormattingEscapesSpecialCharacters() {
        assertThat(TpchCsvExporter.escapeCsv("a,b")).isEqualTo("\"a,b\"");
        assertThat(TpchCsvExporter.escapeCsv("a\"b")).isEqualTo("\"a\"\"b\"");
        assertThat(TpchCsvExporter.escapeCsv("a\nb")).isEqualTo("\"a\nb\"");
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
}

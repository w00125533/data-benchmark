package com.example.databenchmark.tpch;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
        String lineitemContent = Files.readString(files.get("lineitem"), StandardCharsets.UTF_8);

        assertThat(regionLines).hasSize(5);
        assertThat(lineitemLines).hasSize(600);
        assertThat(lineitemContent).contains("\n").doesNotContain("\r\n");
        assertThat(lineitemLines.get(0))
            .startsWith("1,1,1,1,10.0,10.0,10.0,10.0,R,O,1994-06-20,1994-06-21,1994-06-22,");
    }

    @Test
    void csvFormattingEscapesSpecialCharacters() {
        assertThat(TpchCsvExporter.escapeCsv("a,b")).isEqualTo("\"a,b\"");
        assertThat(TpchCsvExporter.escapeCsv("a\"b")).isEqualTo("\"a\"\"b\"");
        assertThat(TpchCsvExporter.escapeCsv("a\rb")).isEqualTo("\"a\rb\"");
        assertThat(TpchCsvExporter.escapeCsv("a\nb")).isEqualTo("\"a\nb\"");
    }

    @Test
    void exportOverwritesExistingCsvFiles() throws Exception {
        TpchDatasetResult dataset = new TpchDataGenerator().generate(config(), "tpch-overwrite");
        TpchCsvExporter exporter = new TpchCsvExporter();

        var files = exporter.export(dataset);
        Path lineitem = files.get("lineitem");
        Files.writeString(lineitem, "\nstale", StandardCharsets.UTF_8, StandardOpenOption.APPEND);

        exporter.export(dataset);

        List<String> lineitemLines = Files.readAllLines(lineitem, StandardCharsets.UTF_8);
        assertThat(lineitemLines).hasSize(600);
        assertThat(Files.readString(lineitem, StandardCharsets.UTF_8)).doesNotContain("stale");
    }

    @Test
    void exportedLineitemMatchesFormattedGeneratorValues() throws Exception {
        TpchDataGenerator generator = new TpchDataGenerator();
        TpchDatasetResult dataset = generator.generate(config(), "tpch-parity");

        Path lineitem = new TpchCsvExporter().export(dataset).get("lineitem");
        String firstLine = Files.readAllLines(lineitem, StandardCharsets.UTF_8).get(0);

        Map<String, Long> rowCounts = new LinkedHashMap<>();
        for (TpchTable table : TpchSchema.tables()) {
            rowCounts.put(table.name(), dataset.table(table.name()).rows());
        }

        TpchTable lineitemTable = TpchSchema.table("lineitem");
        List<String> expectedFields = lineitemTable.columns().stream()
            .map(column -> TpchCsvExporter.formatValue(column, generator.valueFor(lineitemTable, column, 0, rowCounts)))
            .toList();

        assertThat(firstLine.split(",", -1)).containsExactlyElementsOf(expectedFields);
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

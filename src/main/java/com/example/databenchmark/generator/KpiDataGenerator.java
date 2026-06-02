package com.example.databenchmark.generator;

import com.example.databenchmark.config.BenchmarkConfig;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Random;

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
        Random random = new Random(config.seed());
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            writer.write("event_time,cell_id,province,city,vendor,rat,band,rsrp_avg,sinr_avg,active_users,load_score");
            writer.newLine();

            for (long row = 0; row < rows; row++) {
                int cell = (int) (row % config.dataset().cells());
                long minute = row / config.dataset().cells();
                LocalDateTime eventTime = start.plusMinutes(minute);
                double loadScore = 5.0 + random.nextDouble() * 95.0;
                int activeUsers = 1 + random.nextInt(600);

                writer.write(String.format(
                    Locale.ROOT,
                    "%s,CELL-%06d,province-%02d,city-%03d,%s,%s,%s,%.3f,%.3f,%d,%.3f",
                    eventTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    cell,
                    cell % 31,
                    cell % 200,
                    vendor(cell),
                    cell % 2 == 0 ? "4G" : "5G",
                    band(cell),
                    -115.0 + random.nextDouble() * 35.0,
                    -5.0 + random.nextDouble() * 35.0,
                    activeUsers,
                    loadScore
                ));
                writer.newLine();
            }
        }
    }

    private String vendor(int cell) {
        return List.of("Huawei", "ZTE", "Ericsson", "Nokia", "Samsung").get(cell % 5);
    }

    private String band(int cell) {
        return List.of("B3", "B7", "B8", "B20", "N78", "N41").get(cell % 6);
    }
}

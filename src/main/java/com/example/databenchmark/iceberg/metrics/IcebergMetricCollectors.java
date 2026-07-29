package com.example.databenchmark.iceberg.metrics;

import java.util.ArrayList;
import java.util.List;

public final class IcebergMetricCollectors {
    private IcebergMetricCollectors() {}

    public static long parseSingleLong(String stdout) {
        return Long.parseLong(parseSingleString(stdout));
    }

    public static String parseSingleString(String stdout) {
        List<String> rows = new ArrayList<>();
        stdout.lines()
            .map(String::trim)
            .filter(line -> !line.isBlank())
            .filter(line -> !isSparkSeparator(line))
            .map(IcebergMetricCollectors::singleColumnValue)
            .filter(value -> !value.isBlank())
            .forEach(rows::add);

        if (rows.isEmpty()) {
            throw new IllegalArgumentException("No data row in Spark SQL output");
        }
        if (rows.size() == 1) {
            return rows.get(0);
        }
        return rows.get(1);
    }

    private static boolean isSparkSeparator(String line) {
        return line.chars().allMatch(character -> character == '+' || character == '-' || character == '|');
    }

    private static String singleColumnValue(String line) {
        if (line.startsWith("|") && line.endsWith("|")) {
            String withoutBorders = line.substring(1, line.length() - 1);
            String[] cells = withoutBorders.split("\\|", -1);
            return cells.length == 0 ? "" : cells[0].trim();
        }
        return line;
    }
}

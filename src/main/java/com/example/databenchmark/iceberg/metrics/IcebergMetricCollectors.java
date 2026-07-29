package com.example.databenchmark.iceberg.metrics;

import java.util.ArrayList;
import java.util.List;

public final class IcebergMetricCollectors {
    private IcebergMetricCollectors() {}

    public static long parseSingleLong(String stdout) {
        return parseSingleLong(stdout, null);
    }

    public static long parseSingleLong(String stdout, String expectedColumn) {
        String value = parseSingleString(stdout, expectedColumn);
        if (value.equalsIgnoreCase("null")) {
            throw new IllegalArgumentException(metricError("Expected numeric value", expectedColumn, value));
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(metricError("Expected numeric value", expectedColumn, value), e);
        }
    }

    public static String parseSingleString(String stdout) {
        return parseSingleString(stdout, null);
    }

    public static String parseSingleString(String stdout, String expectedColumn) {
        List<String> values = values(stdout);
        if (values.isEmpty()) {
            throw new IllegalArgumentException("No data row in Spark SQL output");
        }
        if (expectedColumn != null && !expectedColumn.isBlank()) {
            return valueAfterExpectedColumn(values, expectedColumn);
        }
        if (values.size() == 1) {
            return values.get(0);
        }
        return values.get(1);
    }

    private static List<String> values(String stdout) {
        List<String> values = new ArrayList<>();
        stdout.lines()
            .map(String::trim)
            .filter(line -> !line.isBlank())
            .filter(line -> !isSparkSeparator(line))
            .filter(line -> !isSparkFooter(line))
            .filter(line -> !isSparkLogLine(line))
            .map(IcebergMetricCollectors::singleColumnValue)
            .filter(value -> !value.isBlank())
            .forEach(values::add);
        return values;
    }

    private static String valueAfterExpectedColumn(List<String> values, String expectedColumn) {
        for (int index = 0; index < values.size(); index++) {
            if (values.get(index).equalsIgnoreCase(expectedColumn)) {
                if (index + 1 >= values.size()) {
                    throw new IllegalArgumentException("No data row after Spark SQL column " + expectedColumn);
                }
                return values.get(index + 1);
            }
        }
        if (values.size() == 1) {
            return values.get(0);
        }
        throw new IllegalArgumentException("No data row for Spark SQL column " + expectedColumn);
    }

    private static boolean isSparkSeparator(String line) {
        return line.chars().allMatch(character -> character == '+' || character == '-' || character == '|');
    }

    private static boolean isSparkFooter(String line) {
        return line.startsWith("Time taken:");
    }

    private static boolean isSparkLogLine(String line) {
        return line.startsWith("Setting default log level to ");
    }

    private static String singleColumnValue(String line) {
        if (line.startsWith("|") && line.endsWith("|")) {
            String withoutBorders = line.substring(1, line.length() - 1);
            String[] cells = withoutBorders.split("\\|", -1);
            return cells.length == 0 ? "" : cells[0].trim();
        }
        return line;
    }

    private static String metricError(String message, String expectedColumn, String value) {
        String column = expectedColumn == null || expectedColumn.isBlank() ? "" : " for column " + expectedColumn;
        return message + column + ": " + value;
    }
}

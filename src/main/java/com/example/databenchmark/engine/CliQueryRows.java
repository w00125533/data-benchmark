package com.example.databenchmark.engine;

import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class CliQueryRows {
    private static final Pattern SPARK_FETCHED_ROWS = Pattern.compile("Fetched\\s+(\\d+)\\s+row\\(s\\)");
    private static final Pattern HIVE_SELECTED_ROWS = Pattern.compile("(\\d+)\\s+rows?\\s+selected");

    private CliQueryRows() {}

    static long spark(CommandResult command) {
        return lastMatch(commandText(command), SPARK_FETCHED_ROWS).orElse(0L);
    }

    static long hive(CommandResult command) {
        return lastMatch(commandText(command), HIVE_SELECTED_ROWS).orElse(0L);
    }

    static OptionalLong scalarCount(CommandResult command) {
        OptionalLong count = OptionalLong.empty();
        for (String line : commandText(command).split("\\R")) {
            String normalized = line.trim()
                .replaceAll("^\\|", "")
                .replaceAll("\\|$", "")
                .trim();
            if (normalized.matches("\\d+")) {
                count = OptionalLong.of(Long.parseLong(normalized));
            }
        }
        return count;
    }

    private static String commandText(CommandResult command) {
        return command.stdout() + "\n" + command.stderr();
    }

    private static OptionalLong lastMatch(String text, Pattern pattern) {
        Matcher matcher = pattern.matcher(text);
        OptionalLong rows = OptionalLong.empty();
        while (matcher.find()) {
            rows = OptionalLong.of(Long.parseLong(matcher.group(1)));
        }
        return rows;
    }
}

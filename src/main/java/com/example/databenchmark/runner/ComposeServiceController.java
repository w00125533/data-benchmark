package com.example.databenchmark.runner;

import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ComposeServiceController {
    private static final Path DEFAULT_WORKING_DIRECTORY = Path.of(".");
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private static final int DEFAULT_READINESS_ATTEMPTS = 90;
    private static final Duration DEFAULT_READINESS_DELAY = Duration.ofSeconds(2);
    private static final Pattern TABLET_REPORT_TIME_PATTERN = Pattern.compile(
        "\"?lastSuccessReportTabletsTime\"?\\s*[:=]\\s*\"?([^\",}\\s][^\",}]*)\"?",
        Pattern.CASE_INSENSITIVE
    );

    private final CommandRunner commandRunner;
    private final Path workingDirectory;
    private final Duration timeout;
    private final int readinessAttempts;
    private final Duration readinessDelay;
    private final Sleeper sleeper;
    private final InfraComposeTarget infraComposeTarget;

    public ComposeServiceController() {
        this(new CommandRunner());
    }

    ComposeServiceController(CommandRunner commandRunner) {
        this(commandRunner, DEFAULT_WORKING_DIRECTORY, DEFAULT_TIMEOUT);
    }

    ComposeServiceController(CommandRunner commandRunner, Path workingDirectory, Duration timeout) {
        this(
            commandRunner,
            workingDirectory,
            timeout,
            DEFAULT_READINESS_ATTEMPTS,
            DEFAULT_READINESS_DELAY,
            delay -> Thread.sleep(delay.toMillis())
        );
    }

    ComposeServiceController(
        CommandRunner commandRunner,
        Path workingDirectory,
        Duration timeout,
        int readinessAttempts,
        Duration readinessDelay,
        Sleeper sleeper
    ) {
        this(
            commandRunner,
            workingDirectory,
            timeout,
            readinessAttempts,
            readinessDelay,
            sleeper,
            InfraComposeTarget.fromEnvironment(System.getenv())
        );
    }

    ComposeServiceController(
        CommandRunner commandRunner,
        Path workingDirectory,
        Duration timeout,
        int readinessAttempts,
        Duration readinessDelay,
        Sleeper sleeper,
        InfraComposeTarget infraComposeTarget
    ) {
        this.commandRunner = commandRunner;
        this.workingDirectory = workingDirectory;
        this.timeout = timeout;
        this.readinessAttempts = Math.max(1, readinessAttempts);
        this.readinessDelay = readinessDelay;
        this.sleeper = sleeper;
        this.infraComposeTarget = infraComposeTarget;
    }

    public void restart(BenchmarkRoute route) {
        if (route == BenchmarkRoute.STARROCKS_INTERNAL || route == BenchmarkRoute.STARROCKS_EXTERNAL_ICEBERG) {
            List<List<String>> commands = restartCommands(route);
            runChecked(route, commands.get(0), "restart");
            runChecked(route, commands.get(1), "restart");
            runChecked(route, commands.get(2), "restart");
            runChecked(route, commands.get(3), "restart");
            waitForStarRocksFeMysql(route);
            return;
        }

        for (List<String> command : restartCommands(route)) {
            runChecked(route, command, "restart");
        }
    }

    public void waitUntilReady(BenchmarkRoute route) {
        List<List<String>> commands = readinessCommands(route);
        List<String> lastCommand = commands.get(commands.size() - 1);
        String lastOutput = "";
        for (int attempt = 1; attempt <= readinessAttempts; attempt++) {
            boolean ready = true;
            for (List<String> command : commands) {
                lastCommand = command;
                try {
                    CommandResult result = commandRunner.run(command, workingDirectory, timeout);
                    lastOutput = commandOutput(result);
                    if (result.exitCode() != 0 || !readinessOutputIsReady(command, result)) {
                        ready = false;
                        break;
                    }
                } catch (IOException e) {
                    lastOutput = e.getMessage();
                    ready = false;
                    break;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(errorMessage(route, command, "readiness check", e.getMessage()), e);
                }
            }
            if (ready) {
                return;
            }

            if (attempt < readinessAttempts) {
                sleepBeforeRetry(route, lastCommand);
            }
        }

        throw new IllegalStateException(errorMessage(route, lastCommand, "readiness check", lastOutput));
    }

    List<List<String>> restartCommands(BenchmarkRoute route) {
        return switch (route) {
            case SPARK_NATIVE_PARQUET, SPARK_ICEBERG -> List.of(
                composeCommand("restart", "spark")
            );
            case STARROCKS_INTERNAL, STARROCKS_EXTERNAL_ICEBERG -> List.of(
                composeCommand("stop", "starrocks-be"),
                composeCommand("stop", "starrocks-fe"),
                composeCommand("start", "starrocks-fe"),
                composeCommand("start", "starrocks-be")
            );
            case HIVE_HDFS_PARQUET -> List.of(
                composeCommand("stop", "hive-server"),
                composeCommand("start", "hive-server")
            );
        };
    }

    private List<List<String>> readinessCommands(BenchmarkRoute route) {
        return switch (route) {
            case SPARK_NATIVE_PARQUET, SPARK_ICEBERG -> List.of(composeCommand(
                "exec",
                "-T",
                "spark",
                "/opt/spark/bin/spark-sql",
                "-e",
                "SELECT 1"
            ));
            case STARROCKS_INTERNAL, STARROCKS_EXTERNAL_ICEBERG -> List.of(
                starRocksMysqlCommand("SELECT 1"),
                starRocksMysqlCommand("SHOW PROC '/backends'"),
                starRocksMysqlCommand("SHOW BACKEND BLACKLIST")
            );
            case HIVE_HDFS_PARQUET -> List.of(composeCommand(
                "exec",
                "-T",
                "hive-server",
                "beeline",
                "-u",
                "jdbc:hive2://hive-server:10000/default",
                "-e",
                "SELECT 1"
            ));
        };
    }

    private List<String> starRocksMysqlCommand(String sql) {
        return composeCommand(
            "exec",
            "-T",
            "starrocks-fe",
            "mysql",
            "-h",
            "127.0.0.1",
            "-P",
            "9030",
            "-uroot",
            "-e",
            sql
        );
    }

    private List<String> composeCommand(String... args) {
        return infraComposeTarget.composeCommand(args);
    }

    private void waitForStarRocksFeMysql(BenchmarkRoute route) {
        List<String> command = starRocksMysqlCommand("SELECT 1");
        String lastOutput = "";
        for (int attempt = 1; attempt <= readinessAttempts; attempt++) {
            try {
                CommandResult result = commandRunner.run(command, workingDirectory, timeout);
                lastOutput = commandOutput(result);
                if (result.exitCode() == 0) {
                    return;
                }
            } catch (IOException e) {
                lastOutput = e.getMessage();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(errorMessage(route, command, "restart readiness check", e.getMessage()), e);
            }

            if (attempt < readinessAttempts) {
                sleepBeforeRetry(route, command);
            }
        }

        throw new IllegalStateException(errorMessage(route, command, "restart readiness check", lastOutput));
    }

    private boolean readinessOutputIsReady(List<String> command, CommandResult result) {
        String output = nullToEmpty(result.stdout()) + "\n" + nullToEmpty(result.stderr());
        if (command.contains("SHOW BACKEND BLACKLIST")) {
            return backendBlacklistIsEmpty(output);
        }
        if (!command.contains("SHOW PROC '/backends'") && !command.contains("SHOW BACKENDS")) {
            return true;
        }
        String lowerOutput = output.toLowerCase(Locale.ROOT);
        if (lowerOutput.contains("empty set")) {
            return false;
        }
        if (lowerOutput.matches("(?s).*\\b(?:inblacklist|blacklist)\\b\\s*[:=]\\s*true\\b.*")) {
            return false;
        }

        BackendReadiness readiness = parseBackendReadiness(output);
        if (readiness.hasBlacklistedBackend()) {
            return false;
        }
        if (readiness.hasAliveBackend()) {
            return readiness.hasUsableCapacity()
                && (!readiness.hasStatusJson() || readiness.hasTabletReportTime());
        }
        if (!lowerOutput.matches("(?s).*\\balive\\b\\s*[:=]\\s*true\\b.*")) {
            return false;
        }
        return !containsStatusJson(output) || hasTabletReportTime(output);
    }

    private BackendReadiness parseBackendReadiness(String output) {
        List<List<String>> rows = parseBackendRows(output);
        for (int headerIndex = 0; headerIndex < rows.size(); headerIndex++) {
            List<String> header = rows.get(headerIndex);
            int aliveIndex = indexOfNormalized(header, "alive");
            if (aliveIndex < 0) {
                continue;
            }

            List<Integer> blacklistIndexes = blacklistIndexes(header);
            int statusIndex = indexOfNormalized(header, "status");
            int availCapacityIndex = indexOfNormalized(header, "availcapacity");
            boolean hasAliveBackend = false;
            boolean hasStatusJson = false;
            boolean hasTabletReportTime = false;
            boolean hasUsableCapacity = availCapacityIndex < 0;
            for (int rowIndex = headerIndex + 1; rowIndex < rows.size(); rowIndex++) {
                List<String> row = rows.get(rowIndex);
                if (hasTrueValue(row, blacklistIndexes)) {
                    return new BackendReadiness(false, true, false, false, false);
                }
                if (hasValue(row, aliveIndex, "true")) {
                    hasAliveBackend = true;
                    if (availCapacityIndex >= 0 && capacityBytes(valueAt(row, availCapacityIndex)) > 0) {
                        hasUsableCapacity = true;
                    }
                    String status = valueAt(row, statusIndex);
                    if (containsStatusJson(status)) {
                        hasStatusJson = true;
                        hasTabletReportTime = hasTabletReportTime || hasTabletReportTime(status);
                    }
                }
            }
            return new BackendReadiness(hasAliveBackend, false, hasStatusJson, hasTabletReportTime, hasUsableCapacity);
        }
        return new BackendReadiness(false, false, false, false, false);
    }

    private boolean backendBlacklistIsEmpty(String output) {
        String lowerOutput = output.toLowerCase(Locale.ROOT);
        if (lowerOutput.contains("empty set")) {
            return true;
        }

        List<List<String>> rows = parseBackendRows(output);
        for (int headerIndex = 0; headerIndex < rows.size(); headerIndex++) {
            List<String> header = rows.get(headerIndex);
            int backendIdIndex = indexOfNormalized(header, "backendid");
            int blacklistTypeIndex = indexOfNormalized(header, "addblacklisttype");
            if (backendIdIndex < 0 && blacklistTypeIndex < 0) {
                continue;
            }

            for (int rowIndex = headerIndex + 1; rowIndex < rows.size(); rowIndex++) {
                if (isBackendBlacklistDataRow(rows.get(rowIndex), backendIdIndex)) {
                    return false;
                }
            }
            return true;
        }

        for (List<String> row : rows) {
            if (isBackendBlacklistDataRow(row, 0)) {
                return false;
            }
        }
        return true;
    }

    private boolean isBackendBlacklistDataRow(List<String> row, int backendIdIndex) {
        String backendId = valueAt(row, backendIdIndex);
        return !backendId.isEmpty() && backendId.matches("\\d+");
    }

    private boolean containsStatusJson(String value) {
        String trimmed = nullToEmpty(value).trim();
        return trimmed.startsWith("{") && trimmed.endsWith("}");
    }

    private boolean hasTabletReportTime(String value) {
        Matcher matcher = TABLET_REPORT_TIME_PATTERN.matcher(nullToEmpty(value));
        if (!matcher.find()) {
            return false;
        }
        String reportTime = matcher.group(1).trim();
        return !reportTime.isEmpty() && !reportTime.equalsIgnoreCase("null");
    }

    private double capacityBytes(String value) {
        String normalized = nullToEmpty(value).trim().toUpperCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            return 0.0;
        }
        Matcher matcher = Pattern.compile("([0-9]+(?:\\.[0-9]+)?)\\s*([KMGTPE]?B)?").matcher(normalized);
        if (!matcher.find()) {
            return 0.0;
        }
        double amount = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(2) == null ? "B" : matcher.group(2);
        return amount * switch (unit) {
            case "KB" -> 1024.0;
            case "MB" -> 1024.0 * 1024.0;
            case "GB" -> 1024.0 * 1024.0 * 1024.0;
            case "TB" -> 1024.0 * 1024.0 * 1024.0 * 1024.0;
            case "PB" -> 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0;
            case "EB" -> 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0;
            default -> 1.0;
        };
    }

    private List<List<String>> parseBackendRows(String output) {
        List<List<String>> rows = new ArrayList<>();
        for (String line : output.split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.matches("\\+-+.*")) {
                continue;
            }
            List<String> columns = splitBackendRow(trimmed);
            if (!columns.isEmpty()) {
                rows.add(columns);
            }
        }
        return rows;
    }

    private List<String> splitBackendRow(String line) {
        String[] rawColumns;
        if (line.startsWith("|") || line.contains("|")) {
            rawColumns = line.replaceAll("^\\|", "").replaceAll("\\|$", "").split("\\|");
        } else if (line.contains("\t")) {
            rawColumns = line.split("\\t");
        } else {
            rawColumns = line.split("\\s{2,}");
        }

        List<String> columns = new ArrayList<>();
        for (String rawColumn : rawColumns) {
            String column = rawColumn.trim();
            if (!column.isEmpty()) {
                columns.add(column);
            }
        }
        return columns;
    }

    private int indexOfNormalized(List<String> values, String expected) {
        for (int index = 0; index < values.size(); index++) {
            if (normalizeBackendColumn(values.get(index)).equals(expected)) {
                return index;
            }
        }
        return -1;
    }

    private List<Integer> blacklistIndexes(List<String> header) {
        List<Integer> indexes = new ArrayList<>();
        for (int index = 0; index < header.size(); index++) {
            String normalized = normalizeBackendColumn(header.get(index));
            if (normalized.equals("blacklist") || normalized.equals("inblacklist")) {
                indexes.add(index);
            }
        }
        return indexes;
    }

    private boolean hasTrueValue(List<String> row, List<Integer> indexes) {
        for (int index : indexes) {
            if (hasValue(row, index, "true")) {
                return true;
            }
        }
        return false;
    }

    private boolean hasValue(List<String> row, int index, String expected) {
        return index >= 0
            && index < row.size()
            && row.get(index).trim().equalsIgnoreCase(expected);
    }

    private String valueAt(List<String> row, int index) {
        if (index < 0 || index >= row.size()) {
            return "";
        }
        return row.get(index).trim();
    }

    private String normalizeBackendColumn(String value) {
        return value.toLowerCase(Locale.ROOT).replaceAll("[^a-z]", "");
    }

    private record BackendReadiness(
        boolean hasAliveBackend,
        boolean hasBlacklistedBackend,
        boolean hasStatusJson,
        boolean hasTabletReportTime,
        boolean hasUsableCapacity
    ) {}

    private void runChecked(BenchmarkRoute route, List<String> command, String action) {
        CommandResult result;
        try {
            result = commandRunner.run(command, workingDirectory, timeout);
        } catch (IOException e) {
            throw new IllegalStateException(errorMessage(route, command, action, e.getMessage()), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(errorMessage(route, command, action, e.getMessage()), e);
        }

        if (result.exitCode() != 0) {
            throw new IllegalStateException(errorMessage(route, command, action, commandOutput(result)));
        }
    }

    private void sleepBeforeRetry(BenchmarkRoute route, List<String> command) {
        try {
            sleeper.sleep(readinessDelay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(errorMessage(route, command, "readiness check", e.getMessage()), e);
        }
    }

    private String commandOutput(CommandResult result) {
        return "stdout: " + nullToEmpty(result.stdout()) + "; stderr: " + nullToEmpty(result.stderr());
    }

    private String errorMessage(BenchmarkRoute route, List<String> command, String action, String detail) {
        return "Compose service " + action + " failed for route " + route
            + " using command `" + String.join(" ", command) + "`"
            + ": " + nullToEmpty(detail);
    }

    private String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    @FunctionalInterface
    interface Sleeper {
        void sleep(Duration duration) throws InterruptedException;
    }
}

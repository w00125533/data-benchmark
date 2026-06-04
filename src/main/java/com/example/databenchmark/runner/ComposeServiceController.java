package com.example.databenchmark.runner;

import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

public class ComposeServiceController {
    private static final Path DEFAULT_WORKING_DIRECTORY = Path.of(".");
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private static final int DEFAULT_READINESS_ATTEMPTS = 10;
    private static final Duration DEFAULT_READINESS_DELAY = Duration.ofSeconds(2);

    private final CommandRunner commandRunner;
    private final Path workingDirectory;
    private final Duration timeout;
    private final int readinessAttempts;
    private final Duration readinessDelay;
    private final Sleeper sleeper;

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
        this.commandRunner = commandRunner;
        this.workingDirectory = workingDirectory;
        this.timeout = timeout;
        this.readinessAttempts = Math.max(1, readinessAttempts);
        this.readinessDelay = readinessDelay;
        this.sleeper = sleeper;
    }

    public void restart(BenchmarkRoute route) {
        for (List<String> command : restartCommands(route)) {
            runChecked(route, command, "restart");
        }
    }

    public void waitUntilReady(BenchmarkRoute route) {
        List<String> command = readinessCommand(route);
        String lastOutput = "";
        for (int attempt = 1; attempt <= readinessAttempts; attempt++) {
            try {
                CommandResult result = commandRunner.run(command, workingDirectory, timeout);
                if (result.exitCode() == 0) {
                    return;
                }
                lastOutput = commandOutput(result);
            } catch (IOException e) {
                lastOutput = e.getMessage();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(errorMessage(route, command, "readiness check", e.getMessage()), e);
            }

            if (attempt < readinessAttempts) {
                sleepBeforeRetry(route, command);
            }
        }

        throw new IllegalStateException(errorMessage(route, command, "readiness check", lastOutput));
    }

    List<List<String>> restartCommands(BenchmarkRoute route) {
        return switch (route) {
            case SPARK_ICEBERG -> List.of(
                List.of("docker", "compose", "-f", "docker-compose.yml", "restart", "spark")
            );
            case STARROCKS_INTERNAL, STARROCKS_EXTERNAL_ICEBERG -> List.of(
                List.of("docker", "compose", "-f", "docker-compose.yml", "restart", "starrocks-fe", "starrocks-be")
            );
            case HIVE_HDFS_PARQUET -> List.of(
                List.of("docker", "compose", "-f", "docker-compose.yml", "restart", "hive-server")
            );
        };
    }

    private List<String> readinessCommand(BenchmarkRoute route) {
        return switch (route) {
            case SPARK_ICEBERG -> List.of(
                "docker",
                "compose",
                "-f",
                "docker-compose.yml",
                "exec",
                "-T",
                "spark",
                "/opt/spark/bin/spark-sql",
                "-e",
                "SELECT 1"
            );
            case STARROCKS_INTERNAL, STARROCKS_EXTERNAL_ICEBERG ->
                List.of(
                    "docker",
                    "compose",
                    "-f",
                    "docker-compose.yml",
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
                    "SELECT 1"
                );
            case HIVE_HDFS_PARQUET -> List.of(
                "docker",
                "compose",
                "-f",
                "docker-compose.yml",
                "exec",
                "-T",
                "hive-server",
                "beeline",
                "-u",
                "jdbc:hive2://hive-server:10000/default",
                "-e",
                "SELECT 1"
            );
        };
    }

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

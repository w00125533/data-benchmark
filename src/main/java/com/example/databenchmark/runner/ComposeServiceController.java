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

    private final CommandRunner commandRunner;
    private final Path workingDirectory;
    private final Duration timeout;

    public ComposeServiceController() {
        this(new CommandRunner());
    }

    ComposeServiceController(CommandRunner commandRunner) {
        this(commandRunner, DEFAULT_WORKING_DIRECTORY, DEFAULT_TIMEOUT);
    }

    ComposeServiceController(CommandRunner commandRunner, Path workingDirectory, Duration timeout) {
        this.commandRunner = commandRunner;
        this.workingDirectory = workingDirectory;
        this.timeout = timeout;
    }

    public void restart(BenchmarkRoute route) {
        for (List<String> command : restartCommands(route)) {
            runChecked(route, command, "restart");
        }
    }

    public void waitUntilReady(BenchmarkRoute route) {
        runChecked(route, readinessCommand(route), "readiness check");
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
            case SPARK_ICEBERG -> List.of("docker", "compose", "-f", "docker-compose.yml", "ps", "spark");
            case STARROCKS_INTERNAL, STARROCKS_EXTERNAL_ICEBERG ->
                List.of("docker", "compose", "-f", "docker-compose.yml", "ps", "starrocks-fe", "starrocks-be");
            case HIVE_HDFS_PARQUET -> List.of("docker", "compose", "-f", "docker-compose.yml", "ps", "hive-server");
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
            throw new IllegalStateException(errorMessage(route, command, action, result.stderr()));
        }
    }

    private String errorMessage(BenchmarkRoute route, List<String> command, String action, String stderr) {
        return "Compose service " + action + " failed for route " + route
            + " using command `" + String.join(" ", command) + "`"
            + ": " + (stderr == null ? "" : stderr);
    }
}

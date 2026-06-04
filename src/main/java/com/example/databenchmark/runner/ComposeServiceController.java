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
    private static final int DEFAULT_READINESS_ATTEMPTS = 90;
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
        if (route == BenchmarkRoute.STARROCKS_INTERNAL || route == BenchmarkRoute.STARROCKS_EXTERNAL_ICEBERG) {
            List<List<String>> commands = restartCommands(route);
            runChecked(route, commands.get(0), "restart");
            runChecked(route, commands.get(1), "restart");
            waitForStarRocksFeMysql(route);
            runChecked(route, commands.get(2), "restart");
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
            case SPARK_ICEBERG -> List.of(
                List.of("docker", "compose", "-f", "docker-compose.yml", "restart", "spark")
            );
            case STARROCKS_INTERNAL, STARROCKS_EXTERNAL_ICEBERG -> List.of(
                List.of("docker", "compose", "-f", "docker-compose.yml", "stop", "starrocks-be"),
                List.of("docker", "compose", "-f", "docker-compose.yml", "up", "-d", "--force-recreate", "starrocks-fe"),
                List.of("docker", "compose", "-f", "docker-compose.yml", "up", "-d", "--force-recreate", "starrocks-be")
            );
            case HIVE_HDFS_PARQUET -> List.of(
                List.of("docker", "compose", "-f", "docker-compose.yml", "stop", "hive-server"),
                List.of("docker", "compose", "-f", "docker-compose.yml", "rm", "-f", "hive-server"),
                List.of("docker", "compose", "-f", "docker-compose.yml", "up", "-d", "hive-server")
            );
        };
    }

    private List<List<String>> readinessCommands(BenchmarkRoute route) {
        return switch (route) {
            case SPARK_ICEBERG -> List.of(List.of(
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
                ));
            case STARROCKS_INTERNAL, STARROCKS_EXTERNAL_ICEBERG -> List.of(
                starRocksMysqlCommand("SELECT 1"),
                starRocksMysqlCommand("SHOW PROC '/backends'")
            );
            case HIVE_HDFS_PARQUET -> List.of(List.of(
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
                ));
        };
    }

    private List<String> starRocksMysqlCommand(String sql) {
        return List.of(
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
            sql
        );
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
        if (!command.contains("SHOW PROC '/backends'")) {
            return true;
        }
        String output = nullToEmpty(result.stdout()).toLowerCase();
        if (output.contains("empty set")) {
            return false;
        }
        if (output.contains("alive")) {
            return output.contains("true");
        }
        return !output.isBlank();
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

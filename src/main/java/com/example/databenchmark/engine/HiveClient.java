package com.example.databenchmark.engine;

import com.example.databenchmark.runner.InfraComposeTarget;
import com.example.databenchmark.runner.RoutePhase;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

public class HiveClient {
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(10);
    private static final String IN_CONTAINER_ENV = "BENCHMARK_COMPOSE_IN_CONTAINER";
    private static final String BEELINE = "beeline";
    private static final String JDBC_URL = "jdbc:hive2://hive-server:10000/default";
    private static final String HDFS_DEFAULT_FS = "hdfs://hdfs-namenode:8020";
    private static final String TABLE_SHAPE = "hive_hdfs_parquet";
    private static final String LOAD_STAGE = "HIVE_HDFS_PARQUET_LOAD";

    private final CommandRunner commandRunner;
    private final Path workingDirectory;
    private final Duration timeout;
    private final boolean inContainer;

    public HiveClient() {
        this(new CommandRunner());
    }

    public HiveClient(CommandRunner commandRunner) {
        this(commandRunner, Path.of("."), DEFAULT_TIMEOUT);
    }

    public HiveClient(CommandRunner commandRunner, Path workingDirectory, Duration timeout) {
        this(commandRunner, workingDirectory, timeout, defaultInContainer());
    }

    public HiveClient(CommandRunner commandRunner, Path workingDirectory, Duration timeout, boolean inContainer) {
        this.commandRunner = commandRunner;
        this.workingDirectory = workingDirectory;
        this.timeout = timeout;
        this.inContainer = inContainer;
    }

    public EngineRunResult createExternalTable(Path parquetRoot) {
        return createExternalTable(normalizedPath(parquetRoot));
    }

    public EngineRunResult createExternalTable(String parquetRoot) {
        String location = hdfsLocation(parquetRoot);
        if (location == null) {
            return failed(LOAD_STAGE, null, RoutePhase.HOT, 0.0,
                "Invalid HDFS location: " + normalizedPath(parquetRoot)
                    + ". Expected hdfs://... or absolute HDFS path such as /data/...");
        }

        CommandResult command = runHiveSql(SqlTemplates.hiveCreateExternalParquetTable(location));
        if (command.exitCode() == 0) {
            return new EngineRunResult(
                "hive",
                TABLE_SHAPE,
                LOAD_STAGE,
                null,
                0,
                0,
                command.durationSeconds(),
                true,
                ""
            );
        }
        return failed(LOAD_STAGE, null, RoutePhase.HOT, command);
    }

    public EngineRunResult runQuery(String queryName, RoutePhase phase) {
        CommandResult command = runHiveSql(SqlRenderer.render(queryName, TABLE_SHAPE));
        if (command.exitCode() == 0) {
            return new EngineRunResult(
                "hive",
                TABLE_SHAPE,
                EngineStage.QUERY.name(),
                queryName,
                phase.name(),
                0,
                0,
                command.durationSeconds(),
                true,
                ""
            );
        }
        return failed(EngineStage.QUERY.name(), queryName, phase, command);
    }

    private CommandResult runHiveSql(String sql) {
        try {
            return commandRunner.run(hiveCommand(sql), workingDirectory, timeout);
        } catch (Exception e) {
            return new CommandResult(hiveCommand(sql), -1, "", e.getMessage(), 0.0);
        }
    }

    private List<String> hiveCommand(String sql) {
        List<String> beeline = List.of(BEELINE, "-u", JDBC_URL, "-e", sql);
        if (inContainer) {
            return beeline;
        }
        return InfraComposeTarget.fromEnvironment(System.getenv()).composeCommand(
            "exec", "-T", "hive-server",
            BEELINE, "-u", JDBC_URL, "-e", sql
        );
    }

    private static EngineRunResult failed(String stage, String queryName, RoutePhase phase, CommandResult command) {
        return new EngineRunResult(
            "hive",
            TABLE_SHAPE,
            stage,
            queryName,
            phase.name(),
            0,
            0,
            command.durationSeconds(),
            false,
            commandError(command)
        );
    }

    private static EngineRunResult failed(
        String stage,
        String queryName,
        RoutePhase phase,
        double durationSeconds,
        String error
    ) {
        return new EngineRunResult(
            "hive",
            TABLE_SHAPE,
            stage,
            queryName,
            phase.name(),
            0,
            0,
            durationSeconds,
            false,
            error
        );
    }

    private static String commandError(CommandResult command) {
        return command.stderr().isBlank() ? command.stdout() : command.stderr();
    }

    private static String hdfsLocation(String path) {
        String normalized = normalizedPath(path);
        if (normalized.startsWith("hdfs://")) {
            return normalized;
        }
        if (normalized.matches("^[A-Za-z]:/.*") || !normalized.startsWith("/")) {
            return null;
        }
        return HDFS_DEFAULT_FS + normalized;
    }

    private static String normalizedPath(Path path) {
        return path.toString().replace('\\', '/');
    }

    private static String normalizedPath(String path) {
        return path.replace('\\', '/');
    }

    private static boolean defaultInContainer() {
        return Boolean.parseBoolean(System.getenv().getOrDefault(IN_CONTAINER_ENV, "false"));
    }
}

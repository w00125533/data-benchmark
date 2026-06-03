package com.example.databenchmark.engine;

import com.example.databenchmark.generator.DatasetResult;
import com.example.databenchmark.query.QueryCatalog;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class SparkIcebergClient {
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(10);

    private final CommandRunner commandRunner;
    private final Path workingDirectory;
    private final Duration timeout;

    public SparkIcebergClient() {
        this(new CommandRunner());
    }

    public SparkIcebergClient(CommandRunner commandRunner) {
        this(commandRunner, Path.of("."), DEFAULT_TIMEOUT);
    }

    public SparkIcebergClient(CommandRunner commandRunner, Path workingDirectory, Duration timeout) {
        this.commandRunner = commandRunner;
        this.workingDirectory = workingDirectory;
        this.timeout = timeout;
    }

    public EngineRunResult load(DatasetResult dataset, String runId, String profile) {
        String workspacePath;
        try {
            workspacePath = toWorkspacePath(dataset.outputPath());
        } catch (IllegalArgumentException e) {
            return failed("spark_iceberg", EngineStage.SPARK_ICEBERG_LOAD.name(), null, e.getMessage());
        }
        String sql = SqlTemplates.sparkCreateIcebergTable()
            + "\n"
            + SqlTemplates.sparkInsertFromParquet(workspacePath);
        CommandResult command = runSparkSql(sql);
        if (command.exitCode() != 0) {
            return failed("spark_iceberg", EngineStage.SPARK_ICEBERG_LOAD.name(), null, command);
        }
        return new EngineRunResult(
            "spark",
            "spark_iceberg",
            EngineStage.SPARK_ICEBERG_LOAD.name(),
            null,
            dataset.rows(),
            dataset.bytesWritten(),
            command.durationSeconds(),
            true,
            ""
        );
    }

    public List<EngineRunResult> runQueries(String runId, String profile) {
        List<EngineRunResult> results = new ArrayList<>();
        for (var query : QueryCatalog.queries()) {
            String sql = SqlRenderer.render(query.name(), "spark_iceberg");
            CommandResult command = runSparkSql(sql);
            if (command.exitCode() == 0) {
                results.add(new EngineRunResult(
                    "spark",
                    "spark_iceberg",
                    EngineStage.QUERY.name(),
                    query.name(),
                    0,
                    0,
                    command.durationSeconds(),
                    true,
                    ""
                ));
            } else {
                results.add(failed("spark_iceberg", EngineStage.QUERY.name(), query.name(), command));
            }
        }
        return results;
    }

    private CommandResult runSparkSql(String sql) {
        try {
            return commandRunner.run(sparkSqlCommand(sql), workingDirectory, timeout);
        } catch (Exception e) {
            return new CommandResult(sparkSqlCommand(sql), -1, "", e.getMessage(), 0.0);
        }
    }

    private static List<String> sparkSqlCommand(String sql) {
        return List.of(
            "docker", "compose", "-f", "docker-compose.yml", "exec", "-T", "spark", "spark-sql",
            "--conf", "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog",
            "--conf", "spark.sql.catalog.iceberg_catalog.type=hive",
            "--conf", "spark.sql.catalog.iceberg_catalog.uri=thrift://hive-metastore:9083",
            "--conf", "spark.sql.catalog.iceberg_catalog.warehouse=hdfs://hdfs-namenode:8020/warehouse/iceberg",
            "-e", sql
        );
    }

    private static EngineRunResult failed(String tableShape, String stage, String queryName, CommandResult command) {
        return new EngineRunResult(
            "spark",
            tableShape,
            stage,
            queryName,
            0,
            0,
            command.durationSeconds(),
            false,
            command.stderr().isBlank() ? command.stdout() : command.stderr()
        );
    }

    private static EngineRunResult failed(String tableShape, String stage, String queryName, String error) {
        return new EngineRunResult(
            "spark",
            tableShape,
            stage,
            queryName,
            0,
            0,
            0.0,
            false,
            error
        );
    }

    private String toWorkspacePath(Path path) {
        Path workspace = workingDirectory.toAbsolutePath().normalize();
        Path output = path.toAbsolutePath().normalize();
        if (!output.startsWith(workspace)) {
            throw new IllegalArgumentException("Dataset output path is outside workspace: " + output);
        }
        String relative = workspace.relativize(output).toString().replace('\\', '/');
        return relative.isEmpty() ? "/workspace" : "/workspace/" + relative;
    }
}

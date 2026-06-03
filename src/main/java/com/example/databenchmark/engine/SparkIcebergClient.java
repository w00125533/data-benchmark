package com.example.databenchmark.engine;

import com.example.databenchmark.generator.DatasetResult;
import com.example.databenchmark.query.QueryCatalog;
import com.example.databenchmark.schema.KpiSchema;
import com.example.databenchmark.tpch.TpchDatasetResult;
import com.example.databenchmark.tpch.TpchQueryCatalog;
import com.example.databenchmark.tpch.TpchSchema;
import com.example.databenchmark.tpch.TpchSqlRenderer;
import com.example.databenchmark.tpch.TpchSqlTemplates;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class SparkIcebergClient {
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(10);
    private static final String IN_CONTAINER_ENV = "BENCHMARK_COMPOSE_IN_CONTAINER";
    private static final String SPARK_SQL = "/opt/spark/bin/spark-sql";
    private static final String ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1";
    private static final String IVY_CACHE_CONF = "spark.jars.ivy=/tmp/.ivy2";

    private final CommandRunner commandRunner;
    private final Path workingDirectory;
    private final Duration timeout;
    private final boolean inContainer;

    public SparkIcebergClient() {
        this(new CommandRunner());
    }

    public SparkIcebergClient(CommandRunner commandRunner) {
        this(commandRunner, Path.of("."), DEFAULT_TIMEOUT);
    }

    public SparkIcebergClient(CommandRunner commandRunner, Path workingDirectory, Duration timeout) {
        this(commandRunner, workingDirectory, timeout, defaultInContainer());
    }

    public SparkIcebergClient(CommandRunner commandRunner, Path workingDirectory, Duration timeout, boolean inContainer) {
        this.commandRunner = commandRunner;
        this.workingDirectory = workingDirectory;
        this.timeout = timeout;
        this.inContainer = inContainer;
    }

    public EngineRunResult load(DatasetResult dataset, String runId, String profile) {
        String workspacePath;
        try {
            workspacePath = toWorkspacePath(dataset.files().isEmpty() ? dataset.outputPath() : dataset.files().get(0));
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
            String sql = "smoke".equals(profile)
                ? "SELECT COUNT(*) FROM " + KpiSchema.tableShapes().get("spark_iceberg")
                : SqlRenderer.render(query.name(), "spark_iceberg");
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

    public EngineRunResult loadTpch(TpchDatasetResult dataset, String runId, String profile) {
        StringJoiner sql = new StringJoiner("\n");
        sql.add(TpchSqlTemplates.sparkCreateDatabase());
        try {
            for (var table : TpchSchema.tables()) {
                String parquetPath = toWorkspacePath(dataset.table(table.name()).parquetPath());
                sql.add(TpchSqlTemplates.sparkCreateTable(table));
                sql.add(TpchSqlTemplates.sparkInsertFromParquet(table, parquetPath));
            }
        } catch (IllegalArgumentException e) {
            return failed("tpch_iceberg", EngineStage.SPARK_ICEBERG_LOAD.name(), null, e.getMessage());
        }
        CommandResult command = runSparkSql(sql.toString());
        if (command.exitCode() != 0) {
            return failed("tpch_iceberg", EngineStage.SPARK_ICEBERG_LOAD.name(), null, command);
        }
        return new EngineRunResult(
            "spark",
            "tpch_iceberg",
            EngineStage.SPARK_ICEBERG_LOAD.name(),
            null,
            dataset.rows(),
            dataset.bytesWritten(),
            command.durationSeconds(),
            true,
            ""
        );
    }

    public List<EngineRunResult> runTpchQueries(String runId, String profile, String querySet) {
        List<EngineRunResult> results = new ArrayList<>();
        for (var query : TpchQueryCatalog.queries(querySet)) {
            CommandResult command = runSparkSql(TpchSqlRenderer.render(query.name(), "spark_iceberg"));
            if (command.exitCode() == 0) {
                results.add(new EngineRunResult(
                    "spark",
                    "tpch_iceberg",
                    EngineStage.QUERY.name(),
                    query.name(),
                    0,
                    0,
                    command.durationSeconds(),
                    true,
                    ""
                ));
            } else {
                results.add(failed("tpch_iceberg", EngineStage.QUERY.name(), query.name(), command));
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

    private List<String> sparkSqlCommand(String sql) {
        List<String> sparkSql = List.of(
            SPARK_SQL,
            "--conf", IVY_CACHE_CONF,
            "--packages", ICEBERG_RUNTIME,
            "--conf", "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog",
            "--conf", "spark.sql.catalog.iceberg_catalog.type=hive",
            "--conf", "spark.sql.catalog.iceberg_catalog.uri=thrift://hive-metastore:9083",
            "--conf", "spark.sql.catalog.iceberg_catalog.warehouse=hdfs://hdfs-namenode:8020/warehouse/iceberg",
            "-e", sql
        );
        if (inContainer) {
            return sparkSql;
        }
        List<String> command = new ArrayList<>();
        command.addAll(List.of(
            "docker", "compose", "-f", "docker-compose.yml", "exec", "-T", "spark", SPARK_SQL,
            "--conf", IVY_CACHE_CONF,
            "--packages", ICEBERG_RUNTIME,
            "--conf", "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog",
            "--conf", "spark.sql.catalog.iceberg_catalog.type=hive",
            "--conf", "spark.sql.catalog.iceberg_catalog.uri=thrift://hive-metastore:9083",
            "--conf", "spark.sql.catalog.iceberg_catalog.warehouse=hdfs://hdfs-namenode:8020/warehouse/iceberg",
            "-e", sql
        ));
        return command;
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

    private static boolean defaultInContainer() {
        return Boolean.parseBoolean(System.getenv().getOrDefault(IN_CONTAINER_ENV, "false"));
    }
}

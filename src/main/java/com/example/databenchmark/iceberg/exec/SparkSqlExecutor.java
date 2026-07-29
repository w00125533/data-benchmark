package com.example.databenchmark.iceberg.exec;

import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class SparkSqlExecutor {
    private final CommandRunner commandRunner;

    public SparkSqlExecutor() {
        this(new CommandRunner());
    }

    public SparkSqlExecutor(CommandRunner commandRunner) {
        this.commandRunner = commandRunner;
    }

    public CommandResult run(IcebergValidationConfig config, String sql)
        throws IOException, InterruptedException {
        CommandResult result = runRaw(config, sql);
        if (result.exitCode() != 0) {
            throw new IllegalStateException("Spark SQL failed: " + result.stderr());
        }
        return result;
    }

    public CommandResult runRaw(IcebergValidationConfig config, String sql)
        throws IOException, InterruptedException {
        return commandRunner.run(
            commandFor(config, sql),
            Path.of("."),
            Duration.ofSeconds(config.spark().timeoutSeconds())
        );
    }

    public List<String> commandFor(IcebergValidationConfig config, String sql) {
        List<String> command = new ArrayList<>();
        command.addAll(List.of(
            "docker", "compose",
            "-f", "../shared-data-infra/compose.yaml",
            "-f", "../shared-data-infra/compose.lakehouse.yaml",
            "-f", "../shared-data-infra/compose.starrocks.yaml",
            "--profile", "lakehouse",
            "--profile", "lakehouse-tools",
            "--profile", "spark-tools",
            "--profile", "starrocks",
            "exec", "-T", config.spark().service(),
            "/opt/spark/bin/spark-sql"
        ));
        for (String dependency : config.spark().packages()) {
            command.add("--packages");
            command.add(dependency);
        }
        command.add("--conf");
        command.add("spark.sql.catalog." + config.iceberg().catalog() + "=org.apache.iceberg.spark.SparkCatalog");
        command.add("--conf");
        command.add("spark.sql.catalog." + config.iceberg().catalog() + ".type=hive");
        command.add("--conf");
        command.add("spark.sql.catalog." + config.iceberg().catalog() + ".uri=thrift://hive-metastore:9083");
        command.add("--conf");
        command.add("spark.sql.catalog." + config.iceberg().catalog() + ".warehouse=" + config.iceberg().warehouse());
        command.add("--conf");
        command.add("spark.sql.extensions=" + String.join(",", config.spark().extensions()));
        command.add("-e");
        command.add(sql);
        return command;
    }
}

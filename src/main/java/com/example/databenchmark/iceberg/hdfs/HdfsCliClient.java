package com.example.databenchmark.iceberg.hdfs;

import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class HdfsCliClient {
    private final CommandRunner commandRunner;

    public HdfsCliClient() {
        this(new CommandRunner());
    }

    public HdfsCliClient(CommandRunner commandRunner) {
        this.commandRunner = commandRunner;
    }

    public CommandResult dfs(IcebergValidationConfig config, List<String> args)
        throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.addAll(List.of("docker", "compose",
            "-f", "../shared-data-infra/compose.yaml",
            "-f", "../shared-data-infra/compose.lakehouse.yaml",
            "--profile", "lakehouse",
            "--profile", "lakehouse-tools",
            "exec", "-T", "namenode",
            "hdfs", "dfs", "-fs", config.hdfs().defaultFs()));
        command.addAll(args);
        CommandResult result = commandRunner.run(command, Path.of("."), Duration.ofSeconds(config.spark().timeoutSeconds()));
        if (result.exitCode() != 0) {
            throw new IllegalStateException("HDFS command failed: " + result.stderr());
        }
        return result;
    }
}

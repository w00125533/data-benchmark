package com.example.databenchmark.iceberg.hdfs;

import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class HdfsFaultInjector {
    private final CommandRunner commandRunner;
    private final List<String> stoppedServices = new ArrayList<>();

    public HdfsFaultInjector() {
        this(new CommandRunner());
    }

    public HdfsFaultInjector(CommandRunner commandRunner) {
        this.commandRunner = commandRunner;
    }

    public void stopDataNode(String serviceName) throws IOException, InterruptedException {
        runCompose("stop", serviceName);
        stoppedServices.add(serviceName);
    }

    public void startDataNode(String serviceName) throws IOException, InterruptedException {
        runCompose("start", serviceName);
        stoppedServices.remove(serviceName);
    }

    public void restartStoppedDataNodes() throws IOException, InterruptedException {
        for (String serviceName : List.copyOf(stoppedServices)) {
            startDataNode(serviceName);
        }
    }

    private void runCompose(String action, String serviceName) throws IOException, InterruptedException {
        List<String> command = List.of("docker", "compose",
            "-f", "../shared-data-infra/compose.yaml",
            "-f", "../shared-data-infra/compose.lakehouse.yaml",
            "--profile", "lakehouse",
            "--profile", "lakehouse-tools",
            action,
            serviceName);
        CommandResult result = commandRunner.run(command, Path.of("."), Duration.ofMinutes(5));
        if (result.exitCode() != 0) {
            throw new IllegalStateException("HDFS fault injection command failed: " + result.stderr());
        }
    }
}

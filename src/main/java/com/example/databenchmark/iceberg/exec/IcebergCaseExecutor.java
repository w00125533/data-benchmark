package com.example.databenchmark.iceberg.exec;

import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
import com.example.databenchmark.iceberg.IcebergExecutionEvidence;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

public class IcebergCaseExecutor {
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(30);

    private final CommandRunner commandRunner;

    public IcebergCaseExecutor(CommandRunner commandRunner) {
        this.commandRunner = commandRunner;
    }

    public IcebergExecutionEvidence record(String phase, String label, List<String> command)
        throws IOException, InterruptedException {
        CommandResult result = commandRunner.run(command, Path.of("."), DEFAULT_TIMEOUT);
        return new IcebergExecutionEvidence(
            phase,
            label,
            String.join(" ", command),
            result.exitCode(),
            result.durationSeconds(),
            result.stdout(),
            result.stderr()
        );
    }
}

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
    private final Path workingDirectory;
    private final Duration timeout;

    public IcebergCaseExecutor(CommandRunner commandRunner) {
        this(commandRunner, Path.of("."), DEFAULT_TIMEOUT);
    }

    public IcebergCaseExecutor(CommandRunner commandRunner, Path workingDirectory, Duration timeout) {
        this.commandRunner = commandRunner;
        this.workingDirectory = workingDirectory;
        this.timeout = timeout;
    }

    public IcebergExecutionEvidence record(String phase, String label, List<String> command)
        throws IOException, InterruptedException {
        CommandResult result = commandRunner.run(command, workingDirectory, timeout);
        return new IcebergExecutionEvidence(
            phase,
            label,
            renderCommand(command),
            result.exitCode(),
            result.durationSeconds(),
            result.stdout(),
            result.stderr()
        );
    }

    private static String renderCommand(List<String> command) {
        return command.stream()
            .map(IcebergCaseExecutor::renderArgument)
            .reduce((left, right) -> left + " " + right)
            .orElse("");
    }

    private static String renderArgument(String argument) {
        if (argument.matches("[A-Za-z0-9_./:=@+-]+")) {
            return argument;
        }
        return "'" + argument.replace("'", "''") + "'";
    }
}

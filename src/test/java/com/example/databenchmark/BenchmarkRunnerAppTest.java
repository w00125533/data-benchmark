package com.example.databenchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

class BenchmarkRunnerAppTest {
    @TempDir
    Path tempDir;

    @Test
    void cliExposesCoreCommandsAndStandardOptions() {
        CommandLine commandLine = new CommandLine(new BenchmarkRunnerApp());

        assertThat(commandLine.getSubcommands().keySet()).contains("generate", "run", "report");
        assertThat(commandLine.getCommandSpec().optionsMap().keySet()).contains("-h", "--help", "-V", "--version");
        assertThat(commandLine.getCommandSpec().version()).containsExactly("data-benchmark 0.1.0-SNAPSHOT");
    }

    @Test
    void helpListsCoreCommands() {
        String usage = new CommandLine(new BenchmarkRunnerApp()).getUsageMessage();

        assertThat(usage).contains("generate");
        assertThat(usage).contains("run");
        assertThat(usage).contains("report");
    }

    @Test
    void generateCommandWritesToConfiguredOutput() {
        CommandResult result = execute(
            "generate",
            "--output", tempDir.toString(),
            "--cells", "3",
            "--days", "1",
            "--row-cap", "12",
            "--seed", "123"
        );

        assertThat(result.exitCode()).isZero();
        assertThat(result.out()).contains("rows=12");
        assertThat(result.out()).contains(tempDir.toString());
    }

    @Test
    void runAndReportCommandsPrintAvailability() {
        assertAvailabilityOutput("run", "run command is available");
        assertAvailabilityOutput("report", "report command is available");
    }

    @Test
    void unknownCommandReturnsNonZeroExitCode() {
        CommandResult result = execute("missing");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.err()).contains("Unmatched argument");
    }

    private static void assertAvailabilityOutput(String command, String expectedOutput) {
        CommandResult result = execute(command);

        assertThat(result.exitCode()).isZero();
        assertThat(result.out()).contains(expectedOutput);
    }

    private static CommandResult execute(String... args) {
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        CommandLine commandLine = new CommandLine(new BenchmarkRunnerApp())
            .setOut(new PrintWriter(out))
            .setErr(new PrintWriter(err));

        int exitCode = commandLine.execute(args);

        return new CommandResult(exitCode, out.toString(), err.toString());
    }

    private record CommandResult(int exitCode, String out, String err) {}
}

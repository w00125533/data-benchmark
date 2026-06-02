package com.example.databenchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class BenchmarkRunnerAppTest {
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
    void generateRunAndReportCommandsPrintAvailability() {
        assertAvailabilityOutput("generate", "generate command is available");
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

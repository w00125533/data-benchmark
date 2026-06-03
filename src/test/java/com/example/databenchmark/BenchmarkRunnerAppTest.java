package com.example.databenchmark;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
    void runCommandExecutesLocalSmokeWorkflow() throws Exception {
        Path dataDir = tempDir.resolve("cli-data");
        Path reportDir = tempDir.resolve("cli-reports");
        Path config = tempDir.resolve("benchmark-smoke.yml");
        Files.writeString(config, """
            profile: smoke
            seed: 99
            dataset:
              cells: 2
              days: 1
              columns: 50
              startTime: "2026-01-01T00:00:00"
              output: "%s"
              rowCap: 8
            query:
              coldRuns: 1
              warmRuns: 3
              concurrency: 1
            report:
              format: html
              output: "%s"
            monitoring:
              prometheus: true
              grafana: true
            """.formatted(escapeYamlPath(dataDir), escapeYamlPath(reportDir)));

        CommandResult result = execute("run", "--config", config.toString(), "--run-id", "run-cli-test");

        assertThat(result.exitCode()).isZero();
        assertThat(result.out()).contains("rows=8");
        assertThat(result.out()).contains("report=" + reportDir.resolve("run-cli-test").resolve("index.html"));
        assertThat(reportDir.resolve("run-cli-test").resolve("index.html")).exists();
    }

    @Test
    void runCommandModeLocalDispatchesToLocalRunner() throws Exception {
        Path reportDir = tempDir.resolve("reports");
        Path config = writeConfig(tempDir.resolve("data"), reportDir);
        FakeRunnerFactory runners = new FakeRunnerFactory();

        CommandResult result = execute(new BenchmarkRunnerApp(runners),
            "run", "--config", config.toString(), "--mode", "local", "--run-id", "test-local");

        assertThat(result.exitCode()).isZero();
        assertThat(result.out()).contains("rows=11").contains("report=" + reportDir.resolve("test-local.html"));
        assertThat(runners.calls).containsExactly("local:test-local");
    }

    @Test
    void runCommandModeComposeDispatchesToComposeRunner() throws Exception {
        Path reportDir = tempDir.resolve("reports");
        Path config = writeConfig(tempDir.resolve("data"), reportDir);
        FakeRunnerFactory runners = new FakeRunnerFactory();

        CommandResult result = execute(new BenchmarkRunnerApp(runners),
            "run", "--config", config.toString(), "--mode", "compose", "--run-id", "compose-test");

        assertThat(result.exitCode()).isZero();
        assertThat(result.out()).contains("rows=22").contains("report=" + reportDir.resolve("compose-test.html"));
        assertThat(runners.calls).containsExactly("compose:compose-test");
    }

    @Test
    void runCommandRejectsUnknownModeWithValidValues() throws Exception {
        Path config = writeConfig(tempDir.resolve("data"), tempDir.resolve("reports"));

        CommandResult result = execute(new BenchmarkRunnerApp(new FakeRunnerFactory()),
            "run", "--config", config.toString(), "--mode", "remote", "--run-id", "bad-mode");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.err()).contains("Unknown mode: remote");
        assertThat(result.err()).contains("local").contains("compose");
    }

    @Test
    void reportCommandPrintsAvailability() {
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
        return execute(new BenchmarkRunnerApp(), args);
    }

    private static CommandResult execute(BenchmarkRunnerApp app, String... args) {
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        CommandLine commandLine = new CommandLine(app)
            .setOut(new PrintWriter(out))
            .setErr(new PrintWriter(err));

        int exitCode = commandLine.execute(args);

        return new CommandResult(exitCode, out.toString(), err.toString());
    }

    private static String escapeYamlPath(Path path) {
        return path.toString().replace("\\", "\\\\");
    }

    private Path writeConfig(Path dataDir, Path reportDir) throws Exception {
        Path config = tempDir.resolve("benchmark-%d.yml".formatted(System.nanoTime()));
        Files.writeString(config, """
            profile: smoke
            seed: 99
            dataset:
              cells: 2
              days: 1
              columns: 50
              startTime: "2026-01-01T00:00:00"
              output: "%s"
              rowCap: 8
            query:
              coldRuns: 1
              warmRuns: 3
              concurrency: 1
            report:
              format: html
              output: "%s"
            monitoring:
              prometheus: true
              grafana: true
            """.formatted(escapeYamlPath(dataDir), escapeYamlPath(reportDir)));
        return config;
    }

    private static final class FakeRunnerFactory implements BenchmarkRunnerApp.RunnerFactory {
        private final List<String> calls = new ArrayList<>();

        @Override
        public BenchmarkRunnerApp.CliRunResult runLocal(BenchmarkConfig config, Path reportRoot, String runId) {
            calls.add("local:" + runId);
            return new BenchmarkRunnerApp.CliRunResult(11L, reportRoot.resolve(runId + ".html"), true);
        }

        @Override
        public BenchmarkRunnerApp.CliRunResult runCompose(BenchmarkConfig config, Path reportRoot, String runId) {
            calls.add("compose:" + runId);
            return new BenchmarkRunnerApp.CliRunResult(22L, reportRoot.resolve(runId + ".html"), true);
        }
    }

    private record CommandResult(int exitCode, String out, String err) {}
}

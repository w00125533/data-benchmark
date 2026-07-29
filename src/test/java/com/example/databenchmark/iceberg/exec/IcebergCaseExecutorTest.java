package com.example.databenchmark.iceberg.exec;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
import com.example.databenchmark.iceberg.IcebergExecutionEvidence;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class IcebergCaseExecutorTest {
    @Test
    void recordsCommandStdoutStderrExitCodeAndDuration() throws Exception {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(
            List.of("docker", "compose", "exec", "spark", "spark-sql"),
            7,
            "count\n1000\n",
            "warning\n",
            0.123
        ));
        IcebergCaseExecutor executor = new IcebergCaseExecutor(runner);

        IcebergExecutionEvidence evidence = executor.record(
            "action",
            "row count",
            List.of("spark-sql", "-e", "SELECT 1")
        );

        assertThat(evidence.phase()).isEqualTo("action");
        assertThat(evidence.label()).isEqualTo("row count");
        assertThat(evidence.script()).isEqualTo("spark-sql -e 'SELECT 1'");
        assertThat(evidence.exitCode()).isEqualTo(7);
        assertThat(evidence.durationSeconds()).isEqualTo(0.123);
        assertThat(evidence.stdout()).isEqualTo("count\n1000\n");
        assertThat(evidence.stderr()).isEqualTo("warning\n");
        assertThat(runner.command).containsExactly("spark-sql", "-e", "SELECT 1");
        assertThat(runner.workingDirectory).isEqualTo(Path.of("."));
        assertThat(runner.timeout).isEqualTo(Duration.ofMinutes(30));
    }

    @Test
    void forwardsConfiguredWorkingDirectoryAndTimeout() throws Exception {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "ok", "", 0.1));
        Path workingDirectory = Path.of("work", "iceberg");
        Duration timeout = Duration.ofSeconds(45);
        IcebergCaseExecutor executor = new IcebergCaseExecutor(runner, workingDirectory, timeout);

        executor.record("setup", "create table", List.of("spark-sql", "-e", "SELECT 1"));

        assertThat(runner.workingDirectory).isEqualTo(workingDirectory);
        assertThat(runner.timeout).isEqualTo(timeout);
    }

    @Test
    void quotesCommandArgumentsWithWhitespaceInEvidenceScript() throws Exception {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "", "", 0.1));
        IcebergCaseExecutor executor = new IcebergCaseExecutor(runner);

        IcebergExecutionEvidence evidence = executor.record(
            "action",
            "count rows",
            List.of("spark-sql", "-e", "SELECT COUNT(*) FROM t")
        );

        assertThat(evidence.script()).isEqualTo("spark-sql -e 'SELECT COUNT(*) FROM t'");
    }

    @Test
    void rendersUnsafeCommandArgumentsWithPowerShellSingleQuotes() throws Exception {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "", "", 0.1));
        IcebergCaseExecutor executor = new IcebergCaseExecutor(runner);

        IcebergExecutionEvidence evidence = executor.record(
            "action",
            "count rows",
            List.of("tool", "C:\\Program Files\\Data", "SELECT COUNT(*) FROM t")
        );

        assertThat(evidence.script()).contains("'C:\\Program Files\\Data'");
        assertThat(evidence.script()).contains("'SELECT COUNT(*) FROM t'");
    }

    private static final class FakeCommandRunner extends CommandRunner {
        private final CommandResult result;
        private List<String> command;
        private Path workingDirectory;
        private Duration timeout;

        private FakeCommandRunner(CommandResult result) {
            this.result = result;
        }

        @Override
        public CommandResult run(List<String> command, Path workingDirectory, Duration timeout) {
            this.command = command;
            this.workingDirectory = workingDirectory;
            this.timeout = timeout;
            return result;
        }
    }
}

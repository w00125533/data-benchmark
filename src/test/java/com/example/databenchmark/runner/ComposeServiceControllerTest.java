package com.example.databenchmark.runner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import org.junit.jupiter.api.Test;

class ComposeServiceControllerTest {
    @Test
    void restartCommandsReturnsRouteSpecificDockerComposeRestartCommands() {
        FakeCommandRunner commandRunner = new FakeCommandRunner();
        ComposeServiceController controller = new ComposeServiceController(commandRunner);

        assertThat(controller.restartCommands(BenchmarkRoute.SPARK_ICEBERG))
            .containsExactly(List.of("docker", "compose", "-f", "docker-compose.yml", "restart", "spark"));
        assertThat(controller.restartCommands(BenchmarkRoute.STARROCKS_INTERNAL))
            .containsExactly(List.of("docker", "compose", "-f", "docker-compose.yml", "restart", "starrocks-fe", "starrocks-be"));
        assertThat(controller.restartCommands(BenchmarkRoute.STARROCKS_EXTERNAL_ICEBERG))
            .containsExactly(List.of("docker", "compose", "-f", "docker-compose.yml", "restart", "starrocks-fe", "starrocks-be"));
        assertThat(controller.restartCommands(BenchmarkRoute.HIVE_HDFS_PARQUET))
            .containsExactly(List.of("docker", "compose", "-f", "docker-compose.yml", "restart", "hive-server"));
    }

    @Test
    void restartRunsRouteSpecificCommands() throws Exception {
        FakeCommandRunner commandRunner = new FakeCommandRunner();
        commandRunner.enqueueSuccess();
        ComposeServiceController controller = new ComposeServiceController(commandRunner);

        controller.restart(BenchmarkRoute.SPARK_ICEBERG);

        assertThat(commandRunner.commands)
            .containsExactly(List.of("docker", "compose", "-f", "docker-compose.yml", "restart", "spark"));
    }

    @Test
    void restartThrowsRouteCommandAndStderrWhenCommandFails() {
        FakeCommandRunner commandRunner = new FakeCommandRunner();
        commandRunner.enqueueFailure("spark restart failed");
        ComposeServiceController controller = new ComposeServiceController(commandRunner);

        assertThatThrownBy(() -> controller.restart(BenchmarkRoute.SPARK_ICEBERG))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("SPARK_ICEBERG")
            .hasMessageContaining("docker compose -f docker-compose.yml restart spark")
            .hasMessageContaining("spark restart failed");
    }

    @Test
    void waitUntilReadyRunsLightweightReadinessCommandForEachRouteFamily() throws Exception {
        FakeCommandRunner commandRunner = new FakeCommandRunner();
        commandRunner.enqueueSuccess();
        commandRunner.enqueueSuccess();
        commandRunner.enqueueSuccess();
        commandRunner.enqueueSuccess();
        commandRunner.enqueueSuccess();
        commandRunner.enqueueSuccess();
        ComposeServiceController controller = new ComposeServiceController(
            commandRunner,
            Path.of("."),
            Duration.ofSeconds(30),
            10,
            Duration.ZERO,
            ignored -> { }
        );

        controller.waitUntilReady(BenchmarkRoute.SPARK_ICEBERG);
        controller.waitUntilReady(BenchmarkRoute.STARROCKS_INTERNAL);
        controller.waitUntilReady(BenchmarkRoute.STARROCKS_EXTERNAL_ICEBERG);
        controller.waitUntilReady(BenchmarkRoute.HIVE_HDFS_PARQUET);

        assertThat(commandRunner.commands).containsExactly(
            List.of("docker", "compose", "-f", "docker-compose.yml", "exec", "-T", "spark",
                "/opt/spark/bin/spark-sql", "-e", "SELECT 1"),
            List.of("docker", "compose", "-f", "docker-compose.yml", "exec", "-T", "starrocks-fe",
                "mysql", "-h", "127.0.0.1", "-P", "9030", "-uroot", "-e", "SELECT 1"),
            List.of("docker", "compose", "-f", "docker-compose.yml", "exec", "-T", "starrocks-fe",
                "mysql", "-h", "127.0.0.1", "-P", "9030", "-uroot", "-e", "SHOW PROC '/backends'"),
            List.of("docker", "compose", "-f", "docker-compose.yml", "exec", "-T", "starrocks-fe",
                "mysql", "-h", "127.0.0.1", "-P", "9030", "-uroot", "-e", "SELECT 1"),
            List.of("docker", "compose", "-f", "docker-compose.yml", "exec", "-T", "starrocks-fe",
                "mysql", "-h", "127.0.0.1", "-P", "9030", "-uroot", "-e", "SHOW PROC '/backends'"),
            List.of("docker", "compose", "-f", "docker-compose.yml", "exec", "-T", "hive-server",
                "beeline", "-u", "jdbc:hive2://hive-server:10000/default", "-e", "SELECT 1")
        );
    }

    @Test
    void waitUntilReadyRetriesStarRocksUntilBackendIsAlive() {
        FakeCommandRunner commandRunner = new FakeCommandRunner();
        commandRunner.enqueueSuccess("1\n1");
        commandRunner.enqueueSuccess("BackendId\tAlive\n10001\tfalse");
        commandRunner.enqueueSuccess("1\n1");
        commandRunner.enqueueSuccess("BackendId\tAlive\n10001\ttrue");
        ComposeServiceController controller = new ComposeServiceController(
            commandRunner,
            Path.of("."),
            Duration.ofSeconds(30),
            3,
            Duration.ZERO,
            ignored -> { }
        );

        controller.waitUntilReady(BenchmarkRoute.STARROCKS_INTERNAL);

        assertThat(commandRunner.commands).hasSize(4);
        assertThat(commandRunner.commands.get(0)).containsSequence("-e", "SELECT 1");
        assertThat(commandRunner.commands.get(1)).containsSequence("-e", "SHOW PROC '/backends'");
        assertThat(commandRunner.commands.get(2)).containsSequence("-e", "SELECT 1");
        assertThat(commandRunner.commands.get(3)).containsSequence("-e", "SHOW PROC '/backends'");
    }

    @Test
    void waitUntilReadyRetriesUntilReadinessCommandSucceeds() {
        FakeCommandRunner commandRunner = new FakeCommandRunner();
        commandRunner.enqueueFailure("spark not ready");
        commandRunner.enqueueFailure("spark still not ready");
        commandRunner.enqueueSuccess();
        ComposeServiceController controller = new ComposeServiceController(
            commandRunner,
            Path.of("."),
            Duration.ofSeconds(30),
            10,
            Duration.ZERO,
            ignored -> { }
        );

        controller.waitUntilReady(BenchmarkRoute.SPARK_ICEBERG);

        assertThat(commandRunner.commands).hasSize(3);
    }

    @Test
    void waitUntilReadyThrowsRouteCommandAndLastOutputWhenReadinessCommandNeverSucceeds() {
        FakeCommandRunner commandRunner = new FakeCommandRunner();
        commandRunner.enqueueFailure("hive-server booting");
        commandRunner.enqueueFailure("hive-server is missing");
        ComposeServiceController controller = new ComposeServiceController(
            commandRunner,
            Path.of("."),
            Duration.ofSeconds(30),
            2,
            Duration.ZERO,
            ignored -> { }
        );

        assertThatThrownBy(() -> controller.waitUntilReady(BenchmarkRoute.HIVE_HDFS_PARQUET))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("HIVE_HDFS_PARQUET")
            .hasMessageContaining("readiness check")
            .hasMessageContaining("docker compose -f docker-compose.yml exec -T hive-server beeline -u jdbc:hive2://hive-server:10000/default -e SELECT 1")
            .hasMessageContaining("hive-server is missing");

        assertThat(commandRunner.commands).hasSize(2);
    }

    private static final class FakeCommandRunner extends CommandRunner {
        private final Queue<CommandResult> results = new ArrayDeque<>();
        private final List<List<String>> commands = new ArrayList<>();

        private void enqueueSuccess() {
            enqueueSuccess("ok");
        }

        private void enqueueSuccess(String stdout) {
            results.add(new CommandResult(List.of(), 0, stdout, "", 0.0));
        }

        private void enqueueFailure(String stderr) {
            results.add(new CommandResult(List.of(), 7, "", stderr, 0.0));
        }

        @Override
        public CommandResult run(List<String> command, Path workingDirectory, Duration timeout)
            throws IOException, InterruptedException {
            commands.add(command);
            CommandResult result = results.poll();
            if (result == null) {
                throw new AssertionError("No fake result queued for command: " + command);
            }
            return new CommandResult(command, result.exitCode(), result.stdout(), result.stderr(), result.durationSeconds());
        }
    }
}

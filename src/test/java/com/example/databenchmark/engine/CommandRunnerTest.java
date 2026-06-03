package com.example.databenchmark.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class CommandRunnerTest {
    @Test
    void successfulCommandCapturesStdoutAndExitCode() throws Exception {
        CommandResult result = new CommandRunner()
            .run(command("Write-Output ok", "printf 'ok%n'"), Path.of("."), Duration.ofSeconds(5));

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("ok");
        assertThat(result.stderr()).isEmpty();
        assertThat(result.durationSeconds()).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void failedCommandCapturesStderrAndNonZeroExitCode() throws Exception {
        CommandResult result = new CommandRunner()
            .run(command("Write-Error boom; exit 7", "printf 'boom%n' >&2; exit 7"), Path.of("."), Duration.ofSeconds(5));

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.stderr()).contains("boom");
    }

    @Test
    void timedOutCommandReturnsMinusOneAndTimedOutStderr() throws Exception {
        CommandResult result = new CommandRunner()
            .run(command("Start-Sleep -Seconds 5", "sleep 5"), Path.of("."), Duration.ofMillis(200));

        assertThat(result.exitCode()).isEqualTo(-1);
        assertThat(result.stderr()).contains("Timed out");
    }

    private static List<String> command(String powershell, String sh) {
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            return List.of("powershell", "-NoProfile", "-Command", powershell);
        }
        return List.of("sh", "-c", sh);
    }
}

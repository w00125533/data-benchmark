package com.example.databenchmark.engine;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

public class CommandRunner {
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(2);

    public CommandResult run(List<String> command, Path workingDirectory, Duration timeout)
        throws IOException, InterruptedException {
        long started = System.nanoTime();
        ProcessBuilder builder = new ProcessBuilder(command);
        if (workingDirectory != null) {
            builder.directory(workingDirectory.toFile());
        }

        Process process = builder.start();
        CompletableFuture<String> stdout = readAsync(process.getInputStream());
        CompletableFuture<String> stderr = readAsync(process.getErrorStream());

        boolean finished = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (!finished) {
            process.destroyForcibly();
            process.waitFor(SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            String capturedStdout = safeGet(stdout);
            String capturedStderr = appendLine(safeGet(stderr), "Timed out after " + timeout.toMillis() + " ms");
            return new CommandResult(command, -1, capturedStdout, capturedStderr, elapsedSeconds(started));
        }

        return new CommandResult(command, process.exitValue(), stdout.join(), stderr.join(), elapsedSeconds(started));
    }

    private static CompletableFuture<String> readAsync(InputStream stream) {
        return CompletableFuture.supplyAsync(() -> {
            try (stream) {
                return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                return e.getMessage();
            }
        });
    }

    private static String safeGet(CompletableFuture<String> future) {
        try {
            return future.get(SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "";
        } catch (ExecutionException | TimeoutException e) {
            return "";
        }
    }

    private static String appendLine(String existing, String line) {
        if (existing == null || existing.isEmpty()) {
            return line;
        }
        return existing.endsWith(System.lineSeparator()) ? existing + line : existing + System.lineSeparator() + line;
    }

    private static double elapsedSeconds(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000_000.0;
    }
}

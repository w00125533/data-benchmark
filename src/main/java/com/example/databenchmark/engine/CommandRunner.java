package com.example.databenchmark.engine;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class CommandRunner {
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
            process.waitFor();
            String capturedStdout = stdout.join();
            String capturedStderr = appendLine(stderr.join(), "Timed out after " + timeout.toMillis() + " ms");
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

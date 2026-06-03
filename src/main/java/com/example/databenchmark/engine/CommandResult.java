package com.example.databenchmark.engine;

import java.util.List;

public record CommandResult(
    List<String> command,
    int exitCode,
    String stdout,
    String stderr,
    double durationSeconds
) {}

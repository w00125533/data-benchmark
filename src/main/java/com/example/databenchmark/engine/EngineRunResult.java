package com.example.databenchmark.engine;

public record EngineRunResult(
    String engine,
    String tableShape,
    String stage,
    String queryName,
    long rows,
    long bytes,
    double durationSeconds,
    boolean success,
    String error
) {}

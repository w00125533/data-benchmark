package com.example.databenchmark.engine;

import com.example.databenchmark.runner.RoutePhase;

public record EngineRunResult(
    String engine,
    String tableShape,
    String stage,
    String queryName,
    String phase,
    long rows,
    long bytes,
    double durationSeconds,
    boolean success,
    String error
) {
    public EngineRunResult {
        phase = phase == null || phase.isBlank() ? RoutePhase.HOT.name() : phase;
    }

    public EngineRunResult(
        String engine,
        String tableShape,
        String stage,
        String queryName,
        long rows,
        long bytes,
        double durationSeconds,
        boolean success,
        String error
    ) {
        this(engine, tableShape, stage, queryName, RoutePhase.HOT.name(), rows, bytes, durationSeconds, success, error);
    }
}

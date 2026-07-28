package com.example.databenchmark.iceberg;

import java.util.List;

public record IcebergValidationReport(
    String runId,
    String icebergVersion,
    String profile,
    String status,
    String startedAt,
    String endedAt,
    List<IcebergValidationResult> results
) {}

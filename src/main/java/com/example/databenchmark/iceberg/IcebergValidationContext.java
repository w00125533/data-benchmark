package com.example.databenchmark.iceberg;

import java.nio.file.Path;

public record IcebergValidationContext(
    IcebergValidationConfig config,
    String runId,
    Path workspace,
    Path reportRoot,
    boolean keepArtifacts
) {}

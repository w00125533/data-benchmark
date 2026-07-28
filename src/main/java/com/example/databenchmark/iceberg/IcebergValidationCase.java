package com.example.databenchmark.iceberg;

import java.util.Map;

public record IcebergValidationCase(
    String scenario,
    String caseId,
    String purpose,
    Map<String, String> parameters,
    boolean requiredForSuite
) {}

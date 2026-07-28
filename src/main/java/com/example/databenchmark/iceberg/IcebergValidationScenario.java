package com.example.databenchmark.iceberg;

import java.util.List;

public interface IcebergValidationScenario {
    String name();

    List<IcebergValidationCase> cases(IcebergValidationConfig config);

    IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) throws Exception;
}

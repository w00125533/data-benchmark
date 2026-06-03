package com.example.databenchmark.engine;

public enum EngineStage {
    GENERATE,
    SPARK_ICEBERG_LOAD,
    STARROCKS_INTERNAL_LOAD,
    STARROCKS_EXTERNAL_REFRESH,
    QUERY
}

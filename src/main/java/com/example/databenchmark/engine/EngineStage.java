package com.example.databenchmark.engine;

public enum EngineStage {
    GENERATE,
    SPARK_NATIVE_PARQUET_LOAD,
    SPARK_ICEBERG_LOAD,
    STARROCKS_INTERNAL_LOAD,
    STARROCKS_EXTERNAL_REFRESH,
    QUERY
}

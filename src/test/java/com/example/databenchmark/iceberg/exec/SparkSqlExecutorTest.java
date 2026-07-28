package com.example.databenchmark.iceberg.exec;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationConfigLoader;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class SparkSqlExecutorTest {
    @Test
    void sparkSqlCommandUsesIceberg1101RuntimeAndExtensions() throws Exception {
        IcebergValidationConfig config = new IcebergValidationConfigLoader()
            .load(Path.of("configs", "iceberg-validation.yml"));

        List<String> command = new SparkSqlExecutor().commandFor(config, "SELECT 1");

        assertThat(command).contains(
            "docker", "compose",
            "-f", "../shared-data-infra/compose.yaml",
            "-f", "../shared-data-infra/compose.lakehouse.yaml",
            "-f", "../shared-data-infra/compose.starrocks.yaml",
            "--profile", "lakehouse",
            "--profile", "lakehouse-tools",
            "--profile", "spark-tools",
            "--profile", "starrocks",
            "exec", "-T", "spark",
            "/opt/spark/bin/spark-sql"
        );
        assertThat(command).anySatisfy(value ->
            assertThat(value).contains("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1"));
        assertThat(command).anySatisfy(value ->
            assertThat(value).contains("spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"));
    }
}

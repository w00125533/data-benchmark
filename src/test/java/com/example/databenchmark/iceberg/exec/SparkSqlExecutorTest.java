package com.example.databenchmark.iceberg.exec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationConfigLoader;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
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
        assertThat(command).containsSubsequence("--conf", "spark.jars.ivy=/tmp/.ivy2");
        assertThat(command).anySatisfy(value ->
            assertThat(value).contains("spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"));
    }

    @Test
    void runRawReturnsNonZeroSparkResultWithoutThrowing() throws Exception {
        IcebergValidationConfig config = new IcebergValidationConfigLoader()
            .load(Path.of("configs", "iceberg-validation.yml"));
        SparkSqlExecutor executor = new SparkSqlExecutor(new FixedCommandRunner(
            new CommandResult(List.of("spark-sql"), 12, "stdout\n", "stderr\n", 0.250)
        ));

        CommandResult result = executor.runRaw(config, "SELECT broken");

        assertThat(result.exitCode()).isEqualTo(12);
        assertThat(result.stdout()).isEqualTo("stdout\n");
        assertThat(result.stderr()).isEqualTo("stderr\n");
        assertThat(result.durationSeconds()).isEqualTo(0.250);
    }

    @Test
    void runPreservesThrowingSemanticsForNonZeroSparkResult() throws Exception {
        IcebergValidationConfig config = new IcebergValidationConfigLoader()
            .load(Path.of("configs", "iceberg-validation.yml"));
        SparkSqlExecutor executor = new SparkSqlExecutor(new FixedCommandRunner(
            new CommandResult(List.of("spark-sql"), 12, "stdout\n", "stderr\n", 0.250)
        ));

        assertThatThrownBy(() -> executor.run(config, "SELECT broken"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Spark SQL failed: stderr");
    }

    private static final class FixedCommandRunner extends CommandRunner {
        private final CommandResult result;

        private FixedCommandRunner(CommandResult result) {
            this.result = result;
        }

        @Override
        public CommandResult run(List<String> command, Path workingDirectory, Duration timeout)
            throws IOException, InterruptedException {
            return new CommandResult(command, result.exitCode(), result.stdout(), result.stderr(), result.durationSeconds());
        }
    }
}

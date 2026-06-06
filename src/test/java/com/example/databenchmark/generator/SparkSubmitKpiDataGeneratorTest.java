package com.example.databenchmark.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SparkSubmitKpiDataGeneratorTest {
    @TempDir
    Path tempDir;

    @Test
    void submitsGenerateCommandToSparkComposeService() throws Exception {
        Path output = tempDir.resolve("generated");
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(10, 1, null, output.toString(), 100L);
        CapturingCommandRunner commandRunner = new CapturingCommandRunner(output);

        DatasetResult result = new SparkSubmitKpiDataGenerator(commandRunner, tempDir, Duration.ofSeconds(30))
            .generate(config);

        assertThat(result.rows()).isEqualTo(100L);
        assertThat(result.files()).hasSize(1);
        assertThat(commandRunner.command).containsExactly(
            "docker", "compose", "-p", "shared-data-infra",
            "-f", "../shared-data-infra/compose.yaml",
            "-f", "../shared-data-infra/compose.lakehouse.yaml",
            "-f", "../shared-data-infra/compose.starrocks.yaml",
            "--profile", "lakehouse",
            "--profile", "lakehouse-tools",
            "--profile", "spark-tools",
            "--profile", "starrocks",
            "exec", "-T", "spark",
            "/opt/spark/bin/spark-submit",
            "--class", "com.example.databenchmark.BenchmarkRunnerApp",
            "--master", "local[*]",
            "--driver-memory", "8g",
            "--conf", "spark.driver.maxResultSize=2g",
            "--conf", "spark.driver.host=127.0.0.1",
            "--conf", "spark.driver.bindAddress=127.0.0.1",
            "--conf", "spark.sql.shuffle.partitions=1",
            "--conf", "spark.default.parallelism=1",
            "--conf", "spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:8020",
            "target/data-benchmark-0.1.0-SNAPSHOT.jar",
            "generate", "--config", "target/generated-configs/smoke-spark-generate.yml"
        );
        Path generatedConfig = tempDir.resolve("target/generated-configs/smoke-spark-generate.yml");
        assertThat(generatedConfig).exists();
        assertThat(Files.readString(generatedConfig))
            .contains("profile: \"smoke\"")
            .contains("rowCap: 100");
    }

    @Test
    void returnsHdfsDatasetAfterSparkSubmitGeneration() throws Exception {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(10, 1, null, "hdfs://hdfs-namenode:8020/benchmark/kpi-1b/generated", 100L);
        CapturingCommandRunner commandRunner = new CapturingCommandRunner(null);

        DatasetResult result = new SparkSubmitKpiDataGenerator(commandRunner, tempDir, Duration.ofSeconds(30))
            .generate(config);

        assertThat(result.outputPath().toString().replace('\\', '/')).isEqualTo("/benchmark/kpi-1b/generated");
        assertThat(result.files()).isEmpty();
        assertThat(result.rows()).isEqualTo(100L);
        assertThat(result.bytesWritten()).isEqualTo(4096L);
        assertThat(commandRunner.commands).hasSize(2);
        assertThat(commandRunner.commands.get(1)).containsExactly(
            "docker", "compose", "-p", "shared-data-infra",
            "-f", "../shared-data-infra/compose.yaml",
            "-f", "../shared-data-infra/compose.lakehouse.yaml",
            "-f", "../shared-data-infra/compose.starrocks.yaml",
            "--profile", "lakehouse",
            "--profile", "lakehouse-tools",
            "--profile", "spark-tools",
            "--profile", "starrocks",
            "exec", "-T", "spark",
            "bash", "-lc",
            "unset JAVA_TOOL_OPTIONS; /opt/spark/bin/spark-class org.apache.hadoop.fs.FsShell "
                + "-fs 'hdfs://hdfs-namenode:8020' -du -s '/benchmark/kpi-1b/generated'"
        );
    }

    @Test
    void returnsHdfsUriDatasetWithoutLocalParquetWalk() throws Exception {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(10, 1, null, "hdfs://hdfs-namenode:8020/benchmark/kpi-1b/generated", 100L);
        CapturingCommandRunner commandRunner = new CapturingCommandRunner(null);

        DatasetResult result = new SparkSubmitKpiDataGenerator(commandRunner, tempDir, Duration.ofSeconds(30))
            .generate(config);

        assertThat(result.outputPath().toString().replace('\\', '/')).isEqualTo("/benchmark/kpi-1b/generated");
        assertThat(result.files()).isEmpty();
        assertThat(result.rows()).isEqualTo(100L);
        assertThat(result.bytesWritten()).isEqualTo(4096L);
        assertThat(commandRunner.commands).hasSize(2);
        assertThat(commandRunner.commands.get(1).get(commandRunner.commands.get(1).size() - 1))
            .isEqualTo("unset JAVA_TOOL_OPTIONS; /opt/spark/bin/spark-class org.apache.hadoop.fs.FsShell "
                + "-fs 'hdfs://hdfs-namenode:8020' -du -s '/benchmark/kpi-1b/generated'");
    }

    @Test
    void reusesExistingHdfsDatasetWhenExplicitlyEnabled() throws Exception {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(10, 1, null, "hdfs://hdfs-namenode:8020/benchmark/kpi-1b/generated", 100L);
        CapturingCommandRunner commandRunner = new CapturingCommandRunner(null);

        DatasetResult result = new SparkSubmitKpiDataGenerator(
            commandRunner,
            tempDir,
            Duration.ofSeconds(30),
            true
        ).generate(config);

        assertThat(result.outputPath().toString().replace('\\', '/')).isEqualTo("/benchmark/kpi-1b/generated");
        assertThat(result.files()).isEmpty();
        assertThat(result.rows()).isEqualTo(100L);
        assertThat(result.bytesWritten()).isEqualTo(4096L);
        assertThat(commandRunner.commands).hasSize(1);
        assertThat(commandRunner.commands.get(0))
            .contains("exec", "-T", "spark", "bash", "-lc")
            .doesNotContain("/opt/spark/bin/spark-submit");
    }

    @Test
    void bareAbsoluteOutputIsTreatedAsLocalPath() throws Exception {
        Path output = tempDir.resolve("absolute-generated");
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(10, 1, null, output.toString(), 100L);
        CapturingCommandRunner commandRunner = new CapturingCommandRunner(output);

        DatasetResult result = new SparkSubmitKpiDataGenerator(commandRunner, tempDir, Duration.ofSeconds(30))
            .generate(config);

        assertThat(result.outputPath()).isEqualTo(output);
        assertThat(result.files()).hasSize(1);
        assertThat(result.bytesWritten()).isGreaterThan(0L);
        assertThat(commandRunner.commands).hasSize(1);
        assertThat(commandRunner.commands.get(0)).contains("/opt/spark/bin/spark-submit");
    }

    private static final class CapturingCommandRunner extends CommandRunner {
        private final Path output;
        private List<String> command;
        private final java.util.ArrayList<List<String>> commands = new java.util.ArrayList<>();

        private CapturingCommandRunner(Path output) {
            this.output = output;
        }

        @Override
        public CommandResult run(List<String> command, Path workingDirectory, Duration timeout) throws java.io.IOException {
            this.command = command;
            this.commands.add(command);
            if (command.toString().contains("-du")) {
                return new CommandResult(command, 0, "4096 /benchmark/kpi-1b/generated\n", "", 1.0);
            }
            if (output == null) {
                return new CommandResult(command, 0, "rows=100", "", 1.0);
            }
            Path partition = output.resolve("event_date=2026-01-01");
            Files.createDirectories(partition);
            Files.writeString(partition.resolve("part-00000.parquet"), "fake");
            return new CommandResult(command, 0, "rows=100", "", 1.0);
        }
    }
}

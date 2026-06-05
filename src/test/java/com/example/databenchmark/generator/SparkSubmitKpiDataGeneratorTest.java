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
            "exec", "-T", "spark",
            "java", "-jar", "target/data-benchmark-0.1.0-SNAPSHOT.jar",
            "generate", "--config", "target/generated-configs/smoke-spark-generate.yml"
        );
        Path generatedConfig = tempDir.resolve("target/generated-configs/smoke-spark-generate.yml");
        assertThat(generatedConfig).exists();
        assertThat(Files.readString(generatedConfig))
            .contains("profile: \"smoke\"")
            .contains("rowCap: 100");
    }

    private static final class CapturingCommandRunner extends CommandRunner {
        private final Path output;
        private List<String> command;

        private CapturingCommandRunner(Path output) {
            this.output = output;
        }

        @Override
        public CommandResult run(List<String> command, Path workingDirectory, Duration timeout) throws java.io.IOException {
            this.command = command;
            Path partition = output.resolve("event_date=2026-01-01");
            Files.createDirectories(partition);
            Files.writeString(partition.resolve("part-00000.parquet"), "fake");
            return new CommandResult(command, 0, "rows=100", "", 1.0);
        }
    }
}

package com.example.databenchmark.generator;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;

public class SparkSubmitKpiDataGenerator implements KpiDatasetGenerator {
    private static final String RUNNER_JAR = "target/data-benchmark-0.1.0-SNAPSHOT.jar";

    private final CommandRunner commandRunner;
    private final Path workingDirectory;
    private final Duration timeout;

    public SparkSubmitKpiDataGenerator(CommandRunner commandRunner) {
        this(commandRunner, Path.of("."), Duration.ofHours(12));
    }

    SparkSubmitKpiDataGenerator(CommandRunner commandRunner, Path workingDirectory, Duration timeout) {
        this.commandRunner = commandRunner;
        this.workingDirectory = workingDirectory;
        this.timeout = timeout;
    }

    @Override
    public DatasetResult generate(BenchmarkConfig config) throws Exception {
        KpiGenerationConfig generation = KpiGenerationConfig.from(config);
        Path configPath = writeEffectiveConfig(config);
        String configArgument = workingDirectory.relativize(configPath).toString().replace('\\', '/');
        List<String> command = List.of(
            "docker", "compose", "-f", "docker-compose.yml", "exec", "-T", "spark",
            "java", "-jar", RUNNER_JAR,
            "generate", "--config", configArgument
        );

        CommandResult result = commandRunner.run(command, workingDirectory, timeout);
        if (result.exitCode() != 0) {
            throw new IOException("Spark compose KPI generation failed: " + result.stderr());
        }

        List<Path> parquetFiles = parquetFiles(generation.outputPath());
        if (parquetFiles.isEmpty()) {
            throw new IOException("Spark compose KPI generation wrote no Parquet files under " + generation.outputPath());
        }
        long bytesWritten = 0L;
        for (Path file : parquetFiles) {
            bytesWritten += Files.size(file);
        }
        return new DatasetResult(generation.outputPath(), parquetFiles, generation.targetRows(), bytesWritten);
    }

    private Path writeEffectiveConfig(BenchmarkConfig config) throws IOException {
        Path configPath = workingDirectory.resolve("target/generated-configs")
            .resolve(config.profile() + "-spark-generate.yml");
        Files.createDirectories(configPath.getParent());
        Files.writeString(configPath, renderYaml(config));
        return configPath;
    }

    private static String renderYaml(BenchmarkConfig config) {
        StringBuilder yaml = new StringBuilder();
        yaml.append("profile: \"").append(escape(config.profile())).append("\"\n");
        yaml.append("seed: ").append(config.seed()).append('\n');
        yaml.append("suite:\n");
        yaml.append("  name: ").append(config.suite().name()).append('\n');
        yaml.append("  scaleFactor: ").append(config.suite().scaleFactor().toPlainString()).append('\n');
        yaml.append("  querySet: ").append(config.suite().querySet()).append('\n');
        yaml.append("dataset:\n");
        yaml.append("  cells: ").append(config.dataset().cells()).append('\n');
        yaml.append("  days: ").append(config.dataset().days()).append('\n');
        yaml.append("  columns: ").append(config.dataset().columns()).append('\n');
        yaml.append("  startTime: \"").append(escape(config.dataset().startTime())).append("\"\n");
        yaml.append("  output: \"").append(escape(config.dataset().output())).append("\"\n");
        if (config.dataset().rowCap() != null) {
            yaml.append("  rowCap: ").append(config.dataset().rowCap()).append('\n');
        }
        if (config.dataset().spark() != null) {
            BenchmarkConfig.DatasetSparkConfig spark = config.dataset().spark();
            yaml.append("  spark:\n");
            if (spark.master() != null) {
                yaml.append("    master: \"").append(escape(spark.master())).append("\"\n");
            }
            if (spark.partitions() != null) {
                yaml.append("    partitions: ").append(spark.partitions()).append('\n');
            }
            if (spark.rowsPerPartition() != null) {
                yaml.append("    rowsPerPartition: ").append(spark.rowsPerPartition()).append('\n');
            }
            if (spark.outputMode() != null) {
                yaml.append("    outputMode: \"").append(escape(spark.outputMode())).append("\"\n");
            }
        }
        yaml.append("query:\n");
        yaml.append("  coldRuns: ").append(config.query().coldRuns()).append('\n');
        yaml.append("  warmRuns: ").append(config.query().warmRuns()).append('\n');
        yaml.append("  concurrency: ").append(config.query().concurrency()).append('\n');
        yaml.append("report:\n");
        yaml.append("  format: ").append(config.report().format()).append('\n');
        yaml.append("  output: \"").append(escape(config.report().output())).append("\"\n");
        return yaml.toString();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static List<Path> parquetFiles(Path outputPath) throws IOException {
        if (!Files.exists(outputPath)) {
            return List.of();
        }
        try (var paths = Files.walk(outputPath)) {
            return paths
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().endsWith(".parquet"))
                .sorted(Comparator.comparing(Path::toString))
                .toList();
        }
    }
}

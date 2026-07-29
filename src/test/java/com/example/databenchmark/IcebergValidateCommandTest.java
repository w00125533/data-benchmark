package com.example.databenchmark;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.generator.DatasetResult;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationReport;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

class IcebergValidateCommandTest {
    @TempDir
    Path tempDir;

    @Test
    void commandDispatchesToIcebergValidationRunner() throws Exception {
        Path reportDir = tempDir.resolve("reports");
        Path config = tempDir.resolve("iceberg-validation.yml");
        Files.writeString(config, validConfig(reportDir));
        FakeRunnerFactory runners = new FakeRunnerFactory();

        CommandResult result = execute(new BenchmarkRunnerApp(runners),
            "iceberg-validate",
            "--config", config.toString(),
            "--run-id", "iceberg-run",
            "--scenario", "schemaEvolution",
            "--case", "schema-add-drop-rename",
            "--keep-artifacts"
        );

        assertThat(result.exitCode()).isZero();
        assertThat(result.out()).contains("cases=1");
        assertThat(result.out()).contains("report=" + reportDir.resolve("iceberg-run").resolve("report.html"));
        assertThat(runners.calls).containsExactly("iceberg:iceberg-run:[schemaEvolution]:[schema-add-drop-rename]:true");
    }

    private static String validConfig(Path reportDir) {
        return """
            iceberg:
              version: "1.10.1"
              catalog: "iceberg_catalog"
              namespace: "iceberg_validation"
              warehouse: "hdfs://hdfs-namenode:8020/warehouse/iceberg"
              formatVersion: 2
            spark:
              service: "spark"
              timeoutSeconds: 900
              packages:
                - "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1"
              extensions:
                - "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            hdfs:
              defaultFs: "hdfs://hdfs-namenode:8020"
              replicationBaseline: 2
              ecPolicies: ["RS-10-4-1024k"]
            scale:
              profile: "smoke"
              rows: 100000
              partitions: 8
              smallFileCommits: 100
              filesPerCommit: 4
              concurrentWriters: [2, 4, 8]
            scenarios:
              schemaEvolution:
                enabled: true
            report:
              output: "%s"
              formats: ["json", "html"]
            """.formatted(reportDir.toString().replace("\\", "\\\\"));
    }

    private static CommandResult execute(BenchmarkRunnerApp app, String... args) {
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        int exitCode = new CommandLine(app)
            .setOut(new PrintWriter(out))
            .setErr(new PrintWriter(err))
            .execute(args);
        return new CommandResult(exitCode, out.toString(), err.toString());
    }

    private static final class FakeRunnerFactory implements BenchmarkRunnerApp.RunnerFactory {
        private final List<String> calls = new ArrayList<>();

        @Override
        public DatasetResult generateKpi(BenchmarkConfig config) {
            return new DatasetResult(Path.of(config.dataset().output()), List.of(), 0L, 0L);
        }

        @Override
        public BenchmarkRunnerApp.CliRunResult runLocal(BenchmarkConfig config, Path reportRoot, String runId) {
            return new BenchmarkRunnerApp.CliRunResult(0L, reportRoot.resolve(runId + ".html"), true);
        }

        @Override
        public BenchmarkRunnerApp.CliRunResult runCompose(BenchmarkConfig config, Path reportRoot, String runId) {
            return new BenchmarkRunnerApp.CliRunResult(0L, reportRoot.resolve(runId + ".html"), true);
        }

        @Override
        public IcebergValidationReport runIcebergValidation(
            IcebergValidationConfig config,
            String runId,
            List<String> scenarios,
            List<String> cases,
            boolean keepArtifacts
        ) {
            calls.add("iceberg:" + runId + ":" + scenarios + ":" + cases + ":" + keepArtifacts);
            return new IcebergValidationReport(runId, config.iceberg().version(), config.scale().profile(),
                "SUCCESS", "2026-07-28T00:00:00Z", "2026-07-28T00:00:01Z",
                List.of(new com.example.databenchmark.iceberg.IcebergValidationResult(
                    "schemaEvolution", "schema-add-drop-rename", "purpose", java.util.Map.of(),
                    List.of(), List.of(), List.of(), java.util.Map.of(), java.util.Map.of(), java.util.Map.of(),
                    com.example.databenchmark.iceberg.IcebergConclusion.FunctionStatus.PASS,
                    com.example.databenchmark.iceberg.IcebergConclusion.PerformanceStatus.ACCEPTABLE,
                    "ok", List.of(), List.of()
                )));
        }
    }

    private record CommandResult(int exitCode, String out, String err) {}
}

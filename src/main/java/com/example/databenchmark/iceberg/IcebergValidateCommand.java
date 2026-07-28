package com.example.databenchmark.iceberg;

import com.example.databenchmark.BenchmarkRunnerApp;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(name = "iceberg-validate", description = "Run Apache Iceberg capability validation scenarios.")
public class IcebergValidateCommand implements Callable<Integer> {
    @ParentCommand
    BenchmarkRunnerApp parent;

    @Spec
    CommandSpec spec;

    @CommandLine.Option(names = "--config", defaultValue = "configs/iceberg-validation.yml")
    Path configPath;

    @CommandLine.Option(names = "--run-id")
    String runId;

    @CommandLine.Option(names = "--scenario")
    List<String> scenarios = new ArrayList<>();

    @CommandLine.Option(names = "--case")
    List<String> cases = new ArrayList<>();

    @CommandLine.Option(names = "--keep-artifacts")
    boolean keepArtifacts;

    @Override
    public Integer call() throws Exception {
        IcebergValidationConfig config = new IcebergValidationConfigLoader().load(configPath);
        IcebergValidationReport report = parent.runnerFactory()
            .runIcebergValidation(config, runId, scenarios, cases, keepArtifacts);
        Path reportPath = Path.of(config.report().output()).resolve(report.runId()).resolve("report.md");
        spec.commandLine().getOut().printf("cases=%d report=%s%n", report.results().size(), reportPath);
        return "SUCCESS".equals(report.status()) ? 0 : 1;
    }
}

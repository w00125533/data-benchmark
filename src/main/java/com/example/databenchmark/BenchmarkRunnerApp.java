package com.example.databenchmark;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.runner.ComposeBenchmarkRunner;
import com.example.databenchmark.runner.LocalBenchmarkRunner;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(
    name = "benchmark-runner",
    mixinStandardHelpOptions = true,
    version = "data-benchmark 0.1.0-SNAPSHOT",
    description = "Run the StarRocks and Iceberg benchmark workflow.",
    subcommands = {
        BenchmarkRunnerApp.GenerateCommand.class,
        BenchmarkRunnerApp.RunCommand.class,
        BenchmarkRunnerApp.ReportCommand.class
    })
public class BenchmarkRunnerApp implements Callable<Integer> {
    private final RunnerFactory runnerFactory;

    public BenchmarkRunnerApp() {
        this(new DefaultRunnerFactory());
    }

    BenchmarkRunnerApp(RunnerFactory runnerFactory) {
        this.runnerFactory = runnerFactory;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new BenchmarkRunnerApp()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return 0;
    }

    @Command(name = "generate", description = "Generate deterministic wireless KPI data skeleton.")
    static class GenerateCommand implements Callable<Integer> {
        @Spec
        CommandSpec spec;

        @CommandLine.Option(names = "--config", defaultValue = "configs/benchmark-smoke.yml")
        Path configPath;

        @CommandLine.Option(names = "--cells")
        Integer cells;

        @CommandLine.Option(names = "--days")
        Integer days;

        @CommandLine.Option(names = "--seed")
        Long seed;

        @CommandLine.Option(names = "--output")
        String output;

        @CommandLine.Option(names = "--row-cap")
        Long rowCap;

        @Override
        public Integer call() throws Exception {
            var config = new com.example.databenchmark.config.BenchmarkConfigLoader()
                .load(configPath)
                .withOverrides(cells, days, seed, output, rowCap);
            var result = new com.example.databenchmark.generator.KpiDataGenerator().generate(config);
            spec.commandLine().getOut().println("generate command is available");
            spec.commandLine().getOut().printf(
                "rows=%d bytes=%d output=%s%n",
                result.rows(),
                result.bytesWritten(),
                result.outputPath()
            );
            return 0;
        }
    }

    @Command(name = "run", description = "Run the local benchmark workflow.")
    static class RunCommand implements Callable<Integer> {
        @ParentCommand
        BenchmarkRunnerApp parent;

        @Spec
        CommandSpec spec;

        @CommandLine.Option(names = "--config", defaultValue = "configs/benchmark-smoke.yml")
        Path configPath;

        @CommandLine.Option(names = "--mode", defaultValue = "local", description = "Run mode: local or compose.")
        String mode;

        @CommandLine.Option(names = "--run-id")
        String runId;

        @Override
        public Integer call() throws Exception {
            var config = new com.example.databenchmark.config.BenchmarkConfigLoader().load(configPath);
            Path reportRoot = Path.of(config.report().output());
            CliRunResult result;
            if ("local".equals(mode)) {
                result = parent.runnerFactory.runLocal(config, reportRoot, runId);
            } else if ("compose".equals(mode)) {
                result = parent.runnerFactory.runCompose(config, reportRoot, runId);
            } else {
                throw new CommandLine.ParameterException(
                    spec.commandLine(),
                    "Unknown mode: " + mode + ". Valid values: local, compose."
                );
            }
            spec.commandLine().getOut().printf(
                "rows=%d report=%s%n",
                result.rows(),
                result.reportPath()
            );
            return result.success() ? 0 : 1;
        }
    }

    @Command(name = "report", description = "Generate an HTML report from benchmark results.")
    static class ReportCommand implements Callable<Integer> {
        @Spec
        CommandSpec spec;

        @Override
        public Integer call() {
            spec.commandLine().getOut().println("report command is available");
            return 0;
        }
    }

    interface RunnerFactory {
        CliRunResult runLocal(BenchmarkConfig config, Path reportRoot, String runId) throws Exception;

        CliRunResult runCompose(BenchmarkConfig config, Path reportRoot, String runId) throws Exception;
    }

    record CliRunResult(long rows, Path reportPath, boolean success) {}

    private static final class DefaultRunnerFactory implements RunnerFactory {
        @Override
        public CliRunResult runLocal(BenchmarkConfig config, Path reportRoot, String runId) throws Exception {
            LocalBenchmarkRunner.LocalRunResult result = new LocalBenchmarkRunner().run(config, reportRoot, runId);
            return new CliRunResult(result.dataset().rows(), result.reportPath(), true);
        }

        @Override
        public CliRunResult runCompose(BenchmarkConfig config, Path reportRoot, String runId) throws Exception {
            ComposeBenchmarkRunner.ComposeRunResult result = new ComposeBenchmarkRunner().run(config, reportRoot, runId);
            long rows = result.dataset() == null ? 0L : result.dataset().rows();
            return new CliRunResult(rows, result.reportPath(), result.success());
        }
    }
}

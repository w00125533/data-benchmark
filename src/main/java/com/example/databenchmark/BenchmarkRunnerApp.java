package com.example.databenchmark;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
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
        @Spec
        CommandSpec spec;

        @Override
        public Integer call() {
            spec.commandLine().getOut().println("run command is available");
            return 0;
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
}

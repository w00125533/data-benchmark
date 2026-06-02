package com.example.databenchmark;

import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "benchmark-runner",
    mixinStandardHelpOptions = true,
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

    @Command(name = "generate", description = "Generate wireless KPI Parquet data.")
    static class GenerateCommand implements Callable<Integer> {
        @Override
        public Integer call() {
            System.out.println("generate command is available");
            return 0;
        }
    }

    @Command(name = "run", description = "Run the local benchmark workflow.")
    static class RunCommand implements Callable<Integer> {
        @Override
        public Integer call() {
            System.out.println("run command is available");
            return 0;
        }
    }

    @Command(name = "report", description = "Generate an HTML report from benchmark results.")
    static class ReportCommand implements Callable<Integer> {
        @Override
        public Integer call() {
            System.out.println("report command is available");
            return 0;
        }
    }
}

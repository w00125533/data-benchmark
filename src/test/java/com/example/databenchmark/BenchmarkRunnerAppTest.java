package com.example.databenchmark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class BenchmarkRunnerAppTest {
    @Test
    void helpListsCoreCommands() {
        CommandLine commandLine = new CommandLine(new BenchmarkRunnerApp());

        String usage = commandLine.getUsageMessage();

        assertThat(usage).contains("generate");
        assertThat(usage).contains("run");
        assertThat(usage).contains("report");
    }
}

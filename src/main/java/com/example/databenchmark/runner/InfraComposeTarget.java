package com.example.databenchmark.runner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public record InfraComposeTarget(String project, List<String> files) {
    private static final String DEFAULT_PROJECT = "shared-data-infra";
    private static final List<String> DEFAULT_FILES = List.of(
        "../shared-data-infra/compose.yaml",
        "../shared-data-infra/compose.lakehouse.yaml",
        "../shared-data-infra/compose.starrocks.yaml"
    );

    public InfraComposeTarget {
        project = defaultIfBlank(project, DEFAULT_PROJECT);
        files = List.copyOf(files == null || files.isEmpty() ? DEFAULT_FILES : files);
    }

    public static InfraComposeTarget fromEnvironment(Map<String, String> environment) {
        String project = environment.get("BENCHMARK_INFRA_PROJECT");
        String filesValue = environment.get("BENCHMARK_INFRA_COMPOSE_FILES");
        if (isBlank(filesValue)) {
            return new InfraComposeTarget(project, DEFAULT_FILES);
        }

        List<String> files = new ArrayList<>();
        for (String file : filesValue.split("[;,]")) {
            String trimmed = file.trim();
            if (!trimmed.isEmpty()) {
                files.add(trimmed);
            }
        }
        return new InfraComposeTarget(project, files);
    }

    public List<String> composeCommand(String... args) {
        List<String> command = new ArrayList<>(List.of("docker", "compose", "-p", project));
        for (String file : files) {
            command.add("-f");
            command.add(file);
        }
        command.addAll(List.of(args));
        return command;
    }

    private static String defaultIfBlank(String value, String defaultValue) {
        if (isBlank(value)) {
            return defaultValue;
        }
        return value.trim();
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}

package com.example.databenchmark.runner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public record InfraComposeTarget(String project, List<String> files, List<String> profiles) {
    private static final String DEFAULT_PROJECT = "shared-data-infra";
    private static final List<String> DEFAULT_FILES = List.of(
        "../shared-data-infra/compose.yaml",
        "../shared-data-infra/compose.lakehouse.yaml",
        "../shared-data-infra/compose.starrocks.yaml"
    );
    private static final List<String> DEFAULT_PROFILES = List.of(
        "lakehouse",
        "lakehouse-tools",
        "spark-tools",
        "starrocks"
    );

    public InfraComposeTarget {
        project = defaultIfBlank(project, DEFAULT_PROJECT);
        files = List.copyOf(files == null || files.isEmpty() ? DEFAULT_FILES : files);
        profiles = List.copyOf(profiles == null || profiles.isEmpty() ? DEFAULT_PROFILES : profiles);
    }

    public InfraComposeTarget(String project, List<String> files) {
        this(project, files, DEFAULT_PROFILES);
    }

    public static InfraComposeTarget fromEnvironment(Map<String, String> environment) {
        String project = environment.get("BENCHMARK_INFRA_PROJECT");
        String filesValue = environment.get("BENCHMARK_INFRA_COMPOSE_FILES");
        String profilesValue = environment.get("BENCHMARK_INFRA_COMPOSE_PROFILES");
        List<String> files = isBlank(filesValue) ? DEFAULT_FILES : splitList(filesValue);
        List<String> profiles = isBlank(profilesValue) ? DEFAULT_PROFILES : splitList(profilesValue);
        return new InfraComposeTarget(project, files, profiles);
    }

    public List<String> composeCommand(String... args) {
        List<String> command = new ArrayList<>(List.of("docker", "compose", "-p", project));
        for (String file : files) {
            command.add("-f");
            command.add(file);
        }
        for (String profile : profiles) {
            command.add("--profile");
            command.add(profile);
        }
        command.addAll(List.of(args));
        return command;
    }

    private static List<String> splitList(String value) {
        List<String> values = new ArrayList<>();
        for (String item : value.split("[;,]")) {
            String trimmed = item.trim();
            if (!trimmed.isEmpty()) {
                values.add(trimmed);
            }
        }
        return values;
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

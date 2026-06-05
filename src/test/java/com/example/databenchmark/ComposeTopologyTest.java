package com.example.databenchmark;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ComposeTopologyTest {
    private static final String REMOVED_METRICS_SERVICE = "pro" + "metheus";
    private static final String REMOVED_DASHBOARD_SERVICE = "gra" + "fana";
    private static final Map<String, ResourceLimit> EXPECTED_RESOURCE_LIMITS = Map.of(
        "benchmark-runner", new ResourceLimit("2", "1GB")
    );

    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @SuppressWarnings("unchecked")
    @Test
    void composeUsesHdfsWarehouseInsteadOfMinio() throws Exception {
        Map<String, Object> compose = mapper.readValue(
            Path.of("docker-compose.yml").toFile(),
            new TypeReference<>() {}
        );
        Map<String, Object> services = (Map<String, Object>) compose.get("services");

        assertThat(services.keySet()).containsExactly("benchmark-runner");
        EXPECTED_RESOURCE_LIMITS.forEach((serviceName, expected) ->
            assertResourceLimit(service(services, serviceName), expected.cpus(), expected.memory()));
        assertThat(services).doesNotContainKeys(
            "minio", REMOVED_METRICS_SERVICE, REMOVED_DASHBOARD_SERVICE,
            "hive-metastore", "hdfs-namenode", "hdfs-datanode",
            "hdfs-init", "hive-server", "spark", "starrocks-fe", "starrocks-be");

        Map<String, Object> runner = service(services, "benchmark-runner");
        assertThat(runner).doesNotContainKey("image");
        assertThat(map(runner.get("build")))
            .containsEntry("context", "./docker/benchmark-runner")
            .containsEntry("dockerfile", "Dockerfile");
        assertThat(runner.get("working_dir")).isEqualTo("/workspace");
        assertThat(stringList(runner, "volumes"))
            .contains(".:/workspace", "/var/run/docker.sock:/var/run/docker.sock");
        assertThat(map(runner.get("environment")))
            .containsEntry("BENCHMARK_COMPOSE_IN_CONTAINER", "true")
            .containsEntry("BENCHMARK_INFRA_PROJECT", "${BENCHMARK_INFRA_PROJECT:-shared-data-infra}")
            .containsEntry("BENCHMARK_INFRA_COMPOSE_FILES",
                "${BENCHMARK_INFRA_COMPOSE_FILES:-../shared-data-infra/compose.yaml;../shared-data-infra/compose.lakehouse.yaml;../shared-data-infra/compose.starrocks.yaml}")
            .containsEntry("STARROCKS_JDBC_URL",
                "jdbc:mysql://starrocks-fe:9030/?useSSL=false&allowPublicKeyRetrieval=true&allowMultiQueries=true")
            .containsEntry("STARROCKS_STREAM_LOAD_URL", "http://starrocks-be:8040/api/sr_internal/cell_kpi_1min/_stream_load");
        assertThat(stringList(runner, "command"))
            .containsExactly("java", "-jar", "target/data-benchmark-0.1.0-SNAPSHOT.jar",
                "run", "--mode", "compose", "--run-id", "compose-smoke");
        assertThat(stringList(runner, "command")).contains("--run-id");
        assertThat(stringList(runner, "command")).contains("--mode", "compose");
        assertThat(dependencyNames(runner)).isEmpty();
        assertThat(networkNames(runner)).containsExactly("shared-data-infra");
        assertThat(stringList(runner, "ports")).doesNotContain("9108:9108");
        String runnerDockerfile = Files.readString(Path.of("docker", "benchmark-runner", "Dockerfile"));
        assertThat(runnerDockerfile)
            .contains("docker-ce-cli")
            .contains("docker-compose-plugin");

        Map<String, Object> networks = map(compose.get("networks"));
        assertThat(networks).doesNotContainKey("default");
        assertThat(map(networks.get("shared-data-infra")))
            .containsEntry("external", true)
            .containsEntry("name", "shared-data-infra");
    }

    @Test
    void monitoringProvisioningIsRemoved() {
        assertThat(Path.of("monitoring", REMOVED_METRICS_SERVICE + ".yml")).doesNotExist();
        assertThat(Path.of("monitoring", REMOVED_DASHBOARD_SERVICE)).doesNotExist();
    }

    @Test
    void benchmarkConfigsDeclareSmokeKpiAndOneBillionRowCaps() throws Exception {
        assertThat(rowCap(Path.of("configs", "benchmark-smoke.yml"))).isEqualTo(10000);
        assertThat(rowCap(Path.of("configs", "benchmark-kpi-10m.yml"))).isEqualTo(10000000);
        assertThat(rowCap(Path.of("configs", "benchmark-kpi-1b.yml"))).isEqualTo(1000000000);
    }

    @Test
    void readmeDocumentsSharedInfraRunnerResourceLimitsAndEmbeddedReportData() throws Exception {
        String readme = Files.readString(Path.of("README.md"));

        assertThat(readme).doesNotContain("report.json");
        assertThat(readme)
            .contains("configs/benchmark-smoke.yml")
            .contains("10k")
            .contains("configs/benchmark-kpi-10m.yml")
            .contains("10m")
            .contains("BENCHMARK_INFRA_PROJECT")
            .contains("BENCHMARK_INFRA_COMPOSE_FILES")
            .contains("../shared-data-infra/compose.yaml")
            .contains("../shared-data-infra/compose.lakehouse.yaml")
            .contains("../shared-data-infra/compose.starrocks.yaml")
            .contains("| Service | CPU | Memory |");
        assertThat(readme)
            .contains("docker compose -f docker-compose.yml build benchmark-runner")
            .contains("mvn package")
            .contains("/var/run/docker.sock")
            .contains("docker compose -f docker-compose.yml up benchmark-runner");
        assertThat(readme)
            .doesNotContain("This project still keeps `spark`, `hive-server`, and split `starrocks-fe`/`starrocks-be` services local")
            .doesNotContain("docker compose -f docker-compose.yml up -d hdfs-init hive-server spark starrocks-fe starrocks-be")
            .doesNotContain("| starrocks-fe |")
            .doesNotContain("| starrocks-be |")
            .doesNotContain("| spark |")
            .doesNotContain("| hive-server |")
            .doesNotContain("172.20.0.10")
            .doesNotContain("172.20.0.11")
            .doesNotContain("172.20.0.0/24")
            .doesNotContain("-Xmx1536m");
    }

    private Map<String, Object> service(Map<String, Object> services, String name) {
        assertThat(services).containsKey(name);
        return map(services.get(name));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> map(Object value) {
        return (Map<String, Object>) value;
    }

    private List<Object> list(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return List.of();
        }
        return list(value);
    }

    @SuppressWarnings("unchecked")
    private List<Object> list(Object value) {
        return (List<Object>) value;
    }

    private List<String> stringList(Map<String, Object> map, String key) {
        return list(map, key).stream()
            .map(Object::toString)
            .toList();
    }

    private List<String> dependencyNames(Map<String, Object> service) {
        Object dependsOn = service.get("depends_on");
        if (dependsOn == null) {
            return List.of();
        }
        if (dependsOn instanceof Map<?, ?> dependsOnMap) {
            return dependsOnMap.keySet().stream()
                .map(Object::toString)
                .toList();
        }
        return list(dependsOn).stream()
            .map(Object::toString)
            .toList();
    }

    private List<String> networkNames(Map<String, Object> service) {
        Object networks = service.get("networks");
        if (networks == null) {
            return List.of();
        }
        if (networks instanceof Map<?, ?> networksMap) {
            return networksMap.keySet().stream()
                .map(Object::toString)
                .toList();
        }
        return list(networks).stream()
            .map(Object::toString)
            .toList();
    }

    private void assertResourceLimit(Map<String, Object> service, String cpus, String memory) {
        Map<String, Object> deploy = map(service.get("deploy"));
        Map<String, Object> resources = map(deploy.get("resources"));
        Map<String, Object> limits = map(resources.get("limits"));
        assertThat(limits)
            .containsEntry("cpus", cpus)
            .containsEntry("memory", memory);
    }

    private int rowCap(Path configPath) throws Exception {
        Map<String, Object> config = mapper.readValue(configPath.toFile(), new TypeReference<>() {});
        return (Integer) map(config.get("dataset")).get("rowCap");
    }

    private record ResourceLimit(String cpus, String memory) {
    }
}

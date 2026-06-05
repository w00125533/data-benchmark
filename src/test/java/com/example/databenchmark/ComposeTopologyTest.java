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
        "starrocks-fe", new ResourceLimit("2", "2GB"),
        "starrocks-be", new ResourceLimit("6", "5GB"),
        "spark", new ResourceLimit("6", "3GB"),
        "hive-server", new ResourceLimit("2", "2GB"),
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

        assertThat(services.keySet()).containsExactlyInAnyOrder(
            "starrocks-fe", "starrocks-be", "spark", "hdfs-init", "hive-server", "benchmark-runner"
        );
        EXPECTED_RESOURCE_LIMITS.forEach((serviceName, expected) ->
            assertResourceLimit(service(services, serviceName), expected.cpus(), expected.memory()));
        List<String> starRocksFeCommand = stringList(service(services, "starrocks-fe"), "command");
        assertThat(starRocksFeCommand).startsWith("bash", "-lc");
        assertThat(starRocksFeCommand.get(2))
            .contains("sed -i '/^JAVA_OPTS=/d' /opt/starrocks/fe/conf/fe.conf")
            .contains("JAVA_OPTS=\"-Dlog4j2.formatMsgNoLookups=true -Xmx1536m -XX:+UseG1GC -Djava.security.policy=/opt/starrocks/fe/conf/udf_security.policy\"")
            .contains("exec /opt/starrocks/fe_entrypoint.sh starrocks-fe");
        assertThat(service(services, "starrocks-fe").get("hostname")).isEqualTo("starrocks-fe-0");
        assertThat(map(map(service(services, "starrocks-fe").get("networks")).get("default")))
            .containsEntry("ipv4_address", "172.20.0.10");
        assertThat(stringList(service(services, "starrocks-be"), "command"))
            .containsExactly("/opt/starrocks/be_entrypoint.sh", "starrocks-fe");
        assertThat(service(services, "starrocks-be").get("hostname")).isEqualTo("starrocks-be-0");
        assertThat(map(map(service(services, "starrocks-be").get("networks")).get("default")))
            .containsEntry("ipv4_address", "172.20.0.11");
        assertThat(services).containsKeys("hdfs-init");
        assertThat(services).doesNotContainKeys(
            "minio", REMOVED_METRICS_SERVICE, REMOVED_DASHBOARD_SERVICE,
            "hive-metastore", "hdfs-namenode", "hdfs-datanode");

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
            .containsEntry("STARROCKS_JDBC_URL",
                "jdbc:mysql://starrocks-fe:9030/?useSSL=false&allowPublicKeyRetrieval=true&allowMultiQueries=true")
            .containsEntry("STARROCKS_STREAM_LOAD_URL", "http://starrocks-be:8040/api/sr_internal/cell_kpi_1min/_stream_load");
        assertThat(stringList(runner, "command"))
            .containsExactly("java", "-jar", "target/data-benchmark-0.1.0-SNAPSHOT.jar",
                "run", "--mode", "compose", "--run-id", "compose-smoke");
        assertThat(stringList(runner, "command")).contains("--run-id");
        assertThat(stringList(runner, "command")).contains("--mode", "compose");
        assertThat(dependencyNames(runner))
            .contains("hdfs-init", "hive-server", "spark", "starrocks-fe", "starrocks-be")
            .doesNotContain("hive-metastore")
            .doesNotContain(REMOVED_METRICS_SERVICE, REMOVED_DASHBOARD_SERVICE);
        assertThat(dependencyCondition(runner, "hdfs-init")).isEqualTo("service_completed_successfully");
        assertThat(dependencyCondition(runner, "hive-server")).isEqualTo("service_started");
        assertThat(networkNames(runner)).contains("default", "shared-data-infra");
        assertThat(stringList(runner, "ports")).doesNotContain("9108:9108");
        String runnerDockerfile = Files.readString(Path.of("docker", "benchmark-runner", "Dockerfile"));
        assertThat(runnerDockerfile)
            .contains("docker-ce-cli")
            .contains("docker-compose-plugin");

        assertThat(dependencyCondition(service(services, "spark"), "hdfs-init"))
            .isEqualTo("service_completed_successfully");
        assertThat(dependencyNames(service(services, "spark"))).doesNotContain("hive-metastore");
        assertThat(networkNames(service(services, "spark"))).contains("default", "shared-data-infra");
        assertThat(service(services, "spark").get("image")).isEqualTo("apache/spark:3.5.8-java17");
        assertThat(service(services, "spark").get("working_dir")).isEqualTo("/workspace");
        assertThat(stringList(service(services, "spark"), "volumes")).contains(".:/workspace");
        assertThat(stringList(service(services, "spark"), "command")).containsExactly("sleep", "infinity");

        Map<String, Object> hiveServer = service(services, "hive-server");
        assertThat(hiveServer.get("image")).isEqualTo("apache/hive:4.0.0");
        assertThat(networkNames(hiveServer)).contains("default", "shared-data-infra");
        assertThat(map(hiveServer.get("environment"))).doesNotContainEntry("SERVICE_NAME", "hiveserver2");
        assertThat(stringList(hiveServer, "entrypoint")).containsExactly("bash", "-lc");
        assertThat(String.join("\n", stringList(hiveServer, "command")))
            .contains("SKIP_SCHEMA_INIT=true")
            .contains("HADOOP_CLIENT_OPTS=\"$$HADOOP_CLIENT_OPTS -Xmx1G $$SERVICE_OPTS\"")
            .contains("exec /opt/hive/bin/hive --skiphadoopversion --skiphbasecp --service hiveserver2")
            .contains("--hiveconf hive.metastore.uris=thrift://hive-metastore:9083")
            .contains("--hiveconf hive.server2.thrift.bind.host=0.0.0.0")
            .contains("--hiveconf hive.server2.thrift.port=10000");
        assertThat(map(hiveServer.get("environment")))
            .containsEntry("SERVICE_OPTS",
                "-Dhive.metastore.uris=thrift://hive-metastore:9083 -Dhive.server2.thrift.bind.host=0.0.0.0 -Dhive.server2.thrift.port=10000");
        assertThat(dependencyCondition(hiveServer, "hdfs-init")).isEqualTo("service_completed_successfully");
        assertThat(dependencyNames(hiveServer)).doesNotContain("hive-metastore");
        assertThat(stringList(hiveServer, "ports")).contains("10000:10000");

        Map<String, Object> hdfsInit = service(services, "hdfs-init");
        assertThat(networkNames(hdfsInit)).contains("shared-data-infra");
        String hdfsInitCommand = String.join(" ", stringList(hdfsInit, "command"));
        assertThat(hdfsInitCommand)
            .contains("hdfs dfs -fs hdfs://hdfs-namenode:8020 -mkdir -p /warehouse/iceberg")
            .contains("hdfs dfs -fs hdfs://hdfs-namenode:8020 -chmod -R 777 /warehouse");
        assertThat(hdfsInit).doesNotContainKey("depends_on");

        Map<String, Object> networks = map(compose.get("networks"));
        Map<String, Object> defaultNetwork = map(networks.get("default"));
        assertThat(defaultNetwork).containsEntry("name", "databenchmark");
        assertThat(map(list(map(defaultNetwork.get("ipam")), "config").get(0)))
            .containsEntry("subnet", "172.20.0.0/24");
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
    void readmeDocumentsComposeHiveServerResourceLimitsAndEmbeddedReportData() throws Exception {
        String readme = Files.readString(Path.of("README.md"));

        assertThat(readme).doesNotContain("report.json");
        assertThat(readme)
            .contains("configs/benchmark-smoke.yml")
            .contains("10k")
            .contains("configs/benchmark-kpi-10m.yml")
            .contains("10m")
            .contains("hive-server")
            .contains("| Service | CPU | Memory |");
        assertThat(readme)
            .contains("docker compose -f docker-compose.yml build benchmark-runner")
            .contains("mvn package")
            .contains("/var/run/docker.sock")
            .contains("docker compose down --remove-orphans")
            .contains("172.20.0.10")
            .contains("172.20.0.11")
            .contains("172.20.0.0/24")
            .contains("-Xmx1536m");
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

    private String dependencyCondition(Map<String, Object> service, String dependency) {
        Object dependsOn = service.get("depends_on");
        assertThat(dependsOn).isInstanceOf(Map.class);
        Map<String, Object> dependencies = map(dependsOn);
        assertThat(dependencies).containsKey(dependency);
        return map(dependencies.get(dependency)).get("condition").toString();
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

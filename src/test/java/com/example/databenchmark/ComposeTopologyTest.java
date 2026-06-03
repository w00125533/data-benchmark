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
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    private final ObjectMapper jsonMapper = new ObjectMapper();

    @SuppressWarnings("unchecked")
    @Test
    void composeUsesHdfsWarehouseInsteadOfMinio() throws Exception {
        Map<String, Object> compose = mapper.readValue(
            Path.of("docker-compose.yml").toFile(),
            new TypeReference<>() {}
        );
        Map<String, Object> services = (Map<String, Object>) compose.get("services");

        assertThat(services.keySet()).containsExactlyInAnyOrder(
            "starrocks-fe", "starrocks-be", "spark", "hive-metastore", "hdfs-namenode",
            "hdfs-datanode", "hdfs-init", "prometheus", "grafana", "benchmark-runner"
        );
        assertThat(stringList(service(services, "starrocks-fe"), "command"))
            .containsExactly("/opt/starrocks/fe_entrypoint.sh", "starrocks-fe");
        assertThat(service(services, "starrocks-fe").get("hostname")).isEqualTo("starrocks-fe-0");
        assertThat(stringList(service(services, "starrocks-be"), "command"))
            .containsExactly("/opt/starrocks/be_entrypoint.sh", "starrocks-fe");
        assertThat(service(services, "starrocks-be").get("hostname")).isEqualTo("starrocks-be-0");
        assertThat(services).containsKeys("hdfs-namenode", "hdfs-datanode", "hdfs-init");
        assertThat(services).doesNotContainKey("minio");

        Map<String, Object> runner = service(services, "benchmark-runner");
        assertThat(runner.get("image").toString()).isEqualTo("apache/spark:3.5.8-java17");
        assertThat(runner.get("working_dir")).isEqualTo("/workspace");
        assertThat(stringList(runner, "volumes")).contains(".:/workspace");
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
            .contains("hdfs-init", "hive-metastore", "spark", "starrocks-fe", "starrocks-be", "prometheus");
        assertThat(dependencyCondition(runner, "hdfs-init")).isEqualTo("service_completed_successfully");
        assertThat(stringList(runner, "ports")).doesNotContain("9108:9108");

        assertThat(dependencyCondition(service(services, "spark"), "hdfs-init"))
            .isEqualTo("service_completed_successfully");
        assertThat(service(services, "spark").get("image")).isEqualTo("apache/spark:3.5.8-java17");
        assertThat(service(services, "spark").get("working_dir")).isEqualTo("/workspace");
        assertThat(stringList(service(services, "spark"), "volumes")).contains(".:/workspace");
        assertThat(stringList(service(services, "spark"), "command")).containsExactly("sleep", "infinity");
        assertThat(dependencyCondition(service(services, "hive-metastore"), "hdfs-init"))
            .isEqualTo("service_completed_successfully");
        assertThat(service(services, "hive-metastore").get("hostname")).isEqualTo("hive-metastore");

        Map<String, Object> hdfsNamenode = service(services, "hdfs-namenode");
        assertThat(stringList(hdfsNamenode, "ports")).contains("9870:9870", "8020:8020");
        assertThat(map(hdfsNamenode.get("environment")))
            .containsEntry("CORE-SITE.XML_fs.defaultFS", "hdfs://hdfs-namenode:8020")
            .containsEntry("HDFS-SITE.XML_dfs.replication", "1")
            .containsEntry("HDFS-SITE.XML_dfs.namenode.rpc-address", "hdfs-namenode:8020");

        Map<String, Object> hdfsDatanode = service(services, "hdfs-datanode");
        assertThat(stringList(hdfsDatanode, "ports")).contains("9864:9864");
        assertThat(map(hdfsDatanode.get("environment")))
            .containsEntry("CORE-SITE.XML_fs.defaultFS", "hdfs://hdfs-namenode:8020")
            .containsEntry("HDFS-SITE.XML_dfs.replication", "1");

        Map<String, Object> hdfsInit = service(services, "hdfs-init");
        String hdfsInitCommand = String.join(" ", stringList(hdfsInit, "command"));
        assertThat(hdfsInitCommand)
            .contains("hdfs dfs -fs hdfs://hdfs-namenode:8020 -mkdir -p /warehouse/iceberg")
            .contains("hdfs dfs -fs hdfs://hdfs-namenode:8020 -chmod -R 777 /warehouse");
        assertThat(dependencyNames(hdfsInit)).contains("hdfs-namenode", "hdfs-datanode");

        Map<String, Object> prometheus = service(services, "prometheus");
        assertThat(stringList(prometheus, "volumes"))
            .contains("./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro");
        assertThat(stringList(prometheus, "ports")).contains("9090:9090");

        Map<String, Object> grafana = service(services, "grafana");
        assertThat(stringList(grafana, "ports")).contains("3000:3000");
        assertThat(stringList(grafana, "volumes"))
            .contains("./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro")
            .contains("./monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro");

        assertThat(map(map(compose.get("networks")).get("default"))).containsEntry("name", "databenchmark");
    }

    @SuppressWarnings("unchecked")
    @Test
    void prometheusScrapesBenchmarkStarRocksAndHdfsTargets() throws Exception {
        Map<String, Object> prometheus = mapper.readValue(
            Path.of("monitoring/prometheus.yml").toFile(),
            new TypeReference<>() {}
        );
        List<Map<String, Object>> scrapeConfigs = (List<Map<String, Object>>) prometheus.get("scrape_configs");

        assertThat(scrapeConfigs)
            .extracting(config -> config.get("job_name"))
            .contains("benchmark-runner", "starrocks-fe", "starrocks-be", "hdfs-namenode", "hdfs-datanode")
            .doesNotContain("minio");

        String prometheusText = prometheus.toString();
        assertThat(prometheusText).contains("benchmark-runner:9108");
        assertThat(prometheusText).contains("hdfs-namenode:9870");
        assertThat(prometheusText).contains("hdfs-datanode:9864");
        assertThat(prometheusText).doesNotContain("minio");

        assertThat(targets(scrapeJob(scrapeConfigs, "benchmark-runner"))).containsExactly("benchmark-runner:9108");

        assertThat(targets(scrapeJob(scrapeConfigs, "starrocks-fe"))).containsExactly("starrocks-fe:8030");
        assertThat(targets(scrapeJob(scrapeConfigs, "starrocks-be"))).containsExactly("starrocks-be:8040");

        Map<String, Object> hdfsNamenode = scrapeJob(scrapeConfigs, "hdfs-namenode");
        assertThat(hdfsNamenode).containsEntry("metrics_path", "/jmx");
        assertThat(targets(hdfsNamenode)).containsExactly("hdfs-namenode:9870");

        Map<String, Object> hdfsDatanode = scrapeJob(scrapeConfigs, "hdfs-datanode");
        assertThat(hdfsDatanode).containsEntry("metrics_path", "/jmx");
        assertThat(targets(hdfsDatanode)).containsExactly("hdfs-datanode:9864");
    }

    @Test
    @SuppressWarnings("unchecked")
    void grafanaDashboardProvisioningFilesExist() throws Exception {
        Path datasourcePath = Path.of("monitoring/grafana/provisioning/datasources/prometheus.yml");
        Path dashboardProviderPath = Path.of("monitoring/grafana/provisioning/dashboards/benchmark.yml");
        Path dashboardPath = Path.of("monitoring/grafana/dashboards/benchmark.json");

        assertThat(datasourcePath).exists();
        assertThat(dashboardProviderPath).exists();
        assertThat(dashboardPath).exists();

        assertThat(Files.readString(datasourcePath))
            .contains("uid: prometheus")
            .contains("type: prometheus")
            .contains("url: http://prometheus:9090");

        assertThat(Files.readString(dashboardProviderPath))
            .contains("name: data-benchmark")
            .contains("path: /var/lib/grafana/dashboards");

        String dashboardText = Files.readString(dashboardPath);
        Map<String, Object> dashboard = jsonMapper.readValue(dashboardText, new TypeReference<>() {});
        assertThat(dashboard)
            .containsEntry("uid", "benchmark")
            .containsEntry("title", "Data Benchmark");

        Map<String, Object> templating = map(dashboard.get("templating"));
        List<Map<String, Object>> variables = list(templating, "list").stream()
            .map(this::map)
            .toList();
        assertThat(variables)
            .extracting(variable -> variable.get("name"))
            .contains("run_id", "suite", "query_set");
        assertThat(variables)
            .anySatisfy(variable -> assertThat(variable)
                .containsEntry("name", "run_id")
                .containsEntry("type", "textbox"))
            .anySatisfy(variable -> assertThat(variable)
                .containsEntry("name", "suite")
                .containsEntry("query", "label_values(benchmark_query_duration_seconds, suite)"))
            .anySatisfy(variable -> assertThat(variable)
                .containsEntry("name", "query_set")
                .containsEntry("query", "label_values(benchmark_query_duration_seconds{suite=\"$suite\"}, query_set)"));

        assertThat(list(dashboard, "panels"))
            .extracting(panel -> map(panel).get("title"))
            .contains("Load Rows", "Load Duration Seconds", "Query Duration Seconds",
                "Query Failures", "Query Rows", "Run Metadata");

        assertThat(dashboardText).contains("\"uid\": \"prometheus\"");
        assertThat(list(dashboard, "panels"))
            .filteredOn(panel -> map(panel).containsKey("datasource"))
            .allSatisfy(panel -> assertThat(map(map(panel).get("datasource"))).containsEntry("uid", "prometheus"));
        assertThat(dashboardText)
            .contains("label_values(benchmark_query_duration_seconds, suite)")
            .contains("label_values(benchmark_query_duration_seconds{suite=\\\"$suite\\\"}, query_set)");
        assertThat(dashboardText)
            .contains("benchmark_query_duration_seconds{run_id=\\\"$run_id\\\", suite=\\\"$suite\\\", query_set=\\\"$query_set\\\"}");
    }

    private Map<String, Object> service(Map<String, Object> services, String name) {
        assertThat(services).containsKey(name);
        return map(services.get(name));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> scrapeJob(List<Map<String, Object>> scrapeConfigs, String jobName) {
        return scrapeConfigs.stream()
            .filter(config -> jobName.equals(config.get("job_name")))
            .findFirst()
            .map(config -> (Map<String, Object>) config)
            .orElseThrow(() -> new AssertionError("Missing Prometheus scrape job: " + jobName));
    }

    private List<String> targets(Map<String, Object> scrapeJob) {
        Map<String, Object> staticConfig = map(list(scrapeJob, "static_configs").get(0));
        return stringList(staticConfig, "targets");
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
}

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
    void composeDefinesPlaceholderTopology() throws Exception {
        Map<String, Object> compose = mapper.readValue(
            Path.of("docker-compose.yml").toFile(),
            new TypeReference<>() {}
        );
        Map<String, Object> services = (Map<String, Object>) compose.get("services");

        assertThat(services.keySet()).containsExactlyInAnyOrder(
            "starrocks-fe", "starrocks-be", "spark", "hive-metastore",
            "minio", "prometheus", "grafana", "benchmark-runner"
        );

        Map<String, Object> runner = service(services, "benchmark-runner");
        assertThat(runner.get("image").toString()).containsAnyOf("temurin", "java").contains("17");
        assertThat(runner.get("working_dir")).isEqualTo("/workspace");
        assertThat(stringList(runner, "volumes")).contains(".:/workspace");
        assertThat(stringList(runner, "command"))
            .contains("target/data-benchmark-0.1.0-SNAPSHOT.jar", "compose-smoke");
        assertThat(stringList(runner, "ports")).doesNotContain("9108:9108");

        Map<String, Object> prometheus = service(services, "prometheus");
        assertThat(stringList(prometheus, "volumes"))
            .contains("./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro");
        assertThat(stringList(prometheus, "ports")).contains("9090:9090");

        Map<String, Object> grafana = service(services, "grafana");
        assertThat(stringList(grafana, "ports")).contains("3000:3000");
        assertThat(stringList(grafana, "volumes"))
            .contains("./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro")
            .contains("./monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro");

        Map<String, Object> minio = service(services, "minio");
        assertThat(stringList(minio, "ports")).contains("9000:9000", "9001:9001");
        Map<String, Object> minioEnvironment = map(minio.get("environment"));
        assertThat(minioEnvironment)
            .containsEntry("MINIO_ROOT_USER", "minioadmin")
            .containsEntry("MINIO_ROOT_PASSWORD", "minioadmin")
            .containsEntry("MINIO_PROMETHEUS_AUTH_TYPE", "public");
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
        assertThat(list(templating, "list"))
            .anySatisfy(variable -> assertThat((Map<String, Object>) variable)
                .containsEntry("name", "run_id")
                .containsEntry("type", "textbox"));

        assertThat(list(dashboard, "panels"))
            .extracting(panel -> map(panel).get("title"))
            .contains("Load Rows", "Load Duration Seconds", "Query Duration Seconds",
                "Query Failures", "Query Rows", "Run Metadata");

        assertThat(dashboardText).contains("\"uid\": \"prometheus\"");
        assertThat(list(dashboard, "panels"))
            .filteredOn(panel -> map(panel).containsKey("datasource"))
            .allSatisfy(panel -> assertThat(map(map(panel).get("datasource"))).containsEntry("uid", "prometheus"));
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
}

package com.example.databenchmark;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ComposeTopologyTest {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @SuppressWarnings("unchecked")
    @Test
    void composeDefinesRequiredServices() throws Exception {
        Map<String, Object> compose = mapper.readValue(
            Path.of("docker-compose.yml").toFile(),
            new TypeReference<>() {}
        );
        Map<String, Object> services = (Map<String, Object>) compose.get("services");

        assertThat(services.keySet()).contains(
            "starrocks-fe", "starrocks-be", "spark", "hive-metastore",
            "minio", "prometheus", "grafana", "benchmark-runner"
        );
    }

    @Test
    void prometheusScrapesBenchmarkRunner() throws Exception {
        Map<?, ?> prometheus = mapper.readValue(Path.of("monitoring/prometheus.yml").toFile(), Map.class);

        assertThat(prometheus.toString()).contains("benchmark-runner");
        assertThat(prometheus.toString()).contains("benchmark-runner:9108");
    }
}

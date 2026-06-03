package com.example.databenchmark.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;

class PrometheusMetricsServerTest {
    @Test
    void exposesMetricsEndpoint() throws Exception {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.counter("benchmark_test_total").increment(3.0);

        try (PrometheusMetricsServer server = new PrometheusMetricsServer(registry, 0)) {
            server.start();

            HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + "/metrics")).GET().build(),
                HttpResponse.BodyHandlers.ofString()
            );

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).contains("benchmark_test_total");
        }
    }
}

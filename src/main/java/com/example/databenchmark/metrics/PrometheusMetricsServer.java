package com.example.databenchmark.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class PrometheusMetricsServer implements AutoCloseable {
    public static final int DEFAULT_PORT = 9108;

    private final PrometheusMeterRegistry registry;
    private final HttpServer server;
    private final ExecutorService executor;

    public PrometheusMetricsServer(PrometheusMeterRegistry registry) {
        this(registry, DEFAULT_PORT);
    }

    public PrometheusMetricsServer(PrometheusMeterRegistry registry, int port) {
        this.registry = registry;
        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to bind Prometheus metrics server on port " + port, e);
        }
        this.executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "benchmark-metrics-http");
            thread.setDaemon(true);
            return thread;
        });
        this.server.createContext("/metrics", this::handleMetrics);
        this.server.setExecutor(executor);
    }

    public void start() {
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }
}

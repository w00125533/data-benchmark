package com.example.databenchmark.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

public class StarRocksStreamLoadClient {
    private static final URI DEFAULT_URL =
        URI.create("http://localhost:8040/api/sr_internal/cell_kpi_1min/_stream_load");
    private static final String STREAM_LOAD_URL_ENV = "STARROCKS_STREAM_LOAD_URL";
    private static final String STREAM_LOAD_BASE_URL_ENV = "STARROCKS_STREAM_LOAD_BASE_URL";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HttpClient httpClient;
    private final URI url;
    private final String user;
    private final String password;

    public StarRocksStreamLoadClient() {
        this(HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS).build(), defaultUrl(), "root", "");
    }

    public StarRocksStreamLoadClient(HttpClient httpClient, URI url, String user, String password) {
        this.httpClient = httpClient;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public StreamLoadResult loadCsv(Path csv, String label) {
        return send(new StreamLoadRequest(url, headers(label), csv));
    }

    public StreamLoadResult loadCsv(Path csv, String database, String table, String label) {
        return send(new StreamLoadRequest(streamLoadUrl(database, table), headers(label), csv));
    }

    protected StreamLoadResult send(StreamLoadRequest request) {
        long started = System.nanoTime();
        try {
            HttpResponse<String> response = httpClient.send(buildRequest(request), HttpResponse.BodyHandlers.ofString());
            return new StreamLoadResult(response.statusCode(), response.body(), elapsedSeconds(started));
        } catch (IOException e) {
            return new StreamLoadResult(0, e.getMessage(), elapsedSeconds(started));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new StreamLoadResult(0, e.getMessage(), elapsedSeconds(started));
        }
    }

    HttpRequest buildRequest(StreamLoadRequest request) throws IOException {
        HttpRequest.Builder builder = HttpRequest.newBuilder(request.url())
            .timeout(Duration.ofMinutes(5))
            .expectContinue(true)
            .PUT(HttpRequest.BodyPublishers.ofFile(request.csv()));
        request.headers().forEach(builder::header);
        return builder.build();
    }

    private Map<String, String> headers(String label) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8)));
        headers.put("label", label);
        headers.put("column_separator", ",");
        headers.put("row_delimiter", "\\n");
        headers.put("enclose", "\"");
        headers.put("format", "csv");
        return headers;
    }

    URI streamLoadUrl(String database, String table) {
        String authority = url.getPort() == -1
            ? url.getHost()
            : "%s:%d".formatted(url.getHost(), url.getPort());
        return URI.create("%s://%s/api/%s/%s/_stream_load".formatted(
            url.getScheme(),
            authority,
            database,
            table
        ));
    }

    private static double elapsedSeconds(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000_000.0;
    }

    private static URI defaultUrl() {
        return defaultUrl(System.getenv());
    }

    static URI defaultUrl(Map<String, String> environment) {
        String explicitUrl = environment.get(STREAM_LOAD_URL_ENV);
        if (explicitUrl != null && !explicitUrl.isBlank()) {
            return URI.create(explicitUrl);
        }
        String baseUrl = environment.get(STREAM_LOAD_BASE_URL_ENV);
        if (baseUrl != null && !baseUrl.isBlank()) {
            return URI.create(trimTrailingSlash(baseUrl) + "/api/sr_internal/cell_kpi_1min/_stream_load");
        }
        return DEFAULT_URL;
    }

    private static String trimTrailingSlash(String value) {
        String trimmed = value.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    public record StreamLoadRequest(URI url, Map<String, String> headers, Path csv) {}

    public record StreamLoadResult(int statusCode, String body, double durationSeconds) {
        public boolean success() {
            if (statusCode < 200 || statusCode >= 300) {
                return false;
            }
            try {
                JsonNode status = MAPPER.readTree(body).get("Status");
                return status != null && "Success".equalsIgnoreCase(status.asText());
            } catch (IOException e) {
                return false;
            }
        }
    }
}

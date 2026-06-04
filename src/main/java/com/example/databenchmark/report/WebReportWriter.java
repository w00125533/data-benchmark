package com.example.databenchmark.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class WebReportWriter {
    private static final Pattern SAFE_RUN_ID = Pattern.compile("[A-Za-z0-9._-]+");
    private static final String RESOURCE_ROOT = "report-ui";
    private static final String INJECTION_MARKER = "<!-- BENCHMARK_REPORT_DATA -->";

    private final ObjectMapper objectMapper;
    private final WebBenchmarkReportMapper mapper;
    private final Path resourceRoot;

    public WebReportWriter() {
        this(new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT), new WebBenchmarkReportMapper(), null);
    }

    WebReportWriter(Path resourceRoot) {
        this(new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT), new WebBenchmarkReportMapper(), resourceRoot);
    }

    WebReportWriter(ObjectMapper objectMapper, WebBenchmarkReportMapper mapper) {
        this(objectMapper, mapper, null);
    }

    WebReportWriter(ObjectMapper objectMapper, WebBenchmarkReportMapper mapper, Path resourceRoot) {
        this.objectMapper = objectMapper;
        this.mapper = mapper;
        this.resourceRoot = resourceRoot;
    }

    public Path write(BenchmarkReport report, Path outputRoot) throws IOException {
        validateRunId(report.runId());

        Path root = outputRoot.toAbsolutePath().normalize();
        Path outputDir = root.resolve(report.runId()).normalize();
        if (!outputDir.startsWith(root)) {
            throw new IllegalArgumentException("runId resolves outside outputRoot");
        }
        Files.createDirectories(outputDir);

        copyReportUi(outputDir);

        WebBenchmarkReport webReport = mapper.map(report);
        String json = objectMapper.writeValueAsString(webReport);
        Files.writeString(outputDir.resolve("report.json"), json, StandardCharsets.UTF_8);

        Path index = outputDir.resolve("index.html");
        if (!Files.exists(index)) {
            throw new IOException("Missing report UI index.html");
        }
        String html = Files.readString(index, StandardCharsets.UTF_8);
        if (!html.contains(INJECTION_MARKER)) {
            throw new IOException("Missing report UI injection marker: " + INJECTION_MARKER);
        }

        String script = "<script>window.__BENCHMARK_REPORT__ = " + scriptSafeJson(json) + ";</script>";
        Files.writeString(index, html.replace(INJECTION_MARKER, script), StandardCharsets.UTF_8);
        return index;
    }

    private void validateRunId(String runId) {
        if (runId == null || !SAFE_RUN_ID.matcher(runId).matches()) {
            throw new IllegalArgumentException("Invalid runId: " + runId);
        }
    }

    private String scriptSafeJson(String json) {
        return json
            .replace("</", "<\\/")
            .replace("\u2028", "\\u2028")
            .replace("\u2029", "\\u2029");
    }

    private void copyReportUi(Path outputDir) throws IOException {
        if (resourceRoot != null) {
            if (!Files.isDirectory(resourceRoot)) {
                throw new IOException("Missing report UI resources: " + resourceRoot);
            }
            copyDirectory(resourceRoot, outputDir);
            return;
        }

        URL resource = getClass().getClassLoader().getResource(RESOURCE_ROOT);
        if (resource == null) {
            throw new IOException("Missing report UI resources: " + RESOURCE_ROOT);
        }

        if ("file".equals(resource.getProtocol())) {
            try {
                copyDirectory(Paths.get(resource.toURI()), outputDir);
            } catch (URISyntaxException exception) {
                throw new IOException("Invalid report UI resource URI", exception);
            }
            return;
        }

        if ("jar".equals(resource.getProtocol())) {
            try (FileSystem fs = FileSystems.newFileSystem(resource.toURI(), Collections.emptyMap())) {
                copyDirectory(fs.getPath("/" + RESOURCE_ROOT), outputDir);
            } catch (URISyntaxException exception) {
                throw new IOException("Invalid report UI jar resource URI", exception);
            }
            return;
        }

        throw new IOException("Unsupported report UI resource protocol: " + resource.getProtocol());
    }

    private void copyDirectory(Path sourceRoot, Path outputDir) throws IOException {
        try (Stream<Path> paths = Files.walk(sourceRoot)) {
            for (Path source : paths.toList()) {
                Path target = outputDir.resolve(sourceRoot.relativize(source).toString()).normalize();
                if (Files.isDirectory(source)) {
                    Files.createDirectories(target);
                } else {
                    Files.createDirectories(target.getParent());
                    try (InputStream input = Files.newInputStream(source)) {
                        Files.copy(input, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            }
        }
    }
}

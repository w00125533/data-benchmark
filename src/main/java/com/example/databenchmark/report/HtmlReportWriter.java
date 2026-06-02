package com.example.databenchmark.report;

import freemarker.log.Logger;
import freemarker.template.Configuration;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class HtmlReportWriter {
    private static final Pattern SAFE_RUN_ID = Pattern.compile("[A-Za-z0-9._-]+");

    private final Configuration configuration;

    public HtmlReportWriter() {
        disableFreemarkerLogging();
        configuration = new Configuration(Configuration.VERSION_2_3_33);
        configuration.setClassLoaderForTemplateLoading(getClass().getClassLoader(), "");
        configuration.setDefaultEncoding(StandardCharsets.UTF_8.name());
        configuration.setNumberFormat("computer");
        configuration.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    }

    public Path write(BenchmarkReport report, Path outputRoot) throws IOException {
        validateRunId(report.runId());
        validateGrafanaUrl(report.grafanaUrl());

        Path root = outputRoot.toAbsolutePath().normalize();
        Path outputDir = root.resolve(report.runId()).normalize();
        if (!outputDir.startsWith(root)) {
            throw new IllegalArgumentException("runId resolves outside outputRoot");
        }
        Files.createDirectories(outputDir);

        Path output = outputDir.resolve("index.html");
        try (Writer writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            configuration.getTemplate("report.ftl").process(Map.of("report", report), writer);
        } catch (TemplateException exception) {
            throw new IOException("Failed to render HTML report", exception);
        }
        return output;
    }

    private void validateRunId(String runId) {
        if (runId == null || !SAFE_RUN_ID.matcher(runId).matches()) {
            throw new IllegalArgumentException("Invalid runId: " + runId);
        }
    }

    private void validateGrafanaUrl(String grafanaUrl) {
        try {
            URI uri = new URI(grafanaUrl);
            String scheme = uri.getScheme();
            if (scheme == null || !Set.of("http", "https").contains(scheme.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("Invalid grafanaUrl: " + grafanaUrl);
            }
        } catch (URISyntaxException | NullPointerException exception) {
            throw new IllegalArgumentException("Invalid grafanaUrl: " + grafanaUrl, exception);
        }
    }

    private void disableFreemarkerLogging() {
        try {
            Logger.selectLoggerLibrary(Logger.LIBRARY_NONE);
        } catch (ClassNotFoundException exception) {
            throw new IllegalStateException("Failed to configure FreeMarker logging", exception);
        }
    }
}

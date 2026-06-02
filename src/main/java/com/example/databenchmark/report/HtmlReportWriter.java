package com.example.databenchmark.report;

import freemarker.log.Logger;
import freemarker.template.Configuration;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class HtmlReportWriter {
    private final Configuration configuration;

    public HtmlReportWriter() {
        disableFreemarkerLogging();
        configuration = new Configuration(Configuration.VERSION_2_3_33);
        configuration.setClassLoaderForTemplateLoading(getClass().getClassLoader(), "");
        configuration.setDefaultEncoding(StandardCharsets.UTF_8.name());
        configuration.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    }

    public Path write(BenchmarkReport report, Path outputRoot) throws IOException {
        Path outputDir = outputRoot.resolve(report.runId());
        Files.createDirectories(outputDir);

        Path output = outputDir.resolve("index.html");
        try (Writer writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            configuration.getTemplate("report.ftl").process(Map.of("report", report), writer);
        } catch (TemplateException exception) {
            throw new IOException("Failed to render HTML report", exception);
        }
        return output;
    }

    private void disableFreemarkerLogging() {
        try {
            Logger.selectLoggerLibrary(Logger.LIBRARY_NONE);
        } catch (ClassNotFoundException exception) {
            throw new IllegalStateException("Failed to configure FreeMarker logging", exception);
        }
    }
}

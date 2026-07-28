package com.example.databenchmark.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class IcebergValidationReportWriter {
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()
        .enable(SerializationFeature.INDENT_OUTPUT);

    public Path write(IcebergValidationReport report, Path reportRoot) throws IOException {
        Path runDir = reportRoot.resolve(report.runId());
        Files.createDirectories(runDir);
        mapper.writeValue(runDir.resolve("report.json").toFile(), report);
        Files.writeString(runDir.resolve("report.md"), markdown(report));
        return runDir.resolve("report.md");
    }

    private static String markdown(IcebergValidationReport report) {
        StringBuilder builder = new StringBuilder();
        builder.append("# Iceberg Validation Report\n\n");
        builder.append("- Run ID: `").append(report.runId()).append("`\n");
        builder.append("- Iceberg Version: `").append(report.icebergVersion()).append("`\n");
        builder.append("- Profile: `").append(report.profile()).append("`\n");
        builder.append("- Status: `").append(report.status()).append("`\n\n");
        builder.append("| Scenario | Case | Function | Performance | Conclusion |\n");
        builder.append("| --- | --- | --- | --- | --- |\n");
        for (IcebergValidationResult result : report.results()) {
            builder.append("| ")
                .append(result.scenario()).append(" | ")
                .append(result.caseId()).append(" | ")
                .append(result.functionStatus()).append(" | ")
                .append(result.performanceStatus()).append(" | ")
                .append(escape(result.conclusion())).append(" |\n");
        }
        builder.append("\n## Evidence\n\n");
        for (IcebergValidationResult result : report.results()) {
            builder.append("### ").append(result.scenario()).append(" / ").append(result.caseId()).append("\n\n");
            for (String evidence : result.evidence()) {
                builder.append("- ").append(evidence).append("\n");
            }
            for (String error : result.errors()) {
                builder.append("- ERROR: ").append(error).append("\n");
            }
            builder.append("\n");
        }
        return builder.toString();
    }

    private static String escape(String value) {
        return value == null ? "" : value.replace("|", "\\|").replace("\r", " ").replace("\n", " ");
    }
}

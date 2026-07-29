package com.example.databenchmark.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IcebergValidationResultTest {
    @Test
    void storesStructuredExecutionEvidenceSeparatelyFromLegacyEvidence() {
        IcebergExecutionEvidence execution = new IcebergExecutionEvidence(
            "action",
            "count current rows",
            "spark-sql -e SELECT COUNT(*) FROM t",
            0,
            1.25,
            "count\n1000",
            ""
        );

        IcebergValidationResult result = new IcebergValidationResult(
            "schemaEvolution",
            "schema-add-drop-rename",
            "purpose",
            Map.of("rows", "1000"),
            List.of("legacy setup"),
            List.of("legacy action"),
            List.of("row count matched"),
            Map.of("currentRows", "1000"),
            Map.of("baselineRows", "1000"),
            Map.of("currentRows", "1000"),
            IcebergConclusion.FunctionStatus.PASS,
            IcebergConclusion.PerformanceStatus.ACCEPTABLE,
            "conclusion",
            List.of("legacy evidence"),
            List.of(),
            List.of(execution)
        );

        assertThat(result.executionResults()).containsExactly(execution);
        assertThat(result.evidence()).containsExactly("legacy evidence");
    }
}

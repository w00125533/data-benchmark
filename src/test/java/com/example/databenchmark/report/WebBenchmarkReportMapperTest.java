package com.example.databenchmark.report;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

class WebBenchmarkReportMapperTest {
    private final WebBenchmarkReportMapper mapper = new WebBenchmarkReportMapper();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void mapsSuccessfulRunDatasetDetailsAndChartPoints() {
        BenchmarkReport report = BenchmarkReport.sample("run-web");

        WebBenchmarkReport web = mapper.map(report);

        assertThat(web.schemaVersion()).isEqualTo(1);
        assertThat(web.run().runId()).isEqualTo("run-web");
        assertThat(web.run().suite()).isEqualTo("kpi");
        assertThat(web.run().querySet()).isEqualTo("smoke");
        assertThat(web.run().status()).isEqualTo("SUCCESS");
        assertThat(web.run().durationSeconds()).isEqualTo(60.0);
        assertThat(web.dataset().rows()).isEqualTo(14_400L);
        assertThat(web.loads()).hasSize(1);
        assertThat(web.queries()).hasSize(1);
        assertThat(web.charts().loadDurationByEngine())
            .containsExactly(new WebBenchmarkReport.LoadDurationPoint(
                "local",
                "generated_parquet",
                "generate",
                1.2,
                true
            ));
        assertThat(web.charts().queryLatencyByEngine())
            .extracting(WebBenchmarkReport.QueryLatencyPoint::metric)
            .containsExactly("p50", "p95", "p99");
        assertThat(web.charts().queryRowsByEngine())
            .containsExactly(new WebBenchmarkReport.QueryRowsPoint(
                "spark_iceberg",
                "topn_high_load_cells",
                100L,
                true
            ));
        assertThat(web.charts().failureSummary()).isEmpty();
    }

    @Test
    void webReportSerializesWithDefaultJacksonMapper() throws Exception {
        WebBenchmarkReport web = mapper.map(BenchmarkReport.sample("run-web"));

        JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(web));

        assertThat(json.path("loads").path(0).path("stage").asText()).isEqualTo("generate");
        assertThat(json.path("queries").path(0).path("queryName").asText()).isEqualTo("topn_high_load_cells");
        assertThat(json.path("charts").path("failureSummary")).isEmpty();
    }

    @Test
    void mapsDegradedTpchReportNoticesAndFailureSummary() {
        BenchmarkReport report = new BenchmarkReport(
            "run-degraded",
            "tpch-smoke",
            "tpch",
            "smoke",
            "2026-06-04T00:00:00Z",
            "2026-06-04T00:00:10Z",
            0,
            0,
            899,
            0,
            1234,
            List.of(new BenchmarkReport.LoadSummary(
                "spark_iceberg",
                "tpch_iceberg",
                "LOAD",
                0,
                0,
                0.5,
                false,
                "load failed"
            )),
            List.of(new BenchmarkReport.QuerySummary(
                "starrocks_internal",
                "tpch_internal",
                "q01_pricing_summary_report",
                0,
                0,
                0,
                0,
                1,
                false,
                "query failed"
            )),
            false
        );

        WebBenchmarkReport web = mapper.map(report);

        assertThat(web.run().status()).isEqualTo("DEGRADED");
        assertThat(web.notices())
            .contains("TPC-H smoke data is compatible test data, not an official TPC-H benchmark result.");
        assertThat(web.charts().failureSummary())
            .contains(
                new WebBenchmarkReport.FailureSummaryPoint("LOAD", "spark_iceberg", 1),
                new WebBenchmarkReport.FailureSummaryPoint("q01_pricing_summary_report", "starrocks_internal", 1)
            );
    }

    @Test
    void aggregatesOnlyRealFailuresWithoutDelimiterCollisions() {
        BenchmarkReport report = new BenchmarkReport(
            "run-delimiters",
            "smoke",
            "kpi",
            "smoke",
            "2026-06-04T00:00:00Z",
            "2026-06-04T00:00:10Z",
            0,
            0,
            0,
            0,
            0,
            List.of(
                new BenchmarkReport.LoadSummary("engine|a", "shape", "stage", 0, 0, 1, false, "failed"),
                new BenchmarkReport.LoadSummary("a", "shape", "stage|engine", 0, 0, 1, false, "failed"),
                new BenchmarkReport.LoadSummary("engine|a", "shape", "stage", 0, 0, 1, true, "")
            ),
            List.of(
                new BenchmarkReport.QuerySummary("engine|a", "shape", "query", 0, 0, 0, 0, 2, true, "failed"),
                new BenchmarkReport.QuerySummary("engine|a", "shape", "query", 0, 0, 0, 0, 0, true, "")
            ),
            false
        );

        WebBenchmarkReport web = mapper.map(report);

        assertThat(web.charts().failureSummary())
            .containsExactly(
                new WebBenchmarkReport.FailureSummaryPoint("stage", "engine|a", 1),
                new WebBenchmarkReport.FailureSummaryPoint("stage|engine", "a", 1),
                new WebBenchmarkReport.FailureSummaryPoint("query", "engine|a", 2)
            );
    }
}

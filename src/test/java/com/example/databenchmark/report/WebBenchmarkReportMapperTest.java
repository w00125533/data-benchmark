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
    void schemaThreeMatrixAggregatesColdWarmHotForFourRoutesAndChoosesHotWinner() {
        BenchmarkReport report = new BenchmarkReport(
            "four-route-run",
            "smoke",
            "kpi",
            "smoke",
            "2026-06-04T00:00:00Z",
            "2026-06-04T00:00:09Z",
            10000,
            1,
            10000,
            50,
            2048,
            List.of(),
            List.of(
                query("spark", "spark_iceberg", "topn_high_load_cells", "COLD", 900, true),
                query("spark", "spark_iceberg", "topn_high_load_cells", "WARM", 700, true),
                query("spark", "spark_iceberg", "topn_high_load_cells", "HOT", 650, true),
                query("starrocks", "starrocks_internal", "topn_high_load_cells", "COLD", 300, true),
                query("starrocks", "starrocks_internal", "topn_high_load_cells", "WARM", 80, true),
                query("starrocks", "starrocks_internal", "topn_high_load_cells", "HOT", 70, true),
                query("starrocks", "starrocks_external_iceberg", "topn_high_load_cells", "COLD", 500, true),
                query("starrocks", "starrocks_external_iceberg", "topn_high_load_cells", "WARM", 90, true),
                query("starrocks", "starrocks_external_iceberg", "topn_high_load_cells", "HOT", 60, true),
                query("hive", "hive_hdfs_parquet", "topn_high_load_cells", "COLD", 1200, true),
                query("hive", "hive_hdfs_parquet", "topn_high_load_cells", "WARM", 800, true),
                query("hive", "hive_hdfs_parquet", "topn_high_load_cells", "HOT", 760, true)
            ),
            false
        );

        WebBenchmarkReport web = mapper.map(report);
        WebBenchmarkReport.PerformanceMatrixRow row = web.performanceMatrix().get(0);

        assertThat(web.schemaVersion()).isEqualTo(3);
        assertThat(row.routes().get("spark_iceberg").coldMs()).isEqualTo(900);
        assertThat(row.routes().get("spark_iceberg").warmMs()).isEqualTo(700);
        assertThat(row.routes().get("spark_iceberg").hotMs()).isEqualTo(650);
        assertThat(row.routes().get("hive_hdfs_parquet").hotMs()).isEqualTo(760);
        assertThat(row.bestRoute()).isEqualTo("starrocks_external_iceberg");
        assertThat(row.bestRouteHotMs()).isEqualTo(60);
    }

    @Test
    void matrixIncludesSparkNativeParquetBeforeSparkIcebergWithoutCollapsingRoutes() {
        BenchmarkReport report = new BenchmarkReport(
            "native-route-run",
            "smoke",
            "kpi",
            "smoke",
            "2026-06-04T00:00:00Z",
            "2026-06-04T00:00:09Z",
            10000,
            1,
            10000,
            50,
            2048,
            List.of(),
            List.of(
                query("spark", "spark_native_parquet", "topn_high_load_cells", "COLD", 950, true),
                query("spark", "spark_native_parquet", "topn_high_load_cells", "WARM", 740, true),
                query("spark", "spark_native_parquet", "topn_high_load_cells", "HOT", 610, true),
                query("spark", "spark_iceberg", "topn_high_load_cells", "COLD", 900, true),
                query("spark", "spark_iceberg", "topn_high_load_cells", "WARM", 700, true),
                query("spark", "spark_iceberg", "topn_high_load_cells", "HOT", 650, true)
            ),
            false
        );

        WebBenchmarkReport web = mapper.map(report);
        WebBenchmarkReport.PerformanceMatrixRow row = web.performanceMatrix().get(0);

        assertThat(web.queries())
            .extracting(WebBenchmarkReport.QuerySummary::engine)
            .contains("spark_native_parquet", "spark_iceberg");
        assertThat(row.routes().keySet())
            .containsExactly(
                "spark_native_parquet",
                "spark_iceberg",
                "starrocks_internal",
                "starrocks_external_iceberg",
                "hive_hdfs_parquet"
            );
        assertThat(row.routes().get("spark_native_parquet").hotMs()).isEqualTo(610);
        assertThat(row.routes().get("spark_iceberg").hotMs()).isEqualTo(650);
        assertThat(row.bestRoute()).isEqualTo("spark_native_parquet");
    }

    @Test
    void mapsSuccessfulRunDatasetDetailsAndMatrixRows() {
        BenchmarkReport report = BenchmarkReport.sample("run-web");

        WebBenchmarkReport web = mapper.map(report);

        assertThat(web.schemaVersion()).isEqualTo(3);
        assertThat(web.run().runId()).isEqualTo("run-web");
        assertThat(web.run().suite()).isEqualTo("kpi");
        assertThat(web.run().querySet()).isEqualTo("smoke");
        assertThat(web.run().status()).isEqualTo("SUCCESS");
        assertThat(web.run().durationSeconds()).isEqualTo(60.0);
        assertThat(web.dataset().rows()).isEqualTo(14_400L);
        assertThat(web.loads()).hasSize(1);
        assertThat(web.queries()).hasSize(1);
        assertThat(web.performanceMatrix()).hasSize(1);
        assertThat(web.performanceMatrix().get(0).queryName()).isEqualTo("topn_high_load_cells");
        assertThat(web.performanceMatrix().get(0).routes().get("spark_iceberg").status()).isEqualTo("SUCCESS");
    }

    @Test
    void webReportSerializesWithDefaultJacksonMapper() throws Exception {
        WebBenchmarkReport web = mapper.map(BenchmarkReport.sample("run-web"));

        JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(web));

        assertThat(json.path("loads").path(0).path("stage").asText()).isEqualTo("generate");
        assertThat(json.path("queries").path(0).path("queryName").asText()).isEqualTo("topn_high_load_cells");
        assertThat(json.path("performanceMatrix")).hasSize(1);
        assertThat(json.has("charts")).isFalse();
    }

    @Test
    void mapsSchemaVersionThreeMatrixFields() {
        BenchmarkReport report = new BenchmarkReport(
            "matrix-run",
            "tpch-smoke",
            "tpch",
            "smoke",
            "2026-06-04T00:00:00Z",
            "2026-06-04T00:00:03Z",
            10000,
            1,
            60000,
            8,
            1024,
            List.of(),
            List.of(new BenchmarkReport.QuerySummary(
                "starrocks_internal",
                "sr_internal_tpch",
                "q01_pricing_summary_report",
                390,
                410,
                455,
                120,
                0,
                true,
                ""
            )),
            false
        );

        WebBenchmarkReport mapped = mapper.map(report);

        assertThat(mapped.schemaVersion()).isEqualTo(3);
        assertThat(mapped.performanceMatrix()).hasSize(1);
        WebBenchmarkReport.PerformanceMatrixRow row = mapped.performanceMatrix().get(0);
        assertThat(row.datasetId()).isEqualTo("tpch");
        assertThat(row.datasetName()).isEqualTo("TPC-H SF 0.01");
        assertThat(row.querySet()).isEqualTo("smoke");
        assertThat(row.queryName()).isEqualTo("q01_pricing_summary_report");
        assertThat(row.routes().get("starrocks_internal").status()).isEqualTo("SUCCESS");
        assertThat(row.routes().get("starrocks_internal").hotMs()).isEqualTo(410);
        assertThat(row.routes().get("starrocks_internal").hotStatus()).isEqualTo("SUCCESS");
        assertThat(row.routes().get("spark_iceberg").status()).isEqualTo("SKIPPED");
        assertThat(row.routes().get("starrocks_external_iceberg").status()).isEqualTo("SKIPPED");
        assertThat(row.routes().get("hive_hdfs_parquet").status()).isEqualTo("SKIPPED");
        assertThat(row.bestRoute()).isEqualTo("starrocks_internal");
        assertThat(row.bestRouteHotMs()).isEqualTo(410);
    }

    @Test
    void matrixMarksFailedRoutesAndChoosesFastestSuccessfulHot() {
        BenchmarkReport report = new BenchmarkReport(
            "matrix-run",
            "tpch-smoke",
            "tpch",
            "smoke",
            "2026-06-04T00:00:00Z",
            "2026-06-04T00:00:03Z",
            10000,
            1,
            60000,
            8,
            1024,
            List.of(),
            List.of(
                new BenchmarkReport.QuerySummary(
                    "spark_iceberg",
                    "iceberg_catalog.tpch",
                    "q03_shipping_priority",
                    2310,
                    2450,
                    2600,
                    120,
                    0,
                    true,
                    ""
                ),
                new BenchmarkReport.QuerySummary(
                    "starrocks_internal",
                    "sr_internal_tpch",
                    "q03_shipping_priority",
                    720,
                    760,
                    810,
                    120,
                    0,
                    true,
                    ""
                ),
                new BenchmarkReport.QuerySummary(
                    "starrocks_external_iceberg",
                    "sr_external_iceberg.tpch",
                    "q03_shipping_priority",
                    0,
                    0,
                    0,
                    0,
                    1,
                    false,
                    "catalog timeout"
                )
            ),
            false
        );

        WebBenchmarkReport.PerformanceMatrixRow row = mapper.map(report).performanceMatrix().get(0);

        assertThat(row.routes().get("spark_iceberg").status()).isEqualTo("SUCCESS");
        assertThat(row.routes().get("starrocks_internal").status()).isEqualTo("SUCCESS");
        assertThat(row.routes().get("starrocks_external_iceberg").status()).isEqualTo("FAILED");
        assertThat(row.routes().get("starrocks_external_iceberg").hotStatus()).isEqualTo("FAILED");
        assertThat(row.routes().get("starrocks_external_iceberg").error()).isEqualTo("catalog timeout");
        assertThat(row.bestRoute()).isEqualTo("starrocks_internal");
        assertThat(row.bestRouteHotMs()).isEqualTo(760);
    }

    @Test
    void matrixUsesTableShapeWhenEngineIsGeneric() {
        BenchmarkReport report = new BenchmarkReport(
            "matrix-run",
            "smoke",
            "kpi",
            "smoke",
            "2026-06-04T00:00:00Z",
            "2026-06-04T00:00:03Z",
            10000,
            1,
            60000,
            8,
            1024,
            List.of(),
            List.of(
                new BenchmarkReport.QuerySummary("spark", "spark_iceberg", "topn_high_load_cells", 110, 120, 130, 10, 0, true, ""),
                new BenchmarkReport.QuerySummary("starrocks", "starrocks_internal", "topn_high_load_cells", 40, 45, 50, 10, 0, true, ""),
                new BenchmarkReport.QuerySummary(
                    "starrocks",
                    "starrocks_external_iceberg",
                    "topn_high_load_cells",
                    70,
                    75,
                    80,
                    10,
                    0,
                    true,
                    ""
                )
            ),
            false
        );

        WebBenchmarkReport.PerformanceMatrixRow row = mapper.map(report).performanceMatrix().get(0);

        assertThat(row.routes().get("spark_iceberg").status()).isEqualTo("SUCCESS");
        assertThat(row.routes().get("starrocks_internal").hotMs()).isEqualTo(45);
        assertThat(row.routes().get("starrocks_external_iceberg").hotMs()).isEqualTo(75);
        assertThat(row.routes().get("hive_hdfs_parquet").status()).isEqualTo("SKIPPED");
        assertThat(row.bestRoute()).isEqualTo("starrocks_internal");
    }

    @Test
    void usesDefaultDatasetNameWhenSuiteAndProfileAreMissing() {
        BenchmarkReport report = new BenchmarkReport(
            "matrix-run",
            null,
            " ",
            "smoke",
            "2026-06-04T00:00:00Z",
            "2026-06-04T00:00:03Z",
            10000,
            1,
            60000,
            8,
            1024,
            List.of(),
            List.of(new BenchmarkReport.QuerySummary(
                "starrocks_internal",
                "sr_internal_tpch",
                "q01_pricing_summary_report",
                390,
                410,
                455,
                120,
                0,
                true,
                ""
            )),
            false
        );

        WebBenchmarkReport mapped = mapper.map(report);
        WebBenchmarkReport.PerformanceMatrixRow row = mapped.performanceMatrix().get(0);

        assertThat(row.datasetId()).isEqualTo("default");
        assertThat(row.datasetName()).isEqualTo("Default Dataset");
        assertThat(mapped.queries().get(0).datasetId()).isEqualTo("default");
        assertThat(mapped.queries().get(0).datasetName()).isEqualTo("Default Dataset");
    }

    @Test
    void mapsDegradedTpchReportNoticesAndFailedMatrixRoute() {
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
        assertThat(web.performanceMatrix()).hasSize(1);
        assertThat(web.performanceMatrix().get(0).routes().get("starrocks_internal").status()).isEqualTo("FAILED");
        assertThat(web.performanceMatrix().get(0).routes().get("starrocks_internal").error()).isEqualTo("query failed");
    }

    @Test
    void matrixIgnoresUnknownRoutesBecauseTheyAreNotComparable() {
        BenchmarkReport report = new BenchmarkReport(
            "run-local",
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
            List.of(),
            List.of(new BenchmarkReport.QuerySummary("local", "generated_parquet", "catalog_render_check", 0, 0, 0, 0, 0, true, "")),
            false
        );

        WebBenchmarkReport web = mapper.map(report);

        assertThat(web.queries()).hasSize(1);
        assertThat(web.queries().get(0).engine()).isEqualTo("local");
        assertThat(web.performanceMatrix()).isEmpty();
    }

    private static BenchmarkReport.QuerySummary query(
        String engine,
        String tableShape,
        String queryName,
        String phase,
        double millis,
        boolean success
    ) {
        return new BenchmarkReport.QuerySummary(
            engine,
            tableShape,
            queryName,
            phase,
            millis,
            millis,
            millis,
            10,
            success ? 0 : 1,
            success,
            success ? "" : phase + " failed"
        );
    }
}

package com.example.databenchmark.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class BenchmarkReportTest {
    @Test
    void calculatesDurationSecondsFromRunTimestamps() {
        BenchmarkReport report = reportWith(
            "2026-06-04T00:00:00Z",
            "2026-06-04T00:01:00.500Z",
            List.of(successfulLoad()),
            List.of(successfulQuery())
        );

        assertThat(report.durationSeconds()).isEqualTo(60.5);
    }

    @Test
    void invalidTimestampsReturnZeroDurationSeconds() {
        BenchmarkReport report = reportWith(
            "not-a-time",
            "2026-06-04T00:01:00Z",
            List.of(successfulLoad()),
            List.of(successfulQuery())
        );

        assertThat(report.durationSeconds()).isZero();
    }

    @Test
    void emptyLoadSummariesMakeStatusDegraded() {
        BenchmarkReport report = reportWith(
            "2026-06-04T00:00:00Z",
            "2026-06-04T00:00:01Z",
            List.of(),
            List.of(successfulQuery())
        );

        assertThat(report.status()).isEqualTo("DEGRADED");
    }

    @Test
    void emptyQuerySummariesMakeStatusDegraded() {
        BenchmarkReport report = reportWith(
            "2026-06-04T00:00:00Z",
            "2026-06-04T00:00:01Z",
            List.of(successfulLoad()),
            List.of()
        );

        assertThat(report.status()).isEqualTo("DEGRADED");
    }

    @Test
    void queryFailuresMakeStatusDegradedEvenWhenQuerySucceeded() {
        BenchmarkReport report = reportWith(
            "2026-06-04T00:00:00Z",
            "2026-06-04T00:00:01Z",
            List.of(successfulLoad()),
            List.of(new BenchmarkReport.QuerySummary("spark_iceberg", "shape", "query", 1, 1, 1, 1, 1, true, ""))
        );

        assertThat(report.status()).isEqualTo("DEGRADED");
    }

    private BenchmarkReport reportWith(
        String startedAt,
        String endedAt,
        List<BenchmarkReport.LoadSummary> loads,
        List<BenchmarkReport.QuerySummary> queries
    ) {
        return new BenchmarkReport(
            "run-test",
            "smoke",
            "kpi",
            "smoke",
            startedAt,
            endedAt,
            1,
            1,
            1,
            1,
            1,
            loads,
            queries,
            false
        );
    }

    private BenchmarkReport.LoadSummary successfulLoad() {
        return new BenchmarkReport.LoadSummary("local", "generated_parquet", "generate", 1, 1, 1, true, "");
    }

    private BenchmarkReport.QuerySummary successfulQuery() {
        return new BenchmarkReport.QuerySummary("local", "generated_parquet", "query", 1, 1, 1, 1, 0, true, "");
    }
}

package com.example.databenchmark.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class QueryCatalogTest {
    @Test
    void queriesAreReturnedInRequiredOrder() {
        assertThat(QueryCatalog.queries())
            .extracting(QueryDefinition::name)
            .containsExactly(
                "single_cell_day_trend",
                "single_cell_week_trend",
                "city_vendor_band_rat_minute_agg",
                "topn_high_load_cells",
                "weak_coverage_cells",
                "adjacent_window_kpi_spike",
                "recent_hot_cells",
                "wide_filter_group_by",
                "date_partition_pruning",
                "large_cell_id_filter"
            );
    }

    @Test
    void rendersEveryQueryForEveryEngine() {
        for (BenchmarkEngine engine : QueryCatalog.engines()) {
            for (QueryDefinition query : QueryCatalog.queries()) {
                String rendered = QueryCatalog.render(query.name(), engine);

                assertThat(rendered).contains(engine.tableName());
                assertThat(rendered).doesNotContain("{table}");
            }
        }
    }

    @Test
    void topnHighLoadCellsAggregatesByCell() {
        BenchmarkEngine engine = engine("spark_iceberg");

        String normalized = normalizeSql(QueryCatalog.render("topn_high_load_cells", engine));

        assertThat(normalized)
            .isEqualTo(
                "SELECT cell_id, MAX(prb_dl_util) AS prb_dl_util, "
                    + "MAX(active_users) AS active_users, MAX(load_score) AS load_score "
                    + "FROM iceberg_catalog.iceberg_db.cell_kpi_1min "
                    + "WHERE event_time >= TIMESTAMP '2026-01-01 00:00:00' "
                    + "AND event_time < TIMESTAMP '2026-01-02 00:00:00' "
                    + "GROUP BY cell_id ORDER BY load_score DESC LIMIT 100"
            );
    }

    @Test
    void recentHotCellsUsesPeakLoadSoGeneratedDataCanMatch() {
        BenchmarkEngine engine = engine("spark_iceberg");

        String normalized = normalizeSql(QueryCatalog.render("recent_hot_cells", engine));

        assertThat(normalized)
            .contains("MAX(load_score) AS peak_load_score")
            .contains("HAVING MAX(load_score) >= 80");
        assertThat(normalized).doesNotContain("HAVING AVG(load_score) >= 80");
    }

    @Test
    void wideFilterGroupByUsesFractionalPrbUtilRange() {
        BenchmarkEngine engine = engine("spark_iceberg");

        String normalized = normalizeSql(QueryCatalog.render("wide_filter_group_by", engine));

        assertThat(normalized).contains("prb_dl_util BETWEEN 0.40 AND 0.95");
        assertThat(normalized).doesNotContain("prb_dl_util BETWEEN 40 AND 95");
    }

    @Test
    void renderedQueriesUseGeneratorCompatibleLiterals() {
        for (BenchmarkEngine engine : QueryCatalog.engines()) {
            for (QueryDefinition query : QueryCatalog.queries()) {
                String rendered = QueryCatalog.render(query.name(), engine);

                assertDimensionLiteralsMatchGeneratedValues(rendered);
            }
        }
    }

    @Test
    void unknownQueryThrowsIllegalArgumentException() {
        BenchmarkEngine engine = QueryCatalog.engines().get(0);

        assertThatThrownBy(() -> QueryCatalog.render("missing_query", engine))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown query: missing_query");
    }

    @Test
    void enginesUseSchemaTableShapesInRequiredOrder() {
        assertThat(QueryCatalog.engines()).containsExactly(
            new BenchmarkEngine(
                "spark_native_parquet",
                "spark_native_parquet",
                "spark_catalog.benchmark_native.cell_kpi_1min"
            ),
            new BenchmarkEngine("spark_iceberg", "spark_iceberg", "iceberg_catalog.iceberg_db.cell_kpi_1min"),
            new BenchmarkEngine("starrocks_internal", "starrocks_internal", "sr_internal.cell_kpi_1min"),
            new BenchmarkEngine(
                "starrocks_external_iceberg",
                "starrocks_external_iceberg",
                "sr_external_iceberg.iceberg_db.cell_kpi_1min"
            )
        );
    }

    @Test
    void rendersSparkNativeParquetQueriesAgainstNativeTable() {
        String rendered = QueryCatalog.render("topn_high_load_cells", engine("spark_native_parquet"));

        assertThat(rendered).contains("FROM spark_catalog.benchmark_native.cell_kpi_1min");
        assertThat(rendered).doesNotContain("iceberg_catalog");
    }

    @Test
    void queryCatalogReturnsExactlyRequiredScenarios() {
        List<QueryDefinition> queries = QueryCatalog.queries();

        assertThat(queries).hasSize(10);
        assertThat(queries)
            .allSatisfy(query -> assertThat(query.template()).contains("{table}"));
    }

    private String normalizeSql(String sql) {
        return sql.replaceAll("\\s+", " ").trim();
    }

    private BenchmarkEngine engine(String name) {
        return QueryCatalog.engines().stream()
            .filter(candidate -> candidate.name().equals(name))
            .findFirst()
            .orElseThrow();
    }

    private void assertDimensionLiteralsMatchGeneratedValues(String sql) {
        Matcher matcher = Pattern.compile("'([^']*)'").matcher(sql);
        while (matcher.find()) {
            String literal = matcher.group(1);
            if (isNonDimensionSqlLiteral(literal)) {
                continue;
            }

            if (literal.startsWith("CELL-")) {
                assertThat(literal).matches("CELL-\\d{6}");
            } else if (literal.startsWith("province-")) {
                int province = Integer.parseInt(literal.substring("province-".length()));
                assertThat(literal).matches("province-\\d{2}");
                assertThat(province).isBetween(0, 30);
            } else if (literal.startsWith("city-")) {
                int city = Integer.parseInt(literal.substring("city-".length()));
                assertThat(literal).matches("city-\\d{3}");
                assertThat(city).isBetween(0, 199);
            } else if (VENDORS.contains(literal)) {
                assertThat(literal).isIn(VENDORS);
            } else if (literal.endsWith("G")) {
                assertThat(literal).isIn("4G", "5G");
            } else if (literal.startsWith("B") || literal.startsWith("N")) {
                assertThat(literal).isIn(BANDS);
            } else {
                assertThat(literal)
                    .as("single-quoted SQL literal should be a generated dimension value or ignored SQL literal")
                    .isIn(GENERATED_DIMENSION_LITERALS);
            }
        }
    }

    private boolean isNonDimensionSqlLiteral(String literal) {
        return literal.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")
            || Set.of("minute", "hour", "day").contains(literal);
    }

    private static final Set<String> VENDORS = Set.of("Huawei", "ZTE", "Ericsson", "Nokia", "Samsung");
    private static final Set<String> BANDS = Set.of("B3", "B7", "B8", "B20", "N78", "N41");
    private static final Set<String> GENERATED_DIMENSION_LITERALS = Set.of(
        "4G",
        "5G",
        "Huawei",
        "ZTE",
        "Ericsson",
        "Nokia",
        "Samsung",
        "B3",
        "B7",
        "B8",
        "B20",
        "N78",
        "N41"
    );
}

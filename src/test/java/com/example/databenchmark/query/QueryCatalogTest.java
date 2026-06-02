package com.example.databenchmark.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
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
    void rendersTopnQueryForEveryEngine() {
        for (BenchmarkEngine engine : QueryCatalog.engines()) {
            String rendered = QueryCatalog.render("topn_high_load_cells", engine);

            assertThat(rendered).contains("FROM " + engine.tableName());
            assertThat(rendered).contains("ORDER BY load_score DESC");
            assertThat(rendered).doesNotContain("{table}");
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
            new BenchmarkEngine("spark_iceberg", "spark_iceberg", "iceberg_db.cell_kpi_1min"),
            new BenchmarkEngine("starrocks_internal", "starrocks_internal", "sr_internal.cell_kpi_1min"),
            new BenchmarkEngine(
                "starrocks_external_iceberg",
                "starrocks_external_iceberg",
                "sr_external_iceberg.cell_kpi_1min"
            )
        );
    }

    @Test
    void queryCatalogReturnsExactlyRequiredScenarios() {
        List<QueryDefinition> queries = QueryCatalog.queries();

        assertThat(queries).hasSize(10);
        assertThat(queries)
            .allSatisfy(query -> assertThat(query.template()).contains("{table}"));
    }
}

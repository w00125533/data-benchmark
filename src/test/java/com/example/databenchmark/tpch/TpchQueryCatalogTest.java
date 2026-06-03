package com.example.databenchmark.tpch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TpchQueryCatalogTest {
    @Test
    void allQuerySetContainsTwentyTwoQueries() {
        assertThat(TpchQueryCatalog.queries("all")).hasSize(22);
    }

    @Test
    void smokeQuerySetCoversCoreSqlShapes() {
        assertThat(TpchQueryCatalog.queries("smoke"))
            .extracting(TpchQuery::name)
            .containsExactly("q01_pricing_summary_report", "q03_shipping_priority", "q05_local_supplier_volume", "q10_returned_item_reporting");
    }

    @Test
    void unknownQuerySetThrowsClearError() {
        assertThatThrownBy(() -> TpchQueryCatalog.queries("bad"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown TPC-H query set");
    }

    @Test
    void unknownQueryThrowsClearError() {
        assertThatThrownBy(() -> TpchQueryCatalog.query("bad"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown TPC-H query");
    }
}

package com.example.databenchmark.schema;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class KpiTableNamingTest {
    @Test
    void mapsSmokeAndFullProfilesToSeparateStarRocksInternalTables() {
        assertThat(KpiTableNaming.starRocksInternalTableIdentifier("compose-smoke"))
            .isEqualTo("sr_internal.cell_kpi_1min_smoke");
        assertThat(KpiTableNaming.starRocksInternalTableIdentifier("kpi-1b"))
            .isEqualTo("sr_internal.cell_kpi_1min_1b");
        assertThat(KpiTableNaming.starRocksInternalTableIdentifier("custom"))
            .isEqualTo("sr_internal.cell_kpi_1min");
    }

    @Test
    void infersReportTableNameFromKpiRowScale() {
        assertThat(KpiTableNaming.starRocksInternalTableIdentifierForRows(10_000L))
            .isEqualTo("sr_internal.cell_kpi_1min_smoke");
        assertThat(KpiTableNaming.starRocksInternalTableIdentifierForRows(1_000_000_000L))
            .isEqualTo("sr_internal.cell_kpi_1min_1b");
    }
}

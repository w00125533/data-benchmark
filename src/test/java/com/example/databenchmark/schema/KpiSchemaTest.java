package com.example.databenchmark.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class KpiSchemaTest {
    @Test
    void schemaHasExactly50Columns() {
        assertThat(KpiSchema.columns()).hasSize(50);
    }

    @Test
    void columnNamesMatchColumnOrder() {
        assertThat(KpiSchema.columnNames())
            .containsExactlyElementsOf(KpiSchema.columns().stream().map(KpiColumn::name).toList());
    }

    @Test
    void schemaContainsRequiredWirelessFields() {
        assertThat(KpiSchema.columnNames()).contains(
            "event_time", "cell_id", "province", "city", "district", "grid_id",
            "vendor", "rat", "band", "arfcn", "pci", "site_id", "longitude", "latitude",
            "rsrp_avg", "rsrp_p10", "rsrq_avg", "sinr_avg", "prb_dl_util",
            "active_users", "load_score"
        );
    }

    @Test
    void tableShapesMatchSpecNames() {
        assertThat(KpiSchema.tableShapes()).containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
            "spark_iceberg", "iceberg_db.cell_kpi_1min",
            "starrocks_internal", "sr_internal.cell_kpi_1min",
            "starrocks_external_iceberg", "sr_external_iceberg.cell_kpi_1min"
        ));
    }

    @Test
    void returnedCollectionsAreImmutable() {
        assertThatThrownBy(() -> KpiSchema.columns().add(new KpiColumn("extra", "string")))
            .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> KpiSchema.columnNames().add("extra"))
            .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> KpiSchema.tableShapes().put("extra", "db.table"))
            .isInstanceOf(UnsupportedOperationException.class);
    }
}

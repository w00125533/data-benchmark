package com.example.databenchmark.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KpiSchemaTest {
    private static final List<KpiColumn> EXPECTED_COLUMNS = List.of(
        new KpiColumn("event_time", "timestamp_ms"),
        new KpiColumn("cell_id", "string"),
        new KpiColumn("province", "string"),
        new KpiColumn("city", "string"),
        new KpiColumn("district", "string"),
        new KpiColumn("grid_id", "string"),
        new KpiColumn("vendor", "string"),
        new KpiColumn("rat", "string"),
        new KpiColumn("band", "string"),
        new KpiColumn("arfcn", "int"),
        new KpiColumn("pci", "int"),
        new KpiColumn("site_id", "string"),
        new KpiColumn("longitude", "double"),
        new KpiColumn("latitude", "double"),
        new KpiColumn("rsrp_avg", "double"),
        new KpiColumn("rsrp_p10", "double"),
        new KpiColumn("rsrq_avg", "double"),
        new KpiColumn("sinr_avg", "double"),
        new KpiColumn("prb_dl_util", "double"),
        new KpiColumn("prb_ul_util", "double"),
        new KpiColumn("rrc_users", "int"),
        new KpiColumn("active_users", "int"),
        new KpiColumn("dl_traffic_mb", "double"),
        new KpiColumn("ul_traffic_mb", "double"),
        new KpiColumn("dl_throughput_mbps", "double"),
        new KpiColumn("ul_throughput_mbps", "double"),
        new KpiColumn("drop_rate", "double"),
        new KpiColumn("handover_success_rate", "double"),
        new KpiColumn("access_success_rate", "double"),
        new KpiColumn("volte_drop_rate", "double"),
        new KpiColumn("latency_ms", "double"),
        new KpiColumn("availability_rate", "double"),
        new KpiColumn("alarm_count", "int"),
        new KpiColumn("interference_score", "double"),
        new KpiColumn("load_score", "double"),
        new KpiColumn("packet_loss_rate", "double"),
        new KpiColumn("cqi_avg", "double"),
        new KpiColumn("mcs_avg", "double"),
        new KpiColumn("ta_avg", "double"),
        new KpiColumn("ul_noise_avg", "double"),
        new KpiColumn("connected_users_peak", "int"),
        new KpiColumn("rrc_setup_attempts", "int"),
        new KpiColumn("rrc_setup_successes", "int"),
        new KpiColumn("erab_setup_attempts", "int"),
        new KpiColumn("erab_setup_successes", "int"),
        new KpiColumn("handover_attempts", "int"),
        new KpiColumn("handover_successes", "int"),
        new KpiColumn("retransmission_rate", "double"),
        new KpiColumn("backhaul_util", "double"),
        new KpiColumn("energy_kwh", "double")
    );

    @Test
    void schemaHasExactly50ColumnsInSpecOrder() {
        assertThat(KpiSchema.columns()).containsExactlyElementsOf(EXPECTED_COLUMNS);
    }

    @Test
    void columnNamesMatchColumnOrder() {
        assertThat(KpiSchema.columnNames())
            .containsExactlyElementsOf(EXPECTED_COLUMNS.stream().map(KpiColumn::name).toList());
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
        assertThat(KpiSchema.tableShapes()).containsExactlyEntriesOf(expectedTableShapes());
        assertThat(KpiSchema.tableShapes().keySet()).containsExactly(
            "spark_iceberg",
            "starrocks_internal",
            "starrocks_external_iceberg"
        );
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

    private static Map<String, String> expectedTableShapes() {
        Map<String, String> tableShapes = new LinkedHashMap<>();
        tableShapes.put("spark_iceberg", "iceberg_db.cell_kpi_1min");
        tableShapes.put("starrocks_internal", "sr_internal.cell_kpi_1min");
        tableShapes.put("starrocks_external_iceberg", "sr_external_iceberg.cell_kpi_1min");
        return tableShapes;
    }
}

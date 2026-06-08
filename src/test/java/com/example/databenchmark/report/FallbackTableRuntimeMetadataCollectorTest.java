package com.example.databenchmark.report;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.runner.BenchmarkRoute;
import com.example.databenchmark.tpch.TpchSchema;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FallbackTableRuntimeMetadataCollectorTest {
    @Test
    void createsFallbackKpiMetadataForAllFiveRoutes() {
        FallbackTableRuntimeMetadataCollector collector = new FallbackTableRuntimeMetadataCollector();

        var infos = collector.collectKpi(Map.of(), 1_000_000_000L, 12_345L);

        assertThat(infos).hasSize(5);
        assertThat(infos)
            .extracting(BenchmarkReport.TableRuntimeInfo::route)
            .containsExactly(
                "spark_native_parquet",
                "spark_iceberg",
                "starrocks_internal",
                "starrocks_external_iceberg",
                "hive_hdfs_parquet"
            );
        BenchmarkReport.TableRuntimeInfo iceberg = infos.stream()
            .filter(info -> info.route().equals("spark_iceberg"))
            .findFirst()
            .orElseThrow();
        assertThat(iceberg.storageType()).isEqualTo("Iceberg");
        assertThat(iceberg.columns()).isEqualTo(50);
        assertThat(iceberg.tableIdentifier()).isEqualTo("iceberg_catalog.iceberg_db.cell_kpi_1min");
        BenchmarkReport.TableRuntimeInfo starrocks = infos.stream()
            .filter(info -> info.route().equals("starrocks_internal"))
            .findFirst()
            .orElseThrow();
        assertThat(starrocks.tableIdentifier()).isEqualTo("sr_internal.cell_kpi_1min_1b");
    }

    @Test
    void marksRouteMetadataUnavailableWhenRouteLoadFailed() {
        FallbackTableRuntimeMetadataCollector collector = new FallbackTableRuntimeMetadataCollector();

        var infos = collector.collectKpi(
            Map.of(BenchmarkRoute.STARROCKS_INTERNAL, "broker load failed"),
            100L,
            200L
        );

        BenchmarkReport.TableRuntimeInfo starrocks = infos.stream()
            .filter(info -> info.route().equals("starrocks_internal"))
            .findFirst()
            .orElseThrow();
        assertThat(starrocks.success()).isFalse();
        assertThat(starrocks.error()).contains("broker load failed");
    }

    @Test
    void marksRouteMetadataUnavailableWhenRouteLoadFailureMessageIsBlank() {
        FallbackTableRuntimeMetadataCollector collector = new FallbackTableRuntimeMetadataCollector();

        var infos = collector.collectKpi(
            Map.of(BenchmarkRoute.SPARK_ICEBERG, ""),
            100L,
            200L
        );

        BenchmarkReport.TableRuntimeInfo iceberg = infos.stream()
            .filter(info -> info.route().equals("spark_iceberg"))
            .findFirst()
            .orElseThrow();
        assertThat(iceberg.success()).isFalse();
        assertThat(iceberg.error()).isEmpty();
    }

    @Test
    void createsTpchMetadataForEveryRouteAndTable() {
        FallbackTableRuntimeMetadataCollector collector = new FallbackTableRuntimeMetadataCollector();

        var infos = collector.collectTpch(100L, 200L);

        assertThat(infos).hasSize(TpchSchema.enginePrefixes().size() * TpchSchema.tables().size());
        assertThat(infos)
            .anySatisfy(info -> {
                assertThat(info.route()).isEqualTo("spark_iceberg");
                assertThat(info.tableShape()).isEqualTo("spark_iceberg");
                assertThat(info.tableIdentifier()).isEqualTo("iceberg_catalog.tpch.lineitem");
                assertThat(info.columns()).isEqualTo(TpchSchema.table("lineitem").columns().size());
            });
    }

    @Test
    void createsTpchMetadataInDeterministicRouteAndTableOrder() {
        FallbackTableRuntimeMetadataCollector collector = new FallbackTableRuntimeMetadataCollector();

        var infos = collector.collectTpch(100L, 200L);
        int tableCount = TpchSchema.tables().size();
        List<String> routeSequence = infos.stream()
            .map(BenchmarkReport.TableRuntimeInfo::route)
            .distinct()
            .toList();

        assertThat(routeSequence).containsExactly(
            "spark_iceberg",
            "starrocks_internal",
            "starrocks_external_iceberg"
        );
        assertThat(infos.subList(0, tableCount))
            .extracting(BenchmarkReport.TableRuntimeInfo::tableIdentifier)
            .containsExactly(
                "iceberg_catalog.tpch.region",
                "iceberg_catalog.tpch.nation",
                "iceberg_catalog.tpch.supplier",
                "iceberg_catalog.tpch.customer",
                "iceberg_catalog.tpch.part",
                "iceberg_catalog.tpch.partsupp",
                "iceberg_catalog.tpch.orders",
                "iceberg_catalog.tpch.lineitem"
            );
    }

    @Test
    void createsTpchStarRocksInternalDistributionFromTableKeys() {
        FallbackTableRuntimeMetadataCollector collector = new FallbackTableRuntimeMetadataCollector();

        var starrocksInfos = collector.collectTpch(100L, 200L).stream()
            .filter(info -> info.route().equals("starrocks_internal"))
            .toList();

        assertThat(starrocksInfos)
            .extracting(BenchmarkReport.TableRuntimeInfo::bucketingOrDistribution)
            .noneSatisfy(distribution -> assertThat(distribution).contains("cell_id"));
        assertThat(starrocksInfos)
            .filteredOn(info -> info.tableIdentifier().equals("sr_internal_tpch.region"))
            .singleElement()
            .extracting(BenchmarkReport.TableRuntimeInfo::bucketingOrDistribution)
            .isEqualTo("HASH(r_regionkey)");
        assertThat(starrocksInfos)
            .filteredOn(info -> info.tableIdentifier().equals("sr_internal_tpch.lineitem"))
            .singleElement()
            .extracting(BenchmarkReport.TableRuntimeInfo::bucketingOrDistribution)
            .isEqualTo("HASH(l_orderkey, l_linenumber)");
    }
}

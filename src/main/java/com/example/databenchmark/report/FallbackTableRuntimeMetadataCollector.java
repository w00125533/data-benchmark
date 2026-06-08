package com.example.databenchmark.report;

import com.example.databenchmark.runner.BenchmarkRoute;
import com.example.databenchmark.schema.KpiSchema;
import com.example.databenchmark.schema.KpiTableNaming;
import com.example.databenchmark.tpch.TpchSchema;
import com.example.databenchmark.tpch.TpchTable;
import java.util.List;
import java.util.Map;

public class FallbackTableRuntimeMetadataCollector implements TableRuntimeMetadataCollector {
    @Override
    public List<BenchmarkReport.TableRuntimeInfo> collectKpi(
        Map<BenchmarkRoute, String> routeFailures,
        long rows,
        long bytes
    ) {
        Map<BenchmarkRoute, String> failures = routeFailures == null ? Map.of() : routeFailures;
        return KpiTableNaming.tableShapesForRows(rows).entrySet().stream()
            .map(entry -> fallbackKpiInfo(entry.getKey(), entry.getValue(), failures, bytes))
            .toList();
    }

    @Override
    public List<BenchmarkReport.TableRuntimeInfo> collectTpch(long rows, long bytes) {
        return TpchSchema.enginePrefixes().entrySet().stream()
            .flatMap(route -> TpchSchema.tables().stream()
                .map(table -> fallbackTpchInfo(route.getKey(), route.getValue(), table, bytes)))
            .toList();
    }

    private BenchmarkReport.TableRuntimeInfo fallbackKpiInfo(
        String route,
        String tableIdentifier,
        Map<BenchmarkRoute, String> routeFailures,
        long bytes
    ) {
        BenchmarkRoute benchmarkRoute = benchmarkRoute(route);
        boolean failed = routeFailures.containsKey(benchmarkRoute);
        String error = routeFailures.get(benchmarkRoute);
        if (error == null) {
            error = "";
        }
        return new BenchmarkReport.TableRuntimeInfo(
            route,
            displayName(route),
            route,
            tableIdentifier,
            storageType(route),
            "",
            format(route),
            KpiSchema.columns().size(),
            partitioning(route),
            kpiBucketingOrDistribution(route),
            "",
            "",
            0L,
            0L,
            "",
            !failed,
            error
        );
    }

    private BenchmarkReport.TableRuntimeInfo fallbackTpchInfo(
        String route,
        String prefix,
        TpchTable table,
        long bytes
    ) {
        return new BenchmarkReport.TableRuntimeInfo(
            route,
            displayName(route),
            route,
            prefix + table.name(),
            storageType(route),
            "",
            format(route),
            table.columns().size(),
            "",
            tpchBucketingOrDistribution(route, table),
            "",
            "",
            0L,
            0L,
            "",
            true,
            ""
        );
    }

    private static BenchmarkRoute benchmarkRoute(String route) {
        return switch (route) {
            case "spark_native_parquet" -> BenchmarkRoute.SPARK_NATIVE_PARQUET;
            case "spark_iceberg" -> BenchmarkRoute.SPARK_ICEBERG;
            case "starrocks_internal" -> BenchmarkRoute.STARROCKS_INTERNAL;
            case "starrocks_external_iceberg" -> BenchmarkRoute.STARROCKS_EXTERNAL_ICEBERG;
            case "hive_hdfs_parquet" -> BenchmarkRoute.HIVE_HDFS_PARQUET;
            default -> throw new IllegalArgumentException("Unknown benchmark route: " + route);
        };
    }

    private static String displayName(String route) {
        return switch (route) {
            case "spark_native_parquet" -> "Spark SQL Native Parquet";
            case "spark_iceberg" -> "Spark Iceberg";
            case "starrocks_internal" -> "StarRocks Internal";
            case "starrocks_external_iceberg" -> "StarRocks External Iceberg";
            case "hive_hdfs_parquet" -> "Hive HDFS Parquet";
            default -> route;
        };
    }

    private static String storageType(String route) {
        return switch (route) {
            case "spark_native_parquet" -> "Spark SQL Native Parquet";
            case "spark_iceberg" -> "Iceberg";
            case "starrocks_internal" -> "StarRocks Internal";
            case "starrocks_external_iceberg" -> "StarRocks External Iceberg";
            case "hive_hdfs_parquet" -> "Hive External Parquet";
            default -> "";
        };
    }

    private static String format(String route) {
        return switch (route) {
            case "spark_native_parquet", "spark_iceberg", "starrocks_external_iceberg", "hive_hdfs_parquet" -> "Parquet";
            default -> "";
        };
    }

    private static String partitioning(String route) {
        return switch (route) {
            case "spark_iceberg" -> "days(event_time)";
            case "hive_hdfs_parquet" -> "event_date STRING";
            default -> "";
        };
    }

    private static String kpiBucketingOrDistribution(String route) {
        return "starrocks_internal".equals(route) ? "HASH(cell_id)" : "";
    }

    private static String tpchBucketingOrDistribution(String route, TpchTable table) {
        if (!"starrocks_internal".equals(route)) {
            return "";
        }
        return "HASH(" + tpchStarRocksKeyColumns(table) + ")";
    }

    private static String tpchStarRocksKeyColumns(TpchTable table) {
        return switch (table.name()) {
            case "region" -> "r_regionkey";
            case "nation" -> "n_nationkey";
            case "supplier" -> "s_suppkey";
            case "customer" -> "c_custkey";
            case "part" -> "p_partkey";
            case "partsupp" -> "ps_partkey, ps_suppkey";
            case "orders" -> "o_orderkey";
            case "lineitem" -> "l_orderkey, l_linenumber";
            default -> throw new IllegalArgumentException("Unknown TPC-H table: " + table.name());
        };
    }
}

package com.example.databenchmark.tpch;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TpchSchema {
    static final Map<String, String> ENGINE_PREFIXES = enginePrefixesInRouteOrder();

    private static final List<TpchTable> TABLES = List.of(
        table("region", 5, col("r_regionkey", "long"), col("r_name", "string"), col("r_comment", "string")),
        table("nation", 25, col("n_nationkey", "long"), col("n_name", "string"), col("n_regionkey", "long"), col("n_comment", "string")),
        table("supplier", 100, col("s_suppkey", "long"), col("s_name", "string"), col("s_address", "string"), col("s_nationkey", "long"), col("s_phone", "string"), col("s_acctbal", "double"), col("s_comment", "string")),
        table("customer", 1500, col("c_custkey", "long"), col("c_name", "string"), col("c_address", "string"), col("c_nationkey", "long"), col("c_phone", "string"), col("c_acctbal", "double"), col("c_mktsegment", "string"), col("c_comment", "string")),
        table("part", 2000, col("p_partkey", "long"), col("p_name", "string"), col("p_mfgr", "string"), col("p_brand", "string"), col("p_type", "string"), col("p_size", "int"), col("p_container", "string"), col("p_retailprice", "double"), col("p_comment", "string")),
        table("partsupp", 8000, col("ps_partkey", "long"), col("ps_suppkey", "long"), col("ps_availqty", "int"), col("ps_supplycost", "double"), col("ps_comment", "string")),
        table("orders", 15000, col("o_orderkey", "long"), col("o_custkey", "long"), col("o_orderstatus", "string"), col("o_totalprice", "double"), col("o_orderdate", "date"), col("o_orderpriority", "string"), col("o_clerk", "string"), col("o_shippriority", "int"), col("o_comment", "string")),
        table("lineitem", 60000, col("l_orderkey", "long"), col("l_partkey", "long"), col("l_suppkey", "long"), col("l_linenumber", "int"), col("l_quantity", "double"), col("l_extendedprice", "double"), col("l_discount", "double"), col("l_tax", "double"), col("l_returnflag", "string"), col("l_linestatus", "string"), col("l_shipdate", "date"), col("l_commitdate", "date"), col("l_receiptdate", "date"), col("l_shipinstruct", "string"), col("l_shipmode", "string"), col("l_comment", "string"))
    );

    private TpchSchema() {}

    public static List<TpchTable> tables() {
        return TABLES;
    }

    public static Map<String, String> enginePrefixes() {
        return ENGINE_PREFIXES;
    }

    public static TpchTable table(String name) {
        return TABLES.stream()
            .filter(table -> table.name().equals(name))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown TPC-H table: " + name));
    }

    public static String tableName(String table, String engineKey) {
        table(table);
        String prefix = ENGINE_PREFIXES.get(engineKey);
        if (prefix == null) {
            throw new IllegalArgumentException("Unknown engine key: " + engineKey);
        }
        return prefix + table;
    }

    private static TpchTable table(String name, long baseRows, TpchColumn... columns) {
        return new TpchTable(name, baseRows, List.of(columns));
    }

    private static TpchColumn col(String name, String logicalType) {
        return new TpchColumn(name, logicalType);
    }

    private static Map<String, String> enginePrefixesInRouteOrder() {
        Map<String, String> prefixes = new LinkedHashMap<>();
        prefixes.put("spark_iceberg", "iceberg_catalog.tpch.");
        prefixes.put("starrocks_internal", "sr_internal_tpch.");
        prefixes.put("starrocks_external_iceberg", "sr_external_iceberg.tpch.");
        return Collections.unmodifiableMap(prefixes);
    }
}

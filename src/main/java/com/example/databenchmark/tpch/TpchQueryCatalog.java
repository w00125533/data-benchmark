package com.example.databenchmark.tpch;

import java.util.List;
import java.util.Set;

public final class TpchQueryCatalog {
    private static final Set<String> SMOKE = Set.of("smoke", "all");
    private static final Set<String> ALL = Set.of("all");

    private static final List<TpchQuery> QUERIES = List.of(
        query("q01_pricing_summary_report", SMOKE, "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM {lineitem} WHERE l_shipdate <= DATE '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"),
        query("q02_minimum_cost_supplier", ALL, "SELECT s.s_acctbal, s.s_name, n.n_name, p.p_partkey FROM {part} p JOIN {partsupp} ps ON p.p_partkey = ps.ps_partkey JOIN {supplier} s ON s.s_suppkey = ps.ps_suppkey JOIN {nation} n ON s.s_nationkey = n.n_nationkey WHERE p.p_size = 15 ORDER BY s.s_acctbal DESC LIMIT 100"),
        query("q03_shipping_priority", SMOKE, "SELECT l.l_orderkey, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, o.o_orderdate, o.o_shippriority FROM {customer} c JOIN {orders} o ON c.c_custkey = o.o_custkey JOIN {lineitem} l ON l.l_orderkey = o.o_orderkey WHERE c.c_mktsegment = 'BUILDING' AND o.o_orderdate < DATE '1995-03-15' AND l.l_shipdate > DATE '1995-03-15' GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority ORDER BY revenue DESC, o.o_orderdate LIMIT 10"),
        query("q04_order_priority_checking", ALL, "SELECT o_orderpriority, COUNT(*) AS order_count FROM {orders} WHERE o_orderdate >= DATE '1993-07-01' AND o_orderdate < DATE '1993-10-01' GROUP BY o_orderpriority ORDER BY o_orderpriority"),
        query("q05_local_supplier_volume", SMOKE, "SELECT n.n_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue FROM {customer} c JOIN {orders} o ON c.c_custkey = o.o_custkey JOIN {lineitem} l ON l.l_orderkey = o.o_orderkey JOIN {supplier} s ON l.l_suppkey = s.s_suppkey JOIN {nation} n ON c.c_nationkey = n.n_nationkey WHERE c.c_nationkey = s.s_nationkey AND o.o_orderdate >= DATE '1994-01-01' AND o.o_orderdate < DATE '1995-01-01' GROUP BY n.n_name ORDER BY revenue DESC"),
        query("q06_forecast_revenue_change", ALL, "SELECT SUM(l_extendedprice * l_discount) AS revenue FROM {lineitem} WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"),
        query("q07_volume_shipping", ALL, "SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, EXTRACT(YEAR FROM l.l_shipdate) AS l_year, l.l_extendedprice * (1 - l.l_discount) AS volume FROM {supplier} s JOIN {lineitem} l ON s.s_suppkey = l.l_suppkey JOIN {orders} o ON o.o_orderkey = l.l_orderkey JOIN {customer} c ON c.c_custkey = o.o_custkey JOIN {nation} n1 ON s.s_nationkey = n1.n_nationkey JOIN {nation} n2 ON c.c_nationkey = n2.n_nationkey WHERE l.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year"),
        query("q08_national_market_share", ALL, "SELECT o_year, SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share FROM (SELECT EXTRACT(YEAR FROM o.o_orderdate) AS o_year, l.l_extendedprice * (1 - l.l_discount) AS volume, n2.n_name AS nation FROM {part} p JOIN {lineitem} l ON p.p_partkey = l.l_partkey JOIN {supplier} s ON s.s_suppkey = l.l_suppkey JOIN {orders} o ON o.o_orderkey = l.l_orderkey JOIN {customer} c ON c.c_custkey = o.o_custkey JOIN {nation} n1 ON c.c_nationkey = n1.n_nationkey JOIN {nation} n2 ON s.s_nationkey = n2.n_nationkey WHERE o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') all_nations GROUP BY o_year ORDER BY o_year"),
        query("q09_product_type_profit", ALL, "SELECT nation, o_year, SUM(amount) AS sum_profit FROM (SELECT n.n_name AS nation, EXTRACT(YEAR FROM o.o_orderdate) AS o_year, l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity AS amount FROM {part} p JOIN {lineitem} l ON p.p_partkey = l.l_partkey JOIN {partsupp} ps ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey JOIN {orders} o ON o.o_orderkey = l.l_orderkey JOIN {supplier} s ON s.s_suppkey = l.l_suppkey JOIN {nation} n ON s.s_nationkey = n.n_nationkey) profit GROUP BY nation, o_year ORDER BY nation, o_year DESC"),
        query("q10_returned_item_reporting", SMOKE, "SELECT c.c_custkey, c.c_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, c.c_acctbal, n.n_name FROM {customer} c JOIN {orders} o ON c.c_custkey = o.o_custkey JOIN {lineitem} l ON l.l_orderkey = o.o_orderkey JOIN {nation} n ON c.c_nationkey = n.n_nationkey WHERE o.o_orderdate >= DATE '1993-10-01' AND o.o_orderdate < DATE '1994-01-01' AND l.l_returnflag = 'R' GROUP BY c.c_custkey, c.c_name, c.c_acctbal, n.n_name ORDER BY revenue DESC LIMIT 20"),
        query("q11_important_stock_identification", ALL, "SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value FROM {partsupp} GROUP BY ps_partkey ORDER BY value DESC LIMIT 100"),
        query("q12_shipping_modes", ALL, "SELECT l_shipmode, SUM(CASE WHEN o_orderpriority IN ('1-URGENT','2-HIGH') THEN 1 ELSE 0 END) AS high_line_count, SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT','2-HIGH') THEN 1 ELSE 0 END) AS low_line_count FROM {orders} o JOIN {lineitem} l ON o.o_orderkey = l.l_orderkey WHERE l.l_shipdate >= DATE '1994-01-01' AND l.l_shipdate < DATE '1995-01-01' GROUP BY l_shipmode ORDER BY l_shipmode"),
        query("q13_customer_distribution", ALL, "SELECT c_count, COUNT(*) AS custdist FROM (SELECT c.c_custkey, COUNT(o.o_orderkey) AS c_count FROM {customer} c LEFT JOIN {orders} o ON c.c_custkey = o.o_custkey GROUP BY c.c_custkey) c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC"),
        query("q14_promotion_effect", ALL, "SELECT 100.00 * SUM(CASE WHEN p.p_type LIKE 'PROMO%' THEN l.l_extendedprice * (1 - l.l_discount) ELSE 0 END) / SUM(l.l_extendedprice * (1 - l.l_discount)) AS promo_revenue FROM {lineitem} l JOIN {part} p ON l.l_partkey = p.p_partkey WHERE l.l_shipdate >= DATE '1995-09-01' AND l.l_shipdate < DATE '1995-10-01'"),
        query("q15_top_supplier", ALL, "SELECT s.s_suppkey, s.s_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue FROM {supplier} s JOIN {lineitem} l ON s.s_suppkey = l.l_suppkey WHERE l.l_shipdate >= DATE '1996-01-01' AND l.l_shipdate < DATE '1996-04-01' GROUP BY s.s_suppkey, s.s_name ORDER BY total_revenue DESC LIMIT 10"),
        query("q16_parts_supplier_relationship", ALL, "SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt FROM {partsupp} ps JOIN {part} p ON p.p_partkey = ps.ps_partkey GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size LIMIT 100"),
        query("q17_small_quantity_order_revenue", ALL, "SELECT SUM(l.l_extendedprice) / 7.0 AS avg_yearly FROM {lineitem} l JOIN {part} p ON p.p_partkey = l.l_partkey WHERE p.p_brand = 'Brand#23'"),
        query("q18_large_volume_customer", ALL, "SELECT c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice, SUM(l.l_quantity) AS sum_quantity FROM {customer} c JOIN {orders} o ON c.c_custkey = o.o_custkey JOIN {lineitem} l ON o.o_orderkey = l.l_orderkey GROUP BY c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice HAVING SUM(l.l_quantity) > 100 ORDER BY o.o_totalprice DESC, o.o_orderdate LIMIT 100"),
        query("q19_discounted_revenue", ALL, "SELECT SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue FROM {lineitem} l JOIN {part} p ON p.p_partkey = l.l_partkey WHERE p.p_brand IN ('Brand#12','Brand#23','Brand#34') AND l.l_quantity BETWEEN 1 AND 30"),
        query("q20_potential_part_promotion", ALL, "SELECT s.s_name, s.s_address FROM {supplier} s JOIN {nation} n ON s.s_nationkey = n.n_nationkey WHERE s.s_suppkey IN (SELECT ps.ps_suppkey FROM {partsupp} ps WHERE ps.ps_availqty > 100) ORDER BY s.s_name LIMIT 100"),
        query("q21_suppliers_who_kept_orders_waiting", ALL, "SELECT s.s_name, COUNT(*) AS numwait FROM {supplier} s JOIN {lineitem} l ON s.s_suppkey = l.l_suppkey JOIN {orders} o ON o.o_orderkey = l.l_orderkey WHERE o.o_orderstatus = 'F' GROUP BY s.s_name ORDER BY numwait DESC, s.s_name LIMIT 100"),
        query("q22_global_sales_opportunity", ALL, "SELECT SUBSTRING(c_phone, 1, 2) AS cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal FROM {customer} WHERE c_acctbal > 0 GROUP BY SUBSTRING(c_phone, 1, 2) ORDER BY cntrycode")
    );

    private TpchQueryCatalog() {}

    public static List<TpchQuery> queries(String querySet) {
        if (!"smoke".equals(querySet) && !"all".equals(querySet)) {
            throw new IllegalArgumentException("Unknown TPC-H query set: " + querySet);
        }
        return QUERIES.stream()
            .filter(query -> query.querySets().contains(querySet))
            .toList();
    }

    public static TpchQuery query(String name) {
        return QUERIES.stream()
            .filter(query -> query.name().equals(name))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown TPC-H query: " + name));
    }

    private static TpchQuery query(String name, Set<String> querySets, String template) {
        return new TpchQuery(name, template, querySets);
    }
}

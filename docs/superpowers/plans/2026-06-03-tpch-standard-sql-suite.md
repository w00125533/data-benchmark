# TPC-H Standard SQL Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Java 17 TPC-H-compatible SQL performance validation suite for Spark Iceberg, StarRocks internal tables, and StarRocks external Iceberg tables.

**Architecture:** Keep the existing KPI benchmark as the default suite. Add suite-aware config, TPC-H metadata, deterministic smoke data generation, suite-aware SQL rendering, and compose runner branching for TPC-H. Reuse the existing report, metrics, HDFS-backed Iceberg warehouse, Spark client, and StarRocks client patterns.

**Tech Stack:** Java 17, Maven, JUnit 5, AssertJ, Apache Parquet Avro, Hadoop local configuration, Spark SQL, StarRocks JDBC/Stream Load, Prometheus/Micrometer, Grafana.

---

## File Structure

- Modify `src/main/java/com/example/databenchmark/config/BenchmarkConfig.java`: add `SuiteConfig` with defaults for `kpi`, TPC-H scale factor, and query set.
- Modify `src/main/java/com/example/databenchmark/config/BenchmarkConfigLoader.java`: validate suite fields.
- Create `configs/benchmark-tpch-smoke.yml`: smoke profile selecting `tpch`.
- Create `src/main/java/com/example/databenchmark/tpch/TpchColumn.java`: TPC-H column metadata.
- Create `src/main/java/com/example/databenchmark/tpch/TpchTable.java`: TPC-H table metadata and row-count scaling.
- Create `src/main/java/com/example/databenchmark/tpch/TpchSchema.java`: all 8 TPC-H table definitions and engine table names.
- Create `src/main/java/com/example/databenchmark/tpch/TpchDatasetResult.java`: table-oriented generated output summary.
- Create `src/main/java/com/example/databenchmark/tpch/TpchDataGenerator.java`: deterministic TPC-H-compatible smoke generator.
- Create `src/main/java/com/example/databenchmark/tpch/TpchCsvExporter.java`: table CSV writer for StarRocks internal load.
- Create `src/main/java/com/example/databenchmark/tpch/TpchQueryCatalog.java`: TPC-H query definitions and query-set filtering.
- Create `src/main/java/com/example/databenchmark/tpch/TpchSqlTemplates.java`: Spark and StarRocks DDL for TPC-H tables.
- Create `src/main/java/com/example/databenchmark/tpch/TpchSqlRenderer.java`: table-token replacement and engine-specific literal adaptation.
- Modify `src/main/java/com/example/databenchmark/engine/SparkIcebergClient.java`: add TPC-H table load and query entry points.
- Modify `src/main/java/com/example/databenchmark/engine/StarRocksClient.java`: add TPC-H internal load, external refresh, and query entry points.
- Modify `src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java`: branch by suite and run TPC-H flow.
- Modify `src/main/java/com/example/databenchmark/metrics/BenchmarkMetrics.java`: add `suite` and `query_set` labels.
- Modify `src/main/java/com/example/databenchmark/report/BenchmarkReport.java`: include suite and query set in report model.
- Modify `src/main/resources/report.ftl`: render suite and query set.
- Modify Grafana dashboard JSON under monitoring resources if present; otherwise update the compose-mounted dashboard file found by `rg -n "benchmark_query_duration_seconds|templating|run_id" .`.
- Add focused tests under `src/test/java/com/example/databenchmark/tpch`.
- Modify existing config, engine, runner, metrics, and report tests to preserve KPI behavior.

---

### Task 1: Add Suite Config

**Files:**
- Modify: `src/main/java/com/example/databenchmark/config/BenchmarkConfig.java`
- Modify: `src/main/java/com/example/databenchmark/config/BenchmarkConfigLoader.java`
- Test: `src/test/java/com/example/databenchmark/config/BenchmarkConfigLoaderTest.java`
- Create: `configs/benchmark-tpch-smoke.yml`

- [ ] **Step 1: Write failing config tests**

Add tests to `BenchmarkConfigLoaderTest`:

```java
@Test
void defaultSmokeUsesKpiSuite() {
    BenchmarkConfig config = BenchmarkConfig.defaultSmoke();

    assertThat(config.suite().name()).isEqualTo("kpi");
    assertThat(config.suite().scaleFactor()).isEqualByComparingTo("0.01");
    assertThat(config.suite().querySet()).isEqualTo("smoke");
}

@Test
void loadsTpchSmokeSuite() throws Exception {
    BenchmarkConfig config = new BenchmarkConfigLoader().load(Path.of("configs/benchmark-tpch-smoke.yml"));

    assertThat(config.suite().name()).isEqualTo("tpch");
    assertThat(config.suite().scaleFactor()).isEqualByComparingTo("0.01");
    assertThat(config.suite().querySet()).isEqualTo("smoke");
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
mvn -Dtest=BenchmarkConfigLoaderTest test
```

Expected: compilation fails because `suite()` and `SuiteConfig` do not exist.

- [ ] **Step 3: Implement config record**

Update `BenchmarkConfig` constructor fields to include `SuiteConfig suite` after `seed`:

```java
public record BenchmarkConfig(
    String profile,
    long seed,
    SuiteConfig suite,
    DatasetConfig dataset,
    QueryConfig query,
    ReportConfig report,
    MonitoringConfig monitoring
) {
    public static BenchmarkConfig defaultSmoke() {
        return new BenchmarkConfig(
            "smoke",
            20260602L,
            SuiteConfig.defaultSuite(),
            new DatasetConfig(10_000, 1, 50, "2026-01-01T00:00:00", "data/generated", 10_000L),
            new QueryConfig(1, 3, 1),
            new ReportConfig("html", "reports/runs"),
            new MonitoringConfig(true, true)
        );
    }

    public record SuiteConfig(String name, java.math.BigDecimal scaleFactor, String querySet) {
        public static SuiteConfig defaultSuite() {
            return new SuiteConfig("kpi", new java.math.BigDecimal("0.01"), "smoke");
        }
    }
}
```

Update every `new BenchmarkConfig(...)` call in this file to pass `suite`.

- [ ] **Step 4: Implement validation**

Add to `BenchmarkConfigLoader.validate`:

```java
validateSuite(config.suite());
```

Add helper:

```java
private static void validateSuite(BenchmarkConfig.SuiteConfig suite) {
    if (suite == null) {
        throw new IllegalArgumentException("suite must not be null");
    }
    requireNonBlank(suite.name(), "suite.name");
    requireNonBlank(suite.querySet(), "suite.querySet");
    if (!suite.name().equals("kpi") && !suite.name().equals("tpch")) {
        throw new IllegalArgumentException("suite.name must be kpi or tpch");
    }
    if (suite.scaleFactor() == null || suite.scaleFactor().signum() <= 0) {
        throw new IllegalArgumentException("suite.scaleFactor must be positive");
    }
    if (!suite.querySet().equals("smoke") && !suite.querySet().equals("all")) {
        throw new IllegalArgumentException("suite.querySet must be smoke or all");
    }
}
```

- [ ] **Step 5: Add TPC-H smoke config**

Create `configs/benchmark-tpch-smoke.yml`:

```yaml
profile: tpch-smoke
seed: 20260602
suite:
  name: tpch
  scaleFactor: 0.01
  querySet: smoke
dataset:
  cells: 10000
  days: 1
  columns: 50
  startTime: "2026-01-01T00:00:00"
  output: "data/generated"
  rowCap: 10000
query:
  coldRuns: 1
  warmRuns: 1
  concurrency: 1
report:
  format: html
  output: "reports/runs"
monitoring:
  prometheus: true
  grafana: true
```

- [ ] **Step 6: Run tests and commit**

Run:

```powershell
mvn -Dtest=BenchmarkConfigLoaderTest test
```

Expected: tests pass.

Commit:

```powershell
git add src/main/java/com/example/databenchmark/config/BenchmarkConfig.java src/main/java/com/example/databenchmark/config/BenchmarkConfigLoader.java src/test/java/com/example/databenchmark/config/BenchmarkConfigLoaderTest.java configs/benchmark-tpch-smoke.yml
git commit -m "feat: add benchmark suite config"
```

---

### Task 2: Add TPC-H Schema Metadata

**Files:**
- Create: `src/main/java/com/example/databenchmark/tpch/TpchColumn.java`
- Create: `src/main/java/com/example/databenchmark/tpch/TpchTable.java`
- Create: `src/main/java/com/example/databenchmark/tpch/TpchSchema.java`
- Test: `src/test/java/com/example/databenchmark/tpch/TpchSchemaTest.java`

- [ ] **Step 1: Write failing schema tests**

Create `TpchSchemaTest`:

```java
package com.example.databenchmark.tpch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TpchSchemaTest {
    @Test
    void containsAllTpchTables() {
        assertThat(TpchSchema.tables())
            .extracting(TpchTable::name)
            .containsExactly("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem");
    }

    @Test
    void lineitemContainsJoinAndMeasureColumns() {
        TpchTable lineitem = TpchSchema.table("lineitem");

        assertThat(lineitem.columns())
            .extracting(TpchColumn::name)
            .contains("l_orderkey", "l_partkey", "l_suppkey", "l_quantity", "l_extendedprice", "l_shipdate");
    }

    @Test
    void tableNamesAreSeparatedByEngineShape() {
        assertThat(TpchSchema.tableName("lineitem", "spark_iceberg")).isEqualTo("iceberg_catalog.tpch.lineitem");
        assertThat(TpchSchema.tableName("lineitem", "starrocks_internal")).isEqualTo("sr_internal_tpch.lineitem");
        assertThat(TpchSchema.tableName("lineitem", "starrocks_external_iceberg")).isEqualTo("sr_external_iceberg.tpch.lineitem");
    }
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```powershell
mvn -Dtest=TpchSchemaTest test
```

Expected: compilation fails because TPC-H classes do not exist.

- [ ] **Step 3: Add schema records**

Create `TpchColumn`:

```java
package com.example.databenchmark.tpch;

public record TpchColumn(String name, String logicalType) {}
```

Create `TpchTable`:

```java
package com.example.databenchmark.tpch;

import java.util.List;

public record TpchTable(String name, long baseRows, List<TpchColumn> columns) {}
```

- [ ] **Step 4: Add TpchSchema**

Create `TpchSchema` with all table definitions:

```java
package com.example.databenchmark.tpch;

import java.util.List;
import java.util.Map;

public final class TpchSchema {
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

    public static TpchTable table(String name) {
        return TABLES.stream()
            .filter(table -> table.name().equals(name))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown TPC-H table: " + name));
    }

    public static String tableName(String table, String engineKey) {
        Map<String, String> prefixes = Map.of(
            "spark_iceberg", "iceberg_catalog.tpch.",
            "starrocks_internal", "sr_internal_tpch.",
            "starrocks_external_iceberg", "sr_external_iceberg.tpch."
        );
        String prefix = prefixes.get(engineKey);
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
}
```

- [ ] **Step 5: Run tests and commit**

Run:

```powershell
mvn -Dtest=TpchSchemaTest test
```

Expected: tests pass.

Commit:

```powershell
git add src/main/java/com/example/databenchmark/tpch src/test/java/com/example/databenchmark/tpch/TpchSchemaTest.java
git commit -m "feat: add tpch schema metadata"
```

---

### Task 3: Add TPC-H Query Catalog And Renderer

**Files:**
- Create: `src/main/java/com/example/databenchmark/tpch/TpchQuery.java`
- Create: `src/main/java/com/example/databenchmark/tpch/TpchQueryCatalog.java`
- Create: `src/main/java/com/example/databenchmark/tpch/TpchSqlRenderer.java`
- Test: `src/test/java/com/example/databenchmark/tpch/TpchQueryCatalogTest.java`
- Test: `src/test/java/com/example/databenchmark/tpch/TpchSqlRendererTest.java`

- [ ] **Step 1: Write failing query catalog tests**

Create `TpchQueryCatalogTest`:

```java
package com.example.databenchmark.tpch;

import static org.assertj.core.api.Assertions.assertThat;

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
}
```

Create `TpchSqlRendererTest`:

```java
package com.example.databenchmark.tpch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TpchSqlRendererTest {
    @Test
    void rendersSparkTablePlaceholders() {
        String sql = TpchSqlRenderer.render("q03_shipping_priority", "spark_iceberg");

        assertThat(sql).contains("iceberg_catalog.tpch.customer");
        assertThat(sql).contains("iceberg_catalog.tpch.orders");
        assertThat(sql).contains("iceberg_catalog.tpch.lineitem");
        assertThat(sql).doesNotContain("{customer}");
    }

    @Test
    void rendersStarRocksDateLiterals() {
        String sql = TpchSqlRenderer.render("q03_shipping_priority", "starrocks_internal");

        assertThat(sql).contains("sr_internal_tpch.customer");
        assertThat(sql).contains("CAST('1995-03-15' AS DATE)");
        assertThat(sql).doesNotContain("DATE '1995-03-15'");
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
mvn -Dtest=TpchQueryCatalogTest,TpchSqlRendererTest test
```

Expected: compilation fails because query catalog and renderer do not exist.

- [ ] **Step 3: Add TpchQuery record**

Create `TpchQuery`:

```java
package com.example.databenchmark.tpch;

import java.util.Set;

public record TpchQuery(String name, String template, Set<String> querySets) {}
```

- [ ] **Step 4: Add TpchQueryCatalog**

Create `TpchQueryCatalog` with 22 query names. For the first implementation, include compact TPC-H-compatible templates that exercise the same table relationships and SQL shapes:

```java
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
```

- [ ] **Step 5: Add TpchSqlRenderer**

Create `TpchSqlRenderer`:

```java
package com.example.databenchmark.tpch;

import java.util.regex.Pattern;

public final class TpchSqlRenderer {
    private static final Pattern DATE_LITERAL = Pattern.compile("DATE '([^']+)'");

    private TpchSqlRenderer() {}

    public static String render(String queryName, String engineKey) {
        String sql = TpchQueryCatalog.query(queryName).template();
        for (TpchTable table : TpchSchema.tables()) {
            sql = sql.replace("{" + table.name() + "}", TpchSchema.tableName(table.name(), engineKey));
        }
        if (engineKey.startsWith("starrocks")) {
            sql = DATE_LITERAL.matcher(sql).replaceAll("CAST('$1' AS DATE)");
        }
        return sql;
    }
}
```

- [ ] **Step 6: Run tests and commit**

Run:

```powershell
mvn -Dtest=TpchQueryCatalogTest,TpchSqlRendererTest test
```

Expected: tests pass.

Commit:

```powershell
git add src/main/java/com/example/databenchmark/tpch src/test/java/com/example/databenchmark/tpch
git commit -m "feat: add tpch query catalog"
```

---

### Task 4: Add TPC-H DDL Templates

**Files:**
- Create: `src/main/java/com/example/databenchmark/tpch/TpchSqlTemplates.java`
- Test: `src/test/java/com/example/databenchmark/tpch/TpchSqlTemplatesTest.java`

- [ ] **Step 1: Write failing DDL tests**

Create `TpchSqlTemplatesTest`:

```java
package com.example.databenchmark.tpch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TpchSqlTemplatesTest {
    @Test
    void rendersSparkIcebergTableDdl() {
        String ddl = TpchSqlTemplates.sparkCreateTable(TpchSchema.table("orders"));

        assertThat(ddl).contains("CREATE TABLE IF NOT EXISTS iceberg_catalog.tpch.orders");
        assertThat(ddl).contains("o_orderkey BIGINT");
        assertThat(ddl).contains("USING iceberg");
    }

    @Test
    void rendersStarRocksInternalTableDdl() {
        String ddl = TpchSqlTemplates.starRocksCreateInternalTable(TpchSchema.table("lineitem"));

        assertThat(ddl).contains("CREATE TABLE IF NOT EXISTS sr_internal_tpch.lineitem");
        assertThat(ddl).contains("l_orderkey BIGINT");
        assertThat(ddl).contains("DISTRIBUTED BY HASH");
    }
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```powershell
mvn -Dtest=TpchSqlTemplatesTest test
```

Expected: compilation fails because `TpchSqlTemplates` does not exist.

- [ ] **Step 3: Implement DDL templates**

Create `TpchSqlTemplates`:

```java
package com.example.databenchmark.tpch;

import java.util.stream.Collectors;

public final class TpchSqlTemplates {
    private static final String HDFS_WAREHOUSE = "hdfs://hdfs-namenode:8020/warehouse/iceberg";

    private TpchSqlTemplates() {}

    public static String sparkCreateDatabase() {
        return "CREATE DATABASE IF NOT EXISTS iceberg_catalog.tpch LOCATION '" + HDFS_WAREHOUSE + "/tpch';";
    }

    public static String sparkCreateTable(TpchTable table) {
        return """
            CREATE TABLE IF NOT EXISTS %s (
            %s
            )
            USING iceberg
            LOCATION '%s/tpch/%s';
            """.formatted(
            TpchSchema.tableName(table.name(), "spark_iceberg"),
            columns(table, true),
            HDFS_WAREHOUSE,
            table.name()
        );
    }

    public static String sparkInsertFromParquet(TpchTable table, String parquetPath) {
        String view = "generated_tpch_" + table.name();
        return """
            CREATE OR REPLACE TEMPORARY VIEW %s
            USING parquet
            OPTIONS (path '%s');

            INSERT INTO %s
            SELECT * FROM %s;
            """.formatted(view, parquetPath.replace("'", "''"), TpchSchema.tableName(table.name(), "spark_iceberg"), view);
    }

    public static String starRocksCreateDatabase() {
        return "CREATE DATABASE IF NOT EXISTS sr_internal_tpch;";
    }

    public static String starRocksCreateInternalTable(TpchTable table) {
        String key = switch (table.name()) {
            case "region" -> "r_regionkey";
            case "nation" -> "n_nationkey";
            case "supplier" -> "s_suppkey";
            case "customer" -> "c_custkey";
            case "part" -> "p_partkey";
            case "partsupp" -> "ps_partkey";
            case "orders" -> "o_orderkey";
            case "lineitem" -> "l_orderkey";
            default -> throw new IllegalArgumentException("Unknown TPC-H table: " + table.name());
        };
        return """
            CREATE TABLE IF NOT EXISTS %s (
            %s
            )
            DUPLICATE KEY(%s)
            DISTRIBUTED BY HASH(%s)
            PROPERTIES ("replication_num" = "1");
            """.formatted(TpchSchema.tableName(table.name(), "starrocks_internal"), columns(table, false), key, key);
    }

    public static String starRocksRefreshExternalTable(TpchTable table) {
        return "REFRESH EXTERNAL TABLE " + TpchSchema.tableName(table.name(), "starrocks_external_iceberg") + ";";
    }

    private static String columns(TpchTable table, boolean spark) {
        return table.columns().stream()
            .map(column -> "    " + column.name() + " " + type(column.logicalType(), spark))
            .collect(Collectors.joining(",\n"));
    }

    private static String type(String logicalType, boolean spark) {
        return switch (logicalType) {
            case "long" -> "BIGINT";
            case "int" -> "INT";
            case "double" -> "DOUBLE";
            case "date" -> spark ? "DATE" : "DATE";
            case "string" -> spark ? "STRING" : "VARCHAR(128)";
            default -> throw new IllegalArgumentException("Unknown TPC-H type: " + logicalType);
        };
    }
}
```

- [ ] **Step 4: Run tests and commit**

Run:

```powershell
mvn -Dtest=TpchSqlTemplatesTest test
```

Expected: tests pass.

Commit:

```powershell
git add src/main/java/com/example/databenchmark/tpch/TpchSqlTemplates.java src/test/java/com/example/databenchmark/tpch/TpchSqlTemplatesTest.java
git commit -m "feat: add tpch ddl templates"
```

---

### Task 5: Add Deterministic TPC-H Smoke Generator

**Files:**
- Create: `src/main/java/com/example/databenchmark/tpch/TpchDatasetResult.java`
- Create: `src/main/java/com/example/databenchmark/tpch/TpchDataGenerator.java`
- Test: `src/test/java/com/example/databenchmark/tpch/TpchDataGeneratorTest.java`

- [ ] **Step 1: Write failing generator tests**

Create `TpchDataGeneratorTest`:

```java
package com.example.databenchmark.tpch;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TpchDataGeneratorTest {
    @TempDir
    Path tempDir;

    @Test
    void generatesAllTablesWithDeterministicSmokeRows() throws Exception {
        BenchmarkConfig config = new BenchmarkConfig(
            "tpch-smoke",
            20260602L,
            new BenchmarkConfig.SuiteConfig("tpch", new BigDecimal("0.01"), "smoke"),
            new BenchmarkConfig.DatasetConfig(10000, 1, 50, "2026-01-01T00:00:00", tempDir.toString(), 10000L),
            new BenchmarkConfig.QueryConfig(1, 1, 1),
            new BenchmarkConfig.ReportConfig("html", tempDir.resolve("reports").toString()),
            new BenchmarkConfig.MonitoringConfig(true, true)
        );

        TpchDatasetResult result = new TpchDataGenerator().generate(config, "tpch-unit");

        assertThat(result.tables()).containsKeys("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem");
        assertThat(result.table("region").rows()).isEqualTo(5);
        assertThat(result.table("nation").rows()).isEqualTo(25);
        assertThat(Files.exists(result.table("lineitem").parquetPath())).isTrue();
    }
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```powershell
mvn -Dtest=TpchDataGeneratorTest test
```

Expected: compilation fails because generator classes do not exist.

- [ ] **Step 3: Add TpchDatasetResult**

Create `TpchDatasetResult`:

```java
package com.example.databenchmark.tpch;

import java.nio.file.Path;
import java.util.Map;

public record TpchDatasetResult(Path outputPath, Map<String, TableResult> tables, long rows, long bytesWritten) {
    public TableResult table(String name) {
        TableResult result = tables.get(name);
        if (result == null) {
            throw new IllegalArgumentException("Unknown generated TPC-H table: " + name);
        }
        return result;
    }

    public record TableResult(String table, Path parquetPath, Path csvPath, long rows, long bytesWritten) {}
}
```

- [ ] **Step 4: Implement minimal Parquet writer per table**

Create `TpchDataGenerator` using Apache Avro `GenericRecord` and `AvroParquetWriter`, following the same Hadoop configuration pattern used by `KpiDataGenerator`. Generate stable values by row number:

```java
package com.example.databenchmark.tpch;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.hadoop.HadoopLocalConfiguration;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public final class TpchDataGenerator {
    public TpchDatasetResult generate(BenchmarkConfig config, String runId) throws IOException {
        Path root = Path.of(config.dataset().output(), "tpch", runId);
        Files.createDirectories(root);
        Map<String, TpchDatasetResult.TableResult> results = new LinkedHashMap<>();
        long rows = 0L;
        long bytes = 0L;
        for (TpchTable table : TpchSchema.tables()) {
            long tableRows = scaledRows(table, config.suite().scaleFactor());
            Path tableDir = root.resolve(table.name());
            Files.createDirectories(tableDir);
            Path marker = tableDir.resolve("part-00000.parquet");
            Files.writeString(marker, "tpch smoke marker for " + table.name() + System.lineSeparator());
            long tableBytes = Files.size(marker);
            results.put(table.name(), new TpchDatasetResult.TableResult(table.name(), marker, root.resolve("csv").resolve(table.name() + ".csv"), tableRows, tableBytes));
            rows += tableRows;
            bytes += tableBytes;
        }
        return new TpchDatasetResult(root, results, rows, bytes);
    }

    static long scaledRows(TpchTable table, BigDecimal scaleFactor) {
        long scaled = scaleFactor.multiply(BigDecimal.valueOf(table.baseRows())).setScale(0, java.math.RoundingMode.CEILING).longValue();
        if (table.name().equals("region")) {
            return 5L;
        }
        if (table.name().equals("nation")) {
            return 25L;
        }
        return Math.max(1L, scaled);
    }
}
```

This step creates the contract and deterministic row counts first. Replace the temporary marker file with real Parquet records in the next step before compose validation.

- [ ] **Step 5: Replace the temporary marker with real Parquet records**

Implement an Avro schema builder inside `TpchDataGenerator`:

```java
private org.apache.avro.Schema avroSchema(TpchTable table) {
    java.util.List<org.apache.avro.Schema.Field> fields = new java.util.ArrayList<>();
    for (TpchColumn column : table.columns()) {
        fields.add(new org.apache.avro.Schema.Field(column.name(), avroType(column.logicalType()), null, (Object) null));
    }
    org.apache.avro.Schema schema = org.apache.avro.Schema.createRecord("tpch_" + table.name(), null, "com.example.databenchmark.tpch", false);
    schema.setFields(fields);
    return schema;
}

private org.apache.avro.Schema avroType(String logicalType) {
    return switch (logicalType) {
        case "long", "date" -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG);
        case "int" -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT);
        case "double" -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE);
        case "string" -> org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
        default -> throw new IllegalArgumentException("Unknown TPC-H type: " + logicalType);
    };
}
```

Use `AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(marker.toUri()))` with `HadoopLocalConfiguration.create()` and create values with a method:

```java
Object valueFor(TpchTable table, TpchColumn column, long row) {
    String name = column.name();
    if (name.endsWith("key")) {
        return row + 1L;
    }
    if (column.logicalType().equals("int")) {
        return (int) ((row % 100) + 1);
    }
    if (column.logicalType().equals("double")) {
        return ((row % 10_000) + 100) / 10.0;
    }
    if (column.logicalType().equals("date")) {
        return java.time.LocalDate.of(1992, 1, 1).plusDays(row % 2400).toEpochDay();
    }
    return table.name() + "-" + name + "-" + row;
}
```

Keep the row-value method deterministic and package-private so tests can assert key behavior.

- [ ] **Step 6: Run tests and commit**

Run:

```powershell
mvn -Dtest=TpchDataGeneratorTest test
```

Expected: tests pass and generated Parquet files exist.

Commit:

```powershell
git add src/main/java/com/example/databenchmark/tpch/TpchDatasetResult.java src/main/java/com/example/databenchmark/tpch/TpchDataGenerator.java src/test/java/com/example/databenchmark/tpch/TpchDataGeneratorTest.java
git commit -m "feat: add tpch smoke data generator"
```

---

### Task 6: Add TPC-H CSV Export For StarRocks

**Files:**
- Create: `src/main/java/com/example/databenchmark/tpch/TpchCsvExporter.java`
- Test: `src/test/java/com/example/databenchmark/tpch/TpchCsvExporterTest.java`

- [ ] **Step 1: Write failing CSV exporter test**

Create `TpchCsvExporterTest`:

```java
package com.example.databenchmark.tpch;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TpchCsvExporterTest {
    @TempDir
    Path tempDir;

    @Test
    void writesOneCsvFilePerGeneratedTable() throws Exception {
        TpchDatasetResult result = new TpchDatasetResult(
            tempDir,
            Map.of("region", new TpchDatasetResult.TableResult("region", tempDir.resolve("region.parquet"), tempDir.resolve("csv/region.csv"), 5, 10)),
            5,
            10
        );

        Map<String, Path> csv = new TpchCsvExporter().export(result);

        assertThat(csv).containsKey("region");
        assertThat(Files.readString(csv.get("region"))).contains("region");
    }
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```powershell
mvn -Dtest=TpchCsvExporterTest test
```

Expected: compilation fails because `TpchCsvExporter` does not exist.

- [ ] **Step 3: Implement exporter**

Create `TpchCsvExporter`:

```java
package com.example.databenchmark.tpch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public final class TpchCsvExporter {
    public Map<String, Path> export(TpchDatasetResult dataset) throws IOException {
        Map<String, Path> outputs = new LinkedHashMap<>();
        Path csvDir = dataset.outputPath().resolve("csv");
        Files.createDirectories(csvDir);
        for (TpchTable table : TpchSchema.tables()) {
            Path csv = csvDir.resolve(table.name() + ".csv");
            StringBuilder content = new StringBuilder();
            long rows = dataset.table(table.name()).rows();
            for (long row = 0; row < rows; row++) {
                for (int i = 0; i < table.columns().size(); i++) {
                    if (i > 0) {
                        content.append(',');
                    }
                    content.append(value(table, table.columns().get(i), row));
                }
                content.append(System.lineSeparator());
            }
            Files.writeString(csv, content.toString());
            outputs.put(table.name(), csv);
        }
        return outputs;
    }

    private static String value(TpchTable table, TpchColumn column, long row) {
        if (column.name().endsWith("key")) {
            return Long.toString(row + 1);
        }
        if (column.logicalType().equals("int")) {
            return Integer.toString((int) ((row % 100) + 1));
        }
        if (column.logicalType().equals("double")) {
            return Double.toString(((row % 10_000) + 100) / 10.0);
        }
        if (column.logicalType().equals("date")) {
            return java.time.LocalDate.of(1992, 1, 1).plusDays(row % 2400).toString();
        }
        return table.name() + "-" + column.name() + "-" + row;
    }
}
```

- [ ] **Step 4: Run tests and commit**

Run:

```powershell
mvn -Dtest=TpchCsvExporterTest test
```

Expected: tests pass.

Commit:

```powershell
git add src/main/java/com/example/databenchmark/tpch/TpchCsvExporter.java src/test/java/com/example/databenchmark/tpch/TpchCsvExporterTest.java
git commit -m "feat: add tpch csv export"
```

---

### Task 7: Add Engine TPC-H Entry Points

**Files:**
- Modify: `src/main/java/com/example/databenchmark/engine/SparkIcebergClient.java`
- Modify: `src/main/java/com/example/databenchmark/engine/StarRocksClient.java`
- Test: `src/test/java/com/example/databenchmark/engine/SparkIcebergClientTest.java`
- Test: `src/test/java/com/example/databenchmark/engine/StarRocksClientTest.java`

- [ ] **Step 1: Write failing engine tests**

In `SparkIcebergClientTest`, add:

```java
@Test
void tpchLoadCreatesAndInsertsEachTable() {
    CapturingCommandRunner runner = new CapturingCommandRunner();
    SparkIcebergClient client = new SparkIcebergClient(runner);
    TpchDatasetResult dataset = TestTpchFixtures.dataset(Path.of("data/generated/tpch/unit"));

    EngineRunResult result = client.loadTpch(dataset, "tpch-unit", "tpch-smoke");

    assertThat(result.success()).isTrue();
    assertThat(String.join("\n", runner.commands())).contains("iceberg_catalog.tpch.lineitem");
}
```

In `StarRocksClientTest`, add:

```java
@Test
void tpchQueriesRenderStarRocksTableNames() {
    CapturingJdbcExecutor jdbc = new CapturingJdbcExecutor();
    StarRocksClient client = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
        new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
    ));

    List<EngineRunResult> results = client.runTpchQueries("tpch-unit", "tpch-smoke", "smoke");

    assertThat(results).isNotEmpty();
    assertThat(jdbc.sql()).anySatisfy(sql -> assertThat(sql).contains("sr_internal_tpch.lineitem"));
}
```

Add `TestTpchFixtures` under `src/test/java/com/example/databenchmark/tpch/TestTpchFixtures.java` if needed:

```java
package com.example.databenchmark.tpch;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public final class TestTpchFixtures {
    private TestTpchFixtures() {}

    public static TpchDatasetResult dataset(Path root) {
        Map<String, TpchDatasetResult.TableResult> tables = new LinkedHashMap<>();
        for (TpchTable table : TpchSchema.tables()) {
            tables.put(table.name(), new TpchDatasetResult.TableResult(table.name(), root.resolve(table.name()).resolve("part-00000.parquet"), root.resolve("csv").resolve(table.name() + ".csv"), 1, 1));
        }
        return new TpchDatasetResult(root, tables, tables.size(), tables.size());
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
mvn -Dtest=SparkIcebergClientTest,StarRocksClientTest test
```

Expected: compilation fails because TPC-H methods do not exist.

- [ ] **Step 3: Add Spark TPC-H methods**

Add to `SparkIcebergClient`:

```java
public EngineRunResult loadTpch(TpchDatasetResult dataset, String runId, String profile) {
    long started = System.nanoTime();
    try {
        commandRunner.run(TpchSqlTemplates.sparkCreateDatabase());
        for (TpchTable table : TpchSchema.tables()) {
            commandRunner.run(TpchSqlTemplates.sparkCreateTable(table));
            commandRunner.run(TpchSqlTemplates.sparkInsertFromParquet(table, dataset.table(table.name()).parquetPath().toString()));
        }
        return new EngineRunResult("spark_iceberg", "tpch_iceberg", EngineStage.SPARK_ICEBERG_LOAD.name(), null, dataset.rows(), dataset.bytesWritten(), secondsSince(started), true, null);
    } catch (Exception e) {
        return new EngineRunResult("spark_iceberg", "tpch_iceberg", EngineStage.SPARK_ICEBERG_LOAD.name(), null, 0L, 0L, secondsSince(started), false, e.getMessage());
    }
}

public List<EngineRunResult> runTpchQueries(String runId, String profile, String querySet) {
    List<EngineRunResult> results = new ArrayList<>();
    for (TpchQuery query : TpchQueryCatalog.queries(querySet)) {
        long started = System.nanoTime();
        try {
            CommandResult command = commandRunner.run(TpchSqlRenderer.render(query.name(), "spark_iceberg"));
            results.add(new EngineRunResult("spark_iceberg", "tpch_iceberg", EngineStage.QUERY.name(), query.name(), command.rows(), 0L, secondsSince(started), command.success(), command.success() ? null : command.stderr()));
        } catch (Exception e) {
            results.add(new EngineRunResult("spark_iceberg", "tpch_iceberg", EngineStage.QUERY.name(), query.name(), 0L, 0L, secondsSince(started), false, e.getMessage()));
        }
    }
    return results;
}
```

Use existing duration helper style from `SparkIcebergClient`; if no helper exists, add:

```java
private static double secondsSince(long startedNanos) {
    return (System.nanoTime() - startedNanos) / 1_000_000_000.0;
}
```

- [ ] **Step 4: Add StarRocks TPC-H methods**

Add to `StarRocksClient`:

```java
public EngineRunResult loadTpchInternal(Map<String, Path> csvFiles, TpchDatasetResult dataset, String runId, String profile) {
    long started = System.nanoTime();
    try {
        jdbcExecutor.execute(TpchSqlTemplates.starRocksCreateDatabase());
        for (TpchTable table : TpchSchema.tables()) {
            jdbcExecutor.execute(TpchSqlTemplates.starRocksCreateInternalTable(table));
            streamLoadClient.load(csvFiles.get(table.name()));
        }
        return new EngineRunResult("starrocks_internal", "tpch_internal", EngineStage.STARROCKS_INTERNAL_LOAD.name(), null, dataset.rows(), dataset.bytesWritten(), secondsSince(started), true, null);
    } catch (Exception e) {
        return new EngineRunResult("starrocks_internal", "tpch_internal", EngineStage.STARROCKS_INTERNAL_LOAD.name(), null, 0L, 0L, secondsSince(started), false, e.getMessage());
    }
}

public EngineRunResult refreshTpchExternalCatalog(String runId, String profile) {
    long started = System.nanoTime();
    try {
        jdbcExecutor.execute(SqlTemplates.starRocksCreateExternalCatalog());
        for (TpchTable table : TpchSchema.tables()) {
            jdbcExecutor.execute(TpchSqlTemplates.starRocksRefreshExternalTable(table));
        }
        return new EngineRunResult("starrocks_external_iceberg", "tpch_external_iceberg", EngineStage.STARROCKS_EXTERNAL_REFRESH.name(), null, 0L, 0L, secondsSince(started), true, null);
    } catch (Exception e) {
        return new EngineRunResult("starrocks_external_iceberg", "tpch_external_iceberg", EngineStage.STARROCKS_EXTERNAL_REFRESH.name(), null, 0L, 0L, secondsSince(started), false, e.getMessage());
    }
}

public List<EngineRunResult> runTpchQueries(String runId, String profile, String querySet) {
    List<EngineRunResult> results = new ArrayList<>();
    results.addAll(runTpchQueriesFor("starrocks_internal", "tpch_internal", querySet));
    results.addAll(runTpchQueriesFor("starrocks_external_iceberg", "tpch_external_iceberg", querySet));
    return results;
}
```

Add private helper:

```java
private List<EngineRunResult> runTpchQueriesFor(String engineKey, String tableShape, String querySet) {
    List<EngineRunResult> results = new ArrayList<>();
    for (TpchQuery query : TpchQueryCatalog.queries(querySet)) {
        long started = System.nanoTime();
        try {
            JdbcExecutionResult result = jdbcExecutor.query(TpchSqlRenderer.render(query.name(), engineKey));
            results.add(new EngineRunResult(engineKey, tableShape, EngineStage.QUERY.name(), query.name(), result.rows(), 0L, secondsSince(started), true, null));
        } catch (Exception e) {
            results.add(new EngineRunResult(engineKey, tableShape, EngineStage.QUERY.name(), query.name(), 0L, 0L, secondsSince(started), false, e.getMessage()));
        }
    }
    return results;
}
```

- [ ] **Step 5: Run tests and commit**

Run:

```powershell
mvn -Dtest=SparkIcebergClientTest,StarRocksClientTest test
```

Expected: tests pass.

Commit:

```powershell
git add src/main/java/com/example/databenchmark/engine src/test/java/com/example/databenchmark/engine src/test/java/com/example/databenchmark/tpch/TestTpchFixtures.java
git commit -m "feat: add tpch engine execution"
```

---

### Task 8: Wire TPC-H Into Compose Runner

**Files:**
- Modify: `src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java`
- Test: `src/test/java/com/example/databenchmark/runner/ComposeBenchmarkRunnerTest.java`

- [ ] **Step 1: Write failing runner test**

Add to `ComposeBenchmarkRunnerTest`:

```java
@Test
void tpchSuiteRunsTpchFlow() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.defaultSmoke();
    config = new BenchmarkConfig(
        "tpch-smoke",
        config.seed(),
        new BenchmarkConfig.SuiteConfig("tpch", new java.math.BigDecimal("0.01"), "smoke"),
        config.dataset(),
        config.query(),
        config.report(),
        config.monitoring()
    );

    CapturingTpchGenerator tpchGenerator = new CapturingTpchGenerator();
    ComposeBenchmarkRunner runner = ComposeBenchmarkRunner.forTestsWithTpch(
        unused -> { throw new AssertionError("KPI generator must not run for tpch"); },
        dataset -> Path.of("data/generated/tpch/unit/csv"),
        new SuccessfulSparkClient(),
        new SuccessfulStarRocksClient(),
        new NoopReportWriter(),
        new NoopMetricsRecorder(),
        tpchGenerator,
        new TpchCsvExporter()
    );

    ComposeBenchmarkRunner.ComposeRunResult result = runner.run(config, "tpch-unit");

    assertThat(result.success()).isTrue();
    assertThat(tpchGenerator.called()).isTrue();
}
```

Use the test helper style already present in `ComposeBenchmarkRunnerTest`; keep new helpers private static classes in the same test file.

- [ ] **Step 2: Run test to verify failure**

Run:

```powershell
mvn -Dtest=ComposeBenchmarkRunnerTest test
```

Expected: compilation fails because the TPC-H test constructor and flow do not exist.

- [ ] **Step 3: Add TPC-H dependencies and branch**

In `ComposeBenchmarkRunner`, add fields:

```java
private final TpchGenerator tpchGenerator;
private final TpchCsvExport tpchCsvExport;
```

Add interfaces:

```java
interface TpchGenerator {
    TpchDatasetResult generate(BenchmarkConfig config, String runId) throws Exception;
}

interface TpchCsvExport {
    Map<String, Path> export(TpchDatasetResult dataset) throws Exception;
}
```

In production constructor, pass:

```java
new TpchDataGenerator()::generate,
new TpchCsvExporter()::export
```

At the top of `run`, branch:

```java
if (config.suite().name().equals("tpch")) {
    return runTpch(config, actualRunId, started);
}
```

Add `runTpch`:

```java
private ComposeRunResult runTpch(BenchmarkConfig config, String runId, Instant started) {
    List<EngineRunResult> loadResults = new ArrayList<>();
    List<EngineRunResult> queryResults = new ArrayList<>();
    TpchDatasetResult dataset = null;
    Path reportPath = null;
    try {
        dataset = tpchGenerator.generate(config, runId);
        Map<String, Path> csv = tpchCsvExport.export(dataset);
        loadResults.add(sparkClient.loadTpch(dataset, runId, config.profile()));
        loadResults.add(starRocksClient.loadTpchInternal(csv, dataset, runId, config.profile()));
        loadResults.add(starRocksClient.refreshTpchExternalCatalog(runId, config.profile()));
        queryResults.addAll(sparkClient.runTpchQueries(runId, config.profile(), config.suite().querySet()));
        queryResults.addAll(starRocksClient.runTpchQueries(runId, config.profile(), config.suite().querySet()));
    } catch (Exception e) {
        loadResults.add(failed("local", "tpch_generated_parquet", EngineStage.GENERATE.name(), e));
    } finally {
        recordMetrics(runId, config.profile(), config.suite().name(), config.suite().querySet(), loadResults, queryResults);
        BenchmarkReport report = buildReport(config, runId, started, Instant.now(), dataset, loadResults, queryResults);
        reportPath = reportWriter.write(report);
    }
    return new ComposeRunResult(null, dataset == null ? null : dataset.outputPath().resolve("csv"), reportPath, reportSucceeded(loadResults, queryResults));
}
```

Adjust adapter interfaces so Spark and StarRocks facades expose the TPC-H methods from Task 7.

- [ ] **Step 4: Run tests and commit**

Run:

```powershell
mvn -Dtest=ComposeBenchmarkRunnerTest test
```

Expected: tests pass.

Commit:

```powershell
git add src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java src/test/java/com/example/databenchmark/runner/ComposeBenchmarkRunnerTest.java
git commit -m "feat: run tpch suite in compose mode"
```

---

### Task 9: Add Suite Labels To Metrics And Report

**Files:**
- Modify: `src/main/java/com/example/databenchmark/metrics/BenchmarkMetrics.java`
- Modify: `src/main/java/com/example/databenchmark/report/BenchmarkReport.java`
- Modify: `src/main/resources/report.ftl`
- Modify: `src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java`
- Test: `src/test/java/com/example/databenchmark/metrics/BenchmarkMetricsTest.java`
- Test: `src/test/java/com/example/databenchmark/report/HtmlReportWriterTest.java`

- [ ] **Step 1: Write failing metrics/report tests**

In `BenchmarkMetricsTest`, assert labels:

```java
@Test
void labelsIncludeSuiteAndQuerySet() {
    assertThat(BenchmarkMetrics.LABELS).contains("suite", "query_set");
}
```

In `HtmlReportWriterTest`, add an assertion after rendering a report with suite fields:

```java
assertThat(html).contains("Suite");
assertThat(html).contains("tpch");
assertThat(html).contains("Query Set");
assertThat(html).contains("smoke");
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
mvn -Dtest=BenchmarkMetricsTest,HtmlReportWriterTest test
```

Expected: tests fail because labels and report fields are missing.

- [ ] **Step 3: Add metric labels**

Update `BenchmarkMetrics.LABELS`:

```java
public static final List<String> LABELS = List.of(
    "run_id",
    "profile",
    "suite",
    "query_set",
    "engine",
    "table_shape",
    "stage",
    "query_name"
);
```

Add `suite` and `querySet` parameters to `recordLoad`, `recordQuery`, and `tags`. Update call sites in `ComposeBenchmarkRunner`:

```java
metricsRecorder.recordLoad(runId, profile, config.suite().name(), config.suite().querySet(), result);
metricsRecorder.recordQuery(runId, profile, config.suite().name(), config.suite().querySet(), result);
```

- [ ] **Step 4: Add report fields**

Update `BenchmarkReport` record to include:

```java
String suite,
String querySet,
```

Update report construction in both KPI and TPC-H flows:

```java
config.suite().name(),
config.suite().querySet(),
```

Update `report.ftl` dataset summary table:

```html
<tr><th>Suite</th><td>${report.suite()}</td></tr>
<tr><th>Query Set</th><td>${report.querySet()}</td></tr>
```

- [ ] **Step 5: Run tests and commit**

Run:

```powershell
mvn -Dtest=BenchmarkMetricsTest,HtmlReportWriterTest,ComposeBenchmarkRunnerTest test
```

Expected: tests pass.

Commit:

```powershell
git add src/main/java/com/example/databenchmark/metrics/BenchmarkMetrics.java src/main/java/com/example/databenchmark/report/BenchmarkReport.java src/main/resources/report.ftl src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java src/test/java/com/example/databenchmark/metrics/BenchmarkMetricsTest.java src/test/java/com/example/databenchmark/report/HtmlReportWriterTest.java
git commit -m "feat: label reports and metrics by suite"
```

---

### Task 10: Update Grafana Dashboard Filters

**Files:**
- Modify: dashboard JSON found by `rg -n "benchmark_query_duration_seconds|run_id|templating" .`
- Test: `src/test/java/com/example/databenchmark/ComposeTopologyTest.java` or existing dashboard JSON test if present

- [ ] **Step 1: Locate dashboard file**

Run:

```powershell
rg -n "benchmark_query_duration_seconds|templating|run_id" .
```

Expected: output includes the Grafana dashboard JSON path used by Docker Compose.

- [ ] **Step 2: Add failing dashboard assertion**

In the existing topology/dashboard test, add assertions that the dashboard JSON contains:

```java
assertThat(dashboard).contains("\"suite\"");
assertThat(dashboard).contains("\"query_set\"");
assertThat(dashboard).contains("label_values(benchmark_query_duration_seconds");
```

- [ ] **Step 3: Add dashboard variables**

Add Grafana variables:

```json
{
  "name": "suite",
  "type": "query",
  "datasource": { "type": "prometheus", "uid": "prometheus" },
  "query": "label_values(benchmark_query_duration_seconds, suite)",
  "refresh": 1
}
```

```json
{
  "name": "query_set",
  "type": "query",
  "datasource": { "type": "prometheus", "uid": "prometheus" },
  "query": "label_values(benchmark_query_duration_seconds{suite=\"$suite\"}, query_set)",
  "refresh": 1
}
```

Update PromQL panel filters from:

```promql
{run_id="$run_id"}
```

to:

```promql
{run_id="$run_id", suite="$suite", query_set="$query_set"}
```

- [ ] **Step 4: Run topology tests and commit**

Run:

```powershell
mvn -Dtest=ComposeTopologyTest test
```

Expected: tests pass.

Commit:

```powershell
git add monitoring src/test/java/com/example/databenchmark/ComposeTopologyTest.java
git commit -m "feat: add tpch grafana filters"
```

---

### Task 11: Full Verification

**Files:**
- Modify only files required by failing verification.

- [ ] **Step 1: Run full unit test suite**

Run:

```powershell
mvn test
```

Expected: all tests pass.

- [ ] **Step 2: Build runnable jar**

Run:

```powershell
mvn package
```

Expected: build succeeds and creates `target/data-benchmark-0.1.0-SNAPSHOT.jar`.

- [ ] **Step 3: Run local KPI smoke to confirm default compatibility**

Run:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode local --run-id kpi-default-after-tpch
```

Expected: exit code 0 and report under `reports/runs/kpi-default-after-tpch/index.html`.

- [ ] **Step 4: Run TPC-H compose smoke**

Run:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-tpch-smoke.yml --run-id tpch-smoke
```

Expected: exit code 0 when Docker Compose services are healthy. If an engine-specific query fails, report contains the failed SQL stage and error message instead of losing the report.

- [ ] **Step 5: Check report content**

Run:

```powershell
Select-String -Path reports\runs\tpch-smoke\index.html -Pattern "tpch","q01_pricing_summary_report","q03_shipping_priority"
```

Expected: all three patterns are present.

- [ ] **Step 6: Check Grafana URL**

Open:

```text
http://localhost:3000/d/benchmark?var-run_id=tpch-smoke&var-suite=tpch&var-query_set=smoke
```

Expected: dashboard loads and filters to the TPC-H smoke run.

- [ ] **Step 7: Commit final verification fixes**

If verification required changes, commit them:

```powershell
git status --short
git add src/main/java/com/example/databenchmark src/test/java/com/example/databenchmark configs monitoring src/main/resources
git commit -m "fix: stabilize tpch smoke verification"
```

If no changes were required, do not create an empty commit.

---

## Self-Review

- Spec coverage: suite config, TPC-H schema, smoke generation, Parquet/CSV outputs, DDL, 22 query names, smoke query set, Spark/StarRocks execution, metrics/report labels, Grafana filtering, and verification are covered.
- KPI compatibility: Task 1 and Task 11 explicitly preserve and verify the KPI default.
- Red-flag scan: no open-ended implementation steps remain. The plan uses concrete files, commands, method names, and expected outcomes.
- Type consistency: `SuiteConfig`, `TpchTable`, `TpchDatasetResult`, `TpchQuery`, `TpchSqlTemplates`, and `TpchSqlRenderer` names are introduced before later tasks reference them.

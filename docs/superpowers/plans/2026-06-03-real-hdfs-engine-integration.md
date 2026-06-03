# Real HDFS Engine Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the HDFS-backed real Spark Iceberg and StarRocks compose benchmark path while keeping the existing local Java runner fast and stable.

**Architecture:** Keep `run` local by default and add `run --mode compose` for real Docker Compose integration. The compose path generates Parquet locally, initializes HDFS-backed Iceberg through Spark SQL, loads StarRocks internal table through CSV + Stream Load, refreshes StarRocks external Iceberg catalog, executes the shared query catalog across all table shapes, records Prometheus metrics, and writes an HTML report that links to a provisioned Grafana dashboard.

**Tech Stack:** Java 17, Maven, Picocli, Apache Parquet/Avro, JDBC, Java HTTP client, Docker Compose, HDFS, Hive Metastore, Spark SQL, StarRocks, Prometheus, Grafana provisioning, JUnit 5, AssertJ, Jackson YAML/JSON.

---

## Scope

This plan implements the design in `docs/superpowers/specs/2026-06-03-real-engine-integration-design.md` after the storage switch to HDFS. MinIO and S3 must not remain in compose, monitoring, or the HDFS integration documentation.

The implementation is intentionally smoke-first:

- Local mode remains available with no Docker dependency.
- Compose mode attempts real Spark, HDFS, Hive Metastore, StarRocks internal, and StarRocks external Iceberg work.
- Compose mode writes a degraded report with stage errors if one engine fails after report infrastructure is available.
- Full profile remains explicit opt-in and is not run by default.

## File Structure

- Modify: `pom.xml`
  - Add MySQL JDBC driver and any test JSON assertion dependency already accepted by project style.
- Modify: `docker-compose.yml`
  - Replace `minio` with `hdfs-namenode`, `hdfs-datanode`, `hdfs-init`.
  - Mount Grafana provisioning.
  - Run benchmark runner with `run --mode compose`.
- Modify: `monitoring/prometheus.yml`
  - Remove MinIO scrape config.
  - Add benchmark runner and HDFS scrape configs.
- Create: `monitoring/grafana/provisioning/datasources/prometheus.yml`
  - Provision Prometheus datasource.
- Create: `monitoring/grafana/provisioning/dashboards/benchmark.yml`
  - Provision benchmark dashboard provider.
- Create: `monitoring/grafana/dashboards/benchmark.json`
  - Dashboard with uid `benchmark`.
- Create: `src/main/java/com/example/databenchmark/engine/CommandRunner.java`
  - Execute external commands with captured output and duration.
- Create: `src/main/java/com/example/databenchmark/engine/CommandResult.java`
  - Immutable command execution result.
- Create: `src/main/java/com/example/databenchmark/engine/EngineRunResult.java`
  - Load/query stage result model.
- Create: `src/main/java/com/example/databenchmark/engine/EngineStage.java`
  - Enum for `generate`, `spark_iceberg_load`, `starrocks_internal_load`, `starrocks_external_refresh`, and query stages.
- Create: `src/main/java/com/example/databenchmark/engine/SqlTemplates.java`
  - Generate Spark Iceberg DDL/insert SQL and StarRocks DDL/catalog SQL.
- Create: `src/main/java/com/example/databenchmark/engine/SqlRenderer.java`
  - Render `QueryCatalog` SQL for Spark and StarRocks table names.
- Create: `src/main/java/com/example/databenchmark/engine/StarRocksCsvExporter.java`
  - Read generated Parquet and write headerless CSV for Stream Load.
- Create: `src/main/java/com/example/databenchmark/engine/StarRocksStreamLoadClient.java`
  - HTTP Stream Load client.
- Create: `src/main/java/com/example/databenchmark/engine/JdbcExecutor.java`
  - StarRocks JDBC execute/query timing.
- Create: `src/main/java/com/example/databenchmark/engine/SparkIcebergClient.java`
  - Run Spark SQL commands in Docker Compose.
- Create: `src/main/java/com/example/databenchmark/engine/StarRocksClient.java`
  - Prepare internal/external StarRocks objects and run StarRocks queries.
- Create: `src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java`
  - Compose mode orchestrator.
- Modify: `src/main/java/com/example/databenchmark/runner/LocalBenchmarkRunner.java`
  - Preserve local behavior and align report model changes.
- Modify: `src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java`
  - Add `--mode local|compose`.
- Modify: `src/main/java/com/example/databenchmark/report/BenchmarkReport.java`
  - Extend load/query summaries to include status and error detail.
- Modify: `src/main/java/com/example/databenchmark/report/HtmlReportWriter.java`
  - Render degraded reports and engine details.
- Modify: `src/main/resources/report.ftl`
  - Add status/error columns and Grafana dashboard link.
- Modify: tests under `src/test/java/com/example/databenchmark`
  - Add unit tests for compose topology, provisioning JSON/YAML, SQL templates, renderer, CSV exporter, metrics/report changes, and CLI mode selection.
- Modify: `README.md`
  - Document HDFS compose mode, Grafana dashboard, and known degraded behavior.

## Acceptance Criteria

- `rg -n "MinIO|minio|S3|s3|aws|AWS" docker-compose.yml monitoring README.md docs/superpowers/specs docs/superpowers/plans src` returns no live infrastructure references except historical plan text that is explicitly labelled as prior history.
- `docker compose -f docker-compose.yml config` passes.
- `mvn test` passes.
- `mvn package` passes.
- `java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --run-id local-after-compose-plan` still produces a local report.
- `java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --run-id compose-smoke` runs against compose services and writes `reports/runs/compose-smoke/index.html`.
- Grafana health endpoint returns HTTP 200.
- `http://localhost:3000/d/benchmark?var-run_id=compose-smoke` opens the provisioned dashboard after login.
- Prometheus config includes `benchmark-runner:9108` and HDFS targets, and does not include MinIO.
- `main` and `staging` are pushed after verification, and `Deploy to Staging` succeeds.

### Task 1: Monitoring And Grafana Provisioning

**Files:**
- Modify: `monitoring/prometheus.yml`
- Create: `monitoring/grafana/provisioning/datasources/prometheus.yml`
- Create: `monitoring/grafana/provisioning/dashboards/benchmark.yml`
- Create: `monitoring/grafana/dashboards/benchmark.json`
- Modify: `docker-compose.yml`
- Test: `src/test/java/com/example/databenchmark/ComposeTopologyTest.java`

- [ ] **Step 1: Update compose topology test first**

Add test assertions in `src/test/java/com/example/databenchmark/ComposeTopologyTest.java`:

```java
@Test
void grafanaProvisioningAndPrometheusTargetsAreConfigured() throws Exception {
    Map<String, Object> compose = mapper.readValue(Path.of("docker-compose.yml").toFile(), Map.class);
    Map<String, Object> services = (Map<String, Object>) compose.get("services");

    Map<String, Object> grafana = service(services, "grafana");
    assertThat(stringList(grafana, "volumes"))
        .contains("./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro")
        .contains("./monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro");

    Map<?, ?> prometheus = mapper.readValue(Path.of("monitoring/prometheus.yml").toFile(), Map.class);
    String prometheusText = prometheus.toString();
    assertThat(prometheusText).contains("benchmark-runner:9108");
    assertThat(prometheusText).contains("hdfs-namenode:9870");
    assertThat(prometheusText).contains("hdfs-datanode:9864");
    assertThat(prometheusText).doesNotContain("minio");
}

@Test
void grafanaDashboardProvisioningFilesExist() {
    assertThat(Path.of("monitoring/grafana/provisioning/datasources/prometheus.yml")).exists();
    assertThat(Path.of("monitoring/grafana/provisioning/dashboards/benchmark.yml")).exists();
    assertThat(Path.of("monitoring/grafana/dashboards/benchmark.json")).exists();
}
```

- [ ] **Step 2: Run the new tests and verify they fail**

Run:

```powershell
mvn -Dtest=ComposeTopologyTest test
```

Expected: FAIL because Grafana provisioning files and benchmark/HDFS scrape targets do not exist yet.

- [ ] **Step 3: Update Prometheus config**

Replace `monitoring/prometheus.yml` with:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: benchmark-runner
    static_configs:
      - targets: ["benchmark-runner:9108"]

  - job_name: starrocks-fe
    static_configs:
      - targets: ["starrocks-fe:8030"]

  - job_name: starrocks-be
    static_configs:
      - targets: ["starrocks-be:8040"]

  - job_name: hdfs-namenode
    metrics_path: /jmx
    static_configs:
      - targets: ["hdfs-namenode:9870"]

  - job_name: hdfs-datanode
    metrics_path: /jmx
    static_configs:
      - targets: ["hdfs-datanode:9864"]
```

- [ ] **Step 4: Add Grafana datasource provisioning**

Create `monitoring/grafana/provisioning/datasources/prometheus.yml`:

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

- [ ] **Step 5: Add Grafana dashboard provider**

Create `monitoring/grafana/provisioning/dashboards/benchmark.yml`:

```yaml
apiVersion: 1

providers:
  - name: data-benchmark
    orgId: 1
    folder: Data Benchmark
    type: file
    disableDeletion: false
    editable: false
    updateIntervalSeconds: 30
    options:
      path: /var/lib/grafana/dashboards
```

- [ ] **Step 6: Add benchmark dashboard JSON**

Create `monitoring/grafana/dashboards/benchmark.json` with uid `benchmark`, datasource uid `prometheus`, and panels for load duration, load rows, query duration, query rows, query failures, and run metadata. Use this exact dashboard skeleton:

```json
{
  "annotations": {"list": []},
  "editable": false,
  "fiscalYearStartMonth": 0,
  "graphTooltip": 0,
  "id": null,
  "links": [],
  "liveNow": false,
  "panels": [
    {
      "type": "stat",
      "title": "Load Rows",
      "gridPos": {"h": 8, "w": 6, "x": 0, "y": 0},
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "targets": [
        {
          "expr": "sum(benchmark_load_rows_total{run_id=\"$run_id\"}) by (engine, table_shape)",
          "refId": "A"
        }
      ]
    },
    {
      "type": "timeseries",
      "title": "Load Duration Seconds",
      "gridPos": {"h": 8, "w": 6, "x": 6, "y": 0},
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "targets": [
        {
          "expr": "benchmark_load_duration_seconds{run_id=\"$run_id\"}",
          "refId": "A"
        }
      ]
    },
    {
      "type": "timeseries",
      "title": "Query Duration Seconds",
      "gridPos": {"h": 8, "w": 6, "x": 12, "y": 0},
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "targets": [
        {
          "expr": "benchmark_query_duration_seconds{run_id=\"$run_id\"}",
          "refId": "A"
        }
      ]
    },
    {
      "type": "stat",
      "title": "Query Failures",
      "gridPos": {"h": 8, "w": 6, "x": 18, "y": 0},
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "targets": [
        {
          "expr": "sum(benchmark_query_failures_total{run_id=\"$run_id\"}) by (engine, table_shape, query_name)",
          "refId": "A"
        }
      ]
    },
    {
      "type": "table",
      "title": "Query Rows",
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "targets": [
        {
          "expr": "benchmark_query_rows_total{run_id=\"$run_id\"}",
          "format": "table",
          "instant": true,
          "refId": "A"
        }
      ]
    },
    {
      "type": "text",
      "title": "Run Metadata",
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
      "options": {
        "mode": "markdown",
        "content": "Dashboard filtered by run id variable. Use the HTML report for stage errors and degraded run details."
      }
    }
  ],
  "refresh": "10s",
  "schemaVersion": 39,
  "tags": ["data-benchmark"],
  "templating": {
    "list": [
      {
        "name": "run_id",
        "type": "textbox",
        "query": "",
        "current": {"text": "compose-smoke", "value": "compose-smoke"},
        "hide": 0,
        "label": "Run ID"
      }
    ]
  },
  "time": {"from": "now-1h", "to": "now"},
  "timezone": "browser",
  "title": "Data Benchmark",
  "uid": "benchmark",
  "version": 1,
  "weekStart": ""
}
```

- [ ] **Step 7: Mount Grafana provisioning in compose**

Update the `grafana` service in `docker-compose.yml`:

```yaml
  grafana:
    image: grafana/grafana:11.0.0
    ports:
      - "3000:3000"
    volumes:
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro
```

- [ ] **Step 8: Run topology tests**

Run:

```powershell
mvn -Dtest=ComposeTopologyTest test
```

Expected: PASS.

- [ ] **Step 9: Commit monitoring changes**

```powershell
git add docker-compose.yml monitoring src/test/java/com/example/databenchmark/ComposeTopologyTest.java
git commit -m "feat: provision benchmark grafana dashboard"
```

### Task 2: Replace MinIO With HDFS In Compose

**Files:**
- Modify: `docker-compose.yml`
- Modify: `src/test/java/com/example/databenchmark/ComposeTopologyTest.java`
- Modify: `README.md`

- [ ] **Step 1: Add failing compose HDFS assertions**

Update `ComposeTopologyTest` to assert:

```java
@Test
void composeUsesHdfsWarehouseInsteadOfMinio() throws Exception {
    Map<String, Object> compose = mapper.readValue(Path.of("docker-compose.yml").toFile(), Map.class);
    Map<String, Object> services = (Map<String, Object>) compose.get("services");

    assertThat(services).containsKeys("hdfs-namenode", "hdfs-datanode", "hdfs-init");
    assertThat(services).doesNotContainKey("minio");

    Map<String, Object> benchmarkRunner = service(services, "benchmark-runner");
    assertThat(stringList(benchmarkRunner, "depends_on"))
        .contains("hdfs-init", "hive-metastore", "spark", "starrocks-fe", "starrocks-be", "prometheus");
    assertThat(benchmarkRunner.toString()).contains("--mode").contains("compose");
}
```

- [ ] **Step 2: Run compose topology test and verify failure**

Run:

```powershell
mvn -Dtest=ComposeTopologyTest test
```

Expected: FAIL because compose still contains `minio` and no HDFS services.

- [ ] **Step 3: Replace MinIO with HDFS services**

Update `docker-compose.yml`:

```yaml
  hdfs-namenode:
    image: apache/hadoop:3.3.6
    hostname: hdfs-namenode
    command: ["hdfs", "namenode"]
    environment:
      ENSURE_NAMENODE_DIR: "/tmp/hadoop-root/dfs/name"
      HADOOP_HOME: "/opt/hadoop"
      HADOOP_CONF_DIR: "/opt/hadoop/etc/hadoop"
      CORE-SITE.XML_fs.defaultFS: "hdfs://hdfs-namenode:8020"
      HDFS-SITE.XML_dfs.namenode.rpc-address: "hdfs-namenode:8020"
      HDFS-SITE.XML_dfs.replication: "1"
    ports:
      - "9870:9870"
      - "8020:8020"

  hdfs-datanode:
    image: apache/hadoop:3.3.6
    hostname: hdfs-datanode
    command: ["hdfs", "datanode"]
    environment:
      HADOOP_HOME: "/opt/hadoop"
      HADOOP_CONF_DIR: "/opt/hadoop/etc/hadoop"
      CORE-SITE.XML_fs.defaultFS: "hdfs://hdfs-namenode:8020"
      HDFS-SITE.XML_dfs.replication: "1"
    depends_on:
      - hdfs-namenode
    ports:
      - "9864:9864"

  hdfs-init:
    image: apache/hadoop:3.3.6
    command:
      - bash
      - -lc
      - |
        for i in $(seq 1 60); do
          hdfs dfs -fs hdfs://hdfs-namenode:8020 -mkdir -p /warehouse/iceberg && \
          hdfs dfs -fs hdfs://hdfs-namenode:8020 -chmod -R 777 /warehouse && exit 0
          sleep 2
        done
        exit 1
    environment:
      HADOOP_HOME: "/opt/hadoop"
      HADOOP_CONF_DIR: "/opt/hadoop/etc/hadoop"
      CORE-SITE.XML_fs.defaultFS: "hdfs://hdfs-namenode:8020"
    depends_on:
      - hdfs-namenode
      - hdfs-datanode
```

Remove the entire `minio` service.

- [ ] **Step 4: Update Hive/Spark/runner dependencies**

Update `hive-metastore`, `spark`, and `benchmark-runner` services so their `depends_on` includes HDFS where relevant. `benchmark-runner` command must be:

```yaml
    command: ["java", "-jar", "target/data-benchmark-0.1.0-SNAPSHOT.jar", "run", "--mode", "compose", "--run-id", "compose-smoke"]
```

- [ ] **Step 5: Validate compose config**

Run:

```powershell
docker compose -f docker-compose.yml config
```

Expected: PASS.

- [ ] **Step 6: Run topology tests**

Run:

```powershell
mvn -Dtest=ComposeTopologyTest test
```

Expected: PASS.

- [ ] **Step 7: Update README storage description**

Add a Compose section in `README.md`:

```markdown
## HDFS Compose Benchmark

The real engine integration mode uses HDFS as the Iceberg warehouse:

```powershell
docker compose -f docker-compose.yml up -d hdfs-namenode hdfs-datanode hdfs-init hive-metastore spark starrocks-fe starrocks-be prometheus grafana
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --run-id compose-smoke
```

Grafana is available at `http://localhost:3000/d/benchmark?var-run_id=compose-smoke`.
```

- [ ] **Step 8: Commit HDFS compose changes**

```powershell
git add docker-compose.yml README.md src/test/java/com/example/databenchmark/ComposeTopologyTest.java
git commit -m "feat: use hdfs warehouse in compose"
```

### Task 3: SQL Templates And Query Rendering

**Files:**
- Create: `src/main/java/com/example/databenchmark/engine/SqlTemplates.java`
- Create: `src/main/java/com/example/databenchmark/engine/SqlRenderer.java`
- Test: `src/test/java/com/example/databenchmark/engine/SqlTemplatesTest.java`
- Test: `src/test/java/com/example/databenchmark/engine/SqlRendererTest.java`

- [ ] **Step 1: Add SQL template tests**

Create `src/test/java/com/example/databenchmark/engine/SqlTemplatesTest.java`:

```java
package com.example.databenchmark.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.schema.KpiSchema;
import org.junit.jupiter.api.Test;

class SqlTemplatesTest {
    @Test
    void sparkIcebergDdlUsesHiveCatalogAndHdfsWarehouse() {
        String sql = SqlTemplates.sparkCreateIcebergTable();

        assertThat(sql).contains("CREATE TABLE IF NOT EXISTS iceberg_catalog.iceberg_db.cell_kpi_1min");
        assertThat(sql).contains("USING iceberg");
        assertThat(sql).contains("PARTITIONED BY (days(event_time))");
        assertThat(sql).contains("event_time TIMESTAMP");
        assertThat(sql).contains("energy_kwh DOUBLE");
        assertThat(sql).doesNotContain("s3").doesNotContain("minio");
    }

    @Test
    void starrocksInternalDdlContainsAllColumnsInSchemaOrder() {
        String sql = SqlTemplates.starRocksCreateInternalTable();

        assertThat(sql).contains("CREATE DATABASE IF NOT EXISTS sr_internal");
        assertThat(sql).contains("CREATE TABLE IF NOT EXISTS sr_internal.cell_kpi_1min");
        for (String column : KpiSchema.columnNames()) {
            assertThat(sql).contains(column);
        }
        assertThat(sql).contains("DUPLICATE KEY(event_time, cell_id)");
        assertThat(sql).contains("DISTRIBUTED BY HASH(cell_id)");
    }

    @Test
    void externalCatalogUsesHiveMetastoreOnly() {
        String sql = SqlTemplates.starRocksCreateExternalCatalog();

        assertThat(sql).contains("CREATE EXTERNAL CATALOG IF NOT EXISTS sr_external_iceberg");
        assertThat(sql).contains("\"iceberg.catalog.type\" = \"hive\"");
        assertThat(sql).contains("thrift://hive-metastore:9083");
        assertThat(sql).doesNotContain("aws").doesNotContain("s3").doesNotContain("minio");
    }
}
```

- [ ] **Step 2: Add query renderer tests**

Create `src/test/java/com/example/databenchmark/engine/SqlRendererTest.java`:

```java
package com.example.databenchmark.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SqlRendererTest {
    @Test
    void rendersSparkTableName() {
        String sql = SqlRenderer.render("single_cell_day_trend", "spark_iceberg");

        assertThat(sql).contains("iceberg_catalog.iceberg_db.cell_kpi_1min");
        assertThat(sql).doesNotContain("{table}");
    }

    @Test
    void rendersStarRocksInternalTableName() {
        String sql = SqlRenderer.render("topn_high_load_cells", "starrocks_internal");

        assertThat(sql).contains("sr_internal.cell_kpi_1min");
        assertThat(sql).doesNotContain("{table}");
    }

    @Test
    void rendersStarRocksExternalTableName() {
        String sql = SqlRenderer.render("weak_coverage_cells", "starrocks_external_iceberg");

        assertThat(sql).contains("sr_external_iceberg.iceberg_db.cell_kpi_1min");
        assertThat(sql).doesNotContain("{table}");
    }
}
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```powershell
mvn -Dtest=SqlTemplatesTest,SqlRendererTest test
```

Expected: FAIL because `SqlTemplates` and `SqlRenderer` do not exist.

- [ ] **Step 4: Implement SQL templates**

Create `src/main/java/com/example/databenchmark/engine/SqlTemplates.java` with deterministic strings generated from `KpiSchema.columns()`. Map types:

```text
timestamp_ms -> TIMESTAMP
string -> VARCHAR(64) for StarRocks and STRING for Spark
int -> INT
double -> DOUBLE
```

Provide methods:

```java
public static String sparkCreateIcebergTable()
public static String sparkInsertFromParquet(String parquetPath)
public static String starRocksCreateInternalTable()
public static String starRocksCreateExternalCatalog()
public static String starRocksRefreshExternalCatalog()
```

`sparkInsertFromParquet` must use a temp view:

```sql
CREATE OR REPLACE TEMPORARY VIEW generated_kpi
USING parquet
OPTIONS (path '<parquetPath>');

INSERT INTO iceberg_catalog.iceberg_db.cell_kpi_1min
SELECT * FROM generated_kpi;
```

- [ ] **Step 5: Implement SQL renderer**

Create `src/main/java/com/example/databenchmark/engine/SqlRenderer.java`:

```java
package com.example.databenchmark.engine;

import com.example.databenchmark.query.QueryCatalog;
import com.example.databenchmark.query.QueryDefinition;
import java.util.Map;

public final class SqlRenderer {
    private static final Map<String, String> TABLES = Map.of(
        "spark_iceberg", "iceberg_catalog.iceberg_db.cell_kpi_1min",
        "starrocks_internal", "sr_internal.cell_kpi_1min",
        "starrocks_external_iceberg", "sr_external_iceberg.iceberg_db.cell_kpi_1min"
    );

    private SqlRenderer() {}

    public static String render(String queryName, String engineKey) {
        String table = TABLES.get(engineKey);
        if (table == null) {
            throw new IllegalArgumentException("Unknown engine key: " + engineKey);
        }
        QueryDefinition query = QueryCatalog.queries().stream()
            .filter(candidate -> candidate.name().equals(queryName))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown query: " + queryName));
        return query.template().replace("{table}", table);
    }
}
```

- [ ] **Step 6: Run SQL tests**

Run:

```powershell
mvn -Dtest=SqlTemplatesTest,SqlRendererTest test
```

Expected: PASS.

- [ ] **Step 7: Commit SQL renderer**

```powershell
git add src/main/java/com/example/databenchmark/engine src/test/java/com/example/databenchmark/engine
git commit -m "feat: add engine sql templates"
```

### Task 4: StarRocks CSV Exporter

**Files:**
- Create: `src/main/java/com/example/databenchmark/engine/StarRocksCsvExporter.java`
- Test: `src/test/java/com/example/databenchmark/engine/StarRocksCsvExporterTest.java`

- [ ] **Step 1: Add CSV exporter test**

Create `StarRocksCsvExporterTest` that generates 12 Parquet rows, exports CSV, and asserts:

```java
assertThat(lines).hasSize(12);
assertThat(lines.get(0)).startsWith("2026-01-01 00:00:00,CELL-000000,province-00,city-000");
assertThat(lines.get(0).split(",", -1)).hasSize(50);
assertThat(lines).allSatisfy(line -> assertThat(line).doesNotContain("null"));
```

- [ ] **Step 2: Run test and verify failure**

Run:

```powershell
mvn -Dtest=StarRocksCsvExporterTest test
```

Expected: FAIL because exporter does not exist.

- [ ] **Step 3: Implement exporter**

Create `StarRocksCsvExporter`:

```java
public final class StarRocksCsvExporter {
    public Path export(DatasetResult dataset, Path outputDir) throws IOException
}
```

Implementation requirements:

- Read each Parquet file with `AvroParquetReader<GenericRecord>`.
- Write `cell_kpi_1min.csv` under `outputDir`.
- No header row.
- Column order follows `KpiSchema.columnNames()`.
- `event_time` converts epoch millis to `yyyy-MM-dd HH:mm:ss` UTC.
- Escape CSV fields containing comma, quote, CR, or LF using double quotes and doubled internal quotes.

- [ ] **Step 4: Run exporter test**

Run:

```powershell
mvn -Dtest=StarRocksCsvExporterTest test
```

Expected: PASS.

- [ ] **Step 5: Commit exporter**

```powershell
git add src/main/java/com/example/databenchmark/engine/StarRocksCsvExporter.java src/test/java/com/example/databenchmark/engine/StarRocksCsvExporterTest.java
git commit -m "feat: export starrocks load csv"
```

### Task 5: Engine Result Model, Report, And Metrics

**Files:**
- Create: `src/main/java/com/example/databenchmark/engine/EngineRunResult.java`
- Create: `src/main/java/com/example/databenchmark/engine/EngineStage.java`
- Modify: `src/main/java/com/example/databenchmark/report/BenchmarkReport.java`
- Modify: `src/main/java/com/example/databenchmark/report/HtmlReportWriter.java`
- Modify: `src/main/resources/report.ftl`
- Modify: `src/main/java/com/example/databenchmark/metrics/BenchmarkMetrics.java`
- Test: `src/test/java/com/example/databenchmark/report/HtmlReportWriterTest.java`
- Test: `src/test/java/com/example/databenchmark/runner/LocalBenchmarkRunnerTest.java`

- [ ] **Step 1: Add report tests for degraded engine details**

Update `HtmlReportWriterTest` to assert HTML contains:

```text
Status
Error
DEGRADED
starrocks_external_iceberg
catalog refresh failed
```

Use a sample `BenchmarkReport` with one successful load, one failed load, one successful query, and one failed query.

- [ ] **Step 2: Add metrics tests**

Create or extend metrics tests to assert the registry exposes metric names:

```text
benchmark_load_duration_seconds
benchmark_load_rows_total
benchmark_load_bytes_total
benchmark_query_duration_seconds
benchmark_query_rows_total
benchmark_query_failures_total
```

and labels include `run_id`, `profile`, `engine`, `table_shape`, `stage`, `query_name` where applicable.

- [ ] **Step 3: Run report and metrics tests and verify failure**

Run:

```powershell
mvn -Dtest=HtmlReportWriterTest,LocalBenchmarkRunnerTest test
```

Expected: FAIL because report records do not yet carry status/error fields.

- [ ] **Step 4: Implement engine result records**

Create:

```java
package com.example.databenchmark.engine;

public enum EngineStage {
    GENERATE,
    SPARK_ICEBERG_LOAD,
    STARROCKS_INTERNAL_LOAD,
    STARROCKS_EXTERNAL_REFRESH,
    QUERY
}
```

Create:

```java
package com.example.databenchmark.engine;

public record EngineRunResult(
    String engine,
    String tableShape,
    String stage,
    String queryName,
    long rows,
    long bytes,
    double durationSeconds,
    boolean success,
    String error
) {}
```

- [ ] **Step 5: Extend report records**

Modify `BenchmarkReport`:

- `LoadSummary` fields: `engine`, `tableShape`, `stage`, `rows`, `bytes`, `durationSeconds`, `success`, `error`.
- `QuerySummary` fields: `engine`, `tableShape`, `queryName`, `p50Ms`, `p95Ms`, `p99Ms`, `rows`, `failures`, `success`, `error`.
- Add method `status()` returning `SUCCESS` when all load and query summaries are successful, otherwise `DEGRADED`.

- [ ] **Step 6: Update report template**

Update `report.ftl`:

- Add run status near metadata.
- Add `Engine`, `Table Shape`, `Status`, and `Error` columns to load and query tables.
- Escape error text through FreeMarker default HTML escaping.
- Keep Grafana link.

- [ ] **Step 7: Update LocalBenchmarkRunner**

Local mode should create:

- Generate load summary with `engine=local`, `tableShape=generated_parquet`, `success=true`.
- Catalog render query summary with `success=true`, `rows=0`, `failures=0`.

- [ ] **Step 8: Run affected tests**

Run:

```powershell
mvn -Dtest=HtmlReportWriterTest,LocalBenchmarkRunnerTest test
```

Expected: PASS.

- [ ] **Step 9: Commit report and metrics model**

```powershell
git add src/main/java/com/example/databenchmark/engine src/main/java/com/example/databenchmark/report src/main/java/com/example/databenchmark/metrics src/main/java/com/example/databenchmark/runner src/main/resources/report.ftl src/test/java/com/example/databenchmark
git commit -m "feat: report engine stage status"
```

### Task 6: Command, JDBC, Spark, And StarRocks Clients

**Files:**
- Modify: `pom.xml`
- Create: `src/main/java/com/example/databenchmark/engine/CommandResult.java`
- Create: `src/main/java/com/example/databenchmark/engine/CommandRunner.java`
- Create: `src/main/java/com/example/databenchmark/engine/JdbcExecutor.java`
- Create: `src/main/java/com/example/databenchmark/engine/SparkIcebergClient.java`
- Create: `src/main/java/com/example/databenchmark/engine/StarRocksStreamLoadClient.java`
- Create: `src/main/java/com/example/databenchmark/engine/StarRocksClient.java`
- Test: `src/test/java/com/example/databenchmark/engine/CommandRunnerTest.java`
- Test: `src/test/java/com/example/databenchmark/engine/StarRocksClientTest.java`
- Test: `src/test/java/com/example/databenchmark/engine/SparkIcebergClientTest.java`

- [ ] **Step 1: Add JDBC dependency**

Add to `pom.xml`:

```xml
<dependency>
  <groupId>com.mysql</groupId>
  <artifactId>mysql-connector-j</artifactId>
  <version>8.4.0</version>
</dependency>
```

- [ ] **Step 2: Add command runner tests**

Test successful command captures stdout and exit code; failed command captures stderr and nonzero exit code. On Windows use PowerShell:

```java
List<String> command = List.of("powershell", "-NoProfile", "-Command", "Write-Output ok");
```

- [ ] **Step 3: Implement CommandRunner**

`CommandRunner` must:

- Accept `List<String> command`, `Path workingDirectory`, and timeout.
- Capture stdout and stderr separately.
- Return `CommandResult(command, exitCode, stdout, stderr, durationSeconds)`.
- Kill the process on timeout and return exit code `-1` with stderr containing `Timed out`.

- [ ] **Step 4: Implement SparkIcebergClient**

`SparkIcebergClient` builds commands:

```powershell
docker compose -f docker-compose.yml exec -T spark spark-sql --conf spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.iceberg_catalog.type=hive --conf spark.sql.catalog.iceberg_catalog.uri=thrift://hive-metastore:9083 --conf spark.sql.catalog.iceberg_catalog.warehouse=hdfs://hdfs-namenode:8020/warehouse/iceberg -e "<sql>"
```

It exposes:

```java
EngineRunResult load(DatasetResult dataset, String runId, String profile)
List<EngineRunResult> runQueries(String runId, String profile)
```

- [ ] **Step 5: Implement JDBC and StarRocks client**

`JdbcExecutor` connects to:

```text
jdbc:mysql://localhost:9030/?useSSL=false&allowPublicKeyRetrieval=true
```

Default user is `root`, password is empty.

`StarRocksClient` exposes:

```java
EngineRunResult loadInternal(Path csv, String runId, String profile)
EngineRunResult refreshExternalCatalog(String runId, String profile)
List<EngineRunResult> runQueries(String runId, String profile)
```

Stream Load URL:

```text
http://localhost:8030/api/sr_internal/cell_kpi_1min/_stream_load
```

Headers:

```text
Authorization: Basic base64("root:")
label: <run_id>_<timestamp>
column_separator: ,
format: csv
```

- [ ] **Step 6: Add client unit tests**

Tests should verify command construction, JDBC URL defaults, Stream Load URL/headers, and that failed command/JDBC/HTTP results become `EngineRunResult.success=false` with an error string.

- [ ] **Step 7: Run client tests**

Run:

```powershell
mvn -Dtest=CommandRunnerTest,SparkIcebergClientTest,StarRocksClientTest test
```

Expected: PASS without requiring live Docker services by using fake `CommandRunner`, fake HTTP client, or constructor-injected executors.

- [ ] **Step 8: Commit engine clients**

```powershell
git add pom.xml src/main/java/com/example/databenchmark/engine src/test/java/com/example/databenchmark/engine
git commit -m "feat: add spark and starrocks clients"
```

### Task 7: Compose Mode Runner And CLI

**Files:**
- Create: `src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java`
- Modify: `src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java`
- Test: `src/test/java/com/example/databenchmark/BenchmarkRunnerAppTest.java`
- Test: `src/test/java/com/example/databenchmark/runner/ComposeBenchmarkRunnerTest.java`

- [ ] **Step 1: Add CLI tests**

Update `BenchmarkRunnerAppTest`:

- `run --mode local --run-id test-local` behaves like current local run.
- `run --mode compose --run-id compose-test` calls compose runner.
- invalid mode exits nonzero and mentions valid values.

- [ ] **Step 2: Add compose runner tests**

Use fake engine clients to assert `ComposeBenchmarkRunner` calls:

1. Generate dataset.
2. Export CSV.
3. Spark Iceberg load.
4. StarRocks internal load.
5. StarRocks external refresh.
6. Spark queries.
7. StarRocks internal/external queries.
8. HTML report write even when external refresh fails.

- [ ] **Step 3: Run tests and verify failure**

Run:

```powershell
mvn -Dtest=BenchmarkRunnerAppTest,ComposeBenchmarkRunnerTest test
```

Expected: FAIL because compose mode does not exist.

- [ ] **Step 4: Implement ComposeBenchmarkRunner**

`ComposeBenchmarkRunner.run(config, reportRoot, runId)` should:

- Resolve actual run id.
- Generate dataset through `KpiDataGenerator`.
- Export CSV through `StarRocksCsvExporter`.
- Collect load results from Spark and StarRocks.
- Collect query results from Spark and StarRocks.
- Write report even if any stage fails.
- Return nonzero status to CLI when critical prepare/load stages fail, but only after report is written.

- [ ] **Step 5: Add `--mode` to CLI**

Modify `RunCommand`:

```java
@CommandLine.Option(names = "--mode", defaultValue = "local", description = "Run mode: local or compose.")
String mode;
```

Dispatch:

```java
if ("local".equals(mode)) {
    run LocalBenchmarkRunner
} else if ("compose".equals(mode)) {
    run ComposeBenchmarkRunner
} else {
    throw new CommandLine.ParameterException(spec.commandLine(), "Unknown mode: " + mode);
}
```

- [ ] **Step 6: Run CLI tests**

Run:

```powershell
mvn -Dtest=BenchmarkRunnerAppTest,ComposeBenchmarkRunnerTest test
```

Expected: PASS.

- [ ] **Step 7: Commit compose runner**

```powershell
git add src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java src/main/java/com/example/databenchmark/runner src/test/java/com/example/databenchmark
git commit -m "feat: add compose benchmark mode"
```

### Task 8: Documentation And HDFS Consistency Cleanup

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-06-02-starrocks-iceberg-benchmark-design.md`
- Modify: `docs/superpowers/specs/2026-06-03-real-engine-integration-design.md`
- Modify: `docs/superpowers/plans/2026-06-03-real-hdfs-engine-integration.md`

- [ ] **Step 1: Scan for old storage references**

Run:

```powershell
rg -n "MinIO|minio|S3|s3|aws|AWS" README.md docker-compose.yml monitoring src docs/superpowers/specs docs/superpowers/plans
```

Expected: No matches except older historical plans that explicitly describe prior replaced state. If matches appear in live docs or code, update them to HDFS.

- [ ] **Step 2: Update README**

README must include:

- Local mode command.
- Compose mode command.
- HDFS warehouse path.
- Grafana URL.
- Prometheus URL.
- Statement that compose mode writes degraded reports when an engine stage fails.

- [ ] **Step 3: Update this plan if implementation deviates**

If actual compose image names or StarRocks catalog properties differ during implementation, update this plan and the spec with the final verified values before final commit.

- [ ] **Step 4: Commit docs cleanup**

```powershell
git add README.md docs/superpowers/specs docs/superpowers/plans/2026-06-03-real-hdfs-engine-integration.md
git commit -m "docs: document hdfs engine integration"
```

### Task 9: Verification, Compose Smoke, And Staging

**Files:**
- Verify only unless smoke reveals a defect.

- [ ] **Step 1: Run unit tests**

Run:

```powershell
mvn test
```

Expected: PASS with all tests passing.

- [ ] **Step 2: Build package**

Run:

```powershell
mvn package
```

Expected: PASS. Shade overlap warnings are acceptable if the build exits 0.

- [ ] **Step 3: Validate compose**

Run:

```powershell
docker compose -f docker-compose.yml config
```

Expected: PASS.

- [ ] **Step 4: Verify local mode still works**

Run:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --run-id local-after-hdfs
```

Expected: writes `reports/runs/local-after-hdfs/index.html`.

- [ ] **Step 5: Start compose services**

Run:

```powershell
docker compose -f docker-compose.yml up -d hdfs-namenode hdfs-datanode hdfs-init hive-metastore spark starrocks-fe starrocks-be prometheus grafana
```

Expected: services start. If a service exits, inspect logs and fix compose readiness or image configuration before continuing.

- [ ] **Step 6: Run compose mode smoke**

Run:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --run-id compose-smoke
```

Expected: writes `reports/runs/compose-smoke/index.html`. A fully successful run has all engine stages success. A degraded run is acceptable only when the report includes exact failing SQL/command/HTTP error and at least the available stages complete.

- [ ] **Step 7: Verify Grafana and Prometheus**

Run:

```powershell
Invoke-WebRequest -Uri http://localhost:3000/api/health -UseBasicParsing
Invoke-WebRequest -Uri http://localhost:9090/-/ready -UseBasicParsing
Invoke-WebRequest -Uri "http://localhost:3000/d/benchmark?var-run_id=compose-smoke" -UseBasicParsing -MaximumRedirection 0
```

Expected: Grafana health returns 200, Prometheus ready returns 200, dashboard URL redirects to login or returns dashboard HTML.

- [ ] **Step 8: Clean generated data only**

Remove generated `data/` smoke artifacts after verification. Keep `reports/` because it contains user-facing reports.

```powershell
if (Test-Path data) {
  $target = Resolve-Path data
  $workspace = (Get-Location).Path
  if ($target.Path -notlike "$workspace\data*") { throw "Unexpected cleanup target: $($target.Path)" }
  Remove-Item -LiteralPath $target.Path -Recurse -Force
}
```

- [ ] **Step 9: Push and deploy staging**

Run:

```powershell
git push origin main
git checkout staging
git merge --ff-only main
git push origin staging
git checkout main
```

Expected: `origin/main` and `origin/staging` point to the same commit.

- [ ] **Step 10: Verify staging deployment**

Use GitHub API if `gh` is unavailable:

```powershell
$repo='w00125533/data-benchmark'
$runs = Invoke-RestMethod -Uri "https://api.github.com/repos/$repo/actions/runs?branch=staging&per_page=5" -Headers @{ 'User-Agent'='codex' }
$runs.workflow_runs | Select-Object -First 1 name,head_branch,head_sha,status,conclusion,html_url
$deployment = (Invoke-RestMethod -Uri "https://api.github.com/repos/$repo/deployments?environment=staging&per_page=1" -Headers @{ 'User-Agent'='codex' })[0]
Invoke-RestMethod -Uri $deployment.statuses_url -Headers @{ 'User-Agent'='codex' } | Select-Object -First 1 state,created_at,description
```

Expected: latest staging workflow `completed/success` and latest staging deployment status `success`.

## Self-Review

- Spec coverage: This plan covers HDFS compose topology, HDFS warehouse readiness, Spark Iceberg SQL, StarRocks internal CSV load path, StarRocks external Iceberg catalog, shared query rendering, report/metrics extensions, Grafana provisioning, CLI compose mode, local mode preservation, verification, and staging deployment.
- Placeholder scan: This plan does not use open-ended implementation placeholders.
- Type consistency: New Java classes are consistently named under `com.example.databenchmark.engine`, CLI mode is `local|compose`, Grafana dashboard uid is `benchmark`, and HDFS warehouse path is `hdfs://hdfs-namenode:8020/warehouse/iceberg`.

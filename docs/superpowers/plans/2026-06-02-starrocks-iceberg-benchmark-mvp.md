# StarRocks Iceberg Benchmark MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first Java 17 smoke benchmark scaffold for deterministic wireless KPI data generation, benchmark configuration, query definitions, Prometheus metrics, static HTML reporting, and Docker Compose topology.

**Architecture:** Implement a Maven-based Java 17 command-line runner with small focused packages for config, schema, data generation, query catalog, metrics, orchestration, and reporting. The MVP is unit-testable without starting StarRocks or Spark, while Docker Compose defines the target benchmark topology for later integration.

**Tech Stack:** Java 17, Maven, JUnit 5, AssertJ, Jackson YAML, Picocli, Apache Parquet/Avro, Micrometer Prometheus registry, FreeMarker, Docker Compose.

---

## Scope Boundary

The design spec spans multiple independent subsystems. This plan implements the first working Java slice:

- Included: Java 17 Maven project, deterministic `cell_kpi_1min` schema, deterministic data generation skeleton, config profiles, query catalog, Prometheus metric names, HTML report generation, Docker Compose service topology, smoke tests.
- Excluded from this MVP plan: production-grade Spark Iceberg writes, StarRocks internal table load, StarRocks external Iceberg catalog refresh, Grafana dashboard JSON, and default local generation of all 14.4M smoke rows.
- Follow-up plans should cover: `starrocks-spark-ingestion`, `query-execution-engines`, and `observability-dashboards`.

## File Structure

- Create: `pom.xml`
  - Java 17 Maven project, dependency and plugin configuration, executable fat-jar CLI entry point.
- Create: `src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java`
  - Picocli root command with `generate`, `run`, and `report` subcommands.
- Create: `src/main/java/com/example/databenchmark/config/BenchmarkConfig.java`
  - Immutable config model and override helpers.
- Create: `src/main/java/com/example/databenchmark/config/BenchmarkConfigLoader.java`
  - YAML config loader.
- Create: `src/main/java/com/example/databenchmark/schema/KpiColumn.java`
  - Schema column record.
- Create: `src/main/java/com/example/databenchmark/schema/KpiSchema.java`
  - Exact 50-column KPI schema and table shape names.
- Create: `src/main/java/com/example/databenchmark/generator/DatasetResult.java`
  - Generation result record.
- Create: `src/main/java/com/example/databenchmark/generator/KpiDataGenerator.java`
  - Deterministic KPI data generator that writes partitioned real Parquet output.
- Create: `src/main/java/com/example/databenchmark/query/BenchmarkEngine.java`
  - Engine/table-shape record.
- Create: `src/main/java/com/example/databenchmark/query/QueryDefinition.java`
  - Query definition record.
- Create: `src/main/java/com/example/databenchmark/query/QueryCatalog.java`
  - Required logical query scenarios and rendering.
- Create: `src/main/java/com/example/databenchmark/metrics/BenchmarkMetrics.java`
  - Required Prometheus metric names and label names.
- Create: `src/main/java/com/example/databenchmark/report/BenchmarkReport.java`
  - Report data model.
- Create: `src/main/java/com/example/databenchmark/report/HtmlReportWriter.java`
  - Static HTML report writer.
- Create: `src/main/java/com/example/databenchmark/runner/LocalBenchmarkRunner.java`
  - Generate-data-and-report local smoke orchestrator.
- Create: `src/main/resources/report.ftl`
  - HTML report template.
- Create: `configs/benchmark-smoke.yml`
  - Default smoke configuration from the spec with a CI-safe `rowCap`.
- Create: `docker-compose.yml`
  - Service topology for StarRocks FE/BE, Spark, Hive metastore, MinIO, Prometheus, Grafana, and benchmark runner.
- Create: `monitoring/prometheus.yml`
  - Scrape config for runner and infrastructure placeholders.
- Create tests under `src/test/java/com/example/databenchmark/...`.
- Modify: `README.md`
  - Add Java 17 MVP usage.

---

### Task 1: Java 17 Maven Project Skeleton

**Files:**
- Create: `pom.xml`
- Create: `src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java`
- Test: `src/test/java/com/example/databenchmark/BenchmarkRunnerAppTest.java`

- [ ] **Step 1: Write the failing CLI test**

Create `src/test/java/com/example/databenchmark/BenchmarkRunnerAppTest.java`:

```java
package com.example.databenchmark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class BenchmarkRunnerAppTest {
    @Test
    void helpListsCoreCommands() {
        CommandLine commandLine = new CommandLine(new BenchmarkRunnerApp());

        String usage = commandLine.getUsageMessage();

        assertThat(usage).contains("generate");
        assertThat(usage).contains("run");
        assertThat(usage).contains("report");
    }
}
```

- [ ] **Step 2: Run the failing test**

Run:

```powershell
mvn -q -Dtest=BenchmarkRunnerAppTest test
```

Expected:

```text
Compilation failure: package picocli does not exist
```

- [ ] **Step 3: Add Maven project metadata**

Create `pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.example</groupId>
  <artifactId>data-benchmark</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <name>data-benchmark</name>

  <properties>
    <maven.compiler.release>17</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <junit.version>5.10.2</junit.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>info.picocli</groupId>
      <artifactId>picocli</artifactId>
      <version>4.7.6</version>
    </dependency>
    <dependency>
      <groupId>com.fasterxml.jackson.dataformat</groupId>
      <artifactId>jackson-dataformat-yaml</artifactId>
      <version>2.17.1</version>
    </dependency>
    <dependency>
      <groupId>org.apache.parquet</groupId>
      <artifactId>parquet-avro</artifactId>
      <version>1.14.1</version>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-common</artifactId>
      <version>3.3.6</version>
    </dependency>
    <dependency>
      <groupId>io.micrometer</groupId>
      <artifactId>micrometer-registry-prometheus</artifactId>
      <version>1.13.1</version>
    </dependency>
    <dependency>
      <groupId>org.freemarker</groupId>
      <artifactId>freemarker</artifactId>
      <version>2.3.33</version>
    </dependency>
    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <version>${junit.version}</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.assertj</groupId>
      <artifactId>assertj-core</artifactId>
      <version>3.26.0</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>3.2.5</version>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.3</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals>
              <goal>shade</goal>
            </goals>
            <configuration>
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>com.example.databenchmark.BenchmarkRunnerApp</mainClass>
                </transformer>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 4: Add root CLI command**

Create `src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java`:

```java
package com.example.databenchmark;

import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "benchmark-runner",
    mixinStandardHelpOptions = true,
    description = "Run the StarRocks and Iceberg benchmark workflow.",
    subcommands = {
        BenchmarkRunnerApp.GenerateCommand.class,
        BenchmarkRunnerApp.RunCommand.class,
        BenchmarkRunnerApp.ReportCommand.class
    })
public class BenchmarkRunnerApp implements Callable<Integer> {
    public static void main(String[] args) {
        int exitCode = new CommandLine(new BenchmarkRunnerApp()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return 0;
    }

    @Command(name = "generate", description = "Generate deterministic wireless KPI data skeleton.")
    static class GenerateCommand implements Callable<Integer> {
        @Override
        public Integer call() {
            System.out.println("generate command is available");
            return 0;
        }
    }

    @Command(name = "run", description = "Run the local benchmark workflow.")
    static class RunCommand implements Callable<Integer> {
        @Override
        public Integer call() {
            System.out.println("run command is available");
            return 0;
        }
    }

    @Command(name = "report", description = "Generate an HTML report from benchmark results.")
    static class ReportCommand implements Callable<Integer> {
        @Override
        public Integer call() {
            System.out.println("report command is available");
            return 0;
        }
    }
}
```

- [ ] **Step 5: Run the CLI test**

Run:

```powershell
mvn -q -Dtest=BenchmarkRunnerAppTest test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 6: Commit**

Run:

```powershell
git add pom.xml src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java src/test/java/com/example/databenchmark/BenchmarkRunnerAppTest.java
git commit -m "chore: scaffold java benchmark runner"
```

Expected:

```text
[main <sha>] chore: scaffold java benchmark runner
```

---

### Task 2: Config Profiles

**Files:**
- Create: `src/main/java/com/example/databenchmark/config/BenchmarkConfig.java`
- Create: `src/main/java/com/example/databenchmark/config/BenchmarkConfigLoader.java`
- Create: `configs/benchmark-smoke.yml`
- Test: `src/test/java/com/example/databenchmark/config/BenchmarkConfigLoaderTest.java`

- [ ] **Step 1: Write config tests**

Create `src/test/java/com/example/databenchmark/config/BenchmarkConfigLoaderTest.java`:

```java
package com.example.databenchmark.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class BenchmarkConfigLoaderTest {
    @Test
    void loadsSmokeDefaults() throws Exception {
        BenchmarkConfig config = new BenchmarkConfigLoader().load(Path.of("configs/benchmark-smoke.yml"));

        assertThat(config.profile()).isEqualTo("smoke");
        assertThat(config.seed()).isEqualTo(20260602L);
        assertThat(config.dataset().cells()).isEqualTo(10_000);
        assertThat(config.dataset().days()).isEqualTo(1);
        assertThat(config.dataset().columns()).isEqualTo(50);
        assertThat(config.dataset().rowCap()).isEqualTo(10_000L);
        assertThat(config.query().coldRuns()).isEqualTo(1);
        assertThat(config.query().warmRuns()).isEqualTo(3);
        assertThat(config.report().format()).isEqualTo("html");
    }

    @Test
    void overridesKeepProfileNameAndChangeValues() {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(25, 2, 7L, "target/generated-test-data", 100L);

        assertThat(config.profile()).isEqualTo("smoke");
        assertThat(config.seed()).isEqualTo(7L);
        assertThat(config.dataset().cells()).isEqualTo(25);
        assertThat(config.dataset().days()).isEqualTo(2);
        assertThat(config.dataset().output()).isEqualTo("target/generated-test-data");
        assertThat(config.dataset().rowCap()).isEqualTo(100L);
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
mvn -q -Dtest=BenchmarkConfigLoaderTest test
```

Expected:

```text
Compilation failure: cannot find symbol class BenchmarkConfig
```

- [ ] **Step 3: Add smoke config YAML**

Create `configs/benchmark-smoke.yml`:

```yaml
profile: smoke
seed: 20260602
dataset:
  cells: 10000
  days: 1
  columns: 50
  startTime: "2026-01-01T00:00:00"
  output: "data/generated"
  rowCap: 10000
query:
  coldRuns: 1
  warmRuns: 3
  concurrency: 1
report:
  format: html
  output: "reports/runs"
monitoring:
  prometheus: true
  grafana: true
```

- [ ] **Step 4: Implement config model**

Create `src/main/java/com/example/databenchmark/config/BenchmarkConfig.java`:

```java
package com.example.databenchmark.config;

public record BenchmarkConfig(
    String profile,
    long seed,
    DatasetConfig dataset,
    QueryConfig query,
    ReportConfig report,
    MonitoringConfig monitoring
) {
    public static BenchmarkConfig defaultSmoke() {
        return new BenchmarkConfig(
            "smoke",
            20260602L,
            new DatasetConfig(10_000, 1, 50, "2026-01-01T00:00:00", "data/generated", 10_000L),
            new QueryConfig(1, 3, 1),
            new ReportConfig("html", "reports/runs"),
            new MonitoringConfig(true, true)
        );
    }

    public BenchmarkConfig withOverrides(Integer cells, Integer days, Long seed, String output, Long rowCap) {
        DatasetConfig current = dataset;
        DatasetConfig updated = new DatasetConfig(
            cells == null ? current.cells() : cells,
            days == null ? current.days() : days,
            current.columns(),
            current.startTime(),
            output == null ? current.output() : output,
            rowCap == null ? current.rowCap() : rowCap
        );
        return new BenchmarkConfig(
            profile,
            seed == null ? this.seed : seed,
            updated,
            query,
            report,
            monitoring
        );
    }

    public record DatasetConfig(int cells, int days, int columns, String startTime, String output, Long rowCap) {}
    public record QueryConfig(int coldRuns, int warmRuns, int concurrency) {}
    public record ReportConfig(String format, String output) {}
    public record MonitoringConfig(boolean prometheus, boolean grafana) {}
}
```

- [ ] **Step 5: Implement YAML loader**

Create `src/main/java/com/example/databenchmark/config/BenchmarkConfigLoader.java`:

```java
package com.example.databenchmark.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;

public class BenchmarkConfigLoader {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory()).findAndRegisterModules();

    public BenchmarkConfig load(Path path) throws IOException {
        return mapper.readValue(path.toFile(), BenchmarkConfig.class);
    }
}
```

- [ ] **Step 6: Run config tests**

Run:

```powershell
mvn -q -Dtest=BenchmarkConfigLoaderTest test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 7: Commit**

Run:

```powershell
git add src/main/java/com/example/databenchmark/config/BenchmarkConfig.java src/main/java/com/example/databenchmark/config/BenchmarkConfigLoader.java configs/benchmark-smoke.yml src/test/java/com/example/databenchmark/config/BenchmarkConfigLoaderTest.java
git commit -m "feat: add java benchmark configuration"
```

Expected:

```text
[main <sha>] feat: add java benchmark configuration
```

---

### Task 3: Wireless KPI Schema

**Files:**
- Create: `src/main/java/com/example/databenchmark/schema/KpiColumn.java`
- Create: `src/main/java/com/example/databenchmark/schema/KpiSchema.java`
- Test: `src/test/java/com/example/databenchmark/schema/KpiSchemaTest.java`

- [ ] **Step 1: Write schema tests**

Create `src/test/java/com/example/databenchmark/schema/KpiSchemaTest.java`:

```java
package com.example.databenchmark.schema;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class KpiSchemaTest {
    @Test
    void schemaHasExactly50Columns() {
        assertThat(KpiSchema.columns()).hasSize(50);
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
        assertThat(KpiSchema.tableShapes()).containsEntry("spark_iceberg", "iceberg_db.cell_kpi_1min");
        assertThat(KpiSchema.tableShapes()).containsEntry("starrocks_internal", "sr_internal.cell_kpi_1min");
        assertThat(KpiSchema.tableShapes()).containsEntry("starrocks_external_iceberg", "sr_external_iceberg.cell_kpi_1min");
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
mvn -q -Dtest=KpiSchemaTest test
```

Expected:

```text
Compilation failure: cannot find symbol class KpiSchema
```

- [ ] **Step 3: Add schema column record**

Create `src/main/java/com/example/databenchmark/schema/KpiColumn.java`:

```java
package com.example.databenchmark.schema;

public record KpiColumn(String name, String logicalType) {}
```

- [ ] **Step 4: Add schema definition**

Create `src/main/java/com/example/databenchmark/schema/KpiSchema.java`:

```java
package com.example.databenchmark.schema;

import java.util.List;
import java.util.Map;

public final class KpiSchema {
    private static final List<KpiColumn> COLUMNS = List.of(
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

    private static final Map<String, String> TABLE_SHAPES = Map.of(
        "spark_iceberg", "iceberg_db.cell_kpi_1min",
        "starrocks_internal", "sr_internal.cell_kpi_1min",
        "starrocks_external_iceberg", "sr_external_iceberg.cell_kpi_1min"
    );

    private KpiSchema() {}

    public static List<KpiColumn> columns() {
        return COLUMNS;
    }

    public static List<String> columnNames() {
        return COLUMNS.stream().map(KpiColumn::name).toList();
    }

    public static Map<String, String> tableShapes() {
        return TABLE_SHAPES;
    }
}
```

- [ ] **Step 5: Run schema tests**

Run:

```powershell
mvn -q -Dtest=KpiSchemaTest test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 6: Commit**

Run:

```powershell
git add src/main/java/com/example/databenchmark/schema/KpiColumn.java src/main/java/com/example/databenchmark/schema/KpiSchema.java src/test/java/com/example/databenchmark/schema/KpiSchemaTest.java
git commit -m "feat: define java wireless kpi schema"
```

Expected:

```text
[main <sha>] feat: define java wireless kpi schema
```

---

### Task 4: Deterministic Data Generator Skeleton

**Files:**
- Create: `src/main/java/com/example/databenchmark/generator/DatasetResult.java`
- Create: `src/main/java/com/example/databenchmark/generator/KpiDataGenerator.java`
- Modify: `src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java`
- Test: `src/test/java/com/example/databenchmark/generator/KpiDataGeneratorTest.java`

- [ ] **Step 1: Write generator tests**

Create `src/test/java/com/example/databenchmark/generator/KpiDataGeneratorTest.java`:

```java
package com.example.databenchmark.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KpiDataGeneratorTest {
    @TempDir
    Path tempDir;

    @Test
    void generatorWritesDeterministicPartitionedOutput() throws Exception {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(3, 1, 123L, tempDir.toString(), 12L);
        KpiDataGenerator generator = new KpiDataGenerator();

        DatasetResult first = generator.generate(config);
        DatasetResult second = generator.generate(config);

        assertThat(first.rows()).isEqualTo(12L);
        assertThat(first.bytesWritten()).isPositive();
        assertThat(first.files()).isEqualTo(second.files());
        assertThat(first.files()).hasSize(1);
        assertThat(Files.readString(first.files().get(0))).contains("CELL-000000");
        assertThat(first.files().get(0).toString()).contains("event_date=2026-01-01");
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
mvn -q -Dtest=KpiDataGeneratorTest test
```

Expected:

```text
Compilation failure: cannot find symbol class KpiDataGenerator
```

- [ ] **Step 3: Add generator result record**

Create `src/main/java/com/example/databenchmark/generator/DatasetResult.java`:

```java
package com.example.databenchmark.generator;

import java.nio.file.Path;
import java.util.List;

public record DatasetResult(Path outputPath, List<Path> files, long rows, long bytesWritten) {}
```

- [ ] **Step 4: Implement deterministic generator skeleton**

Create `src/main/java/com/example/databenchmark/generator/KpiDataGenerator.java`:

```java
package com.example.databenchmark.generator;

import com.example.databenchmark.config.BenchmarkConfig;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Random;

public class KpiDataGenerator {
    public DatasetResult generate(BenchmarkConfig config) throws IOException {
        long rows = targetRows(config);
        Path outputPath = Path.of(config.dataset().output());
        LocalDateTime start = LocalDateTime.parse(config.dataset().startTime());
        String eventDate = start.toLocalDate().toString();
        Path partition = outputPath.resolve("event_date=" + eventDate);
        Files.createDirectories(partition);
        Path file = partition.resolve("part-00000.parquet");
        writeDeterministicRows(config, rows, start, file);
        return new DatasetResult(outputPath, List.of(file), rows, Files.size(file));
    }

    private long targetRows(BenchmarkConfig config) {
        long fullRows = (long) config.dataset().cells() * config.dataset().days() * 24L * 60L;
        Long rowCap = config.dataset().rowCap();
        return rowCap == null ? fullRows : Math.min(fullRows, rowCap);
    }

    private void writeDeterministicRows(BenchmarkConfig config, long rows, LocalDateTime start, Path file) throws IOException {
        Random random = new Random(config.seed());
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            writer.write("event_time,cell_id,province,city,vendor,rat,band,rsrp_avg,sinr_avg,active_users,load_score");
            writer.newLine();
            for (long row = 0; row < rows; row++) {
                int cell = (int) (row % config.dataset().cells());
                long minute = row / config.dataset().cells();
                LocalDateTime eventTime = start.plusMinutes(minute);
                double load = 5.0 + random.nextDouble() * 95.0;
                int activeUsers = 1 + random.nextInt(600);
                writer.write(String.format(
                    Locale.ROOT,
                    "%s,CELL-%06d,province-%02d,city-%03d,%s,%s,%s,%.3f,%.3f,%d,%.3f",
                    eventTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    cell,
                    cell % 31,
                    cell % 200,
                    vendor(cell),
                    cell % 2 == 0 ? "4G" : "5G",
                    band(cell),
                    -115.0 + random.nextDouble() * 35.0,
                    -5.0 + random.nextDouble() * 35.0,
                    activeUsers,
                    load
                ));
                writer.newLine();
            }
        }
    }

    private String vendor(int cell) {
        return List.of("Huawei", "ZTE", "Ericsson", "Nokia", "Samsung").get(cell % 5);
    }

    private String band(int cell) {
        return List.of("B3", "B7", "B8", "B20", "N78", "N41").get(cell % 6);
    }
}
```

Real Parquet generation is available in the Java 17 runner. Remaining follow-up work is Spark/Iceberg table writes, StarRocks internal/external load paths, and engine-level benchmark execution.

- [ ] **Step 5: Wire CLI generate command to Java generator**

Modify `src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java` so `GenerateCommand` is:

```java
@Command(name = "generate", description = "Generate deterministic wireless KPI data skeleton.")
static class GenerateCommand implements Callable<Integer> {
    @CommandLine.Option(names = "--config", defaultValue = "configs/benchmark-smoke.yml")
    Path configPath;

    @CommandLine.Option(names = "--cells")
    Integer cells;

    @CommandLine.Option(names = "--days")
    Integer days;

    @CommandLine.Option(names = "--seed")
    Long seed;

    @CommandLine.Option(names = "--output")
    String output;

    @CommandLine.Option(names = "--row-cap")
    Long rowCap;

    @Override
    public Integer call() throws Exception {
        var config = new com.example.databenchmark.config.BenchmarkConfigLoader()
            .load(configPath)
            .withOverrides(cells, days, seed, output, rowCap);
        var result = new com.example.databenchmark.generator.KpiDataGenerator().generate(config);
        System.out.printf("rows=%d bytes=%d output=%s%n", result.rows(), result.bytesWritten(), result.outputPath());
        return 0;
    }
}
```

Also add this import at the top of `BenchmarkRunnerApp.java`:

```java
import java.nio.file.Path;
```

- [ ] **Step 6: Run generator tests**

Run:

```powershell
mvn -q -Dtest=KpiDataGeneratorTest,BenchmarkRunnerAppTest test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 7: Commit**

Run:

```powershell
git add src/main/java/com/example/databenchmark/generator/DatasetResult.java src/main/java/com/example/databenchmark/generator/KpiDataGenerator.java src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java src/test/java/com/example/databenchmark/generator/KpiDataGeneratorTest.java
git commit -m "feat: add java deterministic kpi generator"
```

Expected:

```text
[main <sha>] feat: add java deterministic kpi generator
```

---

### Task 5: Query Catalog and SQL Rendering

**Files:**
- Create: `src/main/java/com/example/databenchmark/query/BenchmarkEngine.java`
- Create: `src/main/java/com/example/databenchmark/query/QueryDefinition.java`
- Create: `src/main/java/com/example/databenchmark/query/QueryCatalog.java`
- Test: `src/test/java/com/example/databenchmark/query/QueryCatalogTest.java`

- [ ] **Step 1: Write query tests**

Create `src/test/java/com/example/databenchmark/query/QueryCatalogTest.java`:

```java
package com.example.databenchmark.query;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class QueryCatalogTest {
    @Test
    void catalogContainsRequiredScenarios() {
        assertThat(QueryCatalog.queries()).extracting(QueryDefinition::name).containsExactly(
            "single_cell_day_trend",
            "single_cell_week_trend",
            "city_vendor_band_rat_minute_agg",
            "topn_high_load_cells",
            "weak_coverage_cells",
            "adjacent_window_kpi_spike",
            "recent_hot_cells",
            "wide_filter_group_by",
            "date_partition_pruning",
            "large_cell_id_filter"
        );
    }

    @Test
    void rendersTopNQueryForEveryEngine() {
        for (BenchmarkEngine engine : QueryCatalog.engines()) {
            String sql = QueryCatalog.render("topn_high_load_cells", engine);

            assertThat(sql).contains(engine.tableName());
            assertThat(sql).contains("load_score");
        }
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
mvn -q -Dtest=QueryCatalogTest test
```

Expected:

```text
Compilation failure: cannot find symbol class QueryCatalog
```

- [ ] **Step 3: Implement query records and catalog**

Create `src/main/java/com/example/databenchmark/query/BenchmarkEngine.java`:

```java
package com.example.databenchmark.query;

public record BenchmarkEngine(String name, String tableShapeKey, String tableName) {}
```

Create `src/main/java/com/example/databenchmark/query/QueryDefinition.java`:

```java
package com.example.databenchmark.query;

public record QueryDefinition(String name, String template) {}
```

Create `src/main/java/com/example/databenchmark/query/QueryCatalog.java`:

```java
package com.example.databenchmark.query;

import com.example.databenchmark.schema.KpiSchema;
import java.util.List;

public final class QueryCatalog {
    private static final List<BenchmarkEngine> ENGINES = List.of(
        new BenchmarkEngine("spark_iceberg", "spark_iceberg", KpiSchema.tableShapes().get("spark_iceberg")),
        new BenchmarkEngine("starrocks_internal", "starrocks_internal", KpiSchema.tableShapes().get("starrocks_internal")),
        new BenchmarkEngine("starrocks_external_iceberg", "starrocks_external_iceberg", KpiSchema.tableShapes().get("starrocks_external_iceberg"))
    );

    private static final List<QueryDefinition> QUERIES = List.of(
        new QueryDefinition("single_cell_day_trend", "select event_time, rsrp_avg, sinr_avg, active_users from {table} where cell_id = 'CELL-000001' order by event_time"),
        new QueryDefinition("single_cell_week_trend", "select date_trunc('hour', event_time) as event_hour, avg(rsrp_avg) as rsrp_avg from {table} where cell_id = 'CELL-000001' group by date_trunc('hour', event_time) order by event_hour"),
        new QueryDefinition("city_vendor_band_rat_minute_agg", "select date_trunc('minute', event_time) as event_minute, city, vendor, band, rat, avg(active_users) as active_users from {table} group by date_trunc('minute', event_time), city, vendor, band, rat"),
        new QueryDefinition("topn_high_load_cells", "select cell_id, max(prb_dl_util) as prb_dl_util, max(active_users) as active_users, max(load_score) as load_score from {table} group by cell_id order by load_score desc limit 100"),
        new QueryDefinition("weak_coverage_cells", "select cell_id, avg(rsrp_avg) as rsrp_avg, avg(rsrp_p10) as rsrp_p10, avg(sinr_avg) as sinr_avg from {table} group by cell_id having avg(rsrp_avg) < -105 or avg(sinr_avg) < 5"),
        new QueryDefinition("adjacent_window_kpi_spike", "select cell_id, event_time, active_users, active_users - lag(active_users) over (partition by cell_id order by event_time) as active_users_delta from {table}"),
        new QueryDefinition("recent_hot_cells", "select cell_id, event_time, load_score from {table} order by event_time desc, load_score desc limit 100"),
        new QueryDefinition("wide_filter_group_by", "select province, city, vendor, rat, avg(dl_traffic_mb) as dl_traffic_mb from {table} where prb_dl_util > 0.7 and sinr_avg > 10 group by province, city, vendor, rat"),
        new QueryDefinition("date_partition_pruning", "select count(*) as rows_in_day from {table} where event_time >= timestamp '2026-01-01 00:00:00' and event_time < timestamp '2026-01-02 00:00:00'"),
        new QueryDefinition("large_cell_id_filter", "select cell_id, avg(load_score) as load_score from {table} where cell_id in ('CELL-000001','CELL-000002','CELL-000003','CELL-000004','CELL-000005') group by cell_id")
    );

    private QueryCatalog() {}

    public static List<BenchmarkEngine> engines() {
        return ENGINES;
    }

    public static List<QueryDefinition> queries() {
        return QUERIES;
    }

    public static String render(String queryName, BenchmarkEngine engine) {
        return QUERIES.stream()
            .filter(query -> query.name().equals(queryName))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown query: " + queryName))
            .template()
            .replace("{table}", engine.tableName());
    }
}
```

- [ ] **Step 4: Run query tests**

Run:

```powershell
mvn -q -Dtest=QueryCatalogTest test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 5: Commit**

Run:

```powershell
git add src/main/java/com/example/databenchmark/query/BenchmarkEngine.java src/main/java/com/example/databenchmark/query/QueryDefinition.java src/main/java/com/example/databenchmark/query/QueryCatalog.java src/test/java/com/example/databenchmark/query/QueryCatalogTest.java
git commit -m "feat: add java benchmark query catalog"
```

Expected:

```text
[main <sha>] feat: add java benchmark query catalog
```

---

### Task 6: Metrics and HTML Report

**Files:**
- Create: `src/main/java/com/example/databenchmark/metrics/BenchmarkMetrics.java`
- Create: `src/main/java/com/example/databenchmark/report/BenchmarkReport.java`
- Create: `src/main/java/com/example/databenchmark/report/HtmlReportWriter.java`
- Create: `src/main/resources/report.ftl`
- Test: `src/test/java/com/example/databenchmark/report/HtmlReportWriterTest.java`

- [ ] **Step 1: Write report test**

Create `src/test/java/com/example/databenchmark/report/HtmlReportWriterTest.java`:

```java
package com.example.databenchmark.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HtmlReportWriterTest {
    @TempDir
    Path tempDir;

    @Test
    void reportContainsRequiredSections() throws Exception {
        BenchmarkReport report = BenchmarkReport.sample("run-test");

        Path output = new HtmlReportWriter().write(report, tempDir);

        String html = Files.readString(output);
        assertThat(output).isEqualTo(tempDir.resolve("run-test").resolve("index.html"));
        assertThat(html).contains("Dataset Summary");
        assertThat(html).contains("Load Summary");
        assertThat(html).contains("Query Summary");
        assertThat(html).contains("run-test");
        assertThat(html).contains("not a 4.032B row full-profile validation");
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
mvn -q -Dtest=HtmlReportWriterTest test
```

Expected:

```text
Compilation failure: cannot find symbol class BenchmarkReport
```

- [ ] **Step 3: Implement metric constants**

Create `src/main/java/com/example/databenchmark/metrics/BenchmarkMetrics.java`:

```java
package com.example.databenchmark.metrics;

import java.util.List;

public final class BenchmarkMetrics {
    public static final List<String> LABELS = List.of("run_id", "profile", "engine", "table_shape", "stage", "query_name");
    public static final String LOAD_DURATION_SECONDS = "benchmark_load_duration_seconds";
    public static final String LOAD_ROWS_TOTAL = "benchmark_load_rows_total";
    public static final String LOAD_BYTES_TOTAL = "benchmark_load_bytes_total";
    public static final String QUERY_DURATION_SECONDS = "benchmark_query_duration_seconds";
    public static final String QUERY_ROWS_TOTAL = "benchmark_query_rows_total";
    public static final String QUERY_FAILURES_TOTAL = "benchmark_query_failures_total";

    private BenchmarkMetrics() {}
}
```

- [ ] **Step 4: Implement report model**

Create `src/main/java/com/example/databenchmark/report/BenchmarkReport.java`:

```java
package com.example.databenchmark.report;

import java.util.List;

public record BenchmarkReport(
    String runId,
    String profile,
    String startedAt,
    String endedAt,
    int cells,
    int days,
    long rows,
    int columns,
    long bytesWritten,
    List<LoadSummary> loadSummaries,
    List<QuerySummary> querySummaries,
    String grafanaUrl,
    boolean fullProfile
) {
    public static BenchmarkReport sample(String runId) {
        return new BenchmarkReport(
            runId,
            "smoke",
            "2026-06-02T00:00:00Z",
            "2026-06-02T00:01:00Z",
            10,
            1,
            14400,
            50,
            2048,
            List.of(new LoadSummary("generate", 14400, 2048, 1.2)),
            List.of(new QuerySummary("spark_iceberg", "iceberg_db.cell_kpi_1min", "topn_high_load_cells", 10, 12, 15, 0)),
            "http://localhost:3000/d/benchmark?var-run_id=" + runId,
            false
        );
    }

    public record LoadSummary(String stage, long rows, long bytes, double durationSeconds) {}
    public record QuerySummary(String engine, String tableShape, String queryName, double p50Ms, double p95Ms, double p99Ms, int failures) {}
}
```

- [ ] **Step 5: Add report template**

Create `src/main/resources/report.ftl`:

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Benchmark Report ${report.runId()}</title>
</head>
<body>
  <h1>Benchmark Report ${report.runId()}</h1>
  <h2>Run Metadata</h2>
  <p>Profile: ${report.profile()} | Started: ${report.startedAt()} | Ended: ${report.endedAt()}</p>
  <#if !report.fullProfile()>
  <p>This run is not a 4.032B row full-profile validation.</p>
  </#if>
  <h2>Dataset Summary</h2>
  <p>Cells: ${report.cells()} | Days: ${report.days()} | Rows: ${report.rows()} | Columns: ${report.columns()} | Bytes: ${report.bytesWritten()}</p>
  <h2>Load Summary</h2>
  <table>
    <tr><th>Stage</th><th>Rows</th><th>Bytes</th><th>Duration Seconds</th></tr>
    <#list report.loadSummaries() as item>
    <tr><td>${item.stage()}</td><td>${item.rows()}</td><td>${item.bytes()}</td><td>${item.durationSeconds()}</td></tr>
    </#list>
  </table>
  <h2>Query Summary</h2>
  <table>
    <tr><th>Engine</th><th>Table Shape</th><th>Query</th><th>P50 ms</th><th>P95 ms</th><th>P99 ms</th><th>Failures</th></tr>
    <#list report.querySummaries() as item>
    <tr><td>${item.engine()}</td><td>${item.tableShape()}</td><td>${item.queryName()}</td><td>${item.p50Ms()}</td><td>${item.p95Ms()}</td><td>${item.p99Ms()}</td><td>${item.failures()}</td></tr>
    </#list>
  </table>
  <p><a href="${report.grafanaUrl()}">Grafana dashboard for this run</a></p>
</body>
</html>
```

- [ ] **Step 6: Implement HTML writer**

Create `src/main/java/com/example/databenchmark/report/HtmlReportWriter.java`:

```java
package com.example.databenchmark.report;

import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class HtmlReportWriter {
    private final Configuration configuration;

    public HtmlReportWriter() {
        configuration = new Configuration(Configuration.VERSION_2_3_33);
        configuration.setClassLoaderForTemplateLoading(getClass().getClassLoader(), "");
        configuration.setDefaultEncoding("UTF-8");
        configuration.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    }

    public Path write(BenchmarkReport report, Path outputRoot) throws IOException {
        Path outputDir = outputRoot.resolve(report.runId());
        Files.createDirectories(outputDir);
        Path output = outputDir.resolve("index.html");
        try (Writer writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            configuration.getTemplate("report.ftl").process(Map.of("report", report), writer);
        } catch (Exception exception) {
            throw new IOException("Failed to write HTML report", exception);
        }
        return output;
    }
}
```

- [ ] **Step 7: Run report tests**

Run:

```powershell
mvn -q -Dtest=HtmlReportWriterTest test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 8: Commit**

Run:

```powershell
git add src/main/java/com/example/databenchmark/metrics/BenchmarkMetrics.java src/main/java/com/example/databenchmark/report/BenchmarkReport.java src/main/java/com/example/databenchmark/report/HtmlReportWriter.java src/main/resources/report.ftl src/test/java/com/example/databenchmark/report/HtmlReportWriterTest.java
git commit -m "feat: add java benchmark metrics and report"
```

Expected:

```text
[main <sha>] feat: add java benchmark metrics and report
```

---

### Task 7: Local Runner and CLI Commands

**Files:**
- Create: `src/main/java/com/example/databenchmark/runner/LocalBenchmarkRunner.java`
- Modify: `src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java`
- Test: `src/test/java/com/example/databenchmark/runner/LocalBenchmarkRunnerTest.java`

- [ ] **Step 1: Write local runner test**

Create `src/test/java/com/example/databenchmark/runner/LocalBenchmarkRunnerTest.java`:

```java
package com.example.databenchmark.runner;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalBenchmarkRunnerTest {
    @TempDir
    Path tempDir;

    @Test
    void localSmokeGeneratesDataAndReport() throws Exception {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(2, 1, 99L, tempDir.resolve("data").toString(), 8L);

        LocalBenchmarkRunner.LocalRunResult result = new LocalBenchmarkRunner()
            .run(config, tempDir.resolve("reports"), "run-local-test");

        assertThat(result.dataset().rows()).isEqualTo(8L);
        assertThat(result.reportPath()).isEqualTo(tempDir.resolve("reports").resolve("run-local-test").resolve("index.html"));
        assertThat(result.reportPath()).exists();
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
mvn -q -Dtest=LocalBenchmarkRunnerTest test
```

Expected:

```text
Compilation failure: cannot find symbol class LocalBenchmarkRunner
```

- [ ] **Step 3: Implement local runner**

Create `src/main/java/com/example/databenchmark/runner/LocalBenchmarkRunner.java`:

```java
package com.example.databenchmark.runner;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.generator.DatasetResult;
import com.example.databenchmark.generator.KpiDataGenerator;
import com.example.databenchmark.report.BenchmarkReport;
import com.example.databenchmark.report.HtmlReportWriter;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public class LocalBenchmarkRunner {
    public LocalRunResult run(BenchmarkConfig config, Path reportRoot, String runId) throws Exception {
        String actualRunId = runId == null ? "run-" + Instant.now().toString().replace(":", "").replace(".", "") : runId;
        Instant started = Instant.now();
        long startedNanos = System.nanoTime();
        DatasetResult dataset = new KpiDataGenerator().generate(config);
        double durationSeconds = (System.nanoTime() - startedNanos) / 1_000_000_000.0;
        Instant ended = Instant.now();
        BenchmarkReport report = new BenchmarkReport(
            actualRunId,
            config.profile(),
            started.toString(),
            ended.toString(),
            config.dataset().cells(),
            config.dataset().days(),
            dataset.rows(),
            config.dataset().columns(),
            dataset.bytesWritten(),
            List.of(new BenchmarkReport.LoadSummary("generate", dataset.rows(), dataset.bytesWritten(), Math.round(durationSeconds * 1000.0) / 1000.0)),
            List.of(new BenchmarkReport.QuerySummary("local", "generated_parquet", "catalog_render_check", 0, 0, 0, 0)),
            "http://localhost:3000/d/benchmark?var-run_id=" + actualRunId,
            "full".equals(config.profile())
        );
        Path reportPath = new HtmlReportWriter().write(report, reportRoot);
        return new LocalRunResult(dataset, reportPath);
    }

    public record LocalRunResult(DatasetResult dataset, Path reportPath) {}
}
```

- [ ] **Step 4: Wire CLI run command**

Modify `RunCommand` in `src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java`:

```java
@Command(name = "run", description = "Run the local benchmark workflow.")
static class RunCommand implements Callable<Integer> {
    @CommandLine.Option(names = "--config", defaultValue = "configs/benchmark-smoke.yml")
    Path configPath;

    @CommandLine.Option(names = "--run-id")
    String runId;

    @Override
    public Integer call() throws Exception {
        var config = new com.example.databenchmark.config.BenchmarkConfigLoader().load(configPath);
        var result = new com.example.databenchmark.runner.LocalBenchmarkRunner()
            .run(config, Path.of(config.report().output()), runId);
        System.out.printf("rows=%d report=%s%n", result.dataset().rows(), result.reportPath());
        return 0;
    }
}
```

- [ ] **Step 5: Run local runner tests**

Run:

```powershell
mvn -q -Dtest=LocalBenchmarkRunnerTest,BenchmarkRunnerAppTest test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 6: Commit**

Run:

```powershell
git add src/main/java/com/example/databenchmark/runner/LocalBenchmarkRunner.java src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java src/test/java/com/example/databenchmark/runner/LocalBenchmarkRunnerTest.java
git commit -m "feat: add java local smoke benchmark runner"
```

Expected:

```text
[main <sha>] feat: add java local smoke benchmark runner
```

---

### Task 8: Docker Compose and Prometheus Topology

**Files:**
- Create: `docker-compose.yml`
- Create: `monitoring/prometheus.yml`
- Test: `src/test/java/com/example/databenchmark/ComposeTopologyTest.java`

- [ ] **Step 1: Write topology tests**

Create `src/test/java/com/example/databenchmark/ComposeTopologyTest.java`:

```java
package com.example.databenchmark;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ComposeTopologyTest {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    void composeDefinesRequiredServices() throws Exception {
        Map<?, ?> compose = mapper.readValue(Path.of("docker-compose.yml").toFile(), Map.class);
        Map<?, ?> services = (Map<?, ?>) compose.get("services");

        assertThat(services.keySet()).contains(
            "starrocks-fe", "starrocks-be", "spark", "hive-metastore",
            "minio", "prometheus", "grafana", "benchmark-runner"
        );
    }

    @Test
    void prometheusScrapesBenchmarkRunner() throws Exception {
        Map<?, ?> prometheus = mapper.readValue(Path.of("monitoring/prometheus.yml").toFile(), Map.class);

        assertThat(prometheus.toString()).contains("benchmark-runner");
        assertThat(prometheus.toString()).contains("benchmark-runner:9108");
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
mvn -q -Dtest=ComposeTopologyTest test
```

Expected:

```text
NoSuchFileException: docker-compose.yml
```

- [ ] **Step 3: Add Compose topology**

Create `docker-compose.yml`:

```yaml
services:
  starrocks-fe:
    image: starrocks/fe-ubuntu:3.3-latest
    ports:
      - "8030:8030"
      - "9030:9030"

  starrocks-be:
    image: starrocks/be-ubuntu:3.3-latest
    depends_on:
      - starrocks-fe
    ports:
      - "8040:8040"

  spark:
    image: bitnami/spark:3.5
    environment:
      SPARK_MODE: master
    ports:
      - "8080:8080"

  hive-metastore:
    image: apache/hive:4.0.0
    environment:
      SERVICE_NAME: metastore
    ports:
      - "9083:9083"

  minio:
    image: minio/minio:RELEASE.2024-06-13T22-53-53Z
    command: ["server", "/data", "--console-address", ":9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"

  prometheus:
    image: prom/prometheus:v2.52.0
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:11.0.0
    ports:
      - "3000:3000"

  benchmark-runner:
    image: eclipse-temurin:17-jre
    working_dir: /workspace
    volumes:
      - .:/workspace
    command: ["java", "-jar", "target/data-benchmark-0.1.0-SNAPSHOT.jar", "run", "--run-id", "compose-smoke"]
    ports:
      - "9108:9108"
    depends_on:
      - minio
      - prometheus
```

- [ ] **Step 4: Add Prometheus config**

Create `monitoring/prometheus.yml`:

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

  - job_name: minio
    static_configs:
      - targets: ["minio:9000"]
```

- [ ] **Step 5: Run topology tests**

Run:

```powershell
mvn -q -Dtest=ComposeTopologyTest test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 6: Commit**

Run:

```powershell
git add docker-compose.yml monitoring/prometheus.yml src/test/java/com/example/databenchmark/ComposeTopologyTest.java
git commit -m "feat: add java benchmark compose topology"
```

Expected:

```text
[main <sha>] feat: add java benchmark compose topology
```

---

### Task 9: README Usage and Full Verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README usage**

Append this section to `README.md`:

```markdown
## Java 17 Local Benchmark MVP

Build and test:

```powershell
mvn test
```

Package the runner:

```powershell
mvn package
```

Run the CI-sized local smoke workflow:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --run-id local-smoke
```

Generate data only:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar generate --cells 10 --days 1 --row-cap 100
```

The default smoke config is [configs/benchmark-smoke.yml](configs/benchmark-smoke.yml). It preserves the spec values for `10,000` cells and `1` day, and uses `rowCap: 10000` so local verification does not accidentally generate all `14,400,000` rows.
```

- [ ] **Step 2: Run full test suite**

Run:

```powershell
mvn test
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 3: Package the runner**

Run:

```powershell
mvn package
```

Expected:

```text
BUILD SUCCESS
```

- [ ] **Step 4: Run local smoke CLI**

Run:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --run-id plan-smoke
```

Expected:

```text
rows=10000 report=reports\runs\plan-smoke\index.html
```

- [ ] **Step 5: Verify generated report exists**

Run:

```powershell
Test-Path reports\runs\plan-smoke\index.html
```

Expected:

```text
True
```

- [ ] **Step 6: Commit**

Run:

```powershell
git add README.md
git commit -m "docs: add java benchmark mvp usage"
```

Expected:

```text
[main <sha>] docs: add java benchmark mvp usage
```

---

## Self-Review

- Spec coverage: This Java 17 MVP plan covers deterministic wireless KPI generation scaffolding, 50-column schema, smoke config, query catalog, Prometheus metric names, HTML report generation, and Docker Compose topology. Real Parquet generation is available in the Java 17 runner; Spark Iceberg writes, StarRocks internal/external load paths, engine-level benchmark execution, external catalog refresh, and Grafana dashboard JSON remain follow-up integration subsystems.
- Placeholder scan: The plan does not use open-ended placeholders such as `TBD`, `TODO`, `implement later`, or undefined "appropriate" work. Follow-up subsystem names are scoped exclusions, not missing task steps.
- Type consistency: `BenchmarkConfig`, `DatasetResult`, `KpiColumn`, `KpiSchema`, `BenchmarkEngine`, `QueryDefinition`, `BenchmarkReport`, and `LocalBenchmarkRunner` are introduced before use and referenced consistently.
- Verification: Every implementation task includes a failing test, implementation content, a passing test command, and a commit step.

# Spark KPI Data Generation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace active KPI data generation with one Spark-based generator that works for local smoke data, compose runs, and 1b-row generation.

**Architecture:** Add a focused Spark KPI generation boundary that returns the existing `DatasetResult`. Local CLI and local benchmark mode run Spark with `local[*]`; compose mode delegates generation to the existing Spark container and then keeps the current downstream load/query/report flow unchanged.

**Tech Stack:** Java 17, Maven, Spark SQL Java API, Parquet, Docker Compose, JUnit 5, AssertJ, picocli.

---

## File Structure

- Modify `pom.xml`: add Spark SQL dependency needed by the in-process local Spark generator.
- Modify `src/main/java/com/example/databenchmark/config/BenchmarkConfig.java`: add optional `DatasetSparkConfig`.
- Modify `src/main/java/com/example/databenchmark/config/BenchmarkConfigLoader.java`: keep YAML compatibility for configs without `dataset.spark`.
- Create `src/main/java/com/example/databenchmark/generator/KpiDatasetGenerator.java`: shared generator interface.
- Create `src/main/java/com/example/databenchmark/generator/KpiGenerationConfig.java`: validates target rows, partitions, Spark master, and output mode.
- Create `src/main/java/com/example/databenchmark/generator/SparkKpiGenerationJob.java`: builds Spark `Dataset<Row>` and writes partitioned Parquet.
- Create `src/main/java/com/example/databenchmark/generator/SparkKpiDataGenerator.java`: local in-process Spark generator.
- Create `src/main/java/com/example/databenchmark/generator/SparkSubmitKpiDataGenerator.java`: compose adapter that runs generation inside the `spark` service.
- Modify `src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java`: route `generate` through the same generator factory.
- Modify `src/main/java/com/example/databenchmark/runner/LocalBenchmarkRunner.java`: use injected `KpiDatasetGenerator`.
- Modify `src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java`: default to `SparkSubmitKpiDataGenerator`.
- Add `configs/benchmark-kpi-1b.yml`: 1b-row KPI generation config.
- Modify `configs/benchmark-smoke.yml` and `configs/benchmark-kpi-10m.yml`: add explicit Spark generation settings only where useful.
- Modify `README.md`: document Spark generation behavior and 1b config.

## Task 1: Add Spark Generation Config Model

**Files:**
- Modify: `src/main/java/com/example/databenchmark/config/BenchmarkConfig.java`
- Test: `src/test/java/com/example/databenchmark/config/BenchmarkConfigLoaderTest.java`

- [ ] **Step 1: Write the failing config compatibility test**

Add this test to `BenchmarkConfigLoaderTest`:

```java
@Test
void loadsDatasetSparkConfigWhenPresent() throws Exception {
    Path configFile = tempDir.resolve("spark-generation.yml");
    Files.writeString(configFile, """
        profile: spark-test
        seed: 20260602
        suite:
          name: kpi
          scaleFactor: 0.01
          querySet: smoke
        dataset:
          cells: 100
          days: 1
          columns: 50
          startTime: "2026-01-01T00:00:00"
          output: "data/generated"
          rowCap: 10000
          spark:
            master: "local[2]"
            partitions: 8
            rowsPerPartition: 1250
            outputMode: "overwrite"
        query:
          coldRuns: 1
          warmRuns: 1
          concurrency: 1
        report:
          format: html
          output: reports/runs
        """);

    BenchmarkConfig config = new BenchmarkConfigLoader().load(configFile);

    assertThat(config.dataset().spark()).isNotNull();
    assertThat(config.dataset().spark().master()).isEqualTo("local[2]");
    assertThat(config.dataset().spark().partitions()).isEqualTo(8);
    assertThat(config.dataset().spark().rowsPerPartition()).isEqualTo(1250L);
    assertThat(config.dataset().spark().outputMode()).isEqualTo("overwrite");
}
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```sh
mvn -Dtest=BenchmarkConfigLoaderTest#loadsDatasetSparkConfigWhenPresent test
```

Expected: compilation fails because `DatasetConfig.spark()` does not exist.

- [ ] **Step 3: Implement the config model**

Change `BenchmarkConfig.DatasetConfig` to include the new field:

```java
public record DatasetConfig(
    int cells,
    int days,
    int columns,
    String startTime,
    String output,
    Long rowCap,
    DatasetSparkConfig spark
) {
    private DatasetConfig withRowCap(Long rowCap) {
        return new DatasetConfig(cells, days, columns, startTime, output, rowCap, spark);
    }
}

public record DatasetSparkConfig(
    String master,
    Integer partitions,
    Long rowsPerPartition,
    String outputMode
) {}
```

Update all existing `new DatasetConfig(...)` calls to pass `null` for `spark` unless the source already has a value.

- [ ] **Step 4: Run the focused config tests and verify GREEN**

Run:

```sh
mvn -Dtest=BenchmarkConfigLoaderTest test
```

Expected: all `BenchmarkConfigLoaderTest` tests pass.

- [ ] **Step 5: Commit**

```sh
git add src/main/java/com/example/databenchmark/config/BenchmarkConfig.java src/test/java/com/example/databenchmark/config/BenchmarkConfigLoaderTest.java
git commit -m "feat: add spark generation config"
```

## Task 2: Add Generation Config Derivation and Validation

**Files:**
- Create: `src/main/java/com/example/databenchmark/generator/KpiDatasetGenerator.java`
- Create: `src/main/java/com/example/databenchmark/generator/KpiGenerationConfig.java`
- Test: `src/test/java/com/example/databenchmark/generator/KpiGenerationConfigTest.java`

- [ ] **Step 1: Write failing tests for target rows and defaults**

Create `KpiGenerationConfigTest`:

```java
package com.example.databenchmark.generator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.databenchmark.config.BenchmarkConfig;
import org.junit.jupiter.api.Test;

class KpiGenerationConfigTest {
    @Test
    void capsTargetRowsByRowCap() {
        KpiGenerationConfig config = KpiGenerationConfig.from(BenchmarkConfig.defaultSmoke());

        assertThat(config.targetRows()).isEqualTo(10_000L);
        assertThat(config.partitions()).isGreaterThanOrEqualTo(1);
        assertThat(config.master()).isEqualTo("local[*]");
        assertThat(config.outputMode()).isEqualTo("overwrite");
    }

    @Test
    void derivesLargePartitionCountFromRowsPerPartition() {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(10_000, 100, null, "data/generated", 1_000_000_000L);

        KpiGenerationConfig generation = KpiGenerationConfig.from(config);

        assertThat(generation.targetRows()).isEqualTo(1_000_000_000L);
        assertThat(generation.partitions()).isEqualTo(1000);
    }

    @Test
    void rejectsInvalidCells() {
        BenchmarkConfig invalid = new BenchmarkConfig(
            "bad",
            1L,
            BenchmarkConfig.SuiteConfig.defaultSuite(),
            new BenchmarkConfig.DatasetConfig(0, 1, 50, "2026-01-01T00:00:00", "data/generated", 10L, null),
            new BenchmarkConfig.QueryConfig(1, 1, 1),
            new BenchmarkConfig.ReportConfig("html", "reports/runs")
        );

        assertThatThrownBy(() -> KpiGenerationConfig.from(invalid))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cells must be positive");
    }
}
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```sh
mvn -Dtest=KpiGenerationConfigTest test
```

Expected: compilation fails because `KpiGenerationConfig` does not exist.

- [ ] **Step 3: Implement interface and config derivation**

Create `KpiDatasetGenerator`:

```java
package com.example.databenchmark.generator;

import com.example.databenchmark.config.BenchmarkConfig;

public interface KpiDatasetGenerator {
    DatasetResult generate(BenchmarkConfig config) throws Exception;
}
```

Create `KpiGenerationConfig` with:

```java
public static KpiGenerationConfig from(BenchmarkConfig config) {
    int cells = config.dataset().cells();
    int days = config.dataset().days();
    if (cells <= 0) throw new IllegalArgumentException("cells must be positive");
    if (days <= 0) throw new IllegalArgumentException("days must be positive");
    long fullRows = (long) cells * days * 24L * 60L;
    long targetRows = config.dataset().rowCap() == null ? fullRows : Math.min(fullRows, config.dataset().rowCap());
    if (targetRows <= 0) throw new IllegalArgumentException("targetRows must be positive");

    var spark = config.dataset().spark();
    String master = spark != null && spark.master() != null && !spark.master().isBlank()
        ? spark.master()
        : "local[*]";
    long rowsPerPartition = spark != null && spark.rowsPerPartition() != null
        ? spark.rowsPerPartition()
        : 1_000_000L;
    int partitions = spark != null && spark.partitions() != null
        ? spark.partitions()
        : (int) Math.max(1L, Math.ceil((double) targetRows / (double) rowsPerPartition));
    String outputMode = spark != null && spark.outputMode() != null && !spark.outputMode().isBlank()
        ? spark.outputMode()
        : "overwrite";

    if (rowsPerPartition <= 0) throw new IllegalArgumentException("rowsPerPartition must be positive");
    if (partitions <= 0) throw new IllegalArgumentException("partitions must be positive");
    return new KpiGenerationConfig(config, targetRows, master, partitions, rowsPerPartition, outputMode);
}
```

- [ ] **Step 4: Run focused tests and verify GREEN**

Run:

```sh
mvn -Dtest=KpiGenerationConfigTest test
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```sh
git add src/main/java/com/example/databenchmark/generator/KpiDatasetGenerator.java src/main/java/com/example/databenchmark/generator/KpiGenerationConfig.java src/test/java/com/example/databenchmark/generator/KpiGenerationConfigTest.java
git commit -m "feat: derive spark kpi generation settings"
```

## Task 3: Implement Local Spark KPI Parquet Generation

**Files:**
- Modify: `pom.xml`
- Create: `src/main/java/com/example/databenchmark/generator/SparkKpiGenerationJob.java`
- Create: `src/main/java/com/example/databenchmark/generator/SparkKpiDataGenerator.java`
- Test: `src/test/java/com/example/databenchmark/generator/SparkKpiDataGeneratorTest.java`

- [ ] **Step 1: Write failing local Spark generation test**

Create `SparkKpiDataGeneratorTest`:

```java
package com.example.databenchmark.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import java.nio.file.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SparkKpiDataGeneratorTest {
    @TempDir
    Path tempDir;

    @Test
    void writesPartitionedParquetWithExpectedRows() throws Exception {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(10, 1, null, tempDir.resolve("generated").toString(), 100L);

        DatasetResult result = new SparkKpiDataGenerator().generate(config);

        assertThat(result.rows()).isEqualTo(100L);
        assertThat(result.bytesWritten()).isPositive();
        assertThat(result.files()).isNotEmpty();
        assertThat(result.files()).allMatch(path -> path.toString().endsWith(".parquet"));
        assertThat(result.files()).anyMatch(path -> path.toString().contains("event_date=2026-01-01"));

        long parquetRows = 0L;
        for (Path file : result.files()) {
            try (ParquetFileReader reader = ParquetFileReader.open(
                HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(file.toUri()), com.example.databenchmark.hadoop.HadoopLocalConfiguration.create())
            )) {
                parquetRows += reader.getRecordCount();
            }
        }
        assertThat(parquetRows).isEqualTo(100L);
    }
}
```

- [ ] **Step 2: Run test and verify RED**

Run:

```sh
mvn -Dtest=SparkKpiDataGeneratorTest test
```

Expected: compilation fails because `SparkKpiDataGenerator` does not exist and Spark SQL dependency is missing.

- [ ] **Step 3: Add Spark SQL dependency**

Add to `pom.xml`:

```xml
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-sql_2.12</artifactId>
  <version>3.5.8</version>
</dependency>
```

- [ ] **Step 4: Implement Spark generation**

Implement `SparkKpiDataGenerator.generate` to:

```java
KpiGenerationConfig generation = KpiGenerationConfig.from(config);
return new SparkKpiGenerationJob().run(generation);
```

Implement `SparkKpiGenerationJob.run` with Spark Java API:

```java
SparkSession spark = SparkSession.builder()
    .appName("data-benchmark-kpi-generation")
    .master(generation.master())
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate();
```

Use `spark.range(0, generation.targetRows(), 1, generation.partitions())`, derive columns with `functions.expr(...)`, write with:

```java
dataset.write()
    .mode(generation.outputMode())
    .partitionBy("event_date")
    .parquet(generation.outputPath().toString());
```

After writing, enumerate `*.parquet` recursively under output, sum file sizes, and return `DatasetResult`.

- [ ] **Step 5: Run focused test and verify GREEN**

Run:

```sh
mvn -Dtest=SparkKpiDataGeneratorTest test
```

Expected: test passes and writes Parquet under the temporary directory.

- [ ] **Step 6: Commit**

```sh
git add pom.xml src/main/java/com/example/databenchmark/generator/SparkKpiGenerationJob.java src/main/java/com/example/databenchmark/generator/SparkKpiDataGenerator.java src/test/java/com/example/databenchmark/generator/SparkKpiDataGeneratorTest.java
git commit -m "feat: generate kpi parquet with spark"
```

## Task 4: Route CLI and Local Runner Through Spark Generator

**Files:**
- Modify: `src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java`
- Modify: `src/main/java/com/example/databenchmark/runner/LocalBenchmarkRunner.java`
- Modify: `src/test/java/com/example/databenchmark/BenchmarkRunnerAppTest.java`
- Modify: `src/test/java/com/example/databenchmark/runner/LocalBenchmarkRunnerTest.java`

- [ ] **Step 1: Write failing CLI injection test**

Update `BenchmarkRunnerAppTest` so `generateCommandWritesToConfiguredOutput` uses a test `RunnerFactory` or generator factory and asserts the generator is called once. If the current app structure blocks injection, first add a package-private constructor taking `KpiDatasetGenerator`.

Expected assertion:

```java
assertThat(generator.calls()).isEqualTo(1);
assertThat(output).contains("rows=8");
```

- [ ] **Step 2: Run focused CLI test and verify RED**

Run:

```sh
mvn -Dtest=BenchmarkRunnerAppTest#generateCommandWritesToConfiguredOutput test
```

Expected: test fails because `GenerateCommand` still directly constructs `KpiDataGenerator`.

- [ ] **Step 3: Refactor app factory**

Add to `BenchmarkRunnerApp.RunnerFactory`:

```java
DatasetResult generateKpi(BenchmarkConfig config) throws Exception;
```

Make `DefaultRunnerFactory.generateKpi` return:

```java
return new SparkKpiDataGenerator().generate(config);
```

Change `GenerateCommand.call()` to use `parent.runnerFactory.generateKpi(config)`.

- [ ] **Step 4: Refactor local runner**

Change `LocalBenchmarkRunner` default constructor to use `new SparkKpiDataGenerator()` and accept `KpiDatasetGenerator` in tests. Keep report behavior unchanged.

- [ ] **Step 5: Run focused tests and verify GREEN**

Run:

```sh
mvn -Dtest=BenchmarkRunnerAppTest,LocalBenchmarkRunnerTest test
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```sh
git add src/main/java/com/example/databenchmark/BenchmarkRunnerApp.java src/main/java/com/example/databenchmark/runner/LocalBenchmarkRunner.java src/test/java/com/example/databenchmark/BenchmarkRunnerAppTest.java src/test/java/com/example/databenchmark/runner/LocalBenchmarkRunnerTest.java
git commit -m "feat: route local kpi generation through spark"
```

## Task 5: Add Compose Spark Submit Generator

**Files:**
- Create: `src/main/java/com/example/databenchmark/generator/SparkSubmitKpiDataGenerator.java`
- Modify: `src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java`
- Test: `src/test/java/com/example/databenchmark/generator/SparkSubmitKpiDataGeneratorTest.java`
- Test: `src/test/java/com/example/databenchmark/runner/ComposeBenchmarkRunnerTest.java`

- [ ] **Step 1: Write failing command construction test**

Create `SparkSubmitKpiDataGeneratorTest` with a fake `CommandRunner` capturing arguments. Assert it calls:

```text
docker compose -f docker-compose.yml exec -T spark java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar generate --config <generated-config>
```

The generated config must preserve `profile`, `seed`, `suite`, `dataset`, `query`, and `report`.

- [ ] **Step 2: Run test and verify RED**

Run:

```sh
mvn -Dtest=SparkSubmitKpiDataGeneratorTest test
```

Expected: compilation fails because `SparkSubmitKpiDataGenerator` does not exist.

- [ ] **Step 3: Implement compose adapter**

Implement `SparkSubmitKpiDataGenerator` to:

- Write an effective config file under `target/generated-configs/<profile>-spark-generate.yml`.
- Run Docker Compose exec against `spark`.
- After command success, enumerate local output path and return `DatasetResult`.
- Throw an `IOException` when command exit code is nonzero or no Parquet files exist.

- [ ] **Step 4: Change Compose default generator**

In `ComposeBenchmarkRunner()` replace:

```java
new KpiDataGenerator()::generate
```

with:

```java
new SparkSubmitKpiDataGenerator(new CommandRunner())::generate
```

Keep package-private constructors unchanged so existing unit tests can still inject fake generators.

- [ ] **Step 5: Run focused tests and verify GREEN**

Run:

```sh
mvn -Dtest=SparkSubmitKpiDataGeneratorTest,ComposeBenchmarkRunnerTest test
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```sh
git add src/main/java/com/example/databenchmark/generator/SparkSubmitKpiDataGenerator.java src/main/java/com/example/databenchmark/runner/ComposeBenchmarkRunner.java src/test/java/com/example/databenchmark/generator/SparkSubmitKpiDataGeneratorTest.java src/test/java/com/example/databenchmark/runner/ComposeBenchmarkRunnerTest.java
git commit -m "feat: generate compose kpi data in spark container"
```

## Task 6: Add 1b Config and Documentation

**Files:**
- Create: `configs/benchmark-kpi-1b.yml`
- Modify: `configs/benchmark-smoke.yml`
- Modify: `configs/benchmark-kpi-10m.yml`
- Modify: `README.md`
- Test: `src/test/java/com/example/databenchmark/ComposeTopologyTest.java`

- [ ] **Step 1: Write failing config test**

Add to `ComposeTopologyTest`:

```java
@Test
void benchmarkConfigsDeclareSmokeKpiAndOneBillionRowCaps() throws Exception {
    assertThat(rowCap(Path.of("configs", "benchmark-smoke.yml"))).isEqualTo(10000);
    assertThat(rowCap(Path.of("configs", "benchmark-kpi-10m.yml"))).isEqualTo(10000000);
    assertThat(rowCap(Path.of("configs", "benchmark-kpi-1b.yml"))).isEqualTo(1000000000);
}
```

- [ ] **Step 2: Run test and verify RED**

Run:

```sh
mvn -Dtest=ComposeTopologyTest#benchmarkConfigsDeclareSmokeKpiAndOneBillionRowCaps test
```

Expected: fails because `configs/benchmark-kpi-1b.yml` does not exist.

- [ ] **Step 3: Add configs**

Create `configs/benchmark-kpi-1b.yml` by copying the 10m shape and changing:

```yaml
profile: "kpi-1b"
dataset:
  rowCap: 1000000000
  spark:
    master: "local[*]"
    partitions: 1024
    rowsPerPartition: 1000000
    outputMode: "overwrite"
```

Add `dataset.spark` to smoke with `partitions: 4`, and 10m with `partitions: 64`.

- [ ] **Step 4: Update README**

Document:

```sh
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar generate --config configs/benchmark-smoke.yml
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-kpi-10m.yml --run-id compose-kpi-10m
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-kpi-1b.yml --run-id compose-kpi-1b
```

State that KPI generation uses Spark in both local and compose modes.

- [ ] **Step 5: Run focused test and verify GREEN**

Run:

```sh
mvn -Dtest=ComposeTopologyTest test
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```sh
git add configs/benchmark-kpi-1b.yml configs/benchmark-smoke.yml configs/benchmark-kpi-10m.yml README.md src/test/java/com/example/databenchmark/ComposeTopologyTest.java
git commit -m "docs: add spark kpi generation configs"
```

## Task 7: Full Verification

**Files:**
- No new source files unless prior tasks reveal compile issues.

- [ ] **Step 1: Run full unit test suite**

Run:

```sh
mvn test
```

Expected: build succeeds with all tests passing.

- [ ] **Step 2: Build packaged runner**

Run:

```sh
mvn package
```

Expected: build succeeds and `target/data-benchmark-0.1.0-SNAPSHOT.jar` exists.

- [ ] **Step 3: Run local Spark smoke generation**

Run:

```sh
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar generate --config configs/benchmark-smoke.yml --output data/generated-spark-smoke --row-cap 10000
```

Expected output contains:

```text
rows=10000
```

- [ ] **Step 4: Run local benchmark smoke**

Run:

```sh
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode local --config configs/benchmark-smoke.yml --run-id spark-local-smoke
```

Expected output contains:

```text
rows=10000 report=
```

The report exists at `reports/runs/spark-local-smoke/index.html`.

- [ ] **Step 5: Run compose smoke**

Run:

```sh
docker compose -f docker-compose.yml down --remove-orphans
docker compose -f docker-compose.yml up -d hdfs-namenode hdfs-datanode hdfs-init hive-metastore hive-server spark starrocks-fe starrocks-be
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-smoke.yml --run-id spark-compose-smoke
```

Expected output contains:

```text
rows=10000 report=
```

Parse the report and verify:

```sh
node - <<'NODE'
const fs = require('fs');
const html = fs.readFileSync('reports/runs/spark-compose-smoke/index.html', 'utf8');
const report = JSON.parse(html.match(/window\.__BENCHMARK_REPORT__\s*=\s*(\{[\s\S]*?\});\s*<\/script>/)[1]);
console.log(report.run.status);
console.log(report.dataset.rows);
console.log(report.performanceMatrix.length);
NODE
```

Expected:

```text
SUCCESS
10000
10
```

- [ ] **Step 6: Optional 10m regression run**

Run only after smoke passes:

```sh
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-kpi-10m.yml --run-id spark-kpi-10m
```

Expected: report status is `SUCCESS`, dataset rows are `10000000`.

- [ ] **Step 7: Final status check**

Run:

```sh
git status --short
```

Expected: no modified tracked files. Generated `data/` and `reports/` may be untracked and should not be committed.

## Self-Review

- Spec coverage: tasks cover config, Spark generator, CLI/local/compose integration, 1b config, docs, and verification.
- Placeholder scan: no unresolved marker wording is used.
- Type consistency: generator interface returns existing `DatasetResult`; config fields match `BenchmarkConfig.DatasetSparkConfig`; compose adapter preserves injected test seams.

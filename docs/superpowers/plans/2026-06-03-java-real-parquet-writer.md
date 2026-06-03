# Java Real Parquet Writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Java 17 KPI 数据生成器已升级为可被标准 Parquet/Avro 读写器读取的真实 Parquet 数据集，并同步更新文档状态。

**Architecture:** 保留现有 CLI、配置、分区路径和 `DatasetResult` 合同，在 `generator` 包内新增 Avro schema 工厂和 record 工厂。`KpiSchema` 继续作为 50 列字段顺序和逻辑类型的唯一来源，`KpiDataGenerator` 只负责行数计算、分区目录创建、Parquet writer 生命周期和结果汇总。

**Tech Stack:** Java 17, Maven, Apache Avro, Apache Parquet Avro, Hadoop Path API, JUnit 5, AssertJ.

---

## 文件结构

- Modify: `src/test/java/com/example/databenchmark/generator/KpiDataGeneratorTest.java`
  - 用 `AvroParquetReader<GenericRecord>` 读取生成结果，验证文件是真 Parquet、行数、字段数量、字段顺序和确定性。
- Create: `src/main/java/com/example/databenchmark/generator/KpiAvroSchemaFactory.java`
  - 将 `KpiSchema.columns()` 的 50 列映射为 Avro schema，支持 `timestamp_ms`、`string`、`int`、`double`。
- Create: `src/main/java/com/example/databenchmark/generator/KpiRecordFactory.java`
  - 按行号、配置和起始时间生成完整 50 列 `GenericRecord`，所有随机值来自传入的 `Random`，保证同 seed 输出稳定。
- Modify: `src/main/java/com/example/databenchmark/generator/KpiDataGenerator.java`
  - 删除 `BufferedWriter` 文本写入，改用 `AvroParquetWriter<GenericRecord>` 写真实 Parquet 文件。
- Modify: `README.md`
  - 明确生成器输出真实 Parquet。
- Modify: `docs/superpowers/plans/2026-06-02-starrocks-iceberg-benchmark-mvp.md`
  - 将已过时的生成器风险描述更新为“真实 Parquet 已具备，后续仍需 Spark/Iceberg/StarRocks 写入链路”。

## 验收标准

- `mvn test` 通过。
- `mvn package` 通过，Shade 依赖 overlap warning 可以存在，但构建必须成功。
- `java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar generate --cells 3 --days 1 --seed 123 --row-cap 12 --output data/parquet-smoke` 生成的 `part-00000.parquet` 可被 `AvroParquetReader` 读取。
- 生成文件保留现有分区路径：`event_date=2026-01-01/part-00000.parquet`。
- 测试不再通过 `Files.readString()` 判断数据内容。

### Task 1: 写真实 Parquet 读取失败测试

**Files:**
- Modify: `src/test/java/com/example/databenchmark/generator/KpiDataGeneratorTest.java`

- [ ] **Step 1: 替换生成器测试为 Parquet reader 断言**

将 `src/test/java/com/example/databenchmark/generator/KpiDataGeneratorTest.java` 替换为：

```java
package com.example.databenchmark.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.config.BenchmarkConfig;
import com.example.databenchmark.schema.KpiSchema;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KpiDataGeneratorTest {
    @TempDir
    Path tempDir;

    @Test
    void generatorWritesDeterministicPartitionedParquetOutput() throws Exception {
        BenchmarkConfig config = BenchmarkConfig.defaultSmoke()
            .withOverrides(3, 1, 123L, tempDir.toString(), 12L);
        KpiDataGenerator generator = new KpiDataGenerator();

        DatasetResult first = generator.generate(config);
        List<GenericRecord> firstRows = readRecords(first.files().get(0));
        DatasetResult second = generator.generate(config);
        List<GenericRecord> secondRows = readRecords(second.files().get(0));

        assertThat(first.rows()).isEqualTo(12L);
        assertThat(first.bytesWritten()).isPositive();
        assertThat(first.files()).isEqualTo(second.files());
        assertThat(first.files()).hasSize(1);
        assertThat(first.files().get(0).toString()).contains("event_date=2026-01-01");
        assertThat(firstRows).hasSize(12);
        assertThat(secondRows).hasSize(12);
        assertThat(firstRows.stream().map(GenericRecord::toString).toList())
            .isEqualTo(secondRows.stream().map(GenericRecord::toString).toList());

        GenericRecord firstRecord = firstRows.get(0);
        assertThat(firstRecord.getSchema().getFields())
            .extracting(org.apache.avro.Schema.Field::name)
            .containsExactlyElementsOf(KpiSchema.columnNames());
        assertThat(firstRecord.getSchema().getFields()).hasSize(50);
        assertThat(firstRecord.get("cell_id").toString()).isEqualTo("CELL-000000");
        assertThat(firstRecord.get("province").toString()).isEqualTo("province-00");
        assertThat(firstRecord.get("city").toString()).isEqualTo("city-000");
        assertThat(firstRecord.get("vendor").toString()).isEqualTo("Huawei");
        assertThat(firstRecord.get("rat").toString()).isEqualTo("4G");
        assertThat(firstRecord.get("band").toString()).isEqualTo("B3");
        assertThat(firstRecord.get("event_time")).isEqualTo(1767225600000L);
        assertThat(firstRecord.get("load_score")).isInstanceOf(Double.class);
    }

    private List<GenericRecord> readRecords(Path file) throws Exception {
        List<GenericRecord> records = new ArrayList<>();
        HadoopInputFile inputFile = HadoopInputFile.fromPath(
            new org.apache.hadoop.fs.Path(file.toUri()),
            new Configuration()
        );
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                records.add(record);
            }
        }
        return records;
    }
}
```

- [ ] **Step 2: 运行测试，确认现状失败**

Run:

```powershell
mvn -Dtest=KpiDataGeneratorTest test
```

Expected: FAIL。失败原因应来自 Parquet reader 无法读取当前文本文件，例如 `RuntimeException`、`ParquetDecodingException` 或 parquet magic bytes 相关错误。

- [ ] **Step 3: 提交失败测试**

```powershell
git add src/test/java/com/example/databenchmark/generator/KpiDataGeneratorTest.java
git commit -m "test: require generator to write readable parquet"
```

### Task 2: 新增 Avro Schema 工厂

**Files:**
- Create: `src/main/java/com/example/databenchmark/generator/KpiAvroSchemaFactory.java`
- Test: `src/test/java/com/example/databenchmark/generator/KpiDataGeneratorTest.java`

- [ ] **Step 1: 创建 `KpiAvroSchemaFactory`**

创建 `src/main/java/com/example/databenchmark/generator/KpiAvroSchemaFactory.java`：

```java
package com.example.databenchmark.generator;

import com.example.databenchmark.schema.KpiColumn;
import com.example.databenchmark.schema.KpiSchema;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

final class KpiAvroSchemaFactory {
    private KpiAvroSchemaFactory() {}

    static Schema createSchema() {
        List<Schema.Field> fields = new ArrayList<>();
        for (KpiColumn column : KpiSchema.columns()) {
            fields.add(new Schema.Field(column.name(), avroType(column), null, (Object) null));
        }
        Schema schema = Schema.createRecord(
            "CellKpiMinute",
            "One-minute cellular KPI benchmark record.",
            "com.example.databenchmark.avro",
            false
        );
        schema.setFields(fields);
        return schema;
    }

    private static Schema avroType(KpiColumn column) {
        return switch (column.logicalType()) {
            case "timestamp_ms" -> LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
            case "string" -> Schema.create(Schema.Type.STRING);
            case "int" -> Schema.create(Schema.Type.INT);
            case "double" -> Schema.create(Schema.Type.DOUBLE);
            default -> throw new IllegalArgumentException(
                "Unsupported KPI logical type '" + column.logicalType() + "' for column '" + column.name() + "'"
            );
        };
    }
}
```

- [ ] **Step 2: 运行目标测试，确认仍失败但 schema 类可编译**

Run:

```powershell
mvn -Dtest=KpiDataGeneratorTest test
```

Expected: FAIL，失败点仍应在读取旧文本 `.parquet` 文件；如果出现 `KpiAvroSchemaFactory` 编译错误，先修正导入和构造器调用。

- [ ] **Step 3: 提交 schema 工厂**

```powershell
git add src/main/java/com/example/databenchmark/generator/KpiAvroSchemaFactory.java
git commit -m "feat: add KPI avro schema factory"
```

### Task 3: 新增完整 50 列 Record 工厂

**Files:**
- Create: `src/main/java/com/example/databenchmark/generator/KpiRecordFactory.java`
- Test: `src/test/java/com/example/databenchmark/generator/KpiDataGeneratorTest.java`

- [ ] **Step 1: 创建 `KpiRecordFactory`**

创建 `src/main/java/com/example/databenchmark/generator/KpiRecordFactory.java`：

```java
package com.example.databenchmark.generator;

import com.example.databenchmark.config.BenchmarkConfig;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

final class KpiRecordFactory {
    private static final List<String> VENDORS = List.of("Huawei", "ZTE", "Ericsson", "Nokia", "Samsung");
    private static final List<String> BANDS = List.of("B3", "B7", "B8", "B20", "N78", "N41");

    private final BenchmarkConfig config;
    private final Schema schema;
    private final Random random;
    private final LocalDateTime start;

    KpiRecordFactory(BenchmarkConfig config, Schema schema, LocalDateTime start) {
        this.config = config;
        this.schema = schema;
        this.random = new Random(config.seed());
        this.start = start;
    }

    GenericRecord create(long rowNumber) {
        int cell = (int) (rowNumber % config.dataset().cells());
        long minute = rowNumber / config.dataset().cells();
        LocalDateTime eventTime = start.plusMinutes(minute);
        GenericRecord record = new GenericData.Record(schema);

        int activeUsers = 1 + random.nextInt(600);
        int rrcUsers = Math.max(1, activeUsers + random.nextInt(80) - 20);
        int connectedUsersPeak = Math.max(activeUsers, activeUsers + random.nextInt(120));
        int rrcSetupAttempts = connectedUsersPeak + 50 + random.nextInt(800);
        int rrcSetupSuccesses = boundedSuccessCount(rrcSetupAttempts, 0.97 + random.nextDouble() * 0.029);
        int erabSetupAttempts = connectedUsersPeak + 30 + random.nextInt(700);
        int erabSetupSuccesses = boundedSuccessCount(erabSetupAttempts, 0.965 + random.nextDouble() * 0.03);
        int handoverAttempts = 10 + random.nextInt(500);
        int handoverSuccesses = boundedSuccessCount(handoverAttempts, 0.94 + random.nextDouble() * 0.055);
        double dropRate = round6(random.nextDouble() * 0.02);
        double handoverSuccessRate = ratio(handoverSuccesses, handoverAttempts);
        double accessSuccessRate = ratio(rrcSetupSuccesses + erabSetupSuccesses, rrcSetupAttempts + erabSetupAttempts);
        double loadScore = round3(5.0 + random.nextDouble() * 95.0);

        record.put("event_time", eventTime.toInstant(ZoneOffset.UTC).toEpochMilli());
        record.put("cell_id", cellId(cell));
        record.put("province", String.format(Locale.ROOT, "province-%02d", cell % 31));
        record.put("city", String.format(Locale.ROOT, "city-%03d", cell % 200));
        record.put("district", String.format(Locale.ROOT, "district-%03d", cell % 500));
        record.put("grid_id", String.format(Locale.ROOT, "grid-%05d", cell % 10000));
        record.put("vendor", VENDORS.get(cell % VENDORS.size()));
        record.put("rat", cell % 2 == 0 ? "4G" : "5G");
        record.put("band", BANDS.get(cell % BANDS.size()));
        record.put("arfcn", 100 + cell % 32768);
        record.put("pci", cell % 1008);
        record.put("site_id", String.format(Locale.ROOT, "SITE-%05d", cell / 3));
        record.put("longitude", round6(73.5 + random.nextDouble() * 61.0));
        record.put("latitude", round6(18.0 + random.nextDouble() * 36.0));
        record.put("rsrp_avg", round3(-115.0 + random.nextDouble() * 35.0));
        record.put("rsrp_p10", round3(-125.0 + random.nextDouble() * 30.0));
        record.put("rsrq_avg", round3(-18.0 + random.nextDouble() * 15.0));
        record.put("sinr_avg", round3(-5.0 + random.nextDouble() * 35.0));
        record.put("prb_dl_util", round6(random.nextDouble()));
        record.put("prb_ul_util", round6(random.nextDouble()));
        record.put("rrc_users", rrcUsers);
        record.put("active_users", activeUsers);
        record.put("dl_traffic_mb", round3(activeUsers * (0.5 + random.nextDouble() * 20.0)));
        record.put("ul_traffic_mb", round3(activeUsers * (0.2 + random.nextDouble() * 8.0)));
        record.put("dl_throughput_mbps", round3(5.0 + random.nextDouble() * 400.0));
        record.put("ul_throughput_mbps", round3(1.0 + random.nextDouble() * 120.0));
        record.put("drop_rate", dropRate);
        record.put("handover_success_rate", handoverSuccessRate);
        record.put("access_success_rate", accessSuccessRate);
        record.put("volte_drop_rate", round6(random.nextDouble() * 0.01));
        record.put("latency_ms", round3(8.0 + random.nextDouble() * 80.0));
        record.put("availability_rate", round6(0.97 + random.nextDouble() * 0.03));
        record.put("alarm_count", random.nextInt(8));
        record.put("interference_score", round3(random.nextDouble() * 100.0));
        record.put("load_score", loadScore);
        record.put("packet_loss_rate", round6(random.nextDouble() * 0.03));
        record.put("cqi_avg", round3(3.0 + random.nextDouble() * 12.0));
        record.put("mcs_avg", round3(1.0 + random.nextDouble() * 27.0));
        record.put("ta_avg", round3(random.nextDouble() * 16.0));
        record.put("ul_noise_avg", round3(-125.0 + random.nextDouble() * 25.0));
        record.put("connected_users_peak", connectedUsersPeak);
        record.put("rrc_setup_attempts", rrcSetupAttempts);
        record.put("rrc_setup_successes", rrcSetupSuccesses);
        record.put("erab_setup_attempts", erabSetupAttempts);
        record.put("erab_setup_successes", erabSetupSuccesses);
        record.put("handover_attempts", handoverAttempts);
        record.put("handover_successes", handoverSuccesses);
        record.put("retransmission_rate", round6(random.nextDouble() * 0.08));
        record.put("backhaul_util", round6(random.nextDouble()));
        record.put("energy_kwh", round3(0.5 + random.nextDouble() * 18.0));

        return record;
    }

    private static String cellId(int cell) {
        return String.format(Locale.ROOT, "CELL-%06d", cell);
    }

    private static int boundedSuccessCount(int attempts, double rate) {
        return Math.min(attempts, Math.max(0, (int) Math.round(attempts * rate)));
    }

    private static double ratio(int numerator, int denominator) {
        if (denominator == 0) {
            return 0.0;
        }
        return round6((double) numerator / (double) denominator);
    }

    private static double round3(double value) {
        return Math.round(value * 1_000.0) / 1_000.0;
    }

    private static double round6(double value) {
        return Math.round(value * 1_000_000.0) / 1_000_000.0;
    }
}
```

- [ ] **Step 2: 运行目标测试，确认仍失败但 record 工厂可编译**

Run:

```powershell
mvn -Dtest=KpiDataGeneratorTest test
```

Expected: FAIL，失败点仍来自旧文本文件不可被 Parquet reader 读取。

- [ ] **Step 3: 提交 record 工厂**

```powershell
git add src/main/java/com/example/databenchmark/generator/KpiRecordFactory.java
git commit -m "feat: generate complete KPI parquet records"
```

### Task 4: 将生成器切换为真实 Parquet writer

**Files:**
- Modify: `src/main/java/com/example/databenchmark/generator/KpiDataGenerator.java`
- Test: `src/test/java/com/example/databenchmark/generator/KpiDataGeneratorTest.java`

- [ ] **Step 1: 替换 `KpiDataGenerator` 写入逻辑**

将 `src/main/java/com/example/databenchmark/generator/KpiDataGenerator.java` 替换为：

```java
package com.example.databenchmark.generator;

import com.example.databenchmark.config.BenchmarkConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

public class KpiDataGenerator {
    public DatasetResult generate(BenchmarkConfig config) throws IOException {
        long rows = targetRows(config);
        Path outputPath = Path.of(config.dataset().output());
        LocalDateTime start = LocalDateTime.parse(config.dataset().startTime());
        Path partitionPath = outputPath.resolve("event_date=" + start.toLocalDate());
        Files.createDirectories(partitionPath);

        Path file = partitionPath.resolve("part-00000.parquet");
        writeRows(config, rows, start, file);

        return new DatasetResult(outputPath, List.of(file), rows, Files.size(file));
    }

    private long targetRows(BenchmarkConfig config) {
        long fullRows = (long) config.dataset().cells() * config.dataset().days() * 24L * 60L;
        Long rowCap = config.dataset().rowCap();
        return rowCap == null ? fullRows : Math.min(fullRows, rowCap);
    }

    private void writeRows(BenchmarkConfig config, long rows, LocalDateTime start, Path file) throws IOException {
        Files.deleteIfExists(file);
        Schema schema = KpiAvroSchemaFactory.createSchema();
        KpiRecordFactory recordFactory = new KpiRecordFactory(config, schema, start);
        HadoopOutputFile outputFile = HadoopOutputFile.fromPath(
            new org.apache.hadoop.fs.Path(file.toUri()),
            new Configuration()
        );

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
            .withSchema(schema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()) {
            for (long row = 0; row < rows; row++) {
                writer.write(recordFactory.create(row));
            }
        }
    }
}
```

- [ ] **Step 2: 运行目标测试，确认通过**

Run:

```powershell
mvn -Dtest=KpiDataGeneratorTest test
```

Expected: PASS，输出包含 `Tests run: 1, Failures: 0, Errors: 0`。

- [ ] **Step 3: 运行全量测试**

Run:

```powershell
mvn test
```

Expected: PASS，所有现有测试通过。

- [ ] **Step 4: 提交 Parquet writer 切换**

```powershell
git add src/main/java/com/example/databenchmark/generator/KpiDataGenerator.java src/test/java/com/example/databenchmark/generator/KpiDataGeneratorTest.java
git commit -m "feat: write benchmark data as real parquet"
```

### Task 5: 更新文档中的生成器状态

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-06-02-starrocks-iceberg-benchmark-mvp.md`

- [x] **Step 1: 更新 README**

将 `README.md` 中最后一段更新为：

```markdown
The generator writes deterministic, partitioned Parquet files under `event_date=YYYY-MM-DD/part-00000.parquet`. The local MVP still needs Spark/Iceberg and StarRocks write paths before it becomes an end-to-end engine benchmark.
```

- [x] **Step 2: 更新 MVP 计划风险描述**

在 `docs/superpowers/plans/2026-06-02-starrocks-iceberg-benchmark-mvp.md` 中搜索旧生成器状态描述，将过时表述替换为：

```markdown
Real Parquet generation is available in the Java 17 runner. Remaining follow-up work is Spark/Iceberg table writes, StarRocks internal/external load paths, and engine-level benchmark execution.
```

- [x] **Step 3: 提交文档更新**

```powershell
git add README.md docs/superpowers/plans/2026-06-02-starrocks-iceberg-benchmark-mvp.md docs/superpowers/plans/2026-06-03-java-real-parquet-writer.md
git commit -m "docs: describe real parquet generator"
```

### Task 6: 端到端验证和清理

**Files:**
- Verify only: no source files should change in this task.

- [ ] **Step 1: 运行全量测试**

Run:

```powershell
mvn test
```

Expected: PASS，输出必须显示 `BUILD SUCCESS`。

- [ ] **Step 2: 打包 shaded jar**

Run:

```powershell
mvn package
```

Expected: PASS，输出必须显示 `BUILD SUCCESS`。Maven Shade 的依赖 overlap warnings 可以记录为已知 warning，不阻塞。

- [ ] **Step 3: 运行 CLI 生成烟测数据**

Run:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar generate --cells 3 --days 1 --seed 123 --row-cap 12 --output data/parquet-smoke
```

Expected: 命令退出码为 0，输出目录存在：

```text
data/parquet-smoke/event_date=2026-01-01/part-00000.parquet
```

- [ ] **Step 4: 清理生成数据**

Run:

```powershell
Remove-Item -Recurse -Force data/parquet-smoke
```

Expected: `data/parquet-smoke` 不再存在。不要删除 `reports/`，它保存了上一轮用户要求保留的本地报告。

- [ ] **Step 5: 检查工作区**

Run:

```powershell
git status --short --branch
```

Expected: 只允许看到本轮已提交后的既有未跟踪项；第一行是 main 相对 origin/main 的分支状态：

```text
## main 与 origin/main 的状态行
?? .github/.idea/
?? reports/
```

如果提交数量不同，`ahead N` 可以不同；不要提交 `.github/.idea/` 或 `reports/`。

- [ ] **Step 6: 推送 main 并部署 staging**

Run:

```powershell
git push origin main
git checkout staging
git merge --ff-only main
git push origin staging
git checkout main
```

Expected: `main` 和 `staging` 指向同一提交。GitHub Actions 的 `Deploy to Staging` workflow 对 `staging` 分支运行成功。

## 自检

- Spec coverage: 计划覆盖真实 Parquet schema、完整 50 列 record、确定性输出、分区路径、测试、文档和 staging 推送。
- Placeholder scan: 本计划没有使用禁止的占位词或省略式实现描述。
- Type consistency: 新增类名为 `KpiAvroSchemaFactory` 和 `KpiRecordFactory`，`KpiDataGenerator`、测试和提交步骤中的名称一致。

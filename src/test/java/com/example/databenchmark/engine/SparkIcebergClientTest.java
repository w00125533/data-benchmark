package com.example.databenchmark.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.generator.DatasetResult;
import com.example.databenchmark.runner.RoutePhase;
import com.example.databenchmark.tpch.TestTpchFixtures;
import com.example.databenchmark.tpch.TpchSqlTemplates;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SparkIcebergClientTest {
    @TempDir
    Path tempDir;

    @Test
    void loadBuildsDockerComposeSparkSqlCommandWithHdfsHiveAndSql() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "ok", "", 1.25));
        Path workspace = tempDir.resolve("workspace");
        DatasetResult dataset = new DatasetResult(
            workspace.resolve("data/run-1"),
            List.of(workspace.resolve("data/run-1/part.parquet")),
            42,
            128
        );

        EngineRunResult result = new SparkIcebergClient(runner, workspace, Duration.ofMinutes(1))
            .load(dataset, "run-1", "smoke");

        assertThat(result.success()).isTrue();
        assertThat(result.rows()).isEqualTo(42);
        List<String> command = runner.commands().get(0);
        assertThat(command).containsExactly(
            "docker", "compose", "-p", "shared-data-infra",
            "-f", "../shared-data-infra/compose.yaml",
            "-f", "../shared-data-infra/compose.lakehouse.yaml",
            "-f", "../shared-data-infra/compose.starrocks.yaml",
            "--profile", "lakehouse",
            "--profile", "lakehouse-tools",
            "--profile", "spark-tools",
            "--profile", "starrocks",
            "exec", "-T", "spark", "/opt/spark/bin/spark-sql",
            "--master", "local[6]",
            "--driver-memory", "10g",
            "--conf", "spark.driver.maxResultSize=2g",
            "--conf", "spark.sql.shuffle.partitions=512",
            "--conf", "spark.default.parallelism=512",
            "--conf", "spark.sql.files.maxPartitionBytes=67108864",
            "--conf", "spark.sql.parquet.enableVectorizedReader=false",
            "--conf", "spark.sql.parquet.columnarReaderBatchSize=1024",
            "--conf", "spark.sql.adaptive.enabled=true",
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1",
            "--conf", "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog",
            "--conf", "spark.sql.catalog.iceberg_catalog.type=hive",
            "--conf", "spark.sql.catalog.iceberg_catalog.uri=thrift://hive-metastore:9083",
            "--conf", "spark.sql.catalog.iceberg_catalog.warehouse=hdfs://hdfs-namenode:8020/warehouse/iceberg",
            "--conf", "spark.sql.iceberg.vectorization.enabled=false",
            "-e", SqlTemplates.sparkCreateIcebergTable("run-1")
                + "\n"
                + SqlTemplates.sparkInsertFromParquet("/workspace/data/run-1/part.parquet")
        );
    }

    @Test
    void inContainerModeRunsSparkSqlDirectlyWithoutDockerCompose() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "ok", "", 1.25));
        Path workspace = tempDir.resolve("workspace");
        DatasetResult dataset = new DatasetResult(
            workspace.resolve("data/run-1"),
            List.of(workspace.resolve("data/run-1/part.parquet")),
            42,
            128
        );

        EngineRunResult result = new SparkIcebergClient(runner, workspace, Duration.ofMinutes(1), true)
            .load(dataset, "run-1", "smoke");

        assertThat(result.success()).isTrue();
        List<String> command = runner.commands().get(0);
        assertThat(command)
            .startsWith("/opt/spark/bin/spark-sql")
            .doesNotContain("docker", "compose", "exec");
        assertThat(command)
            .contains("spark.jars.ivy=/tmp/.ivy2")
            .contains("local[6]")
            .contains("spark.sql.files.maxPartitionBytes=67108864")
            .contains("spark.sql.parquet.enableVectorizedReader=false")
            .contains("spark.sql.catalog.iceberg_catalog.uri=thrift://hive-metastore:9083")
            .contains("spark.sql.catalog.iceberg_catalog.warehouse=hdfs://hdfs-namenode:8020/warehouse/iceberg")
            .contains("spark.sql.iceberg.vectorization.enabled=false")
            .contains("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1");
    }

    @Test
    void failedSparkCommandBecomesFailedEngineResultWithStderr() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 2, "", "spark failed", 0.5));
        Path workspace = tempDir.resolve("workspace");

        EngineRunResult result = new SparkIcebergClient(runner, workspace, Duration.ofMinutes(1))
            .load(new DatasetResult(
                workspace.resolve("data/run-1"),
                List.of(workspace.resolve("data/run-1/part.parquet")),
                42,
                128
            ), "run-1", "smoke");

        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("spark failed");
    }

    @Test
    void loadFailsWhenDatasetPathIsOutsideWorkspace() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "ok", "", 1.25));
        Path workspace = tempDir.resolve("workspace");
        Path outside = tempDir.resolve("outside/run-1");

        EngineRunResult result = new SparkIcebergClient(runner, workspace, Duration.ofMinutes(1))
            .load(new DatasetResult(outside, List.of(), 42, 128), "run-1", "smoke");

        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("outside workspace");
        assertThat(runner.commands()).isEmpty();
    }

    @Test
    void loadUsesAbsolutePathAsHdfsDatasetRoot() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "ok", "", 1.25));
        Path workspace = tempDir.resolve("workspace");

        EngineRunResult result = new SparkIcebergClient(runner, workspace, Duration.ofMinutes(1))
            .load(new DatasetResult(Path.of("/benchmark/kpi-1b/generated"), List.of(), 100, 4096), "run-1", "smoke");

        assertThat(result.success()).isTrue();
        assertThat(runner.commands()).hasSize(1);
        assertThat(runner.commands().get(0).get(runner.commands().get(0).size() - 1))
            .contains("path 'hdfs://hdfs-namenode:8020/benchmark/kpi-1b/generated'");
    }

    @Test
    void loadUsesHdfsUriConfigOutputAsHdfsDatasetRoot() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "ok", "", 1.25));
        Path workspace = tempDir.resolve("workspace");
        Path safeHdfsPath = com.example.databenchmark.generator.KpiGenerationConfig.from(
            com.example.databenchmark.config.BenchmarkConfig.defaultSmoke()
                .withOverrides(10, 1, null, "hdfs://hdfs-namenode:8020/benchmark/kpi-1b/generated", 100L)
        ).outputPath();

        EngineRunResult result = new SparkIcebergClient(runner, workspace, Duration.ofMinutes(1))
            .load(new DatasetResult(safeHdfsPath, List.of(), 100, 4096), "run-1", "smoke");

        assertThat(result.success()).isTrue();
        assertThat(runner.commands().get(0).get(runner.commands().get(0).size() - 1))
            .contains("path 'hdfs://hdfs-namenode:8020/benchmark/kpi-1b/generated'");
    }

    @Test
    void loadNativeParquetCreatesSparkTableAtGeneratedDatasetRoot() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "ok", "", 1.25));
        Path workspace = tempDir.resolve("workspace");
        DatasetResult dataset = new DatasetResult(
            workspace.resolve("data/native run"),
            List.of(workspace.resolve("data/native run/part.parquet")),
            42,
            128
        );

        EngineRunResult result = new SparkIcebergClient(runner, workspace, Duration.ofMinutes(1))
            .loadNativeParquet(dataset, "run-1", "smoke");

        assertThat(result.engine()).isEqualTo("spark");
        assertThat(result.tableShape()).isEqualTo("spark_native_parquet");
        assertThat(result.stage()).isEqualTo(EngineStage.SPARK_NATIVE_PARQUET_LOAD.name());
        assertThat(result.rows()).isEqualTo(42);
        assertThat(result.bytes()).isEqualTo(128);
        assertThat(result.success()).isTrue();
        assertThat(runner.commands()).hasSize(1);
        assertThat(runner.commands().get(0).get(runner.commands().get(0).size() - 1))
            .contains("CREATE DATABASE IF NOT EXISTS spark_catalog.benchmark_native")
            .contains("DROP TABLE IF EXISTS spark_catalog.benchmark_native.cell_kpi_1min")
            .contains("CREATE TABLE spark_catalog.benchmark_native.cell_kpi_1min")
            .contains("USING parquet")
            .contains("LOCATION '/workspace/data/native run'")
            .doesNotContain("INSERT INTO")
            .doesNotContain("USING iceberg")
            .doesNotContain("iceberg_catalog");
    }

    @Test
    void runQueriesUsesSparkSqlForEveryCatalogQuery() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "rows", "", 0.5));

        List<EngineRunResult> results = new SparkIcebergClient(runner).runQueries("run-1", "smoke");

        assertThat(results).hasSize(10).allSatisfy(result -> assertThat(result.success()).isTrue());
        assertThat(runner.commands()).hasSize(10);
        assertThat(runner.commands().get(0)).contains("/opt/spark/bin/spark-sql", "-e");
        assertThat(runner.commands().get(0)).contains("spark.jars.ivy=/tmp/.ivy2");
        assertThat(runner.commands().get(0)).contains("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1");
        assertThat(runner.commands().get(0).get(runner.commands().get(0).size() - 1))
            .contains("SELECT COUNT(*) FROM iceberg_catalog.iceberg_db.cell_kpi_1min");
    }

    @Test
    void runQueryRendersNamedQueryAndPreservesPhase() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "rows", "", 0.5));

        EngineRunResult result = new SparkIcebergClient(runner)
            .runQuery("topn_high_load_cells", RoutePhase.COLD);

        assertThat(result.engine()).isEqualTo("spark");
        assertThat(result.tableShape()).isEqualTo("spark_iceberg");
        assertThat(result.stage()).isEqualTo(EngineStage.QUERY.name());
        assertThat(result.queryName()).isEqualTo("topn_high_load_cells");
        assertThat(result.phase()).isEqualTo(RoutePhase.COLD.name());
        assertThat(result.success()).isTrue();
        assertThat(runner.commands()).hasSize(1);
        assertThat(runner.commands().get(0).get(runner.commands().get(0).size() - 1))
            .isEqualTo(SqlRenderer.render("topn_high_load_cells", "spark_iceberg"))
            .doesNotContain("COUNT(*)");
    }

    @Test
    void runQueryRecordsRowsFetchedBySparkSql() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(
            List.of(),
            0,
            """
            CELL-000001\t2026-01-01 00:00:00
            CELL-000002\t2026-01-01 00:01:00
            Time taken: 4.321 seconds, Fetched 100 row(s)
            """,
            "",
            4.321
        ));

        EngineRunResult result = new SparkIcebergClient(runner)
            .runQuery("topn_high_load_cells", RoutePhase.HOT);

        assertThat(result.success()).isTrue();
        assertThat(result.rows()).isEqualTo(100);
    }

    @Test
    void runNativeQueryRendersNativeTableAndPreservesPhase() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(
            List.of(),
            0,
            "Time taken: 1.000 seconds, Fetched 7 row(s)",
            "",
            1.0
        ));

        EngineRunResult result = new SparkIcebergClient(runner)
            .runNativeQuery("topn_high_load_cells", RoutePhase.WARM);

        assertThat(result.engine()).isEqualTo("spark");
        assertThat(result.tableShape()).isEqualTo("spark_native_parquet");
        assertThat(result.stage()).isEqualTo(EngineStage.QUERY.name());
        assertThat(result.queryName()).isEqualTo("topn_high_load_cells");
        assertThat(result.phase()).isEqualTo(RoutePhase.WARM.name());
        assertThat(result.rows()).isEqualTo(7);
        assertThat(result.success()).isTrue();
        assertThat(runner.commands()).hasSize(1);
        assertThat(runner.commands().get(0).get(runner.commands().get(0).size() - 1))
            .isEqualTo(SqlRenderer.render("topn_high_load_cells", "spark_native_parquet"))
            .contains("spark_catalog.benchmark_native.cell_kpi_1min")
            .doesNotContain("iceberg_catalog")
            .doesNotContain("COUNT(*)");
    }

    @Test
    void validateCountUsesScalarSparkCountAndReportsMismatch() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(
            List.of(),
            0,
            """
            count(1)
            4
            Time taken: 0.100 seconds, Fetched 1 row(s)
            """,
            "",
            0.1
        ));

        EngineRunResult result = new SparkIcebergClient(runner)
            .validateCount("spark_native_parquet", 5L);

        assertThat(result.engine()).isEqualTo("spark");
        assertThat(result.tableShape()).isEqualTo("spark_native_parquet");
        assertThat(result.stage()).isEqualTo(EngineStage.SPARK_NATIVE_PARQUET_VALIDATE.name());
        assertThat(result.rows()).isEqualTo(4L);
        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("row count mismatch").contains("expected=5").contains("actual=4");
        assertThat(runner.commands().get(0).get(runner.commands().get(0).size() - 1))
            .isEqualTo("SELECT COUNT(*) FROM spark_catalog.benchmark_native.cell_kpi_1min");
    }

    @Test
    void tpchLoadCreatesAndInsertsEachTable() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "ok", "", 1.25));
        Path workspace = tempDir.resolve("workspace");

        EngineRunResult result = new SparkIcebergClient(runner, workspace, Duration.ofMinutes(1))
            .loadTpch(TestTpchFixtures.dataset(workspace.resolve("tpch-run")), "run", "tpch-smoke");

        assertThat(result.success()).isTrue();
        assertThat(result.rows()).isEqualTo(8);
        assertThat(runner.commands()).hasSize(9);
        assertThat(runner.commands().get(0).get(runner.commands().get(0).size() - 1))
            .isEqualTo(TpchSqlTemplates.sparkCreateDatabase());
        assertThat(runner.commands())
            .extracting(command -> command.get(command.size() - 1))
            .anySatisfy(sql -> assertThat(sql)
                .contains("iceberg_catalog.tpch.lineitem")
                .contains("/workspace/tpch-run/lineitem/part-00000.parquet"));
    }

    @Test
    void tpchLoadFailureIncludesTableName() {
        Path workspace = tempDir.resolve("workspace");
        FakeCommandRunner runner = new FakeCommandRunner(
            "iceberg_catalog.tpch.lineitem",
            new CommandResult(List.of(), 1, "lineitem stdout", "lineitem stderr", 0.7),
            new CommandResult(List.of(), 0, "ok", "", 0.4)
        );

        EngineRunResult result = new SparkIcebergClient(runner, workspace, Duration.ofMinutes(1))
            .loadTpch(TestTpchFixtures.dataset(workspace.resolve("tpch-run")), "run", "tpch-smoke");

        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("table lineitem");
        assertThat(result.error()).contains("lineitem stderr");
    }

    @Test
    void tpchQueriesRenderSparkTables() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "rows", "", 0.5));

        List<EngineRunResult> results = new SparkIcebergClient(runner).runTpchQueries("run", "tpch-smoke", "smoke");

        assertThat(results).hasSize(4).allSatisfy(result -> {
            assertThat(result.success()).isTrue();
            assertThat(result.tableShape()).isEqualTo("tpch_iceberg");
        });
        assertThat(runner.commands()).hasSize(4);
        assertThat(runner.commands())
            .anySatisfy(command -> assertThat(command.get(command.size() - 1))
                .contains("iceberg_catalog.tpch.customer")
                .contains("iceberg_catalog.tpch.orders")
                .contains("iceberg_catalog.tpch.lineitem"));
    }

    private static final class FakeCommandRunner extends CommandRunner {
        private final List<CommandResult> results;
        private final String failOnSqlFragment;
        private final CommandResult failResult;
        private final List<List<String>> commands = new ArrayList<>();

        private FakeCommandRunner(CommandResult result) {
            this(null, null, List.of(result));
        }

        private FakeCommandRunner(String failOnSqlFragment, CommandResult failResult, CommandResult defaultResult) {
            this(failOnSqlFragment, failResult, List.of(defaultResult));
        }

        private FakeCommandRunner(List<CommandResult> results) {
            this(null, null, results);
        }

        private FakeCommandRunner(String failOnSqlFragment, CommandResult failResult, List<CommandResult> results) {
            this.results = results;
            this.failOnSqlFragment = failOnSqlFragment;
            this.failResult = failResult;
        }

        @Override
        public CommandResult run(List<String> command, Path workingDirectory, Duration timeout) {
            commands.add(command);
            String sql = command.get(command.size() - 1);
            if (failOnSqlFragment != null && sql.contains(failOnSqlFragment)) {
                return failResult;
            }
            int index = Math.min(commands.size() - 1, results.size() - 1);
            return results.get(index);
        }

        private List<List<String>> commands() {
            return commands;
        }
    }
}

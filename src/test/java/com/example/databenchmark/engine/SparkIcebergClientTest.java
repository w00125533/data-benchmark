package com.example.databenchmark.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.generator.DatasetResult;
import com.example.databenchmark.tpch.TestTpchFixtures;
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
            "docker", "compose", "-f", "docker-compose.yml", "exec", "-T", "spark", "/opt/spark/bin/spark-sql",
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1",
            "--conf", "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog",
            "--conf", "spark.sql.catalog.iceberg_catalog.type=hive",
            "--conf", "spark.sql.catalog.iceberg_catalog.uri=thrift://hive-metastore:9083",
            "--conf", "spark.sql.catalog.iceberg_catalog.warehouse=hdfs://hdfs-namenode:8020/warehouse/iceberg",
            "-e", SqlTemplates.sparkCreateIcebergTable() + "\n" + SqlTemplates.sparkInsertFromParquet("/workspace/data/run-1/part.parquet")
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
            .contains("spark.sql.catalog.iceberg_catalog.uri=thrift://hive-metastore:9083")
            .contains("spark.sql.catalog.iceberg_catalog.warehouse=hdfs://hdfs-namenode:8020/warehouse/iceberg")
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
    void tpchLoadCreatesAndInsertsEachTable() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "ok", "", 1.25));
        Path workspace = tempDir.resolve("workspace");

        EngineRunResult result = new SparkIcebergClient(runner, workspace, Duration.ofMinutes(1))
            .loadTpch(TestTpchFixtures.dataset(workspace.resolve("tpch-run")), "run", "tpch-smoke");

        assertThat(result.success()).isTrue();
        assertThat(result.rows()).isEqualTo(8);
        assertThat(runner.commands()).hasSize(1);
        String sql = runner.commands().get(0).get(runner.commands().get(0).size() - 1);
        assertThat(sql).contains("iceberg_catalog.tpch.lineitem");
        assertThat(sql).contains("/workspace/tpch-run/lineitem/part-00000.parquet");
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
        private final CommandResult result;
        private final List<List<String>> commands = new ArrayList<>();

        private FakeCommandRunner(CommandResult result) {
            this.result = result;
        }

        @Override
        public CommandResult run(List<String> command, Path workingDirectory, Duration timeout) {
            commands.add(command);
            return result;
        }

        private List<List<String>> commands() {
            return commands;
        }
    }
}

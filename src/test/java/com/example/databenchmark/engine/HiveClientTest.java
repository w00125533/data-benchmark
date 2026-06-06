package com.example.databenchmark.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.runner.RoutePhase;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HiveClientTest {
    @TempDir
    Path tempDir;

    @Test
    void createExternalTableRunsHiveDdlForHdfsParquetLocation() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "ok", "", 1.25));
        Path parquetRoot = Path.of("/data/generated");

        EngineRunResult result = new HiveClient(runner, tempDir, Duration.ofMinutes(1))
            .createExternalTable(parquetRoot);

        assertThat(result.engine()).isEqualTo("hive");
        assertThat(result.tableShape()).isEqualTo("hive_hdfs_parquet");
        assertThat(result.stage()).isEqualTo("HIVE_HDFS_PARQUET_LOAD");
        assertThat(result.success()).isTrue();
        assertThat(runner.commands()).hasSize(1);
        assertThat(runner.commands().get(0))
            .containsSequence(
                "docker", "compose", "-p", "shared-data-infra",
                "-f", "../shared-data-infra/compose.yaml",
                "-f", "../shared-data-infra/compose.lakehouse.yaml",
                "-f", "../shared-data-infra/compose.starrocks.yaml",
                "--profile", "lakehouse",
                "--profile", "lakehouse-tools",
                "--profile", "spark-tools",
                "--profile", "starrocks",
                "exec", "-T", "hive-server", "beeline"
            );
        assertThat(runner.commands().get(0).get(runner.commands().get(0).size() - 1))
            .contains("DROP TABLE IF EXISTS hive_hdfs_parquet.cell_kpi_1min")
            .contains("CREATE EXTERNAL TABLE hive_hdfs_parquet.cell_kpi_1min")
            .contains("PARTITIONED BY (event_date STRING)")
            .contains("STORED AS PARQUET")
            .contains("LOCATION 'hdfs://hdfs-namenode:8020/data/generated'")
            .contains("MSCK REPAIR TABLE hive_hdfs_parquet.cell_kpi_1min");
    }

    @Test
    void createExternalTableAcceptsExplicitHdfsUriWithoutRewriting() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "ok", "", 1.25));

        EngineRunResult result = new HiveClient(runner, tempDir, Duration.ofMinutes(1))
            .createExternalTable(pathString("hdfs://hdfs-namenode:8020/data/generated"));

        assertThat(result.success()).isTrue();
        assertThat(runner.commands()).hasSize(1);
        assertThat(runner.commands().get(0).get(runner.commands().get(0).size() - 1))
            .contains("LOCATION 'hdfs://hdfs-namenode:8020/data/generated'");
    }

    @Test
    void createExternalTableRejectsWindowsDrivePathWithoutRunningHive() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "ok", "", 1.25));

        EngineRunResult result = new HiveClient(runner, tempDir, Duration.ofMinutes(1))
            .createExternalTable(Path.of("D:/generated/kpi"));

        assertThat(result.engine()).isEqualTo("hive");
        assertThat(result.tableShape()).isEqualTo("hive_hdfs_parquet");
        assertThat(result.stage()).isEqualTo("HIVE_HDFS_PARQUET_LOAD");
        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("D:/generated/kpi");
        assertThat(runner.commands()).isEmpty();
    }

    @Test
    void createExternalTableRejectsRelativePathWithoutRunningHive() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "ok", "", 1.25));

        EngineRunResult result = new HiveClient(runner, tempDir, Duration.ofMinutes(1))
            .createExternalTable(Path.of("data/generated"));

        assertThat(result.engine()).isEqualTo("hive");
        assertThat(result.tableShape()).isEqualTo("hive_hdfs_parquet");
        assertThat(result.stage()).isEqualTo("HIVE_HDFS_PARQUET_LOAD");
        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("data/generated");
        assertThat(runner.commands()).isEmpty();
    }

    @Test
    void runQueryRendersHiveSqlAndPreservesPhase() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "rows", "", 0.5));

        EngineRunResult result = new HiveClient(runner, tempDir, Duration.ofMinutes(1))
            .runQuery("topn_high_load_cells", RoutePhase.WARM);

        assertThat(result.engine()).isEqualTo("hive");
        assertThat(result.tableShape()).isEqualTo("hive_hdfs_parquet");
        assertThat(result.stage()).isEqualTo(EngineStage.QUERY.name());
        assertThat(result.queryName()).isEqualTo("topn_high_load_cells");
        assertThat(result.phase()).isEqualTo(RoutePhase.WARM.name());
        assertThat(result.success()).isTrue();
        assertThat(runner.commands()).hasSize(1);
        assertThat(runner.commands().get(0).get(runner.commands().get(0).size() - 1))
            .isEqualTo(SqlRenderer.render("topn_high_load_cells", "hive_hdfs_parquet"));
    }

    @Test
    void runQueryRecordsRowsSelectedByBeeline() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(
            List.of(),
            0,
            """
            +--------------+
            |   cell_id    |
            +--------------+
            | CELL-000001  |
            +--------------+
            100 rows selected (9.174 seconds)
            """,
            "",
            9.174
        ));

        EngineRunResult result = new HiveClient(runner, tempDir, Duration.ofMinutes(1))
            .runQuery("topn_high_load_cells", RoutePhase.HOT);

        assertThat(result.success()).isTrue();
        assertThat(result.rows()).isEqualTo(100);
    }

    @Test
    void validateCountUsesScalarHiveCountAndReportsMismatch() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(
            List.of(),
            0,
            """
            +------+
            | _c0  |
            +------+
            | 4    |
            +------+
            1 rows selected (0.100 seconds)
            """,
            "",
            0.1
        ));

        EngineRunResult result = new HiveClient(runner, tempDir, Duration.ofMinutes(1))
            .validateCount(5L);

        assertThat(result.engine()).isEqualTo("hive");
        assertThat(result.tableShape()).isEqualTo("hive_hdfs_parquet");
        assertThat(result.stage()).isEqualTo(EngineStage.HIVE_HDFS_PARQUET_VALIDATE.name());
        assertThat(result.rows()).isEqualTo(4L);
        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("row count mismatch").contains("expected=5").contains("actual=4");
        assertThat(runner.commands().get(0).get(runner.commands().get(0).size() - 1))
            .isEqualTo("SELECT COUNT(*) FROM hive_hdfs_parquet.cell_kpi_1min");
    }

    @Test
    void inContainerModeRunsBeelineDirectlyWithoutDockerCompose() {
        FakeCommandRunner runner = new FakeCommandRunner(new CommandResult(List.of(), 0, "ok", "", 1.25));

        EngineRunResult result = new HiveClient(runner, tempDir, Duration.ofMinutes(1), true)
            .createExternalTable(Path.of("/data/generated"));

        assertThat(result.success()).isTrue();
        assertThat(runner.commands()).hasSize(1);
        assertThat(runner.commands().get(0))
            .startsWith("beeline")
            .doesNotContain("docker", "compose", "exec");
        assertThat(runner.commands().get(0))
            .contains("-u", "jdbc:hive2://hive-server:10000/default", "-e");
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

    private static Path pathString(String value) {
        return (Path) Proxy.newProxyInstance(
            Path.class.getClassLoader(),
            new Class<?>[] { Path.class },
            (proxy, method, args) -> {
                if (method.getName().equals("toString") && method.getParameterCount() == 0) {
                    return value;
                }
                throw new UnsupportedOperationException(method.getName());
            }
        );
    }
}

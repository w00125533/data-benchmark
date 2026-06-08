package com.example.databenchmark.report;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
import com.example.databenchmark.engine.JdbcExecutor;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ComposeTableRuntimeMetadataCollectorTest {
    @Test
    void collectsStarRocksInternalMetadataFromRawJdbcRows() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.rows("SHOW CREATE TABLE sr_internal.cell_kpi_1min_1b", List.of(Map.of(
            "Table", "cell_kpi_1min",
            "Create Table",
            "CREATE TABLE cell_kpi_1min (event_time DATETIME, cell_id VARCHAR(64)) "
                + "DUPLICATE KEY(event_time, cell_id) DISTRIBUTED BY HASH(cell_id)"
        )));
        jdbc.rows("SHOW PARTITIONS FROM sr_internal.cell_kpi_1min_1b", List.of());
        jdbc.rows("SHOW INDEX FROM sr_internal.cell_kpi_1min_1b", List.of());

        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            new FakeCommandRunner(),
            jdbc,
            new FallbackTableRuntimeMetadataCollector()
        );

        var infos = collector.collectKpi(Map.of(), 1_000_000_000L, 12_345L);

        BenchmarkReport.TableRuntimeInfo starrocks = infos.stream()
            .filter(info -> info.route().equals("starrocks_internal"))
            .findFirst()
            .orElseThrow();
        assertThat(starrocks.success()).isTrue();
        assertThat(starrocks.bucketingOrDistribution()).contains("HASH(cell_id)");
        assertThat(starrocks.rawDetails()).contains("DUPLICATE KEY(event_time, cell_id)");
    }

    @Test
    void collectsStarRocksInternalTabletRowsetAndSegmentCounts() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.rows("SHOW CREATE TABLE sr_internal.cell_kpi_1min_smoke", List.of(Map.of(
            "Table", "cell_kpi_1min",
            "Create Table",
            "CREATE TABLE cell_kpi_1min (event_time DATETIME, cell_id VARCHAR(64)) "
                + "DUPLICATE KEY(event_time, cell_id) DISTRIBUTED BY HASH(cell_id)"
        )));
        jdbc.rows("SHOW PARTITIONS FROM sr_internal.cell_kpi_1min_smoke", List.of(Map.of(
            "PartitionName", "cell_kpi_1min",
            "DataSize", "193.2GB",
            "RowCount", "1000000000"
        )));
        jdbc.rows("SHOW INDEX FROM sr_internal.cell_kpi_1min_smoke", List.of());
        jdbc.rows("SHOW TABLET FROM sr_internal.cell_kpi_1min_smoke", List.of(
            Map.of("TabletId", "40072"),
            Map.of("TabletId", "40074")
        ));
        jdbc.rows(
            "SELECT COUNT(*) AS tablet_count, SUM(NUM_ROWSET) AS rowset_count, "
                + "SUM(NUM_SEGMENT) AS segment_count, SUM(NUM_ROW) AS row_count, SUM(DATA_SIZE) AS data_bytes "
                + "FROM information_schema.be_tablets WHERE TABLET_ID IN (40072,40074)",
            List.of(Map.of(
                "tablet_count", "2",
                "rowset_count", "2",
                "segment_count", "26",
                "row_count", "124400000",
                "data_bytes", "25858482735"
            ))
        );

        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            new FakeCommandRunner(),
            jdbc,
            new FallbackTableRuntimeMetadataCollector()
        );

        BenchmarkReport.TableRuntimeInfo internal = collector.collectKpi(Map.of(), 100L, 200L).stream()
            .filter(info -> info.route().equals("starrocks_internal"))
            .findFirst()
            .orElseThrow();

        assertThat(internal.fileCount()).isEqualTo(26);
        assertThat(internal.tabletCount()).isEqualTo(2);
        assertThat(internal.rowsetCount()).isEqualTo(2);
        assertThat(internal.segmentCount()).isEqualTo(26);
        assertThat(internal.rawDetails())
            .contains("SHOW TABLET SUMMARY")
            .contains("tablet_count: 2")
            .contains("rowset_count: 2")
            .contains("segment_count: 26");
    }

    @Test
    void returnsFallbackIdentityWhenStarRocksJdbcFails() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.fail("SHOW CREATE TABLE sr_internal.cell_kpi_1min_smoke", new SQLException("metadata failed"));

        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            new FakeCommandRunner(),
            jdbc,
            new FallbackTableRuntimeMetadataCollector()
        );

        var infos = collector.collectKpi(Map.of(), 100L, 200L);

        BenchmarkReport.TableRuntimeInfo starrocks = infos.stream()
            .filter(info -> info.route().equals("starrocks_internal"))
            .findFirst()
            .orElseThrow();
        assertThat(starrocks.success()).isFalse();
        assertThat(starrocks.displayName()).isEqualTo("StarRocks Internal");
        assertThat(starrocks.tableIdentifier()).isEqualTo("sr_internal.cell_kpi_1min_smoke");
        assertThat(starrocks.error()).contains("metadata failed");
    }

    @Test
    void truncatesRawDetailsBeyondLimit() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.rows("SHOW CREATE TABLE sr_internal.cell_kpi_1min_smoke", List.of(Map.of(
            "Create Table",
            "CREATE TABLE cell_kpi_1min (cell_id VARCHAR(64)) DISTRIBUTED BY HASH(cell_id) "
                + "x".repeat(25_000)
        )));
        jdbc.rows("SHOW PARTITIONS FROM sr_internal.cell_kpi_1min_smoke", List.of());
        jdbc.rows("SHOW INDEX FROM sr_internal.cell_kpi_1min_smoke", List.of());

        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            new FakeCommandRunner(),
            jdbc,
            new FallbackTableRuntimeMetadataCollector()
        );

        BenchmarkReport.TableRuntimeInfo starrocks = collector.collectKpi(Map.of(), 100L, 200L).stream()
            .filter(info -> info.route().equals("starrocks_internal"))
            .findFirst()
            .orElseThrow();

        assertThat(starrocks.rawDetails()).hasSize(20_000);
        assertThat(starrocks.rawDetails()).contains("DISTRIBUTED BY HASH(cell_id)");
    }

    @Test
    void skipsStarRocksExternalPartitionsBecauseShowPartitionsDoesNotApplyToIcebergExternalTables() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.rows("SHOW CREATE TABLE sr_external_iceberg.iceberg_db.cell_kpi_1min", List.of(Map.of(
            "Create Table",
            "CREATE TABLE cell_kpi_1min (event_time DATETIME, cell_id VARCHAR(64)) "
                + "DISTRIBUTED BY HASH(cell_id)"
        )));
        jdbc.fail("SHOW INDEX FROM sr_external_iceberg.iceberg_db.cell_kpi_1min", new SQLException("index unavailable"));

        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            new FakeCommandRunner(),
            jdbc,
            new FallbackTableRuntimeMetadataCollector()
        );

        BenchmarkReport.TableRuntimeInfo external = collector.collectKpi(Map.of(), 100L, 200L).stream()
            .filter(info -> info.route().equals("starrocks_external_iceberg"))
            .findFirst()
            .orElseThrow();

        assertThat(external.success()).isTrue();
        assertThat(external.rawDetails()).contains("DISTRIBUTED BY HASH(cell_id)");
        assertThat(jdbc.sql()).doesNotContain("SHOW PARTITIONS FROM sr_external_iceberg.iceberg_db.cell_kpi_1min");
        assertThat(external.rawDetails()).doesNotContain("SHOW PARTITIONS failed");
        assertThat(external.rawDetails()).contains("SHOW INDEX failed: index unavailable");
    }

    @Test
    void parsesQuotedStarRocksExternalIcebergLocationForHdfsStats() {
        String location = "hdfs://hdfs-namenode:8020/warehouse/iceberg/iceberg_db/cell_kpi_1min";
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.rows("SHOW CREATE TABLE sr_external_iceberg.iceberg_db.cell_kpi_1min", List.of(Map.of(
            "Create Table",
            "CREATE TABLE cell_kpi_1min (event_time DATETIME, cell_id VARCHAR(64)) "
                + "PROPERTIES (\"location\" = \"" + location + "\")"
        )));
        jdbc.rows("SHOW PARTITIONS FROM sr_external_iceberg.iceberg_db.cell_kpi_1min", List.of());
        jdbc.rows("SHOW INDEX FROM sr_external_iceberg.iceberg_db.cell_kpi_1min", List.of());
        FakeCommandRunner commandRunner = new FakeCommandRunner();
        commandRunner.whenCommandContains(location, new CommandResult(List.of(), 0, "2 8 9876543 " + location, "", 0.1));

        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            commandRunner,
            jdbc,
            new FallbackTableRuntimeMetadataCollector()
        );

        BenchmarkReport.TableRuntimeInfo external = collector.collectKpi(Map.of(), 100L, 200L).stream()
            .filter(info -> info.route().equals("starrocks_external_iceberg"))
            .findFirst()
            .orElseThrow();

        assertThat(external.location()).isEqualTo(location);
        assertThat(external.fileCount()).isEqualTo(8);
        assertThat(external.totalBytes()).isEqualTo(9_876_543L);
    }

    @Test
    void keepsStarRocksInternalMetadataWhenOptionalPartitionsQueryFails() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.rows("SHOW CREATE TABLE sr_internal.cell_kpi_1min_smoke", List.of(Map.of(
            "Create Table",
            "CREATE TABLE cell_kpi_1min (event_time DATETIME, cell_id VARCHAR(64)) "
                + "DISTRIBUTED BY HASH(cell_id)"
        )));
        jdbc.fail("SHOW PARTITIONS FROM sr_internal.cell_kpi_1min_smoke", new SQLException("partitions unavailable"));
        jdbc.rows("SHOW INDEX FROM sr_internal.cell_kpi_1min_smoke", List.of());

        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            new FakeCommandRunner(),
            jdbc,
            new FallbackTableRuntimeMetadataCollector()
        );

        BenchmarkReport.TableRuntimeInfo internal = collector.collectKpi(Map.of(), 100L, 200L).stream()
            .filter(info -> info.route().equals("starrocks_internal"))
            .findFirst()
            .orElseThrow();

        assertThat(internal.success()).isTrue();
        assertThat(internal.rawDetails()).contains("DISTRIBUTED BY HASH(cell_id)");
        assertThat(internal.rawDetails()).contains("SHOW PARTITIONS failed: partitions unavailable");
    }

    @Test
    void collectsSparkIcebergSnapshotAndHdfsFileStatsFromRuntimeCommands() {
        String location = "hdfs://hdfs-namenode:8020/warehouse/iceberg/iceberg_db/cell_kpi_1min";
        FakeCommandRunner commandRunner = new FakeCommandRunner(new CommandResult(List.of(), 0, "", "", 0.1));
        commandRunner.whenCommandContains(
            "DESCRIBE EXTENDED iceberg_catalog.iceberg_db.cell_kpi_1min",
            new CommandResult(
                List.of(),
                0,
                "# Detailed Table Information\n"
                    + "Name\ticeberg_catalog.iceberg_db.cell_kpi_1min\n"
                    + "Location\t" + location + "\n"
                    + "Table Properties\t[current-snapshot-id=1865824109249660975,format=iceberg/parquet]",
                "",
                0.1
            )
        );
        commandRunner.whenCommandContains(
            location,
            new CommandResult(List.of(), 0, "3 9 4567890 " + location + "\n", "", 0.1)
        );

        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            commandRunner,
            starRocksSuccessJdbc(),
            new FallbackTableRuntimeMetadataCollector()
        );

        BenchmarkReport.TableRuntimeInfo iceberg = collector.collectKpi(Map.of(), 100L, 200L).stream()
            .filter(info -> info.route().equals("spark_iceberg"))
            .findFirst()
            .orElseThrow();

        assertThat(iceberg.success()).isTrue();
        assertThat(iceberg.snapshotOrVersion()).isEqualTo("snapshot=1865824109249660975");
        assertThat(iceberg.fileCount()).isEqualTo(9);
        assertThat(iceberg.totalBytes()).isEqualTo(4_567_890L);
    }

    @Test
    void parsesHiveFilesAndTotalSizeFromFormattedOutput() {
        FakeCommandRunner commandRunner = new FakeCommandRunner(new CommandResult(
            List.of(),
            0,
            "# Detailed Table Information\n"
                + "Location: hdfs://hdfs-namenode:8020/services/data-benchmark/generated/kpi/compose-smoke\n"
                + "numFiles 4\n"
                + "totalSize 2209431\n",
            "",
            0.1
        ));
        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            commandRunner,
            starRocksSuccessJdbc(),
            new FallbackTableRuntimeMetadataCollector()
        );

        BenchmarkReport.TableRuntimeInfo hive = collector.collectKpi(Map.of(), 100L, 200L).stream()
            .filter(info -> info.route().equals("hive_hdfs_parquet"))
            .findFirst()
            .orElseThrow();

        assertThat(hive.success()).isTrue();
        assertThat(hive.fileCount()).isEqualTo(4);
        assertThat(hive.totalBytes()).isEqualTo(2_209_431L);
    }

    @Test
    void collectsStarRocksInternalPartitionSizeInsteadOfFallbackBytes() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.rows("SHOW CREATE TABLE sr_internal.cell_kpi_1min_smoke", List.of(Map.of(
            "Create Table",
            "CREATE TABLE cell_kpi_1min (event_time DATETIME, cell_id VARCHAR(64)) "
                + "DISTRIBUTED BY HASH(cell_id)"
        )));
        jdbc.rows("SHOW PARTITIONS FROM sr_internal.cell_kpi_1min_smoke", List.of(Map.of(
            "PartitionName", "cell_kpi_1min",
            "DataSize", "12.5MB",
            "RowCount", "14400"
        )));
        jdbc.rows("SHOW INDEX FROM sr_internal.cell_kpi_1min_smoke", List.of());

        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            new FakeCommandRunner(),
            jdbc,
            new FallbackTableRuntimeMetadataCollector()
        );

        BenchmarkReport.TableRuntimeInfo internal = collector.collectKpi(Map.of(), 100L, 200L).stream()
            .filter(info -> info.route().equals("starrocks_internal"))
            .findFirst()
            .orElseThrow();

        assertThat(internal.success()).isTrue();
        assertThat(internal.totalBytes()).isEqualTo(13_107_200L);
    }

    @Test
    void failsSparkMetadataWhenCommandStdoutIsBlank() {
        FakeCommandRunner commandRunner = new FakeCommandRunner(new CommandResult(List.of(), 0, " \r\n\t ", "", 0.1));
        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            commandRunner,
            starRocksSuccessJdbc(),
            new FallbackTableRuntimeMetadataCollector()
        );

        BenchmarkReport.TableRuntimeInfo spark = collector.collectKpi(Map.of(), 100L, 200L).stream()
            .filter(info -> info.route().equals("spark_iceberg"))
            .findFirst()
            .orElseThrow();

        assertThat(spark.success()).isFalse();
        assertThat(spark.error()).contains("metadata command returned empty stdout");
        assertThat(spark.tableIdentifier()).isEqualTo("iceberg_catalog.iceberg_db.cell_kpi_1min");
    }

    @Test
    void failsHiveMetadataWhenCommandStdoutIsBlank() {
        FakeCommandRunner commandRunner = new FakeCommandRunner(new CommandResult(List.of(), 0, "", "", 0.1));
        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            commandRunner,
            starRocksSuccessJdbc(),
            new FallbackTableRuntimeMetadataCollector()
        );

        BenchmarkReport.TableRuntimeInfo hive = collector.collectKpi(Map.of(), 100L, 200L).stream()
            .filter(info -> info.route().equals("hive_hdfs_parquet"))
            .findFirst()
            .orElseThrow();

        assertThat(hive.success()).isFalse();
        assertThat(hive.error()).contains("metadata command returned empty stdout");
        assertThat(hive.tableIdentifier()).isEqualTo("hive_hdfs_parquet.cell_kpi_1min");
    }

    @Test
    void parsesSparkTabSeparatedLocation() {
        String location = "hdfs://hdfs-namenode:8020/warehouse/iceberg/iceberg_db/cell_kpi_1min";
        FakeCommandRunner commandRunner = new FakeCommandRunner(new CommandResult(
            List.of(),
            0,
            "# Detailed Table Information\r\nLocation\t" + location + "\r\nProvider\ticeberg",
            "",
            0.1
        ));
        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            commandRunner,
            starRocksSuccessJdbc(),
            new FallbackTableRuntimeMetadataCollector()
        );

        BenchmarkReport.TableRuntimeInfo spark = collector.collectKpi(Map.of(), 100L, 200L).stream()
            .filter(info -> info.route().equals("spark_iceberg"))
            .findFirst()
            .orElseThrow();

        assertThat(spark.success()).isTrue();
        assertThat(spark.location()).isEqualTo(location);
    }

    @Test
    void parsesEqualsSeparatedLocation() {
        String location = "hdfs://hdfs-namenode:8020/path/table";
        FakeCommandRunner commandRunner = new FakeCommandRunner(new CommandResult(
            List.of(),
            0,
            "# Detailed Table Information\r\nLocation=" + location + "\r\nProvider\ticeberg",
            "",
            0.1
        ));
        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            commandRunner,
            starRocksSuccessJdbc(),
            new FallbackTableRuntimeMetadataCollector()
        );

        BenchmarkReport.TableRuntimeInfo spark = collector.collectKpi(Map.of(), 100L, 200L).stream()
            .filter(info -> info.route().equals("spark_iceberg"))
            .findFirst()
            .orElseThrow();

        assertThat(spark.success()).isTrue();
        assertThat(spark.location()).isEqualTo(location);
    }

    @Test
    void parsesHiveColonLocation() {
        String location = "hdfs://hdfs-namenode:8020/data/kpi";
        FakeCommandRunner commandRunner = new FakeCommandRunner(new CommandResult(
            List.of(),
            0,
            "# Detailed Table Information\r\nLocation: " + location + "\r\nSerde Library: parquet",
            "",
            0.1
        ));
        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            commandRunner,
            starRocksSuccessJdbc(),
            new FallbackTableRuntimeMetadataCollector()
        );

        BenchmarkReport.TableRuntimeInfo hive = collector.collectKpi(Map.of(), 100L, 200L).stream()
            .filter(info -> info.route().equals("hive_hdfs_parquet"))
            .findFirst()
            .orElseThrow();

        assertThat(hive.success()).isTrue();
        assertThat(hive.location()).isEqualTo(location);
    }

    @Test
    void parsesHiveBeelineTableLocationWithoutPipeDecorations() {
        String location = "hdfs://hdfs-namenode:8020/services/data-benchmark/generated/kpi/compose-smoke";
        FakeCommandRunner commandRunner = new FakeCommandRunner(new CommandResult(
            List.of(),
            0,
            "| Location: | " + location + " | NULL |\n"
                + "| totalSize | 2209431 | NULL |\n",
            "",
            0.1
        ));
        ComposeTableRuntimeMetadataCollector collector = new ComposeTableRuntimeMetadataCollector(
            commandRunner,
            starRocksSuccessJdbc(),
            new FallbackTableRuntimeMetadataCollector()
        );

        BenchmarkReport.TableRuntimeInfo hive = collector.collectKpi(Map.of(), 100L, 200L).stream()
            .filter(info -> info.route().equals("hive_hdfs_parquet"))
            .findFirst()
            .orElseThrow();

        assertThat(hive.success()).isTrue();
        assertThat(hive.location()).isEqualTo(location);
    }

    private static final class FakeJdbcExecutor extends JdbcExecutor {
        private final Map<String, List<Map<String, String>>> rows = new LinkedHashMap<>();
        private final Map<String, SQLException> failures = new LinkedHashMap<>();
        private final List<String> sql = new ArrayList<>();

        @Override
        public List<Map<String, String>> queryRows(String sql) throws SQLException {
            this.sql.add(sql);
            SQLException failure = failures.get(sql);
            if (failure != null) {
                throw failure;
            }
            return rows.getOrDefault(sql, List.of());
        }

        private void rows(String sql, List<Map<String, String>> rows) {
            this.rows.put(sql, rows);
        }

        private void fail(String sql, SQLException failure) {
            failures.put(sql, failure);
        }

        private List<String> sql() {
            return sql;
        }
    }

    private static final class FakeCommandRunner extends CommandRunner {
        private final List<List<String>> commands = new ArrayList<>();
        private final Map<String, CommandResult> responses = new LinkedHashMap<>();
        private final CommandResult result;

        private FakeCommandRunner() {
            this(new CommandResult(List.of(), 0, "", "", 0.1));
        }

        private FakeCommandRunner(CommandResult result) {
            this.result = result;
        }

        private void whenCommandContains(String text, CommandResult result) {
            responses.put(text, result);
        }

        @Override
        public CommandResult run(List<String> command, Path workingDirectory, Duration timeout)
            throws IOException, InterruptedException {
            commands.add(List.copyOf(command));
            String commandText = String.join(" ", command);
            for (Map.Entry<String, CommandResult> entry : responses.entrySet()) {
                if (commandText.contains(entry.getKey())) {
                    CommandResult response = entry.getValue();
                    return new CommandResult(
                        command,
                        response.exitCode(),
                        response.stdout(),
                        response.stderr(),
                        response.durationSeconds()
                    );
                }
            }
            return new CommandResult(command, result.exitCode(), result.stdout(), result.stderr(), result.durationSeconds());
        }
    }

    private static FakeJdbcExecutor starRocksSuccessJdbc() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.rows("SHOW CREATE TABLE sr_internal.cell_kpi_1min_smoke", List.of(Map.of(
            "Create Table", "CREATE TABLE cell_kpi_1min (cell_id VARCHAR(64)) DISTRIBUTED BY HASH(cell_id)"
        )));
        jdbc.rows("SHOW PARTITIONS FROM sr_internal.cell_kpi_1min_smoke", List.of());
        jdbc.rows("SHOW INDEX FROM sr_internal.cell_kpi_1min_smoke", List.of());
        jdbc.rows("SHOW CREATE TABLE sr_external_iceberg.iceberg_db.cell_kpi_1min", List.of(Map.of(
            "Create Table", "CREATE TABLE cell_kpi_1min (cell_id VARCHAR(64)) DISTRIBUTED BY HASH(cell_id)"
        )));
        jdbc.rows("SHOW PARTITIONS FROM sr_external_iceberg.iceberg_db.cell_kpi_1min", List.of());
        jdbc.rows("SHOW INDEX FROM sr_external_iceberg.iceberg_db.cell_kpi_1min", List.of());
        return jdbc;
    }
}

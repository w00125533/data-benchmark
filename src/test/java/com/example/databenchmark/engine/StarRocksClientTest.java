package com.example.databenchmark.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.runner.RoutePhase;
import com.example.databenchmark.tpch.TestTpchFixtures;
import com.example.databenchmark.tpch.TpchSchema;
import com.example.databenchmark.tpch.TpchSqlTemplates;
import java.net.http.HttpRequest;
import java.net.http.HttpClient;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StarRocksClientTest {
    @TempDir
    Path tempDir;

    @Test
    void jdbcExecutorUsesStarRocksDefaults() {
        JdbcExecutor executor = new JdbcExecutor();

        assertThat(executor.jdbcUrl()).isEqualTo(
            "jdbc:mysql://localhost:9030/?useSSL=false&allowPublicKeyRetrieval=true&allowMultiQueries=true"
        );
        assertThat(executor.user()).isEqualTo("root");
        assertThat(executor.password()).isEmpty();
    }

    @Test
    void jdbcExecutorSplitsMultiStatementSqlForStarRocksProtocolCompatibility() {
        assertThat(JdbcExecutor.splitStatements("""
            CREATE DATABASE IF NOT EXISTS sr_internal;

            CREATE TABLE IF NOT EXISTS sr_internal.cell_kpi_1min (
              event_time DATETIME
            );
            """))
            .containsExactly(
                "CREATE DATABASE IF NOT EXISTS sr_internal",
                """
                CREATE TABLE IF NOT EXISTS sr_internal.cell_kpi_1min (
                  event_time DATETIME
                )""".stripIndent().trim()
            );
    }

    @Test
    void streamLoadBuildsDefaultUrlAndHeaders() throws Exception {
        CapturingStreamLoadClient streamLoad = new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        );

        streamLoad.loadCsv(Path.of("cell_kpi_1min.csv"), "run_1_label");

        assertThat(streamLoad.request().url())
            .isEqualTo(URI.create("http://localhost:8040/api/sr_internal/cell_kpi_1min/_stream_load"));
        assertThat(streamLoad.request().headers())
            .containsEntry("Authorization", "Basic cm9vdDo=")
            .containsEntry("label", "run_1_label")
            .containsEntry("column_separator", ",")
            .containsEntry("row_delimiter", "\\n")
            .containsEntry("enclose", "\"")
            .containsEntry("format", "csv");
    }

    @Test
    void streamLoadHeadersCanBuildJavaHttpRequest() throws Exception {
        CapturingStreamLoadClient streamLoad = new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        );
        Path csv = tempDir.resolve("load.csv");
        Files.writeString(csv, "1\n");

        streamLoad.loadCsv(csv, "run_1_label");

        HttpRequest request = streamLoad.buildRequest(streamLoad.request());
        assertThat(request.headers().firstValue("row_delimiter")).contains("\\n");
        assertThat(request.headers().firstValue("enclose")).contains("\"");
    }

    @Test
    void streamLoadBuildsPerTableUrl() {
        CapturingStreamLoadClient streamLoad = new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        );

        streamLoad.loadCsv(Path.of("lineitem.csv"), "sr_internal_tpch", "lineitem", "label");

        assertThat(streamLoad.request().url())
            .isEqualTo(URI.create("http://localhost:8040/api/sr_internal_tpch/lineitem/_stream_load"));
    }

    @Test
    void streamLoadBaseUrlBuildsComposeReachableDefaultUrl() {
        URI url = StarRocksStreamLoadClient.defaultUrl(Map.of(
            "STARROCKS_STREAM_LOAD_BASE_URL", "http://starrocks-be:8040/"
        ));
        CapturingStreamLoadClient streamLoad = new CapturingStreamLoadClient(
            HttpClient.newHttpClient(),
            url,
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        );

        streamLoad.loadCsv(Path.of("lineitem.csv"), "sr_internal_tpch", "lineitem", "label");

        assertThat(url)
            .isEqualTo(URI.create("http://starrocks-be:8040/api/sr_internal/cell_kpi_1min/_stream_load"));
        assertThat(streamLoad.request().url())
            .isEqualTo(URI.create("http://starrocks-be:8040/api/sr_internal_tpch/lineitem/_stream_load"));
    }

    @Test
    void explicitStreamLoadUrlOverridesBaseUrl() {
        URI url = StarRocksStreamLoadClient.defaultUrl(Map.of(
            "STARROCKS_STREAM_LOAD_URL", "http://custom-be:8040/api/db/table/_stream_load",
            "STARROCKS_STREAM_LOAD_BASE_URL", "http://starrocks-be:8040"
        ));

        assertThat(url).isEqualTo(URI.create("http://custom-be:8040/api/db/table/_stream_load"));
    }

    @Test
    void streamLoadBuildsPerTableUrlWithoutExplicitPort() {
        CapturingStreamLoadClient streamLoad = new CapturingStreamLoadClient(
            HttpClient.newHttpClient(),
            URI.create("http://starrocks-be/api/sr_internal/cell_kpi_1min/_stream_load"),
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        );

        streamLoad.loadCsv(Path.of("lineitem.csv"), "sr_internal_tpch", "lineitem", "label");

        assertThat(streamLoad.request().url())
            .isEqualTo(URI.create("http://starrocks-be/api/sr_internal_tpch/lineitem/_stream_load"));
    }

    @Test
    void loadInternalCreatesTableTruncatesAndSubmitsBrokerLoad() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.loadStates.add(new StarRocksBrokerLoad.LoadState("FINISHED", 0L, ""));
        CapturingStreamLoadClient streamLoad = new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        );

        EngineRunResult result = new StarRocksClient(jdbc, streamLoad).loadInternal(Path.of("generated"), "run-1", "smoke");

        assertThat(result.success()).isTrue();
        assertThat(jdbc.sql()).contains(SqlTemplates.starRocksCreateInternalTable());
        assertThat(jdbc.sql()).contains(SqlTemplates.starRocksTruncateInternalTable());
        assertThat(jdbc.sql()).anySatisfy(sql -> assertThat(sql)
            .contains("LOAD LABEL sr_internal.kpi_run_1_")
            .contains("DATA INFILE(\"hdfs://hdfs-namenode:8020/generated/*/*.parquet\")"));
        assertThat(streamLoad.request()).isNull();
    }

    @Test
    void loadInternalUsesBrokerLoadFromHdfsParquet() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.loadStates.add(new StarRocksBrokerLoad.LoadState("FINISHED", 100L, ""));

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).loadInternal(Path.of("/benchmark/kpi-smoke/generated"), "run-1", "smoke", 100L, 2048L);

        assertThat(result.success()).isTrue();
        assertThat(result.rows()).isEqualTo(100L);
        assertThat(result.bytes()).isEqualTo(2048L);
        assertThat(jdbc.sql()).anySatisfy(sql -> assertThat(sql)
            .contains("LOAD LABEL sr_internal.")
            .contains("DATA INFILE(\"hdfs://hdfs-namenode:8020/benchmark/kpi-smoke/generated/*/*.parquet\")")
            .contains("FORMAT AS \"parquet\"")
            .contains("INTO TABLE cell_kpi_1min"));
        assertThat(jdbc.sql()).noneMatch(sql -> sql.contains("_stream_load"));
    }

    @Test
    void loadInternalFailsWhenBrokerLoadIsCancelled() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.loadStates.add(new StarRocksBrokerLoad.LoadState("CANCELLED", 0L, "bad parquet"));

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).loadInternal(Path.of("/benchmark/kpi-smoke/generated"), "run-1", "smoke", 100L, 2048L);

        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("broker_load failed");
        assertThat(result.error()).contains("CANCELLED");
        assertThat(result.error()).contains("bad parquet");
    }

    @Test
    void failedSqlBecomesFailedEngineResultWithDetails() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.failure = new SQLException("create failed");

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).loadInternal(Path.of("generated"), "run-1", "smoke");

        assertThat(result.success()).isFalse();
        assertThat(result.error()).startsWith("create_internal_table failed:");
        assertThat(result.error()).contains("create failed");
    }

    @Test
    void brokerLoadSqlFailureBecomesFailedEngineResultWithDetails() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.failOnSqlContaining = "LOAD LABEL";
        jdbc.failure = new SQLException("load failed");

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(500, "stream failed", 0.25)
        )).loadInternal(Path.of("generated"), "run-1", "smoke", 100L, 2048L);

        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("broker_load failed");
        assertThat(result.error()).contains("load failed");
    }

    @Test
    void loadInternalUsesExpectedRowsWhenBrokerLoadSinkRowsIsMissing() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.loadStates.add(new StarRocksBrokerLoad.LoadState("FINISHED", 0L, ""));

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).loadInternal(Path.of("generated"), "run-1", "smoke", 100L, 2048L);

        assertThat(result.success()).isTrue();
        assertThat(result.rows()).isEqualTo(100L);
        assertThat(result.bytes()).isEqualTo(2048L);
    }

    @Test
    void loadInternalDurationIncludesBrokerLoadPollingElapsedTime() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.executionDurationSeconds = 0.0;
        jdbc.loadStateDelayMillis = 75L;
        jdbc.loadStates.add(new StarRocksBrokerLoad.LoadState("FINISHED", 100L, ""));

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).loadInternal(Path.of("generated"), "run-1", "smoke", 100L, 2048L);

        assertThat(result.success()).isTrue();
        assertThat(result.durationSeconds()).isGreaterThanOrEqualTo(0.05);
    }

    @Test
    void loadInternalThreeArgumentCompatibilityUsesBrokerLoad() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.loadStates.add(new StarRocksBrokerLoad.LoadState("FINISHED", 10L, ""));

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).loadInternal(Path.of("generated"), "run-1", "smoke");

        assertThat(result.success()).isTrue();
        assertThat(result.rows()).isEqualTo(10L);
    }

    @Test
    void loadInternalThreeArgumentCompatibilityRejectsCsvPath() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).loadInternal(Path.of("cell_kpi_1min.csv"), "run-1", "smoke");

        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("expects an HDFS Parquet root");
        assertThat(jdbc.sql()).isEmpty();
    }

    @Test
    void refreshExternalCatalogUsesCreateAndRefreshSql() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).refreshExternalCatalog("run-1", "smoke");

        assertThat(result.success()).isTrue();
        assertThat(jdbc.sql())
            .contains(SqlTemplates.starRocksCreateExternalCatalog())
            .contains(SqlTemplates.starRocksRefreshExternalCatalog());
    }

    @Test
    void refreshExternalCatalogReportsCreateCatalogFailureContext() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.failure = new SQLException("catalog failed");

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).refreshExternalCatalog("run-1", "smoke");

        assertThat(result.success()).isFalse();
        assertThat(result.error()).startsWith("create_external_catalog failed:");
        assertThat(result.error()).contains("catalog failed");
    }

    @Test
    void refreshExternalCatalogReportsRefreshFailureContext() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.failOnSql = SqlTemplates.starRocksRefreshExternalCatalog();
        jdbc.failure = new SQLException("refresh failed");

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).refreshExternalCatalog("run-1", "smoke");

        assertThat(result.success()).isFalse();
        assertThat(result.error()).startsWith("refresh_external_catalog failed:");
        assertThat(result.error()).contains("refresh failed");
    }

    @Test
    void runQueriesRendersInternalAndExternalStarRocksQueries() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();

        List<EngineRunResult> results = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).runQueries("run-1", "smoke");

        assertThat(results).hasSize(20).allSatisfy(result -> assertThat(result.success()).isTrue());
        assertThat(jdbc.sql()).anySatisfy(sql -> assertThat(sql).contains("sr_internal.cell_kpi_1min"));
        assertThat(jdbc.sql()).anySatisfy(sql -> assertThat(sql).contains("sr_external_iceberg.iceberg_db.cell_kpi_1min"));
    }

    @Test
    void runQueryForRendersRequestedTableShapeAndPreservesPhase() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).runQueryFor("starrocks_external_iceberg", "topn_high_load_cells", RoutePhase.HOT);

        assertThat(result.engine()).isEqualTo("starrocks");
        assertThat(result.tableShape()).isEqualTo("starrocks_external_iceberg");
        assertThat(result.stage()).isEqualTo(EngineStage.QUERY.name());
        assertThat(result.queryName()).isEqualTo("topn_high_load_cells");
        assertThat(result.phase()).isEqualTo(RoutePhase.HOT.name());
        assertThat(result.success()).isTrue();
        assertThat(jdbc.sql()).containsExactly(SqlRenderer.render("topn_high_load_cells", "starrocks_external_iceberg"));
        assertThat(jdbc.sql().get(0)).contains("sr_external_iceberg.iceberg_db.cell_kpi_1min");
    }

    @Test
    void runQueryForUnknownTableShapeReturnsFailedResultWithoutQuerying() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).runQueryFor("missing_shape", "topn_high_load_cells", RoutePhase.COLD);

        assertThat(result.engine()).isEqualTo("starrocks");
        assertThat(result.tableShape()).isEqualTo("missing_shape");
        assertThat(result.stage()).isEqualTo(EngineStage.QUERY.name());
        assertThat(result.queryName()).isEqualTo("topn_high_load_cells");
        assertThat(result.phase()).isEqualTo(RoutePhase.COLD.name());
        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("Unknown engine key: missing_shape");
        assertThat(jdbc.sql()).isEmpty();
    }

    @Test
    void runQueryForUnknownQueryReturnsFailedResultWithoutQuerying() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).runQueryFor("starrocks_internal", "missing_query", RoutePhase.WARM);

        assertThat(result.engine()).isEqualTo("starrocks");
        assertThat(result.tableShape()).isEqualTo("starrocks_internal");
        assertThat(result.stage()).isEqualTo(EngineStage.QUERY.name());
        assertThat(result.queryName()).isEqualTo("missing_query");
        assertThat(result.phase()).isEqualTo(RoutePhase.WARM.name());
        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("Unknown query: missing_query");
        assertThat(jdbc.sql()).isEmpty();
    }

    @Test
    void validateCountUsesJdbcScalarCountAndReportsMismatch() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.queryLongResult = 4L;

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).validateCount("starrocks_internal", 5L);

        assertThat(result.engine()).isEqualTo("starrocks");
        assertThat(result.tableShape()).isEqualTo("starrocks_internal");
        assertThat(result.stage()).isEqualTo(EngineStage.STARROCKS_INTERNAL_VALIDATE.name());
        assertThat(result.rows()).isEqualTo(4L);
        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("row count mismatch").contains("expected=5").contains("actual=4");
        assertThat(jdbc.sql()).containsExactly("SELECT COUNT(*) FROM sr_internal.cell_kpi_1min");
    }

    @Test
    void tpchLoadCreatesTablesAndStreamsEachCsv() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        CapturingStreamLoadClient streamLoad = new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        );
        var dataset = TestTpchFixtures.dataset(tempDir.resolve("tpch-run"));

        EngineRunResult result = new StarRocksClient(jdbc, streamLoad)
            .loadTpchInternal(TestTpchFixtures.csvFiles(dataset), dataset, "run", "tpch-smoke");

        assertThat(result.success()).isTrue();
        assertThat(jdbc.sql()).contains("CREATE DATABASE IF NOT EXISTS sr_internal_tpch;");
        assertThat(jdbc.sql()).anySatisfy(sql -> assertThat(sql).contains("CREATE TABLE IF NOT EXISTS sr_internal_tpch.lineitem"));
        assertThat(streamLoad.requests()).hasSize(TpchSchema.tables().size());
        assertThat(streamLoad.requests())
            .extracting(request -> request.url().toString())
            .anyMatch(url -> url.endsWith("/api/sr_internal_tpch/lineitem/_stream_load"));
        assertThat(streamLoad.requests())
            .extracting(request -> request.url().toString())
            .allSatisfy(url -> assertThat(url).contains("/api/sr_internal_tpch/"));
    }

    @Test
    void tpchLoadReportsCreateTableFailureWithTableName() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.failOnSql = TpchSqlTemplates.starRocksCreateInternalTable(TpchSchema.table("lineitem"));
        jdbc.failure = new SQLException("ddl failed");
        var dataset = TestTpchFixtures.dataset(tempDir.resolve("tpch-run"));

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).loadTpchInternal(TestTpchFixtures.csvFiles(dataset), dataset, "run", "tpch-smoke");

        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("create_tpch_internal_table failed for table lineitem");
        assertThat(result.error()).contains("ddl failed");
    }

    @Test
    void refreshTpchExternalCatalogReportsRefreshFailureWithTableName() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.failOnSql = TpchSqlTemplates.starRocksRefreshExternalTable(TpchSchema.table("lineitem"));
        jdbc.failure = new SQLException("refresh failed");

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).refreshTpchExternalCatalog("run", "tpch-smoke");

        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("refresh_tpch_external_table failed for table lineitem");
        assertThat(result.error()).contains("refresh failed");
    }

    @Test
    void tpchQueriesRenderInternalAndExternalTables() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();

        List<EngineRunResult> results = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).runTpchQueries("run", "tpch-smoke", "smoke");

        assertThat(results).hasSize(8).allSatisfy(result -> assertThat(result.success()).isTrue());
        assertThat(jdbc.sql()).anySatisfy(sql -> assertThat(sql).contains("sr_internal_tpch.lineitem"));
        assertThat(jdbc.sql()).anySatisfy(sql -> assertThat(sql).contains("sr_external_iceberg.tpch.lineitem"));
    }

    private static final class FakeJdbcExecutor extends JdbcExecutor {
        private final List<String> sql = new ArrayList<>();
        private final List<StarRocksBrokerLoad.LoadState> loadStates = new ArrayList<>();
        private double executionDurationSeconds = 0.1;
        private long loadStateDelayMillis;
        private SQLException failure;
        private String failOnSql;
        private String failOnSqlContaining;
        private long queryLongResult = 3L;

        @Override
        public JdbcExecutionResult execute(String sql) throws SQLException {
            this.sql.add(sql);
            if (failure != null && (failOnSql == null || failOnSql.equals(sql))
                && (failOnSqlContaining == null || sql.contains(failOnSqlContaining))) {
                throw failure;
            }
            return new JdbcExecutionResult(0, executionDurationSeconds);
        }

        @Override
        public JdbcExecutionResult query(String sql) throws SQLException {
            this.sql.add(sql);
            if (failure != null && (failOnSql == null || failOnSql.equals(sql))
                && (failOnSqlContaining == null || sql.contains(failOnSqlContaining))) {
                throw failure;
            }
            return new JdbcExecutionResult(3, 0.2);
        }

        @Override
        public long queryLong(String sql) throws SQLException {
            this.sql.add(sql);
            if (failure != null && (failOnSql == null || failOnSql.equals(sql))
                && (failOnSqlContaining == null || sql.contains(failOnSqlContaining))) {
                throw failure;
            }
            return queryLongResult;
        }

        @Override
        public StarRocksBrokerLoad.LoadState latestLoadState(String database, String label) throws SQLException {
            if (loadStateDelayMillis > 0) {
                try {
                    Thread.sleep(loadStateDelayMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("interrupted while delaying load state", e);
                }
            }
            if (loadStates.isEmpty()) {
                return new StarRocksBrokerLoad.LoadState("UNKNOWN", 0L, "");
            }
            return loadStates.remove(0);
        }

        private List<String> sql() {
            return sql;
        }
    }

    private static final class CapturingStreamLoadClient extends StarRocksStreamLoadClient {
        private final StreamLoadResult result;
        private final List<StreamLoadRequest> requests = new ArrayList<>();
        private StreamLoadRequest request;

        private CapturingStreamLoadClient(StreamLoadResult result) {
            this.result = result;
        }

        private CapturingStreamLoadClient(HttpClient httpClient, URI url, StreamLoadResult result) {
            super(httpClient, url, "root", "");
            this.result = result;
        }

        @Override
        protected StreamLoadResult send(StreamLoadRequest request) {
            this.request = request;
            this.requests.add(request);
            return result;
        }

        private StreamLoadRequest request() {
            return request;
        }

        private List<StreamLoadRequest> requests() {
            return requests;
        }
    }
}

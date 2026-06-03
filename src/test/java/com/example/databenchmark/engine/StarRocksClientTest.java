package com.example.databenchmark.engine;

import static org.assertj.core.api.Assertions.assertThat;

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
    void loadInternalCreatesTableAndStreamsCsv() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        CapturingStreamLoadClient streamLoad = new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        );

        EngineRunResult result = new StarRocksClient(jdbc, streamLoad).loadInternal(Path.of("load.csv"), "run-1", "smoke");

        assertThat(result.success()).isTrue();
        assertThat(jdbc.sql()).contains(SqlTemplates.starRocksCreateInternalTable());
        assertThat(streamLoad.request().headers().get("label")).startsWith("run-1_");
    }

    @Test
    void failedSqlBecomesFailedEngineResultWithDetails() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        jdbc.failure = new SQLException("create failed");

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).loadInternal(Path.of("load.csv"), "run-1", "smoke");

        assertThat(result.success()).isFalse();
        assertThat(result.error()).startsWith("create_internal_table failed:");
        assertThat(result.error()).contains("create failed");
    }

    @Test
    void failedHttpBecomesFailedEngineResultWithDetails() {
        EngineRunResult result = new StarRocksClient(new FakeJdbcExecutor(), new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(500, "stream failed", 0.25)
        )).loadInternal(Path.of("load.csv"), "run-1", "smoke");

        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("stream_load failed:");
        assertThat(result.error()).contains("HTTP 500");
        assertThat(result.error()).contains("stream failed");
    }

    @Test
    void httpOkWithFailedStreamLoadStatusBecomesFailedEngineResult() {
        String body = "{\"Status\":\"Fail\",\"Message\":\"bad\"}";

        EngineRunResult result = new StarRocksClient(new FakeJdbcExecutor(), new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, body, 0.25)
        )).loadInternal(Path.of("load.csv"), "run-1", "smoke");

        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("stream_load failed:");
        assertThat(result.error()).contains("HTTP 200");
        assertThat(result.error()).contains("Fail");
        assertThat(result.error()).contains("bad");
        assertThat(result.error()).contains(body);
    }

    @Test
    void httpOkWithSuccessStreamLoadStatusIsSuccessful() {
        EngineRunResult result = new StarRocksClient(new FakeJdbcExecutor(), new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        )).loadInternal(Path.of("load.csv"), "run-1", "smoke");

        assertThat(result.success()).isTrue();
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
        private SQLException failure;
        private String failOnSql;

        @Override
        public JdbcExecutionResult execute(String sql) throws SQLException {
            this.sql.add(sql);
            if (failure != null && (failOnSql == null || failOnSql.equals(sql))) {
                throw failure;
            }
            return new JdbcExecutionResult(0, 0.1);
        }

        @Override
        public JdbcExecutionResult query(String sql) throws SQLException {
            this.sql.add(sql);
            if (failure != null && (failOnSql == null || failOnSql.equals(sql))) {
                throw failure;
            }
            return new JdbcExecutionResult(3, 0.2);
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

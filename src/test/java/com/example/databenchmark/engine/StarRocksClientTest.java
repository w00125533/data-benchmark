package com.example.databenchmark.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StarRocksClientTest {
    @Test
    void jdbcExecutorUsesStarRocksDefaults() {
        JdbcExecutor executor = new JdbcExecutor();

        assertThat(executor.jdbcUrl()).isEqualTo("jdbc:mysql://localhost:9030/?useSSL=false&allowPublicKeyRetrieval=true");
        assertThat(executor.user()).isEqualTo("root");
        assertThat(executor.password()).isEmpty();
    }

    @Test
    void streamLoadBuildsDefaultUrlAndHeaders() throws Exception {
        CapturingStreamLoadClient streamLoad = new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "{\"Status\":\"Success\"}", 0.25)
        );

        streamLoad.loadCsv(Path.of("cell_kpi_1min.csv"), "run_1_label");

        assertThat(streamLoad.request().url())
            .isEqualTo(URI.create("http://localhost:8030/api/sr_internal/cell_kpi_1min/_stream_load"));
        assertThat(streamLoad.request().headers())
            .containsEntry("Authorization", "Basic cm9vdDo=")
            .containsEntry("label", "run_1_label")
            .containsEntry("column_separator", ",")
            .containsEntry("format", "csv");
    }

    @Test
    void loadInternalCreatesTableAndStreamsCsv() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();
        CapturingStreamLoadClient streamLoad = new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "ok", 0.25)
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
            new StarRocksStreamLoadClient.StreamLoadResult(200, "ok", 0.25)
        )).loadInternal(Path.of("load.csv"), "run-1", "smoke");

        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("create failed");
    }

    @Test
    void failedHttpBecomesFailedEngineResultWithDetails() {
        EngineRunResult result = new StarRocksClient(new FakeJdbcExecutor(), new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(500, "stream failed", 0.25)
        )).loadInternal(Path.of("load.csv"), "run-1", "smoke");

        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("stream failed");
    }

    @Test
    void refreshExternalCatalogUsesCreateAndRefreshSql() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();

        EngineRunResult result = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "ok", 0.25)
        )).refreshExternalCatalog("run-1", "smoke");

        assertThat(result.success()).isTrue();
        assertThat(jdbc.sql())
            .contains(SqlTemplates.starRocksCreateExternalCatalog())
            .contains(SqlTemplates.starRocksRefreshExternalCatalog());
    }

    @Test
    void runQueriesRendersInternalAndExternalStarRocksQueries() {
        FakeJdbcExecutor jdbc = new FakeJdbcExecutor();

        List<EngineRunResult> results = new StarRocksClient(jdbc, new CapturingStreamLoadClient(
            new StarRocksStreamLoadClient.StreamLoadResult(200, "ok", 0.25)
        )).runQueries("run-1", "smoke");

        assertThat(results).hasSize(20).allSatisfy(result -> assertThat(result.success()).isTrue());
        assertThat(jdbc.sql()).anySatisfy(sql -> assertThat(sql).contains("sr_internal.cell_kpi_1min"));
        assertThat(jdbc.sql()).anySatisfy(sql -> assertThat(sql).contains("sr_external_iceberg.iceberg_db.cell_kpi_1min"));
    }

    private static final class FakeJdbcExecutor extends JdbcExecutor {
        private final List<String> sql = new ArrayList<>();
        private SQLException failure;

        @Override
        public JdbcExecutionResult execute(String sql) throws SQLException {
            this.sql.add(sql);
            if (failure != null) {
                throw failure;
            }
            return new JdbcExecutionResult(0, 0.1);
        }

        @Override
        public JdbcExecutionResult query(String sql) throws SQLException {
            this.sql.add(sql);
            if (failure != null) {
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
        private StreamLoadRequest request;

        private CapturingStreamLoadClient(StreamLoadResult result) {
            this.result = result;
        }

        @Override
        protected StreamLoadResult send(StreamLoadRequest request) {
            this.request = request;
            return result;
        }

        private StreamLoadRequest request() {
            return request;
        }
    }
}

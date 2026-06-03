package com.example.databenchmark.engine;

import com.example.databenchmark.query.QueryCatalog;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class StarRocksClient {
    private final JdbcExecutor jdbcExecutor;
    private final StarRocksStreamLoadClient streamLoadClient;

    public StarRocksClient() {
        this(new JdbcExecutor(), new StarRocksStreamLoadClient());
    }

    public StarRocksClient(JdbcExecutor jdbcExecutor, StarRocksStreamLoadClient streamLoadClient) {
        this.jdbcExecutor = jdbcExecutor;
        this.streamLoadClient = streamLoadClient;
    }

    public EngineRunResult loadInternal(Path csv, String runId, String profile) {
        JdbcExecutionResult ddl;
        try {
            ddl = jdbcExecutor.execute(SqlTemplates.starRocksCreateInternalTable());
        } catch (SQLException e) {
            return failed("starrocks_internal", EngineStage.STARROCKS_INTERNAL_LOAD.name(), null, 0.0,
                "create_internal_table failed: " + e.getMessage());
        }

        StarRocksStreamLoadClient.StreamLoadResult load =
            streamLoadClient.loadCsv(csv, runId + "_" + Instant.now().toEpochMilli());
        if (!load.success()) {
            return failed("starrocks_internal", EngineStage.STARROCKS_INTERNAL_LOAD.name(), null,
                load.durationSeconds(), "stream_load failed: HTTP " + load.statusCode() + " body: " + load.body());
        }
        return new EngineRunResult(
            "starrocks",
            "starrocks_internal",
            EngineStage.STARROCKS_INTERNAL_LOAD.name(),
            null,
            0,
            0,
            ddl.durationSeconds() + load.durationSeconds(),
            true,
            ""
        );
    }

    public EngineRunResult refreshExternalCatalog(String runId, String profile) {
        JdbcExecutionResult create;
        try {
            create = jdbcExecutor.execute(SqlTemplates.starRocksCreateExternalCatalog());
        } catch (SQLException e) {
            return failed("starrocks_external_iceberg", EngineStage.STARROCKS_EXTERNAL_REFRESH.name(), null, 0.0,
                "create_external_catalog failed: " + e.getMessage());
        }

        try {
            JdbcExecutionResult refresh = jdbcExecutor.execute(SqlTemplates.starRocksRefreshExternalCatalog());
            return new EngineRunResult(
                "starrocks",
                "starrocks_external_iceberg",
                EngineStage.STARROCKS_EXTERNAL_REFRESH.name(),
                null,
                0,
                0,
                create.durationSeconds() + refresh.durationSeconds(),
                true,
                ""
            );
        } catch (SQLException e) {
            return failed("starrocks_external_iceberg", EngineStage.STARROCKS_EXTERNAL_REFRESH.name(), null, 0.0,
                "refresh_external_catalog failed: " + e.getMessage());
        }
    }

    public List<EngineRunResult> runQueries(String runId, String profile) {
        List<EngineRunResult> results = new ArrayList<>();
        results.addAll(runQueriesFor("starrocks_internal"));
        results.addAll(runQueriesFor("starrocks_external_iceberg"));
        return results;
    }

    private List<EngineRunResult> runQueriesFor(String tableShape) {
        List<EngineRunResult> results = new ArrayList<>();
        for (var query : QueryCatalog.queries()) {
            try {
                JdbcExecutionResult result = jdbcExecutor.query(SqlRenderer.render(query.name(), tableShape));
                results.add(new EngineRunResult(
                    "starrocks",
                    tableShape,
                    EngineStage.QUERY.name(),
                    query.name(),
                    result.rows(),
                    0,
                    result.durationSeconds(),
                    true,
                    ""
                ));
            } catch (SQLException e) {
                results.add(failed(tableShape, EngineStage.QUERY.name(), query.name(), 0.0, e.getMessage()));
            }
        }
        return results;
    }

    private static EngineRunResult failed(
        String tableShape,
        String stage,
        String queryName,
        double durationSeconds,
        String error
    ) {
        return new EngineRunResult(
            "starrocks",
            tableShape,
            stage,
            queryName,
            0,
            0,
            durationSeconds,
            false,
            error
        );
    }
}

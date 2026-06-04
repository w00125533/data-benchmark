package com.example.databenchmark.engine;

import com.example.databenchmark.query.QueryCatalog;
import com.example.databenchmark.runner.RoutePhase;
import com.example.databenchmark.tpch.TpchDatasetResult;
import com.example.databenchmark.tpch.TpchQueryCatalog;
import com.example.databenchmark.tpch.TpchSchema;
import com.example.databenchmark.tpch.TpchSqlRenderer;
import com.example.databenchmark.tpch.TpchSqlTemplates;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    public EngineRunResult loadTpchInternal(
        Map<String, Path> csvFiles,
        TpchDatasetResult dataset,
        String runId,
        String profile
    ) {
        double durationSeconds = 0.0;
        try {
            durationSeconds += jdbcExecutor.execute(TpchSqlTemplates.starRocksCreateDatabase()).durationSeconds();
            for (var table : TpchSchema.tables()) {
                Path csv = csvFiles.get(table.name());
                if (csv == null) {
                    return failed(
                        "tpch_internal",
                        EngineStage.STARROCKS_INTERNAL_LOAD.name(),
                        null,
                        durationSeconds,
                        "Missing TPC-H CSV path for table: " + table.name()
                    );
                }
                try {
                    durationSeconds += jdbcExecutor.execute(TpchSqlTemplates.starRocksCreateInternalTable(table)).durationSeconds();
                } catch (SQLException e) {
                    return failed(
                        "tpch_internal",
                        EngineStage.STARROCKS_INTERNAL_LOAD.name(),
                        null,
                        durationSeconds,
                        "create_tpch_internal_table failed for table %s: %s".formatted(table.name(), e.getMessage())
                    );
                }
                StarRocksStreamLoadClient.StreamLoadResult load = streamLoadClient.loadCsv(
                    csv,
                    "sr_internal_tpch",
                    table.name(),
                    runId + "_" + table.name() + "_" + Instant.now().toEpochMilli()
                );
                durationSeconds += load.durationSeconds();
                if (!load.success()) {
                    return failed(
                        "tpch_internal",
                        EngineStage.STARROCKS_INTERNAL_LOAD.name(),
                        null,
                        durationSeconds,
                        "stream_load failed for table %s: HTTP %d body: %s".formatted(
                            table.name(),
                            load.statusCode(),
                            load.body()
                        )
                    );
                }
            }
        } catch (SQLException e) {
            return failed(
                "tpch_internal",
                EngineStage.STARROCKS_INTERNAL_LOAD.name(),
                null,
                durationSeconds,
                "tpch_internal_load failed: " + e.getMessage()
            );
        }

        return new EngineRunResult(
            "starrocks",
            "tpch_internal",
            EngineStage.STARROCKS_INTERNAL_LOAD.name(),
            null,
            dataset.rows(),
            dataset.bytesWritten(),
            durationSeconds,
            true,
            ""
        );
    }

    public EngineRunResult refreshTpchExternalCatalog(String runId, String profile) {
        double durationSeconds = 0.0;
        try {
            durationSeconds += jdbcExecutor.execute(SqlTemplates.starRocksCreateExternalCatalog()).durationSeconds();
            for (var table : TpchSchema.tables()) {
                try {
                    durationSeconds += jdbcExecutor.execute(TpchSqlTemplates.starRocksRefreshExternalTable(table)).durationSeconds();
                } catch (SQLException e) {
                    return failed(
                        "tpch_external_iceberg",
                        EngineStage.STARROCKS_EXTERNAL_REFRESH.name(),
                        null,
                        durationSeconds,
                        "refresh_tpch_external_table failed for table %s: %s".formatted(table.name(), e.getMessage())
                    );
                }
            }
            return new EngineRunResult(
                "starrocks",
                "tpch_external_iceberg",
                EngineStage.STARROCKS_EXTERNAL_REFRESH.name(),
                null,
                0,
                0,
                durationSeconds,
                true,
                ""
            );
        } catch (SQLException e) {
            return failed(
                "tpch_external_iceberg",
                EngineStage.STARROCKS_EXTERNAL_REFRESH.name(),
                null,
                durationSeconds,
                "refresh_tpch_external_catalog failed: " + e.getMessage()
            );
        }
    }

    public List<EngineRunResult> runTpchQueries(String runId, String profile, String querySet) {
        List<EngineRunResult> results = new ArrayList<>();
        results.addAll(runTpchQueriesFor("starrocks_internal", "tpch_internal", querySet));
        results.addAll(runTpchQueriesFor("starrocks_external_iceberg", "tpch_external_iceberg", querySet));
        return results;
    }

    private List<EngineRunResult> runQueriesFor(String tableShape) {
        List<EngineRunResult> results = new ArrayList<>();
        for (var query : QueryCatalog.queries()) {
            results.add(runQueryFor(tableShape, query.name(), RoutePhase.HOT));
        }
        return results;
    }

    public EngineRunResult runQueryFor(String tableShape, String queryName, RoutePhase phase) {
        try {
            JdbcExecutionResult result = jdbcExecutor.query(SqlRenderer.render(queryName, tableShape));
            return new EngineRunResult(
                "starrocks",
                tableShape,
                EngineStage.QUERY.name(),
                queryName,
                phase.name(),
                result.rows(),
                0,
                result.durationSeconds(),
                true,
                ""
            );
        } catch (SQLException | IllegalArgumentException e) {
            return failed(tableShape, EngineStage.QUERY.name(), queryName, phase, 0.0, e.getMessage());
        }
    }

    private List<EngineRunResult> runTpchQueriesFor(String engineKey, String tableShape, String querySet) {
        List<EngineRunResult> results = new ArrayList<>();
        for (var query : TpchQueryCatalog.queries(querySet)) {
            try {
                JdbcExecutionResult result = jdbcExecutor.query(TpchSqlRenderer.render(query.name(), engineKey));
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

    private static EngineRunResult failed(
        String tableShape,
        String stage,
        String queryName,
        RoutePhase phase,
        double durationSeconds,
        String error
    ) {
        return new EngineRunResult(
            "starrocks",
            tableShape,
            stage,
            queryName,
            phase.name(),
            0,
            0,
            durationSeconds,
            false,
            error
        );
    }
}

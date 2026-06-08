package com.example.databenchmark.engine;

import com.example.databenchmark.query.QueryCatalog;
import com.example.databenchmark.query.QueryDefinition;
import com.example.databenchmark.schema.KpiSchema;
import java.util.Map;

public final class SqlRenderer {
    private SqlRenderer() {}

    public static String render(String queryName, String engineKey) {
        return render(queryName, engineKey, KpiSchema.tableShapes());
    }

    public static String render(String queryName, String engineKey, Map<String, String> tableShapes) {
        String table = tableShapes.get(engineKey);
        if (table == null) {
            throw new IllegalArgumentException("Unknown engine key: " + engineKey);
        }

        QueryDefinition query = QueryCatalog.queries().stream()
            .filter(candidate -> candidate.name().equals(queryName))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown query: " + queryName));
        String sql = query.template().replace("{table}", table);
        if (engineKey.startsWith("starrocks")) {
            return sql.replaceAll("TIMESTAMP '([^']+)'", "CAST('$1' AS DATETIME)");
        }
        if (engineKey.equals("hive_hdfs_parquet")) {
            return renderHiveSql(sql, queryName);
        }
        return sql;
    }

    private static String renderHiveSql(String sql, String queryName) {
        String rendered = sql
            .replace("DATE_TRUNC('day', event_time)", "date_format(event_time, 'yyyy-MM-dd 00:00:00')")
            .replace("DATE_TRUNC('minute', event_time)", "date_format(event_time, 'yyyy-MM-dd HH:mm:00')")
            .replace("DATE_TRUNC('hour', event_time)", "date_format(event_time, 'yyyy-MM-dd HH:00:00')");
        return switch (queryName) {
            case "single_cell_week_trend" -> appendAfterFirstTimeLowerBound(
                rendered,
                "  AND event_date >= '2026-01-01'\n  AND event_date < '2026-01-08'"
            );
            case "adjacent_window_kpi_spike" -> rendered
                .replace(
                    "WHERE event_time >= TIMESTAMP '2026-01-01 12:00:00'",
                    "WHERE event_time >= TIMESTAMP '2026-01-01 12:00:00'\n                  AND event_date = '2026-01-01'"
                )
                .replace(
                    "WHERE event_time >= TIMESTAMP '2026-01-01 11:00:00'",
                    "WHERE event_time >= TIMESTAMP '2026-01-01 11:00:00'\n                  AND event_date = '2026-01-01'"
                );
            default -> appendAfterFirstTimeLowerBound(rendered, "  AND event_date = '2026-01-01'");
        };
    }

    private static String appendAfterFirstTimeLowerBound(String sql, String partitionPredicate) {
        String marker = "event_time >= TIMESTAMP '2026-01-01";
        int index = sql.indexOf(marker);
        if (index < 0) {
            return sql;
        }
        int lineEnd = sql.indexOf('\n', index);
        if (lineEnd < 0) {
            return sql + "\n" + partitionPredicate;
        }
        return sql.substring(0, lineEnd + 1) + partitionPredicate + "\n" + sql.substring(lineEnd + 1);
    }
}

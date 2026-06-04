package com.example.databenchmark.engine;

import com.example.databenchmark.query.QueryCatalog;
import com.example.databenchmark.query.QueryDefinition;
import com.example.databenchmark.schema.KpiSchema;

public final class SqlRenderer {
    private SqlRenderer() {}

    public static String render(String queryName, String engineKey) {
        String table = KpiSchema.tableShapes().get(engineKey);
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
            return renderHiveSql(sql);
        }
        return sql;
    }

    private static String renderHiveSql(String sql) {
        return sql
            .replace("DATE_TRUNC('day', event_time)", "date_format(event_time, 'yyyy-MM-dd 00:00:00')")
            .replace("DATE_TRUNC('minute', event_time)", "date_format(event_time, 'yyyy-MM-dd HH:mm:00')")
            .replace("DATE_TRUNC('hour', event_time)", "date_format(event_time, 'yyyy-MM-dd HH:00:00')");
    }
}

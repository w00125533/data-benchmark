package com.example.databenchmark.engine;

import com.example.databenchmark.query.QueryCatalog;
import com.example.databenchmark.query.QueryDefinition;
import java.util.Map;

public final class SqlRenderer {
    private static final Map<String, String> TABLES = Map.of(
        "spark_iceberg",
        "iceberg_catalog.iceberg_db.cell_kpi_1min",
        "starrocks_internal",
        "sr_internal.cell_kpi_1min",
        "starrocks_external_iceberg",
        "sr_external_iceberg.iceberg_db.cell_kpi_1min"
    );

    private SqlRenderer() {}

    public static String render(String queryName, String engineKey) {
        String table = TABLES.get(engineKey);
        if (table == null) {
            throw new IllegalArgumentException("Unknown engine key: " + engineKey);
        }

        QueryDefinition query = QueryCatalog.queries().stream()
            .filter(candidate -> candidate.name().equals(queryName))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown query: " + queryName));
        return query.template().replace("{table}", table);
    }
}

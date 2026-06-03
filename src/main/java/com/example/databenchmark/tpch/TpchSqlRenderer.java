package com.example.databenchmark.tpch;

import java.util.regex.Pattern;

public final class TpchSqlRenderer {
    private static final Pattern DATE_LITERAL = Pattern.compile("DATE '([^']+)'");

    private TpchSqlRenderer() {}

    public static String render(String queryName, String engineKey) {
        String sql = TpchQueryCatalog.query(queryName).template();
        for (TpchTable table : TpchSchema.tables()) {
            sql = sql.replace("{" + table.name() + "}", TpchSchema.tableName(table.name(), engineKey));
        }
        if (engineKey.startsWith("starrocks")) {
            sql = DATE_LITERAL.matcher(sql).replaceAll("CAST('$1' AS DATE)");
        }
        return sql;
    }
}

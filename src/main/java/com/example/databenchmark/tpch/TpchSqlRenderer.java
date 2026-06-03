package com.example.databenchmark.tpch;

import java.util.regex.Pattern;

public final class TpchSqlRenderer {
    private static final Pattern DATE_LITERAL = Pattern.compile("DATE '([^']+)'");
    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\{[^}]+}");

    private TpchSqlRenderer() {}

    public static String render(String queryName, String engineKey) {
        String template = TpchQueryCatalog.query(queryName).template();
        return renderTemplate(template, engineKey);
    }

    static String renderTemplate(String template, String engineKey) {
        String sql = template;
        for (TpchTable table : TpchSchema.tables()) {
            sql = sql.replace("{" + table.name() + "}", TpchSchema.tableName(table.name(), engineKey));
        }
        if (engineKey.startsWith("starrocks")) {
            sql = DATE_LITERAL.matcher(sql).replaceAll("CAST('$1' AS DATE)");
        }
        var unresolved = PLACEHOLDER_PATTERN.matcher(sql);
        if (unresolved.find()) {
            throw new IllegalArgumentException("Unknown TPC-H table placeholder: " + unresolved.group());
        }
        return sql;
    }
}

package com.example.databenchmark.iceberg.sql;

import java.util.ArrayList;
import java.util.List;

public class SparkSqlScriptBuilder {
    private final List<String> statements = new ArrayList<>();

    public SparkSqlScriptBuilder add(String sql) {
        if (sql != null && !sql.isBlank()) {
            statements.add(sql.strip());
        }
        return this;
    }

    public String build() {
        return statements.isEmpty() ? "" : String.join(";\n", statements) + ";\n";
    }
}

package com.example.databenchmark.engine;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcExecutor {
    public static final String DEFAULT_JDBC_URL = "jdbc:mysql://localhost:9030/?useSSL=false&allowPublicKeyRetrieval=true";
    private static final String JDBC_URL_ENV = "STARROCKS_JDBC_URL";
    public static final String DEFAULT_USER = "root";
    public static final String DEFAULT_PASSWORD = "";

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public JdbcExecutor() {
        this(System.getenv().getOrDefault(JDBC_URL_ENV, DEFAULT_JDBC_URL), DEFAULT_USER, DEFAULT_PASSWORD);
    }

    public JdbcExecutor(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    public String jdbcUrl() {
        return jdbcUrl;
    }

    public String user() {
        return user;
    }

    public String password() {
        return password;
    }

    public JdbcExecutionResult execute(String sql) throws SQLException {
        long started = System.nanoTime();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             Statement statement = connection.createStatement()) {
            boolean hasResultSet = statement.execute(sql);
            long rows = hasResultSet ? countRows(statement.getResultSet()) : Math.max(statement.getUpdateCount(), 0);
            return new JdbcExecutionResult(rows, elapsedSeconds(started));
        }
    }

    public JdbcExecutionResult query(String sql) throws SQLException {
        long started = System.nanoTime();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            return new JdbcExecutionResult(countRows(resultSet), elapsedSeconds(started));
        }
    }

    private static long countRows(ResultSet resultSet) throws SQLException {
        long rows = 0;
        while (resultSet != null && resultSet.next()) {
            rows++;
        }
        return rows;
    }

    private static double elapsedSeconds(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000_000.0;
    }
}

record JdbcExecutionResult(long rows, double durationSeconds) {}

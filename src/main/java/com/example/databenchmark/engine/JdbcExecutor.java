package com.example.databenchmark.engine;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class JdbcExecutor {
    public static final String DEFAULT_JDBC_URL =
        "jdbc:mysql://localhost:9030/?useSSL=false&allowPublicKeyRetrieval=true&allowMultiQueries=true";
    private static final String JDBC_URL_ENV = "STARROCKS_JDBC_URL";
    private static final int MAX_CONNECTION_ATTEMPTS = 10;
    private static final long CONNECTION_RETRY_DELAY_MILLIS = 2_000;
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
        try (Connection connection = openConnection();
             Statement statement = connection.createStatement()) {
            long rows = 0;
            for (String statementSql : splitStatements(sql)) {
                boolean hasResultSet = statement.execute(statementSql);
                rows += hasResultSet ? countRows(statement.getResultSet()) : Math.max(statement.getUpdateCount(), 0);
            }
            return new JdbcExecutionResult(rows, elapsedSeconds(started));
        }
    }

    public JdbcExecutionResult query(String sql) throws SQLException {
        long started = System.nanoTime();
        try (Connection connection = openConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            return new JdbcExecutionResult(countRows(resultSet), elapsedSeconds(started));
        }
    }

    private Connection openConnection() throws SQLException {
        SQLException last = null;
        for (int attempt = 1; attempt <= MAX_CONNECTION_ATTEMPTS; attempt++) {
            try {
                return DriverManager.getConnection(jdbcUrl, user, password);
            } catch (SQLException e) {
                last = e;
                if (!isTransientConnectionFailure(e) || attempt == MAX_CONNECTION_ATTEMPTS) {
                    throw e;
                }
                sleepBeforeRetry();
            }
        }
        throw last;
    }

    private static boolean isTransientConnectionFailure(SQLException e) {
        String state = e.getSQLState();
        String message = e.getMessage() == null ? "" : e.getMessage();
        return (state != null && state.startsWith("08"))
            || message.contains("Communications link failure");
    }

    private static void sleepBeforeRetry() throws SQLException {
        try {
            Thread.sleep(CONNECTION_RETRY_DELAY_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while waiting for JDBC connection retry", e);
        }
    }

    static List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        for (String statement : sql.split(";")) {
            String trimmed = statement.trim();
            if (!trimmed.isEmpty()) {
                statements.add(trimmed);
            }
        }
        return statements;
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

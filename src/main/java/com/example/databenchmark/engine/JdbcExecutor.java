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

    public StarRocksBrokerLoad.LoadState latestLoadState(String database, String label) throws SQLException {
        String escapedLabel = escapeSqlLiteral(label);
        String sql = """
            SELECT STATE, IFNULL(SINK_ROWS, 0) AS SINK_ROWS, IFNULL(ERROR_MSG, '') AS ERROR_MSG
            FROM information_schema.loads
            WHERE LABEL = '%s'
            ORDER BY CREATE_TIME DESC
            LIMIT 1
            """.formatted(escapedLabel);
        try (Connection connection = openConnection();
             Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                if (!resultSet.next()) {
                    return new StarRocksBrokerLoad.LoadState("UNKNOWN", 0L, "");
                }
                return new StarRocksBrokerLoad.LoadState(
                    resultSet.getString("STATE"),
                    resultSet.getLong("SINK_ROWS"),
                    resultSet.getString("ERROR_MSG")
                );
            } catch (SQLException informationSchemaFailure) {
                return latestLoadStateFromShowLoad(statement, database, escapedLabel);
            }
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

    private static StarRocksBrokerLoad.LoadState latestLoadStateFromShowLoad(
        Statement statement,
        String database,
        String escapedLabel
    ) throws SQLException {
        String escapedDatabase = escapeSqlIdentifier(database);
        String sql = "SHOW LOAD FROM " + escapedDatabase + " WHERE LABEL = '" + escapedLabel + "'";
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next()) {
                return new StarRocksBrokerLoad.LoadState("UNKNOWN", 0L, "");
            }
            return new StarRocksBrokerLoad.LoadState(
                readString(resultSet, "State", "STATE"),
                readLong(resultSet, "LoadedRows", "SinkRows", "LOADED_ROWS", "SINK_ROWS"),
                readString(resultSet, "ErrorMsg", "ERROR_MSG", "Error")
            );
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

    private static String readString(ResultSet resultSet, String... columns) throws SQLException {
        for (String column : columns) {
            try {
                String value = resultSet.getString(column);
                return value == null ? "" : value;
            } catch (SQLException ignored) {
                // Try the next StarRocks version-specific column name.
            }
        }
        return "";
    }

    private static long readLong(ResultSet resultSet, String... columns) throws SQLException {
        for (String column : columns) {
            try {
                return resultSet.getLong(column);
            } catch (SQLException ignored) {
                // Try the next StarRocks version-specific column name.
            }
        }
        return 0L;
    }

    private static String escapeSqlLiteral(String value) {
        return value.replace("'", "''");
    }

    private static String escapeSqlIdentifier(String value) {
        return value.replaceAll("[^A-Za-z0-9_]", "_");
    }

    private static double elapsedSeconds(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000_000.0;
    }
}

record JdbcExecutionResult(long rows, double durationSeconds) {}

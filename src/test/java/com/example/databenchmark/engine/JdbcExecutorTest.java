package com.example.databenchmark.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JdbcExecutorTest {
    @Test
    void queryRowsReturnsColumnNamesAndValues() throws Exception {
        JdbcExecutor executor = new JdbcExecutor(
            "jdbc:h2:mem:metadata_probe;MODE=MySQL;DB_CLOSE_DELAY=-1",
            "sa",
            ""
        );
        executor.execute("""
            CREATE TABLE metadata_probe (
                TABLE_NAME VARCHAR(64),
                CREATE_TABLE VARCHAR(1024)
            );
            INSERT INTO metadata_probe VALUES (
                'cell_kpi_1min',
                'CREATE TABLE cell_kpi_1min DUPLICATE KEY(event_time, cell_id) DISTRIBUTED BY HASH(cell_id)'
            );
            """);

        List<Map<String, String>> rows = executor.queryRows("SELECT TABLE_NAME, CREATE_TABLE FROM metadata_probe");

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0)).containsEntry("TABLE_NAME", "cell_kpi_1min");
        assertThat(rows.get(0).get("CREATE_TABLE")).contains("DISTRIBUTED BY HASH(cell_id)");
    }
}

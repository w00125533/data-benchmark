package com.example.databenchmark.query;

import com.example.databenchmark.schema.KpiSchema;
import java.util.List;
import java.util.Map;

public final class QueryCatalog {
    private static final List<String> ENGINE_KEYS = List.of(
        "spark_iceberg",
        "starrocks_internal",
        "starrocks_external_iceberg"
    );

    private static final List<BenchmarkEngine> ENGINES = buildEngines();

    private static final List<QueryDefinition> QUERIES = List.of(
        new QueryDefinition(
            "single_cell_day_trend",
            """
            SELECT event_time, cell_id, rsrp_avg, sinr_avg, prb_dl_util, active_users
            FROM {table}
            WHERE cell_id = 'cell-000001'
              AND event_time >= TIMESTAMP '2026-01-01 00:00:00'
              AND event_time < TIMESTAMP '2026-01-02 00:00:00'
            ORDER BY event_time
            """
        ),
        new QueryDefinition(
            "single_cell_week_trend",
            """
            SELECT DATE_TRUNC('day', event_time) AS event_day,
                   cell_id,
                   AVG(rsrp_avg) AS rsrp_avg,
                   AVG(sinr_avg) AS sinr_avg,
                   AVG(load_score) AS load_score
            FROM {table}
            WHERE cell_id = 'cell-000001'
              AND event_time >= TIMESTAMP '2026-01-01 00:00:00'
              AND event_time < TIMESTAMP '2026-01-08 00:00:00'
            GROUP BY DATE_TRUNC('day', event_time), cell_id
            ORDER BY event_day
            """
        ),
        new QueryDefinition(
            "city_vendor_band_rat_minute_agg",
            """
            SELECT DATE_TRUNC('minute', event_time) AS event_minute,
                   city,
                   vendor,
                   band,
                   rat,
                   AVG(prb_dl_util) AS prb_dl_util,
                   SUM(active_users) AS active_users
            FROM {table}
            WHERE city = 'Hangzhou'
            GROUP BY DATE_TRUNC('minute', event_time), city, vendor, band, rat
            ORDER BY event_minute, vendor, band, rat
            """
        ),
        new QueryDefinition(
            "topn_high_load_cells",
            """
            SELECT event_time, cell_id, city, vendor, rat, band, load_score, active_users
            FROM {table}
            WHERE event_time >= TIMESTAMP '2026-01-01 00:00:00'
              AND event_time < TIMESTAMP '2026-01-02 00:00:00'
            ORDER BY load_score DESC
            LIMIT 100
            """
        ),
        new QueryDefinition(
            "weak_coverage_cells",
            """
            SELECT cell_id,
                   city,
                   district,
                   AVG(rsrp_avg) AS rsrp_avg,
                   AVG(sinr_avg) AS sinr_avg,
                   COUNT(*) AS samples
            FROM {table}
            WHERE rsrp_avg < -110
               OR sinr_avg < 0
            GROUP BY cell_id, city, district
            ORDER BY rsrp_avg ASC, sinr_avg ASC
            LIMIT 100
            """
        ),
        new QueryDefinition(
            "adjacent_window_kpi_spike",
            """
            SELECT current_window.cell_id,
                   current_window.load_score AS current_load_score,
                   previous_window.load_score AS previous_load_score,
                   current_window.load_score - previous_window.load_score AS load_score_delta
            FROM (
                SELECT cell_id, AVG(load_score) AS load_score
                FROM {table}
                WHERE event_time >= TIMESTAMP '2026-01-01 12:00:00'
                  AND event_time < TIMESTAMP '2026-01-01 13:00:00'
                GROUP BY cell_id
            ) current_window
            JOIN (
                SELECT cell_id, AVG(load_score) AS load_score
                FROM {table}
                WHERE event_time >= TIMESTAMP '2026-01-01 11:00:00'
                  AND event_time < TIMESTAMP '2026-01-01 12:00:00'
                GROUP BY cell_id
            ) previous_window
              ON current_window.cell_id = previous_window.cell_id
            ORDER BY load_score_delta DESC
            LIMIT 100
            """
        ),
        new QueryDefinition(
            "recent_hot_cells",
            """
            SELECT cell_id,
                   city,
                   MAX(event_time) AS latest_event_time,
                   AVG(load_score) AS load_score,
                   AVG(prb_dl_util) AS prb_dl_util
            FROM {table}
            WHERE event_time >= TIMESTAMP '2026-01-01 23:00:00'
            GROUP BY cell_id, city
            HAVING AVG(load_score) >= 80
            ORDER BY load_score DESC
            LIMIT 100
            """
        ),
        new QueryDefinition(
            "wide_filter_group_by",
            """
            SELECT province,
                   city,
                   district,
                   vendor,
                   rat,
                   band,
                   COUNT(*) AS samples,
                   AVG(active_users) AS active_users,
                   AVG(dl_throughput_mbps) AS dl_throughput_mbps
            FROM {table}
            WHERE province = 'Zhejiang'
              AND vendor IN ('Huawei', 'ZTE')
              AND rat IN ('4G', '5G')
              AND band IN ('n78', 'B3')
              AND prb_dl_util BETWEEN 40 AND 95
            GROUP BY province, city, district, vendor, rat, band
            ORDER BY samples DESC
            """
        ),
        new QueryDefinition(
            "date_partition_pruning",
            """
            SELECT DATE_TRUNC('hour', event_time) AS event_hour,
                   COUNT(*) AS samples,
                   AVG(load_score) AS load_score
            FROM {table}
            WHERE event_time >= TIMESTAMP '2026-01-01 00:00:00'
              AND event_time < TIMESTAMP '2026-01-02 00:00:00'
            GROUP BY DATE_TRUNC('hour', event_time)
            ORDER BY event_hour
            """
        ),
        new QueryDefinition(
            "large_cell_id_filter",
            """
            SELECT cell_id,
                   event_time,
                   rsrp_avg,
                   sinr_avg,
                   active_users,
                   load_score
            FROM {table}
            WHERE cell_id IN (
                'cell-000001', 'cell-000010', 'cell-000100', 'cell-001000',
                'cell-002000', 'cell-003000', 'cell-004000', 'cell-005000'
            )
            ORDER BY cell_id, event_time
            """
        )
    );

    private QueryCatalog() {}

    public static List<BenchmarkEngine> engines() {
        return ENGINES;
    }

    public static List<QueryDefinition> queries() {
        return QUERIES;
    }

    public static String render(String queryName, BenchmarkEngine engine) {
        return QUERIES.stream()
            .filter(query -> query.name().equals(queryName))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown query: " + queryName))
            .template()
            .replace("{table}", engine.tableName());
    }

    private static List<BenchmarkEngine> buildEngines() {
        Map<String, String> tableShapes = KpiSchema.tableShapes();
        return ENGINE_KEYS.stream()
            .map(key -> new BenchmarkEngine(key, key, tableShapes.get(key)))
            .toList();
    }
}

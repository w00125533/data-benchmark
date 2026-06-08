package com.example.databenchmark.schema;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Collections;

public final class KpiTableNaming {
    public static final String STARROCKS_INTERNAL_ROUTE = "starrocks_internal";
    private static final String STARROCKS_INTERNAL_DATABASE = "sr_internal";
    private static final String DEFAULT_INTERNAL_TABLE = "cell_kpi_1min";
    private static final String SMOKE_INTERNAL_TABLE = "cell_kpi_1min_smoke";
    private static final String FULL_INTERNAL_TABLE = "cell_kpi_1min_1b";
    private static final long SMOKE_ROW_THRESHOLD = 1_000_000L;
    private static final long FULL_ROW_THRESHOLD = 1_000_000_000L;

    private KpiTableNaming() {}

    public static String starRocksInternalTableName(String profile) {
        String normalized = profile == null ? "" : profile.toLowerCase(Locale.ROOT);
        if (normalized.contains("smoke")) {
            return SMOKE_INTERNAL_TABLE;
        }
        if (normalized.contains("1b") || normalized.contains("full")) {
            return FULL_INTERNAL_TABLE;
        }
        return DEFAULT_INTERNAL_TABLE;
    }

    public static String starRocksInternalTableIdentifier(String profile) {
        return STARROCKS_INTERNAL_DATABASE + "." + starRocksInternalTableName(profile);
    }

    public static String starRocksInternalTableIdentifierForRows(long rows) {
        if (rows > 0 && rows <= SMOKE_ROW_THRESHOLD) {
            return STARROCKS_INTERNAL_DATABASE + "." + SMOKE_INTERNAL_TABLE;
        }
        if (rows >= FULL_ROW_THRESHOLD) {
            return STARROCKS_INTERNAL_DATABASE + "." + FULL_INTERNAL_TABLE;
        }
        return STARROCKS_INTERNAL_DATABASE + "." + DEFAULT_INTERNAL_TABLE;
    }

    public static Map<String, String> tableShapesForProfile(String profile) {
        Map<String, String> tableShapes = new LinkedHashMap<>(KpiSchema.tableShapes());
        tableShapes.put(STARROCKS_INTERNAL_ROUTE, starRocksInternalTableIdentifier(profile));
        return Collections.unmodifiableMap(tableShapes);
    }

    public static Map<String, String> tableShapesForRows(long rows) {
        Map<String, String> tableShapes = new LinkedHashMap<>(KpiSchema.tableShapes());
        tableShapes.put(STARROCKS_INTERNAL_ROUTE, starRocksInternalTableIdentifierForRows(rows));
        return Collections.unmodifiableMap(tableShapes);
    }
}

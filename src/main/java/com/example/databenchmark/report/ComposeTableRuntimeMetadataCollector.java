package com.example.databenchmark.report;

import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.engine.CommandRunner;
import com.example.databenchmark.engine.JdbcExecutor;
import com.example.databenchmark.runner.BenchmarkRoute;
import com.example.databenchmark.runner.InfraComposeTarget;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ComposeTableRuntimeMetadataCollector implements TableRuntimeMetadataCollector {
    private static final int RAW_DETAILS_LIMIT = 20_000;
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(10);
    private static final Path WORKING_DIRECTORY = Path.of(".");
    private static final String SPARK_SQL = "/opt/spark/bin/spark-sql";
    private static final String ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1";
    private static final String IVY_CACHE_CONF = "spark.jars.ivy=/tmp/.ivy2";
    private static final String ICEBERG_VECTORIZATION_CONF = "spark.sql.iceberg.vectorization.enabled=false";
    private static final List<String> LARGE_DATA_SPARK_SQL_OPTIONS = List.of(
        "--master", "local[6]",
        "--driver-memory", "10g",
        "--conf", "spark.driver.maxResultSize=2g",
        "--conf", "spark.sql.shuffle.partitions=512",
        "--conf", "spark.default.parallelism=512",
        "--conf", "spark.sql.files.maxPartitionBytes=67108864",
        "--conf", "spark.sql.parquet.enableVectorizedReader=false",
        "--conf", "spark.sql.parquet.columnarReaderBatchSize=1024",
        "--conf", "spark.sql.adaptive.enabled=true"
    );
    private static final Pattern HASH_DISTRIBUTION = Pattern.compile(
        "DISTRIBUTED\\s+BY\\s+(HASH\\s*\\([^)]*\\))",
        Pattern.CASE_INSENSITIVE
    );
    private static final Pattern LOCATION = Pattern.compile(
        "(?:Location|LOCATION)\\s*(?::|=)?\\s*([^\\r\\n,;]+)",
        Pattern.CASE_INSENSITIVE
    );
    private static final Pattern QUOTED_LOCATION_PROPERTY = Pattern.compile(
        "\"location\"\\s*=\\s*\"([^\"]+)\"",
        Pattern.CASE_INSENSITIVE
    );
    private static final Pattern LOCATION_HDFS_URL = Pattern.compile(
        "(?:Location|LOCATION)\\s*(?::|=)?\\s*\\|?\\s*(hdfs://[^\\s|,;]+)",
        Pattern.CASE_INSENSITIVE
    );
    private static final Pattern CURRENT_SNAPSHOT_ID = Pattern.compile("current-snapshot-id=([^,\\]\\s]+)");
    private static final Pattern NUM_FILES = Pattern.compile("(?im)(?:^|[|\\s])numFiles\\s*(?:[:=|\\s]+)\\s*(\\d+)");
    private static final Pattern TOTAL_SIZE = Pattern.compile("(?im)(?:^|[|\\s])totalSize\\s*(?:[:=|\\s]+)\\s*(\\d+)");
    private static final Pattern STATISTICS_BYTES = Pattern.compile(
        "(?im)(?:Statistics|STATISTICS)\\s*(?::|=)?\\s*(\\d+)\\s+bytes"
    );
    private static final Pattern STARROCKS_DATA_SIZE = Pattern.compile(
        "(?im)DataSize\\s*:\\s*([0-9]+(?:\\.[0-9]+)?)\\s*([KMGT]?B)"
    );

    private final CommandRunner commandRunner;
    private final JdbcExecutor starRocksJdbc;
    private final FallbackTableRuntimeMetadataCollector fallback;

    public ComposeTableRuntimeMetadataCollector(
        CommandRunner commandRunner,
        JdbcExecutor starRocksJdbc,
        FallbackTableRuntimeMetadataCollector fallback
    ) {
        this.commandRunner = commandRunner;
        this.starRocksJdbc = starRocksJdbc;
        this.fallback = fallback;
    }

    public static ComposeTableRuntimeMetadataCollector fromDefaults() {
        return new ComposeTableRuntimeMetadataCollector(
            new CommandRunner(),
            new JdbcExecutor(),
            new FallbackTableRuntimeMetadataCollector()
        );
    }

    @Override
    public List<BenchmarkReport.TableRuntimeInfo> collectKpi(
        Map<BenchmarkRoute, String> routeFailures,
        long rows,
        long bytes
    ) {
        return fallback.collectKpi(routeFailures, rows, bytes).stream()
            .map(this::collectKpiRoute)
            .toList();
    }

    @Override
    public List<BenchmarkReport.TableRuntimeInfo> collectTpch(long rows, long bytes) {
        return fallback.collectTpch(rows, bytes).stream()
            .map(this::collectTpchTable)
            .toList();
    }

    private BenchmarkReport.TableRuntimeInfo collectKpiRoute(BenchmarkReport.TableRuntimeInfo base) {
        if (!base.success()) {
            return base;
        }
        return switch (base.route()) {
            case "spark_native_parquet", "spark_iceberg" -> collectSpark(base);
            case "starrocks_internal", "starrocks_external_iceberg" -> collectStarRocks(base);
            case "hive_hdfs_parquet" -> collectHive(base);
            default -> base;
        };
    }

    private BenchmarkReport.TableRuntimeInfo collectTpchTable(BenchmarkReport.TableRuntimeInfo base) {
        return switch (base.route()) {
            case "spark_iceberg" -> collectSpark(base);
            case "starrocks_internal", "starrocks_external_iceberg" -> collectStarRocks(base);
            default -> base;
        };
    }

    private BenchmarkReport.TableRuntimeInfo collectSpark(BenchmarkReport.TableRuntimeInfo base) {
        String sql = "DESCRIBE EXTENDED " + base.tableIdentifier();
        CommandResult result;
        try {
            result = commandRunner.run(sparkSqlCommand(sql), WORKING_DIRECTORY, DEFAULT_TIMEOUT);
        } catch (Exception e) {
            return failed(base, e.getMessage());
        }
        if (result.exitCode() != 0) {
            return failed(base, commandError(result));
        }
        if (result.stdout().isBlank()) {
            return failed(base, "metadata command returned empty stdout");
        }
        String rawDetails = truncate(result.stdout());
        String location = firstNonBlank(parseLocation(rawDetails), base.location());
        StorageStats stats = storageStats(location, rawDetails, base.fileCount(), base.totalBytes());
        return enrich(
            base,
            location,
            base.partitioning(),
            base.bucketingOrDistribution(),
            base.indexes(),
            firstNonBlank(parseSnapshot(rawDetails), base.snapshotOrVersion()),
            stats.fileCount(),
            stats.tabletCount(),
            stats.rowsetCount(),
            stats.segmentCount(),
            stats.totalBytes(),
            rawDetails
        );
    }

    private BenchmarkReport.TableRuntimeInfo collectHive(BenchmarkReport.TableRuntimeInfo base) {
        String sql = "DESCRIBE FORMATTED " + base.tableIdentifier();
        CommandResult result;
        try {
            result = commandRunner.run(hiveCommand(sql), WORKING_DIRECTORY, DEFAULT_TIMEOUT);
        } catch (Exception e) {
            return failed(base, e.getMessage());
        }
        if (result.exitCode() != 0) {
            return failed(base, commandError(result));
        }
        if (result.stdout().isBlank()) {
            return failed(base, "metadata command returned empty stdout");
        }
        String rawDetails = truncate(result.stdout());
        String location = firstNonBlank(parseLocation(rawDetails), base.location());
        StorageStats stats = storageStats(location, rawDetails, base.fileCount(), base.totalBytes());
        return enrich(
            base,
            location,
            base.partitioning(),
            base.bucketingOrDistribution(),
            base.indexes(),
            base.snapshotOrVersion(),
            stats.fileCount(),
            stats.tabletCount(),
            stats.rowsetCount(),
            stats.segmentCount(),
            stats.totalBytes(),
            rawDetails
        );
    }

    private BenchmarkReport.TableRuntimeInfo collectStarRocks(BenchmarkReport.TableRuntimeInfo base) {
        try {
            List<Map<String, String>> createRows = starRocksJdbc.queryRows("SHOW CREATE TABLE " + base.tableIdentifier());
            String createTable = showCreateText(createRows);
            if (createTable.isBlank()) {
                return failed(base, "SHOW CREATE TABLE returned no create text");
            }
            boolean internalStarRocks = "starrocks_internal".equals(base.route());
            OptionalRows partitionRows = internalStarRocks
                ? optionalRows("SHOW PARTITIONS FROM " + base.tableIdentifier())
                : new OptionalRows("", "");
            OptionalRows indexRows = optionalRows("SHOW INDEX FROM " + base.tableIdentifier());
            OptionalRows tabletSummary = internalStarRocks ? starRocksTabletSummary(base.tableIdentifier()) : new OptionalRows("", "");
            String partitions = partitionRows.text();
            String indexes = indexRows.text();
            String tabletSummaryText = tabletSummary.text();
            String location = firstNonBlank(parseLocation(createTable), base.location());
            StorageStats stats = internalStarRocks
                ? starRocksStorageStats(tabletSummaryText, partitions, base)
                : storageStats(location, createTable, base.fileCount(), base.totalBytes());
            String rawDetails = truncate(sections(
                "SHOW CREATE TABLE",
                rowsToText(createRows),
                "SHOW PARTITIONS",
                firstNonBlank(partitionRows.error(), partitions),
                "SHOW TABLET SUMMARY",
                firstNonBlank(tabletSummary.error(), tabletSummaryText),
                "SHOW INDEX",
                firstNonBlank(indexRows.error(), indexes)
            ));
            return enrich(
                base,
                location,
                firstNonBlank(parsePartitioning(createTable), base.partitioning()),
                firstNonBlank(parseDistribution(createTable), base.bucketingOrDistribution()),
                firstNonBlank(indexes, base.indexes()),
                base.snapshotOrVersion(),
                stats.fileCount(),
                stats.tabletCount(),
                stats.rowsetCount(),
                stats.segmentCount(),
                stats.totalBytes(),
                rawDetails
            );
        } catch (SQLException e) {
            return failed(base, e.getMessage());
        }
    }

    private OptionalRows optionalRows(String sql) {
        try {
            return new OptionalRows(rowsToText(starRocksJdbc.queryRows(sql)), "");
        } catch (SQLException e) {
            return new OptionalRows("", sql.substring(0, sql.indexOf(" FROM")) + " failed: " + e.getMessage());
        }
    }

    private OptionalRows starRocksTabletSummary(String tableIdentifier) {
        try {
            List<Map<String, String>> tabletRows = starRocksJdbc.queryRows("SHOW TABLET FROM " + tableIdentifier);
            List<String> tabletIds = tabletIds(tabletRows);
            if (tabletIds.isEmpty()) {
                return new OptionalRows("tablet_count: 0", "");
            }
            String sql = "SELECT COUNT(*) AS tablet_count, SUM(NUM_ROWSET) AS rowset_count, "
                + "SUM(NUM_SEGMENT) AS segment_count, SUM(NUM_ROW) AS row_count, SUM(DATA_SIZE) AS data_bytes "
                + "FROM information_schema.be_tablets WHERE TABLET_ID IN (" + String.join(",", tabletIds) + ")";
            List<Map<String, String>> summaryRows = starRocksJdbc.queryRows(sql);
            return new OptionalRows(rowsToText(summaryRows), "");
        } catch (SQLException e) {
            return new OptionalRows("", "SHOW TABLET SUMMARY failed: " + e.getMessage());
        }
    }

    private static List<String> tabletIds(List<Map<String, String>> rows) {
        List<String> ids = new ArrayList<>();
        for (Map<String, String> row : rows) {
            String value = firstNonBlank(rowValue(row, "TabletId"), rowValue(row, "TABLET_ID"));
            if (value.matches("\\d+")) {
                ids.add(value);
            }
        }
        return ids;
    }

    private List<String> sparkSqlCommand(String sql) {
        List<String> command = new ArrayList<>(InfraComposeTarget.fromEnvironment(System.getenv()).composeCommand(
            "exec", "-T", "spark", SPARK_SQL
        ));
        command.addAll(LARGE_DATA_SPARK_SQL_OPTIONS);
        command.addAll(List.of(
            "--conf", IVY_CACHE_CONF,
            "--packages", ICEBERG_RUNTIME,
            "--conf", "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog",
            "--conf", "spark.sql.catalog.iceberg_catalog.type=hive",
            "--conf", "spark.sql.catalog.iceberg_catalog.uri=thrift://hive-metastore:9083",
            "--conf", "spark.sql.catalog.iceberg_catalog.warehouse=hdfs://hdfs-namenode:8020/warehouse/iceberg",
            "--conf", ICEBERG_VECTORIZATION_CONF,
            "-e", sql
        ));
        return command;
    }

    private List<String> hiveCommand(String sql) {
        return InfraComposeTarget.fromEnvironment(System.getenv()).composeCommand(
            "exec", "-T", "hive-server",
            "beeline", "-u", "jdbc:hive2://hive-server:10000/default", "-e", sql
        );
    }

    private static BenchmarkReport.TableRuntimeInfo enrich(
        BenchmarkReport.TableRuntimeInfo base,
        String location,
        String partitioning,
        String bucketingOrDistribution,
            String indexes,
            String snapshotOrVersion,
            long fileCount,
            long tabletCount,
            long rowsetCount,
            long segmentCount,
            long totalBytes,
            String rawDetails
    ) {
        return new BenchmarkReport.TableRuntimeInfo(
            base.route(),
            base.displayName(),
            base.tableShape(),
            base.tableIdentifier(),
            base.storageType(),
            location,
            base.format(),
            base.columns(),
            partitioning,
            bucketingOrDistribution,
            indexes,
            snapshotOrVersion,
            fileCount,
            tabletCount,
            rowsetCount,
            segmentCount,
            totalBytes,
            rawDetails,
            true,
            ""
        );
    }

    private static BenchmarkReport.TableRuntimeInfo failed(BenchmarkReport.TableRuntimeInfo base, String error) {
        return new BenchmarkReport.TableRuntimeInfo(
            base.route(),
            base.displayName(),
            base.tableShape(),
            base.tableIdentifier(),
            base.storageType(),
            base.location(),
            base.format(),
            base.columns(),
            base.partitioning(),
            base.bucketingOrDistribution(),
            base.indexes(),
            base.snapshotOrVersion(),
            base.fileCount(),
            base.tabletCount(),
            base.rowsetCount(),
            base.segmentCount(),
            base.totalBytes(),
            base.rawDetails(),
            false,
            error == null ? "" : error
        );
    }

    private static String showCreateText(List<Map<String, String>> rows) {
        for (Map<String, String> row : rows) {
            for (Map.Entry<String, String> entry : row.entrySet()) {
                String key = entry.getKey() == null ? "" : entry.getKey().toLowerCase(Locale.ROOT);
                if (key.contains("create") && entry.getValue() != null) {
                    return entry.getValue();
                }
            }
        }
        return "";
    }

    private static String parseDistribution(String createTable) {
        Matcher matcher = HASH_DISTRIBUTION.matcher(createTable);
        return matcher.find() ? matcher.group(1).replaceAll("\\s+", " ") : "";
    }

    private static String parsePartitioning(String createTable) {
        String upper = createTable.toUpperCase(Locale.ROOT);
        int index = upper.indexOf("PARTITION BY");
        if (index < 0) {
            return "";
        }
        int end = upper.indexOf("DISTRIBUTED BY", index);
        if (end < 0) {
            end = upper.indexOf("ORDER BY", index);
        }
        if (end < 0) {
            end = createTable.length();
        }
        return createTable.substring(index, end).trim();
    }

    private static String parseLocation(String rawDetails) {
        Matcher quotedProperty = QUOTED_LOCATION_PROPERTY.matcher(rawDetails);
        if (quotedProperty.find()) {
            return quotedProperty.group(1).trim();
        }
        Matcher hdfsUrl = LOCATION_HDFS_URL.matcher(rawDetails);
        if (hdfsUrl.find()) {
            return hdfsUrl.group(1).trim();
        }
        Matcher matcher = LOCATION.matcher(rawDetails);
        return matcher.find() ? matcher.group(1).trim() : "";
    }

    private static String parseSnapshot(String rawDetails) {
        Matcher matcher = CURRENT_SNAPSHOT_ID.matcher(rawDetails);
        return matcher.find() ? "snapshot=" + matcher.group(1).trim() : "";
    }

    private StorageStats storageStats(String location, String rawDetails, long fallbackFileCount, long fallbackTotalBytes) {
        StorageStats hdfsStats = hdfsStats(location);
        if (hdfsStats != null && (hdfsStats.fileCount() > 0 || hdfsStats.totalBytes() > 0)) {
            return hdfsStats;
        }
        return new StorageStats(
            firstPositive(parseLong(NUM_FILES, rawDetails), fallbackFileCount),
            0L,
            0L,
            0L,
            firstPositive(parseLong(TOTAL_SIZE, rawDetails), firstPositive(parseLong(STATISTICS_BYTES, rawDetails), fallbackTotalBytes))
        );
    }

    private StorageStats starRocksStorageStats(
        String tabletSummary,
        String partitions,
        BenchmarkReport.TableRuntimeInfo base
    ) {
        long segmentCount = parseNamedLong(tabletSummary, "segment_count");
        long totalBytes = firstPositive(
            parseNamedLong(tabletSummary, "data_bytes"),
            firstPositive(parseStarRocksDataSize(partitions), base.totalBytes())
        );
        return new StorageStats(
            firstPositive(segmentCount, base.fileCount()),
            firstPositive(parseNamedLong(tabletSummary, "tablet_count"), base.tabletCount()),
            firstPositive(parseNamedLong(tabletSummary, "rowset_count"), base.rowsetCount()),
            firstPositive(segmentCount, base.segmentCount()),
            totalBytes
        );
    }

    private StorageStats hdfsStats(String location) {
        if (location == null || !location.startsWith("hdfs://")) {
            return null;
        }
        CommandResult result;
        try {
            result = commandRunner.run(hdfsCountCommand(location), WORKING_DIRECTORY, DEFAULT_TIMEOUT);
        } catch (Exception e) {
            return null;
        }
        if (result.exitCode() != 0 || result.stdout().isBlank()) {
            return null;
        }
        return parseHdfsCount(result.stdout());
    }

    private List<String> hdfsCountCommand(String location) {
        return InfraComposeTarget.fromEnvironment(System.getenv()).composeCommand(
            "exec", "-T", "namenode",
            "/opt/hadoop-3.2.1/bin/hdfs", "dfs", "-count", location
        );
    }

    private static StorageStats parseHdfsCount(String stdout) {
        String[] lines = stdout.split("\\R");
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isBlank()) {
                continue;
            }
            String[] parts = trimmed.split("\\s+");
            if (parts.length >= 4 && isLong(parts[0]) && isLong(parts[1]) && isLong(parts[2])) {
                return new StorageStats(Long.parseLong(parts[1]), 0L, 0L, 0L, Long.parseLong(parts[2]));
            }
        }
        return null;
    }

    private static long parseLong(Pattern pattern, String text) {
        if (text == null || text.isBlank()) {
            return 0L;
        }
        Matcher matcher = pattern.matcher(text);
        return matcher.find() ? Long.parseLong(matcher.group(1)) : 0L;
    }

    private static long parseStarRocksDataSize(String text) {
        if (text == null || text.isBlank()) {
            return 0L;
        }
        Matcher matcher = STARROCKS_DATA_SIZE.matcher(text);
        if (!matcher.find()) {
            return 0L;
        }
        double value = Double.parseDouble(matcher.group(1));
        return Math.round(value * unitMultiplier(matcher.group(2)));
    }

    private static long parseNamedLong(String text, String name) {
        if (text == null || text.isBlank()) {
            return 0L;
        }
        Pattern pattern = Pattern.compile("(?im)" + Pattern.quote(name) + "\\s*:\\s*(\\d+)");
        return parseLong(pattern, text);
    }

    private static long unitMultiplier(String unit) {
        return switch (unit.toUpperCase(Locale.ROOT)) {
            case "KB" -> 1024L;
            case "MB" -> 1024L * 1024L;
            case "GB" -> 1024L * 1024L * 1024L;
            case "TB" -> 1024L * 1024L * 1024L * 1024L;
            default -> 1L;
        };
    }

    private static boolean isLong(String value) {
        try {
            Long.parseLong(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static String rowsToText(List<Map<String, String>> rows) {
        if (rows.isEmpty()) {
            return "";
        }
        StringBuilder text = new StringBuilder();
        for (Map<String, String> row : rows) {
            for (Map.Entry<String, String> entry : row.entrySet()) {
                text.append(entry.getKey()).append(": ").append(entry.getValue()).append(System.lineSeparator());
            }
            text.append(System.lineSeparator());
        }
        return text.toString().trim();
    }

    private static String rowValue(Map<String, String> row, String key) {
        for (Map.Entry<String, String> entry : row.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(key)) {
                return entry.getValue() == null ? "" : entry.getValue().trim();
            }
        }
        return "";
    }

    private static String sections(String... parts) {
        StringBuilder text = new StringBuilder();
        for (int index = 0; index < parts.length; index += 2) {
            String body = parts[index + 1];
            if (body == null || body.isBlank()) {
                continue;
            }
            if (!text.isEmpty()) {
                text.append(System.lineSeparator()).append(System.lineSeparator());
            }
            text.append(parts[index]).append(System.lineSeparator()).append(body);
        }
        return text.toString();
    }

    private static String truncate(String rawDetails) {
        if (rawDetails == null) {
            return "";
        }
        return rawDetails.length() > RAW_DETAILS_LIMIT ? rawDetails.substring(0, RAW_DETAILS_LIMIT) : rawDetails;
    }

    private static String commandError(CommandResult result) {
        return result.stderr().isBlank() ? result.stdout() : result.stderr();
    }

    private static String firstNonBlank(String first, String fallback) {
        return first == null || first.isBlank() ? fallback : first;
    }

    private static long firstPositive(long first, long fallback) {
        return first > 0 ? first : fallback;
    }

    private record OptionalRows(String text, String error) {}

    private record StorageStats(
        long fileCount,
        long tabletCount,
        long rowsetCount,
        long segmentCount,
        long totalBytes
    ) {}
}

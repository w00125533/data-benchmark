package com.example.databenchmark.generator;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

import com.example.databenchmark.hadoop.HadoopLocalConfiguration;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkKpiGenerationJob {
    public DatasetResult run(KpiGenerationConfig generation) throws IOException {
        Path outputPath = generation.outputPath();
        HadoopLocalConfiguration.create();
        SparkSession spark = SparkSession.builder()
            .appName("data-benchmark-kpi-generation")
            .master(generation.master())
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.hadoop.fs.file.impl", HadoopLocalConfiguration.NoPermissionRawLocalFileSystem.class.getName())
            .config("spark.hadoop.fs.file.impl.disable.cache", "true")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate();

        try {
            Dataset<Row> rows = buildRows(spark, generation);
            rows.write()
                .mode(generation.outputMode())
                .partitionBy("event_date")
                .parquet(outputPath.toString());
        } finally {
            spark.stop();
        }

        List<Path> parquetFiles = parquetFiles(outputPath);
        if (parquetFiles.isEmpty()) {
            throw new IOException("Spark KPI generation wrote no Parquet files under " + outputPath);
        }
        long bytesWritten = 0L;
        for (Path file : parquetFiles) {
            bytesWritten += Files.size(file);
        }
        return new DatasetResult(outputPath, parquetFiles, generation.targetRows(), bytesWritten);
    }

    private Dataset<Row> buildRows(SparkSession spark, KpiGenerationConfig generation) {
        long seed = generation.seed();
        int cells = generation.cells();
        long startEpochMillis = generation.startTime().toInstant(ZoneOffset.UTC).toEpochMilli();

        Dataset<Row> base = spark.range(0L, generation.targetRows(), 1L, generation.partitions())
            .withColumn("cell", expr("CAST(id % " + cells + " AS INT)"))
            .withColumn("minute", expr("CAST(floor(id / " + cells + ") AS BIGINT)"))
            .withColumn("event_time", expr(
                "timestamp_millis(" + startEpochMillis + "L + minute * 60000L)"
            ))
            .withColumn("event_date", expr("date_format(event_time, 'yyyy-MM-dd')"))
            .withColumn("active_users", intMetric(seed, 1, 1, 600))
            .withColumn("rrc_users", expr("CAST(greatest(1, active_users + " + signedMetric(seed, 2, 80, -20) + ") AS INT)"))
            .withColumn("connected_users_peak", expr(
                "CAST(greatest(active_users, active_users + " + intMetricSql(seed, 3, 0, 120) + ") AS INT)"
            ))
            .withColumn("rrc_setup_attempts", expr(
                "CAST(connected_users_peak + 50 + " + intMetricSql(seed, 4, 0, 800) + " AS INT)"
            ))
            .withColumn("erab_setup_attempts", expr(
                "CAST(connected_users_peak + 30 + " + intMetricSql(seed, 5, 0, 700) + " AS INT)"
            ))
            .withColumn("handover_attempts", intMetric(seed, 6, 10, 500));

        return base.select(
            col("event_time"),
            expr("format_string('CELL-%06d', cell)").as("cell_id"),
            expr("format_string('province-%02d', CAST(cell % 31 AS INT))").as("province"),
            expr("format_string('city-%03d', CAST(cell % 200 AS INT))").as("city"),
            expr("format_string('district-%03d', CAST(cell % 500 AS INT))").as("district"),
            expr("format_string('grid-%05d', CAST(cell % 10000 AS INT))").as("grid_id"),
            expr("element_at(array('Huawei', 'ZTE', 'Ericsson', 'Nokia', 'Samsung'), CAST(cell % 5 AS INT) + 1)").as("vendor"),
            expr("CASE WHEN cell % 2 = 0 THEN '4G' ELSE '5G' END").as("rat"),
            expr("element_at(array('B3', 'B7', 'B8', 'B20', 'N78', 'N41'), CAST(cell % 6 AS INT) + 1)").as("band"),
            expr("CAST(100 + cell % 32768 AS INT)").as("arfcn"),
            expr("CAST(cell % 1008 AS INT)").as("pci"),
            expr("format_string('SITE-%05d', CAST(floor(cell / 3) AS INT))").as("site_id"),
            doubleMetric(seed, 7, 73.5, 61.0).as("longitude"),
            doubleMetric(seed, 8, 18.0, 36.0).as("latitude"),
            doubleMetric(seed, 9, -115.0, 35.0).as("rsrp_avg"),
            doubleMetric(seed, 10, -125.0, 30.0).as("rsrp_p10"),
            doubleMetric(seed, 11, -18.0, 15.0).as("rsrq_avg"),
            doubleMetric(seed, 12, -5.0, 35.0).as("sinr_avg"),
            doubleMetric(seed, 13, 0.0, 1.0, 6).as("prb_dl_util"),
            doubleMetric(seed, 14, 0.0, 1.0, 6).as("prb_ul_util"),
            col("rrc_users"),
            col("active_users"),
            expr("round(active_users * (0.5 + " + fractionSql(seed, 15) + " * 20.0), 3)").as("dl_traffic_mb"),
            expr("round(active_users * (0.2 + " + fractionSql(seed, 16) + " * 8.0), 3)").as("ul_traffic_mb"),
            doubleMetric(seed, 17, 5.0, 400.0).as("dl_throughput_mbps"),
            doubleMetric(seed, 18, 1.0, 120.0).as("ul_throughput_mbps"),
            doubleMetric(seed, 19, 0.0, 0.02, 6).as("drop_rate"),
            expr("round(CAST(least(handover_attempts, CAST(round(handover_attempts * (0.94 + " + fractionSql(seed, 20)
                + " * 0.055)) AS INT)) AS DOUBLE) / handover_attempts, 6)").as("handover_success_rate"),
            expr("round((least(rrc_setup_attempts, CAST(round(rrc_setup_attempts * (0.97 + " + fractionSql(seed, 21)
                + " * 0.029)) AS INT)) + least(erab_setup_attempts, CAST(round(erab_setup_attempts * (0.965 + "
                + fractionSql(seed, 22) + " * 0.03)) AS INT))) / CAST(rrc_setup_attempts + erab_setup_attempts AS DOUBLE), 6)")
                .as("access_success_rate"),
            doubleMetric(seed, 23, 0.0, 0.01, 6).as("volte_drop_rate"),
            doubleMetric(seed, 24, 8.0, 80.0).as("latency_ms"),
            doubleMetric(seed, 25, 0.97, 0.03, 6).as("availability_rate"),
            intMetric(seed, 26, 0, 8).as("alarm_count"),
            doubleMetric(seed, 27, 0.0, 100.0).as("interference_score"),
            doubleMetric(seed, 28, 5.0, 95.0).as("load_score"),
            doubleMetric(seed, 29, 0.0, 0.03, 6).as("packet_loss_rate"),
            doubleMetric(seed, 30, 3.0, 12.0).as("cqi_avg"),
            doubleMetric(seed, 31, 1.0, 27.0).as("mcs_avg"),
            doubleMetric(seed, 32, 0.0, 16.0).as("ta_avg"),
            doubleMetric(seed, 33, -125.0, 25.0).as("ul_noise_avg"),
            col("connected_users_peak"),
            col("rrc_setup_attempts"),
            expr("CAST(least(rrc_setup_attempts, CAST(round(rrc_setup_attempts * (0.97 + " + fractionSql(seed, 34)
                + " * 0.029)) AS INT)) AS INT)").as("rrc_setup_successes"),
            col("erab_setup_attempts"),
            expr("CAST(least(erab_setup_attempts, CAST(round(erab_setup_attempts * (0.965 + " + fractionSql(seed, 35)
                + " * 0.03)) AS INT)) AS INT)").as("erab_setup_successes"),
            col("handover_attempts"),
            expr("CAST(least(handover_attempts, CAST(round(handover_attempts * (0.94 + " + fractionSql(seed, 36)
                + " * 0.055)) AS INT)) AS INT)").as("handover_successes"),
            doubleMetric(seed, 37, 0.0, 0.08, 6).as("retransmission_rate"),
            doubleMetric(seed, 38, 0.0, 1.0, 6).as("backhaul_util"),
            doubleMetric(seed, 39, 0.5, 18.0).as("energy_kwh"),
            col("event_date")
        );
    }

    private static org.apache.spark.sql.Column intMetric(long seed, int offset, int minInclusive, int bound) {
        return expr(intMetricSql(seed, offset, minInclusive, bound));
    }

    private static String intMetricSql(long seed, int offset, int minInclusive, int bound) {
        return "CAST(" + minInclusive + " + pmod(xxhash64(id, " + (seed + offset) + "L), " + bound + ") AS INT)";
    }

    private static String signedMetric(long seed, int offset, int bound, int delta) {
        return "CAST(" + delta + " + pmod(xxhash64(id, " + (seed + offset) + "L), " + bound + ") AS INT)";
    }

    private static org.apache.spark.sql.Column doubleMetric(long seed, int offset, double min, double scale) {
        return doubleMetric(seed, offset, min, scale, 3);
    }

    private static org.apache.spark.sql.Column doubleMetric(long seed, int offset, double min, double scale, int places) {
        return expr("round(" + min + " + " + fractionSql(seed, offset) + " * " + scale + ", " + places + ")");
    }

    private static String fractionSql(long seed, int offset) {
        return "(pmod(xxhash64(id, " + (seed + offset) + "L), 1000000) / 1000000.0)";
    }

    private static List<Path> parquetFiles(Path outputPath) throws IOException {
        if (!Files.exists(outputPath)) {
            return List.of();
        }
        try (var paths = Files.walk(outputPath)) {
            return paths
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().endsWith(".parquet"))
                .sorted(Comparator.comparing(Path::toString))
                .toList();
        }
    }
}

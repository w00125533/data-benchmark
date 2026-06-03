package com.example.databenchmark.generator;

import com.example.databenchmark.config.BenchmarkConfig;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

final class KpiRecordFactory {
    private static final List<String> VENDORS = List.of("Huawei", "ZTE", "Ericsson", "Nokia", "Samsung");
    private static final List<String> BANDS = List.of("B3", "B7", "B8", "B20", "N78", "N41");

    private final BenchmarkConfig config;
    private final Schema schema;
    private final Random random;
    private final LocalDateTime start;

    KpiRecordFactory(BenchmarkConfig config, Schema schema, LocalDateTime start) {
        this.config = config;
        this.schema = schema;
        this.random = new Random(config.seed());
        this.start = start;
    }

    GenericRecord create(long rowNumber) {
        int cell = (int) (rowNumber % config.dataset().cells());
        long minute = rowNumber / config.dataset().cells();
        LocalDateTime eventTime = start.plusMinutes(minute);
        GenericRecord record = new GenericData.Record(schema);

        int activeUsers = 1 + random.nextInt(600);
        int rrcUsers = Math.max(1, activeUsers + random.nextInt(80) - 20);
        int connectedUsersPeak = Math.max(activeUsers, activeUsers + random.nextInt(120));
        int rrcSetupAttempts = connectedUsersPeak + 50 + random.nextInt(800);
        int rrcSetupSuccesses = boundedSuccessCount(rrcSetupAttempts, 0.97 + random.nextDouble() * 0.029);
        int erabSetupAttempts = connectedUsersPeak + 30 + random.nextInt(700);
        int erabSetupSuccesses = boundedSuccessCount(erabSetupAttempts, 0.965 + random.nextDouble() * 0.03);
        int handoverAttempts = 10 + random.nextInt(500);
        int handoverSuccesses = boundedSuccessCount(handoverAttempts, 0.94 + random.nextDouble() * 0.055);
        double dropRate = round6(random.nextDouble() * 0.02);
        double handoverSuccessRate = ratio(handoverSuccesses, handoverAttempts);
        double accessSuccessRate = ratio(rrcSetupSuccesses + erabSetupSuccesses, rrcSetupAttempts + erabSetupAttempts);
        double loadScore = round3(5.0 + random.nextDouble() * 95.0);

        record.put("event_time", eventTime.toInstant(ZoneOffset.UTC).toEpochMilli());
        record.put("cell_id", cellId(cell));
        record.put("province", String.format(Locale.ROOT, "province-%02d", cell % 31));
        record.put("city", String.format(Locale.ROOT, "city-%03d", cell % 200));
        record.put("district", String.format(Locale.ROOT, "district-%03d", cell % 500));
        record.put("grid_id", String.format(Locale.ROOT, "grid-%05d", cell % 10000));
        record.put("vendor", VENDORS.get(cell % VENDORS.size()));
        record.put("rat", cell % 2 == 0 ? "4G" : "5G");
        record.put("band", BANDS.get(cell % BANDS.size()));
        record.put("arfcn", 100 + cell % 32768);
        record.put("pci", cell % 1008);
        record.put("site_id", String.format(Locale.ROOT, "SITE-%05d", cell / 3));
        record.put("longitude", round6(73.5 + random.nextDouble() * 61.0));
        record.put("latitude", round6(18.0 + random.nextDouble() * 36.0));
        record.put("rsrp_avg", round3(-115.0 + random.nextDouble() * 35.0));
        record.put("rsrp_p10", round3(-125.0 + random.nextDouble() * 30.0));
        record.put("rsrq_avg", round3(-18.0 + random.nextDouble() * 15.0));
        record.put("sinr_avg", round3(-5.0 + random.nextDouble() * 35.0));
        record.put("prb_dl_util", round6(random.nextDouble()));
        record.put("prb_ul_util", round6(random.nextDouble()));
        record.put("rrc_users", rrcUsers);
        record.put("active_users", activeUsers);
        record.put("dl_traffic_mb", round3(activeUsers * (0.5 + random.nextDouble() * 20.0)));
        record.put("ul_traffic_mb", round3(activeUsers * (0.2 + random.nextDouble() * 8.0)));
        record.put("dl_throughput_mbps", round3(5.0 + random.nextDouble() * 400.0));
        record.put("ul_throughput_mbps", round3(1.0 + random.nextDouble() * 120.0));
        record.put("drop_rate", dropRate);
        record.put("handover_success_rate", handoverSuccessRate);
        record.put("access_success_rate", accessSuccessRate);
        record.put("volte_drop_rate", round6(random.nextDouble() * 0.01));
        record.put("latency_ms", round3(8.0 + random.nextDouble() * 80.0));
        record.put("availability_rate", round6(0.97 + random.nextDouble() * 0.03));
        record.put("alarm_count", random.nextInt(8));
        record.put("interference_score", round3(random.nextDouble() * 100.0));
        record.put("load_score", loadScore);
        record.put("packet_loss_rate", round6(random.nextDouble() * 0.03));
        record.put("cqi_avg", round3(3.0 + random.nextDouble() * 12.0));
        record.put("mcs_avg", round3(1.0 + random.nextDouble() * 27.0));
        record.put("ta_avg", round3(random.nextDouble() * 16.0));
        record.put("ul_noise_avg", round3(-125.0 + random.nextDouble() * 25.0));
        record.put("connected_users_peak", connectedUsersPeak);
        record.put("rrc_setup_attempts", rrcSetupAttempts);
        record.put("rrc_setup_successes", rrcSetupSuccesses);
        record.put("erab_setup_attempts", erabSetupAttempts);
        record.put("erab_setup_successes", erabSetupSuccesses);
        record.put("handover_attempts", handoverAttempts);
        record.put("handover_successes", handoverSuccesses);
        record.put("retransmission_rate", round6(random.nextDouble() * 0.08));
        record.put("backhaul_util", round6(random.nextDouble()));
        record.put("energy_kwh", round3(0.5 + random.nextDouble() * 18.0));

        return record;
    }

    private static String cellId(int cell) {
        return String.format(Locale.ROOT, "CELL-%06d", cell);
    }

    private static int boundedSuccessCount(int attempts, double rate) {
        return Math.min(attempts, Math.max(0, (int) Math.round(attempts * rate)));
    }

    private static double ratio(int numerator, int denominator) {
        if (denominator == 0) {
            return 0.0;
        }
        return round6((double) numerator / (double) denominator);
    }

    private static double round3(double value) {
        return Math.round(value * 1_000.0) / 1_000.0;
    }

    private static double round6(double value) {
        return Math.round(value * 1_000_000.0) / 1_000_000.0;
    }
}

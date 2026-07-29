package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import com.example.databenchmark.iceberg.sql.IcebergSqlTemplates;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SmallFileCompactionScenario extends AbstractIcebergValidationScenario {
    @Override
    public String name() {
        return "smallFileCompaction";
    }

    @Override
    public List<IcebergValidationCase> cases(IcebergValidationConfig config) {
        return List.of(
            testCase("small-files-many-snapshots-build", "Build many snapshots with many small files.", Map.of("commits", Integer.toString(config.scale().smallFileCommits()))),
            testCase("small-files-query-degradation", "Measure query degradation from many snapshots and small files.", Map.of("measure", "query")),
            testCase("small-files-data-compaction", "Run rewrite_data_files and compare before/after.", Map.of("maintenance", "rewrite_data_files")),
            testCase("small-files-manifest-rewrite", "Run manifest rewrite and compare planning cost.", Map.of("maintenance", "rewrite_manifests")),
            testCase("small-files-expire-snapshots", "Expire old snapshots after compaction.", Map.of("maintenance", "expire_snapshots"))
        );
    }

    @Override
    public IcebergValidationResult run(IcebergValidationCase testCase, IcebergValidationContext context) {
        String table = IcebergScenarioSupport.tableName(context, name(), testCase.caseId());
        List<String> actions = new ArrayList<>();
        for (int i = 0; i < context.config().scale().smallFileCommits(); i++) {
            long start = context.config().scale().rows() + (long) i * context.config().scale().filesPerCommit();
            long end = start + context.config().scale().filesPerCommit();
            actions.add(IcebergSqlTemplates.insertRange(table, start, end, "small-file-batch-" + i));
        }
        actions.addAll(maintenanceActions(testCase.caseId(), context, table));
        return scriptedSkipped(
            testCase,
            context,
            actions,
            List.of("row count unchanged after maintenance", "file, manifest, snapshot, HDFS, planning, and query metrics collected before and after"),
            Map.of(
                "targetSnapshotCommits", Integer.toString(context.config().scale().smallFileCommits()),
                "filesPerCommit", Integer.toString(context.config().scale().filesPerCommit()),
                "maintenancePlan", maintenancePlan(testCase.caseId())
            ),
            List.of(
                "snapshotCountBefore",
                "snapshotCountAfter",
                "dataFileCountBefore",
                "dataFileCountAfter",
                "manifestCountBefore",
                "manifestCountAfter",
                "metadataJsonCountBefore",
                "metadataJsonCountAfter",
                "hdfsDiskBytesBefore",
                "hdfsDiskBytesAfter",
                "planningSecondsBefore",
                "planningSecondsAfter",
                "querySecondsBefore",
                "querySecondsAfter"
            ),
            "Small-file and compaction SQL was generated, but Spark/HDFS metric collection did not run.",
            "Small-file compaction validation was not executed, so multi-snapshot, data file, manifest, metadata JSON, HDFS disk, planning, query, and compaction before/after metrics are pending collection."
        );
    }

    private static List<String> maintenanceActions(String caseId, IcebergValidationContext context, String table) {
        if (caseId.contains("data-compaction")) {
            return List.of(IcebergSqlTemplates.rewriteDataFiles(context.config().iceberg().catalog(), table));
        }
        if (caseId.contains("manifest")) {
            return List.of(IcebergSqlTemplates.rewriteManifests(context.config().iceberg().catalog(), table));
        }
        if (caseId.contains("expire")) {
            return List.of(IcebergSqlTemplates.expireSnapshots(context.config().iceberg().catalog(), table, "2026-01-02 00:00:00"));
        }
        if (caseId.contains("query")) {
            return List.of("SELECT COUNT(*), SUM(metric_int) FROM " + table);
        }
        return List.of(
            "SELECT COUNT(*) FROM " + table + ".snapshots",
            "SELECT COUNT(*) FROM " + table + ".files",
            "SELECT COUNT(*) FROM " + table + ".manifests"
        );
    }

    private static String maintenancePlan(String caseId) {
        if (caseId.contains("data-compaction")) {
            return "rewrite_data_files";
        }
        if (caseId.contains("manifest")) {
            return "rewrite_manifests";
        }
        if (caseId.contains("expire")) {
            return "expire_snapshots";
        }
        if (caseId.contains("query")) {
            return "query degradation measurement";
        }
        return "multi-snapshot small-file build";
    }
}

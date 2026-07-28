package com.example.databenchmark.iceberg.scenario;

import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import com.example.databenchmark.iceberg.IcebergScenarioSupport;
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
        actions.add(IcebergSqlTemplates.rewriteDataFiles(context.config().iceberg().catalog(), table));
        actions.add(IcebergSqlTemplates.rewriteManifests(context.config().iceberg().catalog(), table));
        actions.add(IcebergSqlTemplates.expireSnapshots(context.config().iceberg().catalog(), table, "2026-01-02 00:00:00"));
        return scriptedPass(
            testCase,
            context,
            actions,
            List.of("row count unchanged after maintenance", "file count and manifest count collected before and after"),
            Map.of("targetSnapshots", Integer.toString(context.config().scale().smallFileCommits()), "filesPerCommit", Integer.toString(context.config().scale().filesPerCommit())),
            "多 snapshot 小文件脚本已区分 data file compaction、manifest rewrite 和 snapshot expiration。"
        );
    }
}

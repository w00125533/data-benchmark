package com.example.databenchmark.iceberg.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.iceberg.IcebergScenarioRegistry;
import com.example.databenchmark.iceberg.IcebergScenarioSupport;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationConfigLoader;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationScenario;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IcebergScenarioRegistryTest {
    @Test
    void registersAllStandaloneIcebergScenariosInStableOrder() {
        assertThat(IcebergScenarioRegistry.defaultScenarios())
            .extracting(IcebergValidationScenario::name)
            .containsExactly(
                "schemaEvolution",
                "erasureCoding",
                "erasureCodingConversion",
                "concurrentWrite",
                "rowLevelMutation",
                "acidTransaction",
                "incrementalPull",
                "timeTravel",
                "smallFileCompaction"
            );
    }

    @Test
    void scenariosExposeExpectedCaseIds() throws Exception {
        IcebergValidationConfig config = config();

        assertThat(new SchemaEvolutionScenario().cases(config)).extracting("caseId").containsExactly(
            "schema-add-drop-rename",
            "schema-type-promotion",
            "schema-nested-struct",
            "schema-complex-types",
            "schema-long-chain-history"
        );
        assertThat(new ErasureCodingScenario().cases(config)).extracting("caseId").containsExactly(
            "hdfs-replication-1-actual",
            "hdfs-replication-2-baseline",
            "ec-policy-rs-3-2-1024k",
            "ec-policy-rs-6-3-1024k",
            "ec-policy-rs-10-4-1024k",
            "ec-policy-xor-2-1-1024k",
            "ec-rs-10-4-failure-tolerance",
            "ec-policy-matrix-failure"
        );
        assertThat(new ErasureCodingConversionScenario().cases(config)).extracting("caseId").containsExactly(
            "replication-to-ec-policy-only",
            "replication-to-ec-rewrite",
            "ec-to-replication-policy-only",
            "ec-to-replication-rewrite"
        );
        assertThat(new ConcurrentWriteScenario().cases(config)).extracting("caseId").containsExactly(
            "concurrent-append-disjoint-partitions",
            "concurrent-append-same-partition",
            "concurrent-update-overlap",
            "concurrent-mixed-read-write"
        );
        assertThat(new RowLevelMutationScenario().cases(config)).extracting("caseId").containsExactly(
            "row-update-single-range",
            "row-delete-partition-prunable",
            "row-delete-selective",
            "row-merge-upsert-delete"
        );
        assertThat(new AcidTransactionScenario().cases(config)).extracting("caseId").containsExactly(
            "acid-kill-before-commit",
            "acid-kill-during-commit",
            "acid-conflicting-commits",
            "acid-reader-isolation"
        );
        assertThat(new IncrementalPullScenario().cases(config)).extracting("caseId").containsExactly(
            "incremental-append-only",
            "incremental-multi-snapshot-window",
            "incremental-with-delete-update-boundary",
            "incremental-expired-snapshot"
        );
        assertThat(new TimeTravelScenario().cases(config)).extracting("caseId").containsExactly(
            "time-travel-by-snapshot-id",
            "time-travel-by-timestamp",
            "time-travel-after-schema-evolution",
            "time-travel-after-expire"
        );
        assertThat(new SmallFileCompactionScenario().cases(config)).extracting("caseId").containsExactly(
            "small-files-many-snapshots-build",
            "small-files-query-degradation",
            "small-files-data-compaction",
            "small-files-manifest-rewrite",
            "small-files-expire-snapshots"
        );
    }

    @Test
    void supportBuildsRunScopedTableNameAndDataScale() throws Exception {
        IcebergValidationConfig config = config();
        IcebergValidationContext context = new IcebergValidationContext(
            config,
            "run:2026/07/28",
            Path.of("."),
            Path.of("reports"),
            false
        );

        assertThat(IcebergScenarioSupport.tableName(context, "schemaEvolution", "schema-add-drop-rename"))
            .isEqualTo("iceberg_catalog.iceberg_validation.schemaEvolution_schema_add_drop_rename_run_2026_07_28");
        assertThat(IcebergScenarioSupport.tableLocation(context, "schemaEvolution", "schema-add-drop-rename"))
            .contains("/iceberg_validation/schemaEvolution/schema-add-drop-rename/run_2026_07_28");
        assertThat(IcebergScenarioSupport.dataScale(config))
            .containsAllEntriesOf(Map.of("profile", "smoke", "rows", "100000", "partitions", "8"));
    }

    private static IcebergValidationConfig config() throws Exception {
        return new IcebergValidationConfigLoader().load(Path.of("configs", "iceberg-validation.yml"));
    }
}

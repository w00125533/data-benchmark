package com.example.databenchmark.iceberg.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationConfigLoader;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class IcebergScenarioScriptTest {
    @Test
    void rowLevelMergeUsesConcreteSourceViewAndExplicitInsertColumns() throws Exception {
        IcebergValidationConfig config = config();
        RowLevelMutationScenario scenario = new RowLevelMutationScenario();

        IcebergValidationResult result = scenario.run(scenario.cases(config).get(3), context(config));

        assertThat(result.actionCommands()).anySatisfy(command -> assertThat(command)
            .contains("CREATE OR REPLACE TEMP VIEW")
            .doesNotContain("..."));
        assertThat(result.actionCommands()).anySatisfy(command -> assertThat(command)
            .contains("MERGE INTO")
            .contains("WHEN NOT MATCHED THEN INSERT (id, event_day, region"));
    }

    @Test
    void smallFileCompactionGeneratesConfiguredNumberOfConcreteInserts() throws Exception {
        IcebergValidationConfig config = config();
        SmallFileCompactionScenario scenario = new SmallFileCompactionScenario();

        IcebergValidationResult result = scenario.run(scenario.cases(config).get(0), context(config));

        assertThat(result.actionCommands())
            .filteredOn(command -> command.contains("INSERT INTO iceberg_catalog.iceberg_validation.smallFileCompaction_"))
            .hasSize(config.scale().smallFileCommits());
        assertThat(result.actionCommands()).allSatisfy(command -> assertThat(command).doesNotContain("<table>"));
        assertThat(result.metrics()).containsEntry("targetSnapshotCommits", Integer.toString(config.scale().smallFileCommits()));
        assertThat(result.metrics().get("expectedMetricFields")).contains("snapshotCountBefore", "dataFileCountAfter", "hdfsDiskBytesAfter");
    }

    @Test
    void ecFailureCaseSkipsWhenRsTenFourDataNodesAreInsufficient() throws Exception {
        IcebergValidationConfig config = config();
        ErasureCodingScenario scenario = new ErasureCodingScenario();

        IcebergValidationResult result = scenario.run(scenario.cases(config).get(1), context(config));

        assertThat(result.conclusion()).contains("requires at least 14 live DataNode");
        assertThat(result.evidence()).contains("policy=RS-10-4-1024k", "requiredDataNodes=14");
    }

    private static IcebergValidationContext context(IcebergValidationConfig config) {
        return new IcebergValidationContext(config, "unit-run", Path.of("."), Path.of("reports"), false);
    }

    private static IcebergValidationConfig config() throws Exception {
        return new IcebergValidationConfigLoader().load(Path.of("configs", "iceberg-validation.yml"));
    }
}

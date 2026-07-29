package com.example.databenchmark.iceberg.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationConfigLoader;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class SchemaEvolutionScenarioTest {
    @Test
    void schemaCasesExposeDistinctChangeTypesAndConcreteTargets() throws Exception {
        SchemaEvolutionScenario scenario = new SchemaEvolutionScenario();
        IcebergValidationConfig config = new IcebergValidationConfigLoader().load(Path.of("configs/iceberg-validation.yml"));
        IcebergValidationContext context = new IcebergValidationContext(
            config,
            "schema-test",
            Path.of("work"),
            Path.of("reports"),
            false
        );

        List<IcebergValidationCase> cases = scenario.cases(config);

        assertThat(cases).extracting(IcebergValidationCase::caseId)
            .containsExactly(
                "schema-add-drop-rename",
                "schema-type-promotion",
                "schema-nested-struct",
                "schema-complex-types",
                "schema-long-chain-history"
            );

        List<IcebergValidationResult> results = cases.stream()
            .map(testCase -> scenario.run(testCase, context))
            .toList();

        assertThat(results).extracting(result -> result.metrics().get("schemaChangeType"))
            .containsExactly("add/drop/rename", "type promotion", "nested struct", "map/list/struct", "long chain");
        assertThat(results).allSatisfy(result -> {
            assertThat(result.metrics()).doesNotContainKey("schemaChangeTypes");
            assertThat(result.metrics()).containsKeys(
                "schemaChangeType",
                "changeCount",
                "baselineRows",
                "currentRows",
                "snapshotCount",
                "schemaHistoryLength"
            );
            assertThat(result.metrics()).doesNotContainKeys("currentQuerySeconds", "historicalQuerySeconds");
            assertThat(result.metrics().get("baselineRows")).isEqualTo("1000");
            assertThat(result.metrics().get("currentRows")).isEqualTo("1000");
            assertThat(result.comparison()).doesNotContainKey("scriptedActions");
        });
        assertThat(results).extracting(result -> result.metrics().get("changeCount"))
            .containsExactly("4", "3", "3", "3", "100");
        assertThat(results).extracting(IcebergValidationResult::actionCommands).doesNotHaveDuplicates();
        assertThat(results).extracting(IcebergValidationResult::conclusion).doesNotHaveDuplicates();

        IcebergValidationResult addDropRename = resultByCaseId(results, "schema-add-drop-rename");
        int addCategoryIndex = commandIndex(addDropRename.actionCommands(), " ADD COLUMN category STRING");
        int dropCategoryIndex = commandIndex(addDropRename.actionCommands(), " DROP COLUMN category");
        assertThat(addCategoryIndex).isNotNegative();
        assertThat(dropCategoryIndex).isGreaterThan(addCategoryIndex);

        IcebergValidationResult nestedStruct = resultByCaseId(results, "schema-nested-struct");
        assertThat(nestedStruct.actionCommands())
            .noneMatch(command -> command.matches(".*ADD COLUMN\\s+\\S+\\s+payload\\s+STRUCT<.*"));
    }

    private static IcebergValidationResult resultByCaseId(List<IcebergValidationResult> results, String caseId) {
        return results.stream()
            .filter(result -> caseId.equals(result.caseId()))
            .findFirst()
            .orElseThrow();
    }

    private static int commandIndex(List<String> commands, String expectedSuffix) {
        for (int index = 0; index < commands.size(); index++) {
            if (commands.get(index).endsWith(expectedSuffix)) {
                return index;
            }
        }
        return -1;
    }
}

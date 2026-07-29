package com.example.databenchmark.iceberg.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.databenchmark.engine.CommandResult;
import com.example.databenchmark.iceberg.IcebergExecutionEvidence;
import com.example.databenchmark.iceberg.IcebergValidationCase;
import com.example.databenchmark.iceberg.IcebergValidationConfig;
import com.example.databenchmark.iceberg.IcebergValidationConfigLoader;
import com.example.databenchmark.iceberg.IcebergValidationContext;
import com.example.databenchmark.iceberg.IcebergValidationResult;
import com.example.databenchmark.iceberg.exec.SparkSqlExecutor;
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
        List<String> validationPoints = results.stream()
            .map(result -> result.metrics().get("validationPoint"))
            .toList();
        assertThat(validationPoints).allSatisfy(validationPoint ->
            assertThat(validationPoint).isNotBlank().contains("验证"));
        assertThat(validationPoints.stream().distinct().toList()).hasSameSizeAs(results);
        assertThat(validationPoints).anySatisfy(validationPoint -> assertThat(validationPoint).contains("字段新增"));
        assertThat(validationPoints).anySatisfy(validationPoint -> assertThat(validationPoint).contains("数值类型提升"));
        assertThat(validationPoints).anySatisfy(validationPoint -> assertThat(validationPoint).contains("嵌套 struct"));
        assertThat(validationPoints).anySatisfy(validationPoint -> assertThat(validationPoint).contains("复杂类型"));
        assertThat(validationPoints).anySatisfy(validationPoint -> assertThat(validationPoint).contains("长链路"));
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
            assertThat(result.metrics().get("currentRows")).isEqualTo("2000");
            assertThat(result.metrics().get("snapshotCount")).isEqualTo("2");
            assertThat(result.comparison()).containsEntry("snapshotCount", "2");
            assertThat(result.comparison()).doesNotContainKey("scriptedActions");
            assertThat(result.assertions())
                .doesNotContain(
                    "current and historical row counts match",
                    "current rows match the baseline after repeated schema changes"
                );
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
        assertThat(addDropRename.actionCommands()).anySatisfy(command -> assertThat(command)
            .startsWith("SELECT snapshot_id AS baselineSnapshotId FROM ")
            .contains(".snapshots ORDER BY committed_at ASC ")
            .endsWith("LIMIT 1"));
        assertThat(addDropRename.actionCommands()).anySatisfy(command -> assertThat(command)
            .startsWith("SELECT snapshot_id AS postAlterSnapshotId FROM ")
            .contains(".snapshots ORDER BY committed_at DESC ")
            .endsWith("LIMIT 1"));
        assertThat(addDropRename.actionCommands())
            .noneMatch(command -> command.contains("LIMIT 1 AS"));

        IcebergValidationResult nestedStruct = resultByCaseId(results, "schema-nested-struct");
        assertThat(nestedStruct.actionCommands())
            .noneMatch(command -> command.matches(".*ADD COLUMN\\s+\\S+\\s+payload\\s+STRUCT<.*"));
        String nestedStructPostAlterInsert = nestedStruct.actionCommands().stream()
            .filter(command -> command.startsWith("INSERT INTO "))
            .findFirst()
            .orElseThrow();
        assertThat(nestedStructPostAlterInsert)
            .contains("'vendor_id'")
            .contains("'quality_score'")
            .doesNotContain("'vendor_code'");
    }

    @Test
    void schemaCasesExposeHistoricalAndCurrentQuerySqlWithPostAlterData() throws Exception {
        SchemaEvolutionScenario scenario = new SchemaEvolutionScenario();
        IcebergValidationConfig config = new IcebergValidationConfigLoader().load(Path.of("configs/iceberg-validation.yml"));
        IcebergValidationContext context = new IcebergValidationContext(config, "schema-query-test", Path.of("work"), Path.of("reports"), false);

        IcebergValidationResult result = scenario.run(
            scenario.cases(config).stream()
                .filter(testCase -> testCase.caseId().equals("schema-add-drop-rename"))
                .findFirst()
                .orElseThrow(),
            context
        );

        String table = config.iceberg().catalog()
            + "."
            + config.iceberg().namespace()
            + ".schemaEvolution_schema_add_drop_rename_schema_query_test";
        assertThat(result.actionCommands()).anySatisfy(command -> assertThat(command)
            .startsWith("INSERT INTO " + table + " (")
            .contains("id, event_day, metric_int, metric_float, amount, payload, tags, attrs")
            .contains("FROM range(1000, 2000)"));
        assertThat(result.actionCommands()).noneMatch(command -> command.contains("INSERT INTO " + table + "\nSELECT"));
        assertThat(result.actionCommands()).anyMatch(command -> command.contains("VERSION AS OF ${baselineSnapshotId}"));
        assertThat(result.actionCommands()).anyMatch(command -> command.contains("SELECT id"));
        assertThat(result.metrics()).containsKeys(
            "validationPoint",
            "baselineSnapshotIdStatus",
            "postAlterSnapshotIdStatus",
            "historicalQuerySql",
            "currentQuerySql",
            "historicalRowsStatus",
            "currentRowsStatus",
            "historicalQuerySecondsStatus",
            "currentQuerySecondsStatus",
            "historicalSampleRowsStatus",
            "currentSampleRowsStatus"
        );
        assertThat(result.metrics().get("validationPoint")).contains("历史快照");
        assertThat(result.metrics()).containsEntry("historicalQuerySecondsStatus", "notExecuted");
        assertThat(result.metrics()).containsEntry("currentQuerySecondsStatus", "notExecuted");
        assertThat(result.metrics().get("historicalQuerySql")).contains("VERSION AS OF ${baselineSnapshotId}");
        assertThat(result.metrics().get("currentQuerySql")).contains("ORDER BY id DESC LIMIT 20");
        assertThat(result.evidence()).contains("historicalSampleRows=notExecuted", "currentSampleRows=notExecuted");
        assertThat(result.evidence()).anySatisfy(value -> assertThat(value)
            .startsWith("historicalQuerySql=")
            .contains("VERSION AS OF ${baselineSnapshotId}"));
        assertThat(result.evidence()).anySatisfy(value -> assertThat(value)
            .startsWith("currentQuerySql=")
            .contains("ORDER BY id DESC LIMIT 20"));
    }

    @Test
    void injectedSparkExecutorCapturesHistoricalAndCurrentQueryMetrics() throws Exception {
        SchemaEvolutionScenario scenario = new SchemaEvolutionScenario(new FakeSparkSqlExecutor());
        IcebergValidationConfig config = new IcebergValidationConfigLoader().load(Path.of("configs/iceberg-validation.yml"));
        IcebergValidationContext context = new IcebergValidationContext(
            config,
            "schema-execution-test",
            Path.of("work"),
            Path.of("reports"),
            false
        );

        IcebergValidationResult result = scenario.run(
            scenario.cases(config).stream()
                .filter(testCase -> testCase.caseId().equals("schema-add-drop-rename"))
                .findFirst()
                .orElseThrow(),
            context
        );

        assertThat(result.metrics()).containsEntry("baselineSnapshotId", "111");
        assertThat(result.metrics()).containsEntry("postAlterSnapshotId", "222");
        assertThat(result.metrics()).containsEntry("historicalRows", "2");
        assertThat(result.metrics()).containsEntry("currentRows", "2");
        assertThat(result.metrics()).containsEntry("historicalQuerySeconds", "0.321");
        assertThat(result.metrics()).containsEntry("currentQuerySeconds", "0.456");
        assertThat(result.executionResults()).extracting(IcebergExecutionEvidence::label)
            .contains("historical query", "current query");
        assertThat(result.evidence()).contains(
            "historicalSampleRows=id\n1\n2",
            "currentSampleRows=id\n2000\n1999"
        );
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

    private static final class FakeSparkSqlExecutor extends SparkSqlExecutor {
        @Override
        public CommandResult run(IcebergValidationConfig config, String sql) {
            if (sql.contains(".snapshots") && sql.contains("ASC")) {
                return new CommandResult(List.of("spark-sql"), 0, "snapshot_id\n111\n", "", 0.100);
            }
            if (sql.contains(".snapshots") && sql.contains("DESC")) {
                return new CommandResult(List.of("spark-sql"), 0, "snapshot_id\n222\n", "", 0.100);
            }
            if (sql.contains("VERSION AS OF")) {
                return new CommandResult(List.of("spark-sql"), 0, "id\n1\n2\n", "", 0.321);
            }
            if (sql.contains("ORDER BY id DESC")) {
                return new CommandResult(List.of("spark-sql"), 0, "id\n2000\n1999\n", "", 0.456);
            }
            return new CommandResult(List.of("spark-sql"), 0, "OK\n", "", 0.050);
        }
    }
}

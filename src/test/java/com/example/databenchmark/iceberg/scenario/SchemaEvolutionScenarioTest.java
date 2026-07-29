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
        SchemaEvolutionScenario scenario = new SchemaEvolutionScenario(new FakeOrUnusedSparkSqlExecutor(), false);
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
            .contains("id, event_day, region, metric_int, metric_float, amount, payload, tags, attrs")
            .contains("'vendor_id'")
            .contains("'quality_score'")
            .doesNotContain("'vendor_code'");
    }

    @Test
    void schemaCasesExposeHistoricalAndCurrentQuerySqlWithPostAlterData() throws Exception {
        SchemaEvolutionScenario scenario = new SchemaEvolutionScenario(new FakeOrUnusedSparkSqlExecutor(), false);
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
            .contains("id, event_day, service_region, metric_int, metric_float, amount, payload, tags, attrs")
            .contains("'after_evolution'")
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
            .contains(
                "create schema table",
                "schema change",
                "post alter insert",
                "baseline snapshot id",
                "post alter snapshot id",
                "historical query",
                "current query"
            );
        IcebergExecutionEvidence baselineSnapshot = executionByLabel(result, "baseline snapshot id");
        IcebergExecutionEvidence postAlterSnapshot = executionByLabel(result, "post alter snapshot id");
        IcebergExecutionEvidence historicalQuery = executionByLabel(result, "historical query");
        IcebergExecutionEvidence currentQuery = executionByLabel(result, "current query");
        assertThat(baselineSnapshot.stdout()).contains("111");
        assertThat(baselineSnapshot.durationSeconds()).isEqualTo(0.100);
        assertThat(postAlterSnapshot.stdout()).contains("222");
        assertThat(postAlterSnapshot.durationSeconds()).isEqualTo(0.100);
        assertThat(historicalQuery.stdout()).isEqualTo("id\n1\n2\n");
        assertThat(historicalQuery.durationSeconds()).isEqualTo(0.321);
        assertThat(currentQuery.stdout()).isEqualTo("id\n2000\n1999\n");
        assertThat(currentQuery.durationSeconds()).isEqualTo(0.456);
        assertThat(result.evidence()).contains(
            "historicalSampleRows=id\n1\n2",
            "currentSampleRows=id\n2000\n1999"
        );
    }

    @Test
    void nonZeroSparkExitCapturesFailedCommandEvidence() throws Exception {
        IcebergValidationResult result = runAddDropRenameWith(new FakeSparkSqlExecutor(FailureMode.SCHEMA_CHANGE_NON_ZERO));

        assertThat(result.functionStatus()).isEqualTo(com.example.databenchmark.iceberg.IcebergConclusion.FunctionStatus.FAIL);
        assertThat(result.performanceStatus()).isEqualTo(com.example.databenchmark.iceberg.IcebergConclusion.PerformanceStatus.NOT_COMPARABLE);
        assertThat(result.errors()).anySatisfy(error -> assertThat(error).contains("boom stderr"));

        IcebergExecutionEvidence failedChange = result.executionResults().stream()
            .filter(executionResult -> executionResult.label().equals("schema change"))
            .filter(executionResult -> executionResult.exitCode() == 7)
            .findFirst()
            .orElseThrow();
        assertThat(failedChange.script()).startsWith("ALTER TABLE");
        assertThat(failedChange.durationSeconds()).isEqualTo(0.789);
        assertThat(failedChange.stdout()).isEqualTo("partial stdout\n");
        assertThat(failedChange.stderr()).isEqualTo("boom stderr\n");
    }

    @Test
    void dataRowCountHandlesHeaderlessSparkOutput() throws Exception {
        IcebergValidationResult result = runAddDropRenameWith(new FakeSparkSqlExecutor(OutputMode.HEADERLESS));

        assertThat(result.functionStatus()).isEqualTo(com.example.databenchmark.iceberg.IcebergConclusion.FunctionStatus.PASS);
        assertThat(result.metrics()).containsEntry("historicalRows", "2");
        assertThat(result.metrics()).containsEntry("currentRows", "2");
    }

    @Test
    void dataRowCountIgnoresSparkNoiseHeadersAndSeparators() throws Exception {
        IcebergValidationResult result = runAddDropRenameWith(new FakeSparkSqlExecutor(OutputMode.NOISY));

        assertThat(result.functionStatus()).isEqualTo(com.example.databenchmark.iceberg.IcebergConclusion.FunctionStatus.PASS);
        assertThat(result.metrics()).containsEntry("historicalRows", "2");
        assertThat(result.metrics()).containsEntry("currentRows", "2");
    }

    @Test
    void snapshotIdsUseDataRowsFromNoisyAliasedSparkOutput() throws Exception {
        IcebergValidationResult result = runAddDropRenameWith(new FakeSparkSqlExecutor(SnapshotOutputMode.NOISY_ALIASED));

        assertThat(result.functionStatus()).isEqualTo(com.example.databenchmark.iceberg.IcebergConclusion.FunctionStatus.PASS);
        assertThat(result.metrics()).containsEntry("baselineSnapshotId", "111");
        assertThat(result.metrics()).containsEntry("postAlterSnapshotId", "222");
        assertThat(result.metrics().get("historicalQuerySql")).contains("VERSION AS OF 111");
        assertThat(executionByLabel(result, "historical query").script()).contains("VERSION AS OF 111");
    }

    @Test
    void missingSnapshotIdValueFailsWithoutRunningHistoricalQuery() throws Exception {
        IcebergValidationResult result = runAddDropRenameWith(new FakeSparkSqlExecutor(SnapshotOutputMode.MISSING_BASELINE_VALUE));

        assertThat(result.functionStatus()).isEqualTo(com.example.databenchmark.iceberg.IcebergConclusion.FunctionStatus.FAIL);
        assertThat(result.performanceStatus()).isEqualTo(com.example.databenchmark.iceberg.IcebergConclusion.PerformanceStatus.NOT_COMPARABLE);
        assertThat(result.errors()).anySatisfy(error -> assertThat(error).contains("baselineSnapshotId"));
        assertThat(result.executionResults()).extracting(IcebergExecutionEvidence::label)
            .contains("baseline snapshot id", "post alter snapshot id")
            .doesNotContain("historical query");
        assertThat(result.evidence()).anySatisfy(value -> assertThat(value).startsWith("executionError="));
    }

    @Test
    void schemaChangeFailureReturnsFailedResult() throws Exception {
        IcebergValidationResult result = runAddDropRenameWith(new FakeSparkSqlExecutor(FailureMode.SCHEMA_CHANGE));

        assertThat(result.functionStatus()).isEqualTo(com.example.databenchmark.iceberg.IcebergConclusion.FunctionStatus.FAIL);
        assertThat(result.performanceStatus()).isEqualTo(com.example.databenchmark.iceberg.IcebergConclusion.PerformanceStatus.NOT_COMPARABLE);
        assertThat(result.errors()).anySatisfy(error -> assertThat(error).contains("boom"));
        IcebergExecutionEvidence failedChange = result.executionResults().stream()
            .filter(executionResult -> executionResult.label().equals("schema change"))
            .filter(executionResult -> executionResult.exitCode() == -1)
            .findFirst()
            .orElseThrow();
        assertThat(failedChange.script()).startsWith("ALTER TABLE");
        assertThat(failedChange.stdout()).isEmpty();
        assertThat(failedChange.stderr()).isEqualTo("boom");
    }

    @Test
    void historicalQueryFailureReturnsFailedResult() throws Exception {
        IcebergValidationResult result = runAddDropRenameWith(new FakeSparkSqlExecutor(FailureMode.HISTORICAL_QUERY));

        assertThat(result.functionStatus()).isEqualTo(com.example.databenchmark.iceberg.IcebergConclusion.FunctionStatus.FAIL);
        assertThat(result.performanceStatus()).isEqualTo(com.example.databenchmark.iceberg.IcebergConclusion.PerformanceStatus.NOT_COMPARABLE);
        assertThat(result.errors()).anySatisfy(error -> assertThat(error).contains("boom"));
    }

    private static IcebergValidationResult runAddDropRenameWith(FakeSparkSqlExecutor sparkSqlExecutor) throws Exception {
        SchemaEvolutionScenario scenario = new SchemaEvolutionScenario(sparkSqlExecutor);
        IcebergValidationConfig config = new IcebergValidationConfigLoader().load(Path.of("configs/iceberg-validation.yml"));
        IcebergValidationContext context = new IcebergValidationContext(
            config,
            "schema-failure-test",
            Path.of("work"),
            Path.of("reports"),
            false
        );
        return scenario.run(
            scenario.cases(config).stream()
                .filter(testCase -> testCase.caseId().equals("schema-add-drop-rename"))
                .findFirst()
                .orElseThrow(),
            context
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

    private static IcebergExecutionEvidence executionByLabel(IcebergValidationResult result, String label) {
        return result.executionResults().stream()
            .filter(executionResult -> label.equals(executionResult.label()))
            .findFirst()
            .orElseThrow();
    }

    private static final class FakeOrUnusedSparkSqlExecutor extends SparkSqlExecutor {
        @Override
        public CommandResult run(IcebergValidationConfig config, String sql) {
            throw new AssertionError("planned schema tests must not execute Spark SQL");
        }
    }

    private enum FailureMode {
        SCHEMA_CHANGE,
        SCHEMA_CHANGE_NON_ZERO,
        HISTORICAL_QUERY
    }

    private enum OutputMode {
        HEADER,
        HEADERLESS,
        NOISY
    }

    private enum SnapshotOutputMode {
        SIMPLE,
        NOISY_ALIASED,
        MISSING_BASELINE_VALUE
    }

    private static final class FakeSparkSqlExecutor extends SparkSqlExecutor {
        private final FailureMode failureMode;
        private final OutputMode outputMode;
        private final SnapshotOutputMode snapshotOutputMode;

        private FakeSparkSqlExecutor() {
            this(null, OutputMode.HEADER, SnapshotOutputMode.SIMPLE);
        }

        private FakeSparkSqlExecutor(FailureMode failureMode) {
            this(failureMode, OutputMode.HEADER, SnapshotOutputMode.SIMPLE);
        }

        private FakeSparkSqlExecutor(OutputMode outputMode) {
            this(null, outputMode, SnapshotOutputMode.SIMPLE);
        }

        private FakeSparkSqlExecutor(SnapshotOutputMode snapshotOutputMode) {
            this(null, OutputMode.HEADER, snapshotOutputMode);
        }

        private FakeSparkSqlExecutor(FailureMode failureMode, OutputMode outputMode) {
            this(failureMode, outputMode, SnapshotOutputMode.SIMPLE);
        }

        private FakeSparkSqlExecutor(
            FailureMode failureMode,
            OutputMode outputMode,
            SnapshotOutputMode snapshotOutputMode
        ) {
            this.failureMode = failureMode;
            this.outputMode = outputMode;
            this.snapshotOutputMode = snapshotOutputMode;
        }

        @Override
        public CommandResult run(IcebergValidationConfig config, String sql) {
            CommandResult result = runRaw(config, sql);
            if (result.exitCode() != 0) {
                throw new IllegalStateException("Spark SQL failed: " + result.stderr());
            }
            return result;
        }

        @Override
        public CommandResult runRaw(IcebergValidationConfig config, String sql) {
            if (failureMode == FailureMode.SCHEMA_CHANGE && sql.startsWith("ALTER TABLE")) {
                throw new IllegalStateException("boom");
            }
            if (failureMode == FailureMode.SCHEMA_CHANGE_NON_ZERO && sql.startsWith("ALTER TABLE")) {
                return new CommandResult(List.of("spark-sql"), 7, "partial stdout\n", "boom stderr\n", 0.789);
            }
            if (failureMode == FailureMode.HISTORICAL_QUERY && sql.contains("VERSION AS OF")) {
                throw new IllegalStateException("boom");
            }
            if (sql.contains(".snapshots") && sql.contains("ASC")) {
                return new CommandResult(List.of("spark-sql"), 0, baselineSnapshotStdout(), "", 0.100);
            }
            if (sql.contains(".snapshots") && sql.contains("DESC")) {
                return new CommandResult(List.of("spark-sql"), 0, postAlterSnapshotStdout(), "", 0.100);
            }
            if (sql.contains("VERSION AS OF")) {
                return new CommandResult(List.of("spark-sql"), 0, historicalRowsStdout(), "", 0.321);
            }
            if (sql.contains("ORDER BY id DESC")) {
                return new CommandResult(List.of("spark-sql"), 0, currentRowsStdout(), "", 0.456);
            }
            return new CommandResult(List.of("spark-sql"), 0, "OK\n", "", 0.050);
        }

        private String baselineSnapshotStdout() {
            return switch (snapshotOutputMode) {
                case SIMPLE -> "baselineSnapshotId\n111\n";
                case NOISY_ALIASED -> """
                    Setting default log level to "WARN".
                    Spark master: local
                    +------------------+
                    |baselineSnapshotId|
                    +------------------+
                    |111               |
                    +------------------+
                    Time taken: 0.100 seconds, Fetched 1 row(s)
                    """;
                case MISSING_BASELINE_VALUE -> "baselineSnapshotId\n";
            };
        }

        private String postAlterSnapshotStdout() {
            return switch (snapshotOutputMode) {
                case SIMPLE, MISSING_BASELINE_VALUE -> "postAlterSnapshotId\n222\n";
                case NOISY_ALIASED -> """
                    26/07/29 12:00:00 WARN NativeCodeLoader: Unable to load native-hadoop library
                    +-------------------+
                    |postAlterSnapshotId|
                    +-------------------+
                    |222                |
                    +-------------------+
                    Time taken: 0.100 seconds, Fetched 1 row(s)
                    """;
            };
        }

        private String historicalRowsStdout() {
            return switch (outputMode) {
                case HEADER -> "id\n1\n2\n";
                case HEADERLESS -> "1\n2\n";
                case NOISY -> """
                    Setting default log level to "WARN".
                    Spark master: local
                    Hive Session ID = 779e7d32-1ce6-4eb6-bc7f-85aaf7d9fb03
                    +---+
                    | id|
                    +---+
                    | 1|
                    | 2|
                    +---+
                    Time taken: 0.321 seconds, Fetched 2 row(s)
                    """;
            };
        }

        private String currentRowsStdout() {
            return switch (outputMode) {
                case HEADER -> "id\n2000\n1999\n";
                case HEADERLESS -> "2000\n1999\n";
                case NOISY -> """
                    26/07/29 12:00:00 WARN NativeCodeLoader: Unable to load native-hadoop library
                    Hive Session ID = 779e7d32-1ce6-4eb6-bc7f-85aaf7d9fb03
                    +----+
                    |  id|
                    +----+
                    |2000|
                    |1999|
                    +----+
                    Time taken: 0.456 seconds, Fetched 2 row(s)
                    """;
            };
        }
    }
}

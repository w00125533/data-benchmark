package com.example.databenchmark.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class IcebergValidationReportWriter {
    private static final Set<String> PLACEHOLDER_METRIC_KEYS = Set.of(
        "scriptedActions",
        "conversionMetrics",
        "mutationMetrics",
        "incrementalMetrics",
        "timeTravelMetrics",
        "caseImplemented",
        "acidEvidence",
        "ecPolicies",
        "schemaChangeTypes"
    );

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()
        .enable(SerializationFeature.INDENT_OUTPUT);

    public Path write(IcebergValidationReport report, Path reportRoot) throws IOException {
        Path runDir = reportRoot.resolve(report.runId());
        Files.createDirectories(runDir);
        mapper.writeValue(runDir.resolve("report.json").toFile(), report);
        Files.deleteIfExists(runDir.resolve("report.md"));
        Files.writeString(runDir.resolve("report.html"), html(report));
        return runDir.resolve("report.html");
    }

    private static String html(IcebergValidationReport report) {
        StringBuilder builder = new StringBuilder();
        Map<IcebergConclusion.FunctionStatus, Long> statusCounts = report.results().stream()
            .collect(Collectors.groupingBy(IcebergValidationResult::functionStatus, LinkedHashMap::new, Collectors.counting()));
        builder.append("""
            <!doctype html>
            <html lang="zh-CN">
            <head>
              <meta charset="utf-8">
              <meta name="viewport" content="width=device-width, initial-scale=1">
              <title>Iceberg Validation Report</title>
              <style>
                :root { color-scheme: light; --border: #d8dee4; --header: #f6f8fa; --text: #1f2328; --muted: #57606a; --pass: #1a7f37; --skip: #9a6700; --fail: #cf222e; }
                body { margin: 0; font-family: Arial, "Microsoft YaHei", sans-serif; color: var(--text); background: #fff; }
                main { max-width: 1480px; margin: 0 auto; padding: 28px 32px 48px; }
                h1 { font-size: 28px; margin: 0 0 8px; }
                h2 { font-size: 20px; margin: 28px 0 12px; }
                .subtitle { color: var(--muted); margin: 0 0 20px; }
                .scenario-method { margin: -4px 0 12px; color: var(--muted); font-size: 13px; line-height: 1.6; }
                .table-wrap { overflow-x: auto; border: 1px solid var(--border); border-radius: 6px; }
                table { width: 100%; border-collapse: collapse; min-width: 980px; }
                th, td { border-bottom: 1px solid var(--border); border-right: 1px solid var(--border); padding: 9px 10px; text-align: left; vertical-align: top; font-size: 13px; line-height: 1.45; }
                th { background: var(--header); font-weight: 700; white-space: nowrap; }
                td:last-child, th:last-child { border-right: 0; }
                tr:last-child td { border-bottom: 0; }
                code { font-family: Consolas, "Liberation Mono", monospace; font-size: 12px; }
                pre { margin: 0; white-space: pre-wrap; font-family: Consolas, "Liberation Mono", monospace; font-size: 12px; line-height: 1.4; }
                details.evidence-details summary { cursor: pointer; color: #0969da; font-weight: 700; }
                .evidence-block { margin-top: 8px; display: grid; gap: 8px; min-width: 360px; }
                .evidence-block strong { display: block; margin-bottom: 3px; color: var(--text); }
                .status-PASS { color: var(--pass); font-weight: 700; }
                .status-SKIPPED { color: var(--skip); font-weight: 700; }
                .status-FAIL { color: var(--fail); font-weight: 700; }
                .status-DEGRADED { color: var(--skip); font-weight: 700; }
                .compact { min-width: 620px; }
              </style>
            </head>
            <body>
            <main>
              <h1>Iceberg Validation Report</h1>
              <p class="subtitle">Apache Iceberg 功能验证报告，保留机器可读 JSON，同时用 HTML 表格呈现需求关键要素、验证动作、指标和结论。</p>
            """);

        builder.append("<section><h2>运行概览</h2><div class=\"table-wrap\"><table class=\"compact\"><tbody>");
        appendKeyValue(builder, "Run ID", report.runId());
        appendKeyValue(builder, "Iceberg Version", report.icebergVersion());
        appendKeyValue(builder, "Profile", report.profile());
        appendKeyValue(builder, "Status", report.status());
        appendKeyValue(builder, "Started At", report.startedAt());
        appendKeyValue(builder, "Ended At", report.endedAt());
        appendKeyValue(builder, "Total Cases", Integer.toString(report.results().size()));
        appendKeyValue(builder, "PASS / SKIPPED / FAIL / DEGRADED",
            count(statusCounts, IcebergConclusion.FunctionStatus.PASS)
                + " / " + count(statusCounts, IcebergConclusion.FunctionStatus.SKIPPED)
                + " / " + count(statusCounts, IcebergConclusion.FunctionStatus.FAIL)
                + " / " + count(statusCounts, IcebergConclusion.FunctionStatus.DEGRADED));
        builder.append("</tbody></table></div></section>");

        for (ScenarioSection section : scenarioSections()) {
            appendScenarioSection(builder, section, report.results());
        }
        builder.append("</main></body></html>");
        return builder.toString();
    }

    private static void appendScenarioSection(StringBuilder builder, ScenarioSection section, List<IcebergValidationResult> results) {
        List<IcebergValidationResult> sectionResults = results.stream()
            .filter(result -> section.scenario().equals(result.scenario()))
            .toList();
        if (sectionResults.isEmpty()) {
            return;
        }
        builder.append("<section><h2>").append(escapeHtml(section.title())).append("</h2>");
        builder.append("<div class=\"scenario-method\"><strong>验证策略和方法</strong> ")
            .append(escapeHtml(section.method()))
            .append("</div>");
        builder.append("<div class=\"table-wrap\"><table><thead><tr>");
        for (String header : section.headers()) {
            builder.append("<th>").append(escapeHtml(header)).append("</th>");
        }
        builder.append("</tr></thead><tbody>");
        for (IcebergValidationResult result : sectionResults) {
            builder.append("<tr>");
            for (String value : scenarioCells(result)) {
                builder.append(cell(value));
            }
            builder.append("</tr>");
        }
        builder.append("</tbody></table></div></section>");
    }

    private static void appendKeyValue(StringBuilder builder, String key, String value) {
        builder.append("<tr><th>").append(escapeHtml(key)).append("</th><td>").append(escapeHtml(value)).append("</td></tr>");
    }

    private static long count(Map<IcebergConclusion.FunctionStatus, Long> counts, IcebergConclusion.FunctionStatus status) {
        return counts.getOrDefault(status, 0L);
    }

    private static String scenarioDisplayName(String scenario) {
        return switch (scenario) {
            case "schemaEvolution" -> "Schema 长期演进";
            case "erasureCoding" -> "HDFS 纠删码";
            case "erasureCodingConversion" -> "EC/replication 转换";
            case "concurrentWrite" -> "多进程并发写入";
            case "rowLevelMutation" -> "行级更新删除";
            case "acidTransaction" -> "ACID 事务保证";
            case "incrementalPull" -> "增量拉取";
            case "timeTravel" -> "时间旅行";
            case "smallFileCompaction" -> "小文件 Compaction";
            default -> scenario;
        };
    }

    private static String scenarioRequirement(String scenario) {
        return switch (scenario) {
            case "schemaEvolution" -> "Schema 变化类型; 历史数据兼容读取; 字段 ID 兼容";
            case "erasureCoding" -> "EC policy; replicationBaseline=2; 失效副本验证; 文件数统计; HDFS 磁盘占用";
            case "erasureCodingConversion" -> "replication 到 EC; EC 到 replication; policy-only; physical rewrite; 转换效率";
            case "concurrentWrite" -> "多 writer; 同/异分区提交; 冲突检测; 读写隔离";
            case "rowLevelMutation" -> "UPDATE; DELETE; MERGE; delete files; 历史快照读取";
            case "acidTransaction" -> "快照原子发布; 失败写入不可见; 冲突隔离; reader snapshot isolation";
            case "incrementalPull" -> "snapshot window; append-only 增量; update/delete 边界; 过期快照";
            case "timeTravel" -> "snapshot id; timestamp; schema 演进后读取; expire snapshots 行为";
            case "smallFileCompaction" -> "多 snapshot; 小文件数量; data file compaction; manifest rewrite; snapshot expiration";
            default -> scenario;
        };
    }

    private static List<ScenarioSection> scenarioSections() {
        return List.of(
            new ScenarioSection("schemaEvolution", "Schema 长期演进",
                "通过多阶段 Schema 变更覆盖字段新增、删除、重命名、类型提升和复杂类型变化，验证历史快照、字段投影和字段 ID 兼容读取，并记录查询延迟变化。",
                List.of("用例", "Schema 变化", "历史数据读取断言", "兼容性结论", "元数据/性能指标", "状态", "执行脚本与证据")),
            new ScenarioSection("erasureCoding", "HDFS 纠删码",
                "以 replication=2 作为基线，对比多种 EC policy 的读写结果、文件数和 HDFS 磁盘占用；具备足够 DataNode 时执行失效副本容错验证，不满足条件时记录跳过原因。",
                List.of("用例", "EC Policy", "replication 基线", "DataNode 要求", "文件数统计", "HDFS 磁盘占用", "故障注入/Skip 原因", "结论", "状态", "执行脚本与证据")),
            new ScenarioSection("erasureCodingConversion", "EC/replication 转换",
                "分别验证 replication 到 EC、EC 到 replication 的 policy-only 和 physical rewrite 路径，度量转换耗时、吞吐、文件数、磁盘占用和转换前后查询一致性。",
                List.of("用例", "转换方向", "转换方式", "转换前后文件/磁盘", "转换耗时/吞吐", "查询对比", "结论", "状态", "执行脚本与证据")),
            new ScenarioSection("concurrentWrite", "多进程并发写入",
                "构造多 writer 并发提交、同分区冲突、重叠更新和读写混合场景，验证成功提交可见性、失败提交不可见性、隔离断言和提交延迟指标。",
                List.of("用例", "Writer 数", "写入模式", "冲突类型", "成功/失败提交", "隔离断言", "性能指标", "结论", "状态", "执行脚本与证据")),
            new ScenarioSection("rowLevelMutation", "行级更新删除",
                "覆盖 UPDATE、DELETE、MERGE 的窄范围、分区可裁剪和稀疏变更，验证当前快照结果、历史快照可读性，并记录 delete/rewrite 文件和查询性能指标。",
                List.of("用例", "操作类型", "影响范围", "delete/rewrite 文件指标", "历史快照断言", "查询性能", "结论", "状态", "执行脚本与证据")),
            new ScenarioSection("acidTransaction", "ACID 事务保证",
                "通过提交前失败、提交期故障、冲突提交和 reader isolation 场景验证快照原子发布、半提交不可见、冲突隔离和读一致性。",
                List.of("用例", "故障/冲突类型", "快照原子性断言", "读隔离断言", "错误/Skip 原因", "结论", "状态", "执行脚本与证据")),
            new ScenarioSection("incrementalPull", "增量拉取",
                "按 snapshot window 验证 append-only 增量读取、多 snapshot 窗口、update/delete 边界和过期快照行为，对比全量扫描与增量扫描成本。",
                List.of("用例", "Snapshot Window", "数据变更类型", "增量/全量对比", "过期快照行为", "结论", "状态", "执行脚本与证据")),
            new ScenarioSection("timeTravel", "时间旅行",
                "使用 snapshot id、timestamp 和 Schema 演进后的历史访问验证时间旅行结果一致性，同时覆盖快照过期后的访问行为和历史查询性能。",
                List.of("用例", "访问方式", "目标快照/时间", "Schema 兼容断言", "过期行为", "性能指标", "结论", "状态", "执行脚本与证据")),
            new ScenarioSection("smallFileCompaction", "小文件 Compaction",
                "构造多 snapshot、多小文件提交，度量小文件对查询计划和查询耗时的影响，并验证 data file compaction、manifest rewrite、snapshot expiration 前后指标变化。",
                List.of("用例", "Snapshot 数", "小文件构造", "Compaction 类型", "前后文件/manifest/snapshot 指标", "查询对比", "结论", "状态", "执行脚本与证据"))
        );
    }

    private static List<String> scenarioCells(IcebergValidationResult result) {
        String status = statusCell(result);
        return switch (result.scenario()) {
            case "schemaEvolution" -> List.of(
                code(result.caseId()),
                schemaChangeSummary(result),
                schemaHistoricalReadSummary(result),
                result.conclusion(),
                schemaMetadataMetrics(result),
                status,
                evidenceDetails(result)
            );
            case "erasureCoding" -> List.of(
                code(result.caseId()),
                ecPolicyCell(result),
                ecReplicationBaseline(result),
                ecDataNodeRequirements(result),
                ecFileCountMetrics(result),
                ecDiskUsageMetrics(result),
                ecSkipReason(result),
                result.conclusion(),
                status,
                evidenceDetails(result)
            );
            case "erasureCodingConversion" -> List.of(
                code(result.caseId()),
                conversionDirection(result),
                conversionMode(result),
                conversionHdfsUsage(result),
                conversionDurationAndThroughput(result),
                conversionQueryAndChecksum(result),
                result.conclusion(),
                status,
                evidenceDetails(result)
            );
            case "concurrentWrite" -> List.of(
                code(result.caseId()),
                metricOrEmpty(result, "writerPlan", "writerCount"),
                metricOrEmpty(result, "writeMode"),
                metricOrEmpty(result, "conflictType"),
                metricOrEmpty(result, "successfulCommits", "failedCommits", "retryCount", "conflictCount"),
                joinList(result.assertions()),
                auditMetricPlan(result),
                result.conclusion(),
                status,
                evidenceDetails(result)
            );
            case "rowLevelMutation" -> List.of(
                code(result.caseId()),
                metricOrEmpty(result, "operation"),
                result.purpose(),
                metricOrAudit(result, "dataFilesBefore", "dataFilesAfter", "deleteFilesBefore", "deleteFilesAfter"),
                joinList(result.assertions()),
                metricOrAudit(result, "querySecondsBefore", "querySecondsAfter"),
                result.conclusion(),
                status,
                evidenceDetails(result)
            );
            case "acidTransaction" -> List.of(
                code(result.caseId()),
                metricOrEmpty(result, "transactionCase"),
                atomicAssertion(result),
                isolationAssertion(result),
                appendWithBreak(skipReason(result), auditMetricPlan(result)),
                result.conclusion(),
                status,
                evidenceDetails(result)
            );
            case "incrementalPull" -> List.of(
                code(result.caseId()),
                metricOrEmpty(result, "windowPlan", "snapshotWindowSize", "baseSnapshotId", "endSnapshotId"),
                metricOrEmpty(result, "changeType"),
                metricOrAudit(result, "fullScanRows", "incrementalRows", "fullScanSeconds", "incrementalSeconds", "savingRatio"),
                expiredSnapshotBehavior(result),
                result.conclusion(),
                status,
                evidenceDetails(result)
            );
            case "timeTravel" -> List.of(
                code(result.caseId()),
                metricOrEmpty(result, "selector"),
                metricOrAudit(result, "targetSnapshotId", "targetTimestamp"),
                joinList(result.assertions()),
                expiredSnapshotBehavior(result),
                metricOrAudit(result, "currentQuerySeconds", "historicalQuerySeconds", "expiredSnapshotUnavailable"),
                result.conclusion(),
                status,
                evidenceDetails(result)
            );
            case "smallFileCompaction" -> List.of(
                code(result.caseId()),
                metricOrEmpty(result, "targetSnapshotCommits", "snapshotCountBefore", "snapshotCountAfter"),
                metricOrEmpty(result, "filesPerCommit"),
                metricOrEmpty(result, "maintenancePlan"),
                metricOrEmpty(result, "dataFileCountBefore", "dataFileCountAfter", "manifestCountBefore", "manifestCountAfter", "metadataJsonCountBefore", "metadataJsonCountAfter", "hdfsDiskBytesBefore", "hdfsDiskBytesAfter") + "<br>" + auditMetricPlan(result),
                queryMetrics(result),
                result.conclusion(),
                status,
                evidenceDetails(result)
            );
            default -> List.of(code(result.caseId()), result.conclusion(), status, evidenceDetails(result));
        };
    }

    private static String requirementElements(IcebergValidationResult result) {
        StringBuilder builder = new StringBuilder();
        builder.append(escapeHtml(scenarioRequirement(result.scenario())));
        if (!result.dataScale().isEmpty()) {
            builder.append("<br>").append(joinMap(result.dataScale()));
        }
        return builder.toString();
    }

    private static String schemaChangeSummary(IcebergValidationResult result) {
        String metrics = mapValues(result.metrics(), "schemaChangeType", "changeCount");
        String ddlSummary = result.actionCommands().stream()
            .limit(3)
            .map(IcebergValidationReportWriter::escapeHtml)
            .collect(Collectors.joining("<br>"));
        if (ddlSummary.isEmpty()) {
            return metrics;
        }
        if (metrics.isEmpty()) {
            return ddlSummary;
        }
        return metrics + "<br>" + ddlSummary;
    }

    private static String schemaHistoricalReadSummary(IcebergValidationResult result) {
        String values = mapValues(result.metrics(), "baselineRows", "currentRows", "snapshotCount");
        String assertions = joinList(result.assertions());
        if (assertions.isEmpty()) {
            return values;
        }
        if (values.isEmpty()) {
            return assertions;
        }
        return values + "<br>" + assertions;
    }

    private static String schemaMetadataMetrics(IcebergValidationResult result) {
        return mapValues(
            result.metrics(),
            "currentQuerySeconds",
            "historicalQuerySeconds",
            "snapshotCount",
            "schemaHistoryLength",
            "metadataJsonCount"
        );
    }

    private static String ecPolicyCell(IcebergValidationResult result) {
        String policy = result.metrics().get("policy");
        if (policy != null && !policy.isBlank()) {
            return escapeHtml(policy);
        }
        return mapValues(result.metrics(), "ecPolicyCount");
    }

    private static String ecReplicationBaseline(IcebergValidationResult result) {
        return mapValues(result.metrics(), "replicationBaseline");
    }

    private static String ecDataNodeRequirements(IcebergValidationResult result) {
        return mapValues(result.metrics(), "liveDataNodes", "requiredDataNodes");
    }

    private static String ecFileCountMetrics(IcebergValidationResult result) {
        return mapValues(result.metrics(), "replicationFileCount", "ecFileCount", "targetRowCount", "targetChecksum");
    }

    private static String ecDiskUsageMetrics(IcebergValidationResult result) {
        return mapValues(result.metrics(), "replicationDiskBytes", "ecDiskBytes", "diskSavingRatio", "hdfsUsageStatus");
    }

    private static String ecSkipReason(IcebergValidationResult result) {
        String skipReason = result.metrics().get("skipReason");
        if (skipReason != null && !skipReason.isBlank()) {
            return escapeHtml("skipReason=" + skipReason);
        }
        return skipReason(result);
    }

    private static String metricOrEmpty(IcebergValidationResult result, String... keys) {
        return mapValues(result.metrics(), keys);
    }

    private static String metricOrAudit(IcebergValidationResult result, String... keys) {
        String values = metricOrEmpty(result, keys);
        return values.isEmpty() ? auditMetricPlan(result) : values;
    }

    private static String appendWithBreak(String first, String second) {
        if (first == null || first.isBlank()) {
            return second == null ? "" : second;
        }
        if (second == null || second.isBlank()) {
            return first;
        }
        return first + "<br>" + second;
    }

    private static String auditMetricPlan(IcebergValidationResult result) {
        return mapValues(
            result.metrics(),
            "executed",
            "metricCollectionStatus",
            "notExecutedReason",
            "expectedMetricFields",
            "plannedActionCount"
        );
    }

    private static String mapValues(Map<String, String> values, String... keys) {
        StringBuilder builder = new StringBuilder();
        for (String key : keys) {
            if (key == null) {
                continue;
            }
            if (isPlaceholderMetricKey(key)) {
                continue;
            }
            String value = values.get(key);
            if (value != null && !value.isBlank()) {
                if (!builder.isEmpty()) {
                    builder.append("<br>");
                }
                builder.append(escapeHtml(key)).append("=").append(escapeHtml(value));
            }
        }
        return builder.toString();
    }

    private static String firstPresent(IcebergValidationResult result, String evidencePrefix, String metricKey, String fallback) {
        if (evidencePrefix != null) {
            for (String evidence : result.evidence()) {
                if (evidence.startsWith(evidencePrefix + "=")) {
                    return escapeHtml(evidence.substring((evidencePrefix + "=").length()));
                }
            }
        }
        if (metricKey != null && result.metrics().containsKey(metricKey) && !isPlaceholderMetricKey(metricKey)) {
            return escapeHtml(result.metrics().get(metricKey));
        }
        return fallback == null ? "" : escapeHtml(fallback);
    }

    private static String joinBaselineAndComparison(IcebergValidationResult result) {
        String baseline = joinMap(result.baseline());
        String comparison = joinMap(result.comparison());
        if (baseline.isEmpty()) {
            return comparison;
        }
        if (comparison.isEmpty()) {
            return baseline;
        }
        return "Before/Baseline: " + baseline + "<br>After/Comparison: " + comparison;
    }

    private static String queryMetrics(IcebergValidationResult result) {
        String values = metricOrEmpty(
            result,
            "querySecondsBefore",
            "querySecondsAfter",
            "planningSecondsBefore",
            "planningSecondsAfter",
            "readLatencyRatio",
            "currentQuerySeconds",
            "historicalQuerySeconds"
        );
        return values.isEmpty() ? auditMetricPlan(result) : values;
    }

    private static String diskMetrics(IcebergValidationResult result) {
        String values = metricOrEmpty(result, "hdfsDiskBytes", "diskBytesBefore", "diskBytesAfter", "convertedDiskBytesBefore", "convertedDiskBytesAfter");
        return values.isEmpty() ? joinMap(result.metrics()) : values;
    }

    private static String skipReason(IcebergValidationResult result) {
        if (result.functionStatus() == IcebergConclusion.FunctionStatus.SKIPPED || !result.errors().isEmpty()) {
            String errors = joinList(result.errors());
            return errors.isEmpty() ? escapeHtml(result.conclusion()) : errors;
        }
        return "";
    }

    private static String statusCell(IcebergValidationResult result) {
        return "<span class=\"status-" + result.functionStatus() + "\">"
            + result.functionStatus()
            + "</span><br>"
            + escapeHtml(result.performanceStatus().toString());
    }

    private static String evidenceDetails(IcebergValidationResult result) {
        List<IcebergExecutionEvidence> executionResults = result.executionResults() == null
            ? List.of()
            : result.executionResults();
        if (!executionResults.isEmpty()) {
            String rows = executionResults.stream()
                .map(IcebergValidationReportWriter::executionEvidenceBlock)
                .collect(Collectors.joining());
            return "<details class=\"evidence-details\"><summary>展开证据</summary>"
                + "<div class=\"evidence-block\">" + rows + "</div></details>";
        }
        return "<details class=\"evidence-details\"><summary>展开证据</summary>"
            + "<div class=\"evidence-block\">"
            + evidenceGroup("Setup Commands", result.setupCommands())
            + evidenceGroup("Action Commands", result.actionCommands())
            + evidenceGroup("Evidence", result.evidence())
            + evidenceGroup("Errors", result.errors())
            + "</div></details>";
    }

    private static String evidenceGroup(String title, List<String> values) {
        return "<div><strong>" + escapeHtml(title) + "</strong>" + pre(values) + "</div>";
    }

    private static String executionEvidenceBlock(IcebergExecutionEvidence evidence) {
        return "<div class=\"execution-evidence\">"
            + "<strong>" + escapeHtml(evidence.phase() + " - " + evidence.label()) + "</strong>"
            + "<div><strong>执行脚本</strong>" + pre(List.of(evidence.script())) + "</div>"
            + "<div><strong>执行结果</strong>"
            + "<pre>exitCode=" + evidence.exitCode()
            + "\ndurationSeconds=" + String.format(java.util.Locale.ROOT, "%.3f", evidence.durationSeconds())
            + "\nstdout:\n" + escapeHtml(evidence.stdout())
            + "\nstderr:\n" + escapeHtml(evidence.stderr())
            + "</pre></div>"
            + "</div>";
    }

    private static String conversionHdfsUsage(IcebergValidationResult result) {
        String values = mapValues(result.metrics(), "fileCountBefore", "fileCountAfter", "diskBytesBefore", "diskBytesAfter");
        if (!values.isEmpty()) {
            return values;
        }
        return conversionPending(result, "hdfsUsageStatus");
    }

    private static String conversionDurationAndThroughput(IcebergValidationResult result) {
        if (result.metrics().containsKey("conversionSeconds") || result.metrics().containsKey("throughputMbPerSecond")) {
            return mapValues(result.metrics(), "conversionSeconds", "throughputMbPerSecond", "policyCommandStatus");
        }
        return conversionPending(result, "conversionStatus", "policyCommandStatus");
    }

    private static String conversionQueryAndChecksum(IcebergValidationResult result) {
        String values = mapValues(result.metrics(), "querySecondsBefore", "querySecondsAfter", "checksumMatched");
        if (!values.isEmpty()) {
            return values;
        }
        return conversionPending(result, "checksumStatus", "skipReason");
    }

    private static String conversionPending(IcebergValidationResult result, String... keys) {
        String values = mapValues(result.metrics(), keys);
        if (values.isEmpty()) {
            return "待真实执行采集";
        }
        return values + "<br>待真实执行采集";
    }

    private static String conversionDirection(IcebergValidationResult result) {
        String direction = result.metrics().get("conversionDirection");
        if (direction != null && !direction.isBlank()) {
            return escapeHtml("conversionDirection=" + direction);
        }
        if (result.caseId().startsWith("replication-to-ec")) {
            return "replication -> EC";
        }
        if (result.caseId().startsWith("ec-to-replication")) {
            return "EC -> replication";
        }
        return "";
    }

    private static String conversionMode(IcebergValidationResult result) {
        String values = mapValues(result.metrics(), "conversionMode", "targetPolicy", "targetReplication");
        if (!values.isEmpty()) {
            return values;
        }
        return result.caseId().contains("policy-only") ? "policy-only" : "physical rewrite";
    }

    private static String writeMode(IcebergValidationResult result) {
        if (result.caseId().contains("same-partition")) {
            return "same partition append";
        }
        if (result.caseId().contains("disjoint-partitions")) {
            return "disjoint partition append";
        }
        if (result.caseId().contains("mixed")) {
            return "read + write";
        }
        return result.purpose();
    }

    private static String conflictType(IcebergValidationResult result) {
        if (result.caseId().contains("overlap")) {
            return "overlapping update conflict";
        }
        if (result.caseId().contains("same-partition")) {
            return "same partition commit contention";
        }
        return "";
    }

    private static String mutationOperation(IcebergValidationResult result) {
        if (result.caseId().contains("merge")) {
            return "MERGE";
        }
        if (result.caseId().contains("update")) {
            return "UPDATE";
        }
        if (result.caseId().contains("delete")) {
            return "DELETE";
        }
        return "";
    }

    private static String acidType(IcebergValidationResult result) {
        if (result.caseId().contains("kill-before")) {
            return "kill before commit";
        }
        if (result.caseId().contains("kill-during")) {
            return "kill during commit";
        }
        if (result.caseId().contains("conflicting")) {
            return "conflicting commits";
        }
        return "reader isolation";
    }

    private static String atomicAssertion(IcebergValidationResult result) {
        return result.assertions().stream()
            .filter(value -> value.contains("snapshot") || value.contains("half-visible") || value.contains("old snapshot"))
            .map(IcebergValidationReportWriter::escapeHtml)
            .collect(Collectors.joining("<br>"));
    }

    private static String isolationAssertion(IcebergValidationResult result) {
        String assertions = joinList(result.assertions());
        return assertions.isEmpty() ? joinList(result.evidence()) : assertions;
    }

    private static String incrementalChangeType(IcebergValidationResult result) {
        if (result.caseId().contains("append-only")) {
            return "append-only";
        }
        if (result.caseId().contains("delete-update")) {
            return "update/delete boundary";
        }
        if (result.caseId().contains("expired")) {
            return "expired base snapshot";
        }
        return "multi-snapshot";
    }

    private static String expiredSnapshotBehavior(IcebergValidationResult result) {
        return result.caseId().contains("expired") || result.caseId().contains("expire")
            ? result.conclusion()
            : joinList(result.assertions());
    }

    private static String timeTravelSelector(IcebergValidationResult result) {
        if (result.caseId().contains("snapshot-id")) {
            return "snapshot id";
        }
        if (result.caseId().contains("timestamp")) {
            return "timestamp";
        }
        if (result.caseId().contains("schema")) {
            return "after schema evolution";
        }
        return "after expire";
    }

    private static String compactionType(IcebergValidationResult result) {
        if (result.caseId().contains("data-compaction")) {
            return "rewrite_data_files";
        }
        if (result.caseId().contains("manifest")) {
            return "rewrite_manifests";
        }
        if (result.caseId().contains("expire")) {
            return "expire_snapshots";
        }
        if (result.caseId().contains("query")) {
            return "query degradation measurement";
        }
        return "multi-snapshot build";
    }

    private static String joinMap(Map<String, String> values) {
        if (values == null || values.isEmpty()) {
            return "";
        }
        return values.entrySet().stream()
            .filter(entry -> !isPlaceholderMetricKey(entry.getKey()))
            .map(entry -> escapeHtml(entry.getKey()) + "=" + escapeHtml(entry.getValue()))
            .collect(Collectors.joining("<br>"));
    }

    private static boolean isPlaceholderMetricKey(String key) {
        return PLACEHOLDER_METRIC_KEYS.contains(key);
    }

    private static String joinList(List<String> values) {
        if (values == null || values.isEmpty()) {
            return "";
        }
        return values.stream().map(IcebergValidationReportWriter::escapeHtml).collect(Collectors.joining("<br>"));
    }

    private static String pre(List<String> values) {
        if (values == null || values.isEmpty()) {
            return "";
        }
        return "<pre>" + escapeHtml(String.join("\n\n", values)) + "</pre>";
    }

    private static String code(String value) {
        return "<code>" + escapeHtml(value) + "</code>";
    }

    private static String cell(String value) {
        return "<td>" + (value == null ? "" : value) + "</td>";
    }

    private static String escapeHtml(String value) {
        if (value == null) {
            return "";
        }
        return value
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\"", "&quot;")
            .replace("'", "&#39;");
    }

    private record ScenarioSection(String scenario, String title, String method, List<String> headers) {}
}

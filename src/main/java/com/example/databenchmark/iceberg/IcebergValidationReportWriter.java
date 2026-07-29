package com.example.databenchmark.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergValidationReportWriter {
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
                .table-wrap { overflow-x: auto; border: 1px solid var(--border); border-radius: 6px; }
                table { width: 100%; border-collapse: collapse; min-width: 980px; }
                th, td { border-bottom: 1px solid var(--border); border-right: 1px solid var(--border); padding: 9px 10px; text-align: left; vertical-align: top; font-size: 13px; line-height: 1.45; }
                th { background: var(--header); font-weight: 700; white-space: nowrap; }
                td:last-child, th:last-child { border-right: 0; }
                tr:last-child td { border-bottom: 0; }
                code { font-family: Consolas, "Liberation Mono", monospace; font-size: 12px; }
                pre { margin: 0; white-space: pre-wrap; font-family: Consolas, "Liberation Mono", monospace; font-size: 12px; line-height: 1.4; }
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

        builder.append("""
            <section>
              <h2>验证项总览</h2>
              <div class="table-wrap">
              <table>
                <thead><tr>
                  <th>场景</th><th>用例</th><th>验证目标</th><th>关键需求要素</th>
                  <th>功能状态</th><th>性能状态</th><th>性能指标</th><th>基线</th><th>对比</th><th>性能结论</th>
                </tr></thead>
                <tbody>
            """);
        for (IcebergValidationResult result : report.results()) {
            builder.append("<tr>")
                .append(cell(scenarioDisplayName(result.scenario())))
                .append(cell(code(result.caseId())))
                .append(cell(result.purpose()))
                .append(cell(requirementElements(result)))
                .append(cell("<span class=\"status-" + result.functionStatus() + "\">" + result.functionStatus() + "</span>"))
                .append(cell(result.performanceStatus().toString()))
                .append(cell(joinMap(result.metrics())))
                .append(cell(joinMap(result.baseline())))
                .append(cell(joinMap(result.comparison())))
                .append(cell(result.conclusion()))
                .append("</tr>");
        }
        builder.append("</tbody></table></div></section>");

        builder.append("""
            <section>
              <h2>需求要素矩阵</h2>
              <div class="table-wrap">
              <table>
                <thead><tr>
                  <th>场景</th><th>用例</th><th>需求分析关键要素</th><th>断言</th><th>指标项</th><th>基线与对比</th>
                </tr></thead>
                <tbody>
            """);
        for (IcebergValidationResult result : report.results()) {
            builder.append("<tr>")
                .append(cell(scenarioDisplayName(result.scenario())))
                .append(cell(code(result.caseId())))
                .append(cell(requirementElements(result)))
                .append(cell(joinList(result.assertions())))
                .append(cell(joinMap(result.metrics())))
                .append(cell("Baseline: " + joinMap(result.baseline()) + "<br>Comparison: " + joinMap(result.comparison())))
                .append("</tr>");
        }
        builder.append("</tbody></table></div></section>");

        builder.append("""
            <section>
              <h2>执行脚本与证据</h2>
              <div class="table-wrap">
              <table>
                <thead><tr>
                  <th>场景</th><th>用例</th><th>Setup Commands</th><th>Action Commands</th><th>Evidence</th><th>Errors</th>
                </tr></thead>
                <tbody>
            """);
        for (IcebergValidationResult result : report.results()) {
            builder.append("<tr>")
                .append(cell(scenarioDisplayName(result.scenario())))
                .append(cell(code(result.caseId())))
                .append(cell(pre(result.setupCommands())))
                .append(cell(pre(result.actionCommands())))
                .append(cell(pre(result.evidence())))
                .append(cell(pre(result.errors())))
                .append("</tr>");
        }
        builder.append("</tbody></table></div></section>");
        builder.append("</main></body></html>");
        return builder.toString();
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

    private static String requirementElements(IcebergValidationResult result) {
        StringBuilder builder = new StringBuilder();
        builder.append(escapeHtml(scenarioRequirement(result.scenario())));
        if (!result.dataScale().isEmpty()) {
            builder.append("<br>").append(joinMap(result.dataScale()));
        }
        return builder.toString();
    }

    private static String joinMap(Map<String, String> values) {
        if (values == null || values.isEmpty()) {
            return "";
        }
        return values.entrySet().stream()
            .map(entry -> escapeHtml(entry.getKey()) + "=" + escapeHtml(entry.getValue()))
            .collect(Collectors.joining("<br>"));
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
}

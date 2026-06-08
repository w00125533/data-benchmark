package com.example.databenchmark.report;

import com.example.databenchmark.runner.BenchmarkRoute;
import java.util.List;
import java.util.Map;

public interface TableRuntimeMetadataCollector {
    List<BenchmarkReport.TableRuntimeInfo> collectKpi(
        Map<BenchmarkRoute, String> routeFailures,
        long rows,
        long bytes
    );

    List<BenchmarkReport.TableRuntimeInfo> collectTpch(long rows, long bytes);
}

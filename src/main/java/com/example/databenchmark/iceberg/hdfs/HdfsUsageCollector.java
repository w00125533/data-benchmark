package com.example.databenchmark.iceberg.hdfs;

public final class HdfsUsageCollector {
    private HdfsUsageCollector() {}

    public record Usage(long logicalBytes, long diskBytes, long directoryCount, long fileCount) {}

    public static Usage parse(String duOutput, String countOutput) {
        try {
            String[] du = firstLine(duOutput).trim().split("\\s+");
            String[] count = firstLine(countOutput).trim().split("\\s+");
            if (du.length < 2 || count.length < 3) {
                throw new IllegalArgumentException();
            }
            long logicalBytes = Long.parseLong(du[0]);
            long diskBytes = Long.parseLong(du[1]);
            long directoryCount = Long.parseLong(count[0]);
            long fileCount = Long.parseLong(count[1]);
            return new Usage(logicalBytes, diskBytes, directoryCount, fileCount);
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException("Unable to parse HDFS usage output", exception);
        }
    }

    private static String firstLine(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException();
        }
        return value.lines().filter(line -> !line.isBlank()).findFirst().orElseThrow(IllegalArgumentException::new);
    }
}

package com.example.databenchmark.generator;

import java.nio.file.Path;
import java.util.List;

public record DatasetResult(Path outputPath, List<Path> files, long rows, long bytesWritten) {}

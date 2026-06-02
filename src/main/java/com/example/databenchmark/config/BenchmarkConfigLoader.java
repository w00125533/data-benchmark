package com.example.databenchmark.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;

public class BenchmarkConfigLoader {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory()).findAndRegisterModules();

    public BenchmarkConfig load(Path path) throws IOException {
        return mapper.readValue(path.toFile(), BenchmarkConfig.class);
    }
}

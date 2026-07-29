package com.example.databenchmark.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergValidationConfigLoaderTest {
    @TempDir
    Path tempDir;

    @Test
    void rejectsUnsupportedIcebergVersion() throws Exception {
        Path config = tempDir.resolve("iceberg-validation.yml");
        Files.writeString(config, """
            iceberg:
              version: "1.7.1"
              catalog: "iceberg_catalog"
              namespace: "iceberg_validation"
              warehouse: "hdfs://hdfs-namenode:8020/warehouse/iceberg"
              formatVersion: 2
            spark:
              service: "spark"
              timeoutSeconds: 900
              packages:
                - "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1"
              extensions:
                - "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            hdfs:
              defaultFs: "hdfs://hdfs-namenode:8020"
              replicationBaseline: 2
              ecPolicies: ["RS-10-4-1024k"]
            scale:
              profile: "smoke"
              rows: 100000
              partitions: 8
              smallFileCommits: 100
              filesPerCommit: 4
              concurrentWriters: [2, 4, 8]
            scenarios:
              schemaEvolution:
                enabled: true
            report:
              output: "reports/iceberg-validation"
              formats: ["json", "html"]
            """);

        assertThatThrownBy(() -> new IcebergValidationConfigLoader().load(config))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("iceberg.version must be 1.10.1");
    }

    @Test
    void loadsDefaultIcebergValidationConfig() throws Exception {
        IcebergValidationConfig config = new IcebergValidationConfigLoader()
            .load(Path.of("configs", "iceberg-validation.yml"));

        assertThat(config.iceberg().version()).isEqualTo("1.10.1");
        assertThat(config.hdfs().replicationBaseline()).isEqualTo(2);
        assertThat(config.hdfs().ecPolicies()).contains("RS-10-4-1024k");
        assertThat(config.scale().smallFileCommits()).isEqualTo(100);
        assertThat(config.scenarios()).containsKeys(
            "schemaEvolution",
            "erasureCoding",
            "erasureCodingConversion",
            "smallFileCompaction"
        );
    }
}

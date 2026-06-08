package com.example.databenchmark;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SharedInfraTopologyTest {
    private static final Path SHARED_INFRA = Path.of("..", "shared-data-infra");
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @SuppressWarnings("unchecked")
    @Test
    void sharedInfraProvidesBenchmarkCompatibleLakehouseTools() throws Exception {
        Map<String, Object> lakehouse = mapper.readValue(
            SHARED_INFRA.resolve("compose.lakehouse.yaml").toFile(),
            new TypeReference<>() {}
        );
        Map<String, Object> services = (Map<String, Object>) lakehouse.get("services");

        assertThat(services).containsKeys(
            "hms-db", "namenode", "datanode", "hive-metastore", "hdfs-init", "hive-server", "spark"
        );

        Map<String, Object> namenode = service(services, "namenode");
        assertThat(list(map(map(namenode.get("networks")).get("default")), "aliases"))
            .contains("hdfs-namenode");

        Map<String, Object> hdfsInit = service(services, "hdfs-init");
        assertThat(stringList(hdfsInit, "profiles")).contains("lakehouse");
        assertThat(String.join(" ", stringList(hdfsInit, "command")))
            .contains("hdfs dfs -fs hdfs://hdfs-namenode:8020 -mkdir -p /warehouse/iceberg /services/data-benchmark/generated")
            .contains("hdfs dfs -fs hdfs://hdfs-namenode:8020 -chmod 777 /warehouse /warehouse/iceberg /services /services/data-benchmark /services/data-benchmark/generated");
        assertThat(dependencyCondition(hdfsInit, "datanode")).isEqualTo("service_healthy");

        Map<String, Object> hiveMetastore = service(services, "hive-metastore");
        assertThat(map(hiveMetastore.get("environment")).get("SERVICE_OPTS").toString())
            .contains("-Dmetastore.metastore.event.db.notification.api.auth=false");

        Map<String, Object> hiveServer = service(services, "hive-server");
        assertThat(hiveServer.get("image")).isEqualTo("apache/hive:4.0.0");
        assertThat(stringList(hiveServer, "entrypoint")).containsExactly("bash", "-lc");
        assertThat(map(hiveServer.get("environment")))
            .containsEntry("IS_RESUME", "true")
            .containsEntry("HADOOP_HEAPSIZE", "2048")
            .containsEntry("SKIP_SCHEMA_INIT", "true")
            .doesNotContainEntry("SERVICE_NAME", "hiveserver2");
        assertThat(map(hiveServer.get("environment")).get("SERVICE_OPTS").toString())
            .contains("-Dhive.server2.enable.doAs=false");
        assertThat(String.join("\n", stringList(hiveServer, "command")))
            .contains("exec /opt/hive/bin/hive --skiphadoopversion --skiphbasecp --service hiveserver2")
            .contains("--hiveconf hive.metastore.uris=thrift://hive-metastore:9083")
            .contains("--hiveconf metastore.metastore.event.db.notification.api.auth=false")
            .contains("--hiveconf hive.server2.enable.doAs=false")
            .contains("--hiveconf hive.server2.thrift.bind.host=0.0.0.0")
            .contains("--hiveconf hive.server2.thrift.port=10000");
        assertThat(dependencyCondition(hiveServer, "hdfs-init")).isEqualTo("service_completed_successfully");

        Map<String, Object> spark = service(services, "spark");
        assertThat(spark.get("image")).isEqualTo("apache/spark:3.5.8-java17");
        assertThat(spark.get("hostname")).isEqualTo("spark");
        assertThat(spark.get("working_dir")).isEqualTo("/workspace");
        assertThat(stringList(spark, "command")).containsExactly("sleep", "infinity");
        assertThat(map(spark.get("environment"))).doesNotContainKey("JAVA_TOOL_OPTIONS");
        assertThat(stringList(spark, "volumes"))
            .contains("${BENCHMARK_WORKSPACE:-../data-benchmark}:/workspace");
        assertThat(dependencyCondition(spark, "hdfs-init")).isEqualTo("service_completed_successfully");
        assertThat(map(map(spark.get("deploy")).get("resources")).get("limits"))
            .isEqualTo(Map.of("cpus", "16", "memory", "18GB"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void sharedInfraProvidesSplitStarRocks() throws Exception {
        Map<String, Object> starrocks = mapper.readValue(
            SHARED_INFRA.resolve("compose.starrocks.yaml").toFile(),
            new TypeReference<>() {}
        );
        Map<String, Object> services = (Map<String, Object>) starrocks.get("services");

        assertThat(services.keySet()).containsExactlyInAnyOrder("starrocks-fe", "starrocks-be");

        Map<String, Object> fe = service(services, "starrocks-fe");
        assertThat(fe.get("image")).isEqualTo("starrocks/fe-ubuntu:3.3-latest");
        assertThat(fe.get("hostname")).isEqualTo("starrocks-fe-0");
        assertThat(stringList(fe, "profiles")).contains("starrocks");
        assertThat(stringList(fe, "ports"))
            .contains("${STARROCKS_HTTP_PORT:-8030}:8030", "${STARROCKS_MYSQL_PORT:-9030}:9030");
        assertThat(String.join("\n", stringList(fe, "command")))
            .contains("sed -i '/^JAVA_OPTS=/d' /opt/starrocks/fe/conf/fe.conf")
            .contains("JAVA_OPTS=\"-Dlog4j2.formatMsgNoLookups=true -Xmx1536m -XX:+UseG1GC -Djava.security.policy=/opt/starrocks/fe/conf/udf_security.policy\"")
            .contains("exec /opt/starrocks/fe_entrypoint.sh starrocks-fe");

        Map<String, Object> be = service(services, "starrocks-be");
        assertThat(be.get("image")).isEqualTo("starrocks/be-ubuntu:3.3-latest");
        assertThat(be.get("hostname")).isEqualTo("starrocks-be-0");
        assertThat(stringList(be, "profiles")).contains("starrocks");
        assertThat(String.join("\n", stringList(be, "command")))
            .contains("trash_file_expire_time_sec = 1800")
            .contains("exec /opt/starrocks/be_entrypoint.sh starrocks-fe");
        assertThat(stringList(be, "ports")).contains("${STARROCKS_BE_HTTP_PORT:-8040}:8040");
        assertThat(stringList(be, "volumes")).contains("starrocks-be-storage:/opt/starrocks/be/storage");
        assertThat(dependencyNames(be)).contains("starrocks-fe");
        assertThat(map(starrocks.get("volumes"))).containsKey("starrocks-be-storage");
    }

    @Test
    void sharedCommonEnvUsesUnifiedEndpoints() throws Exception {
        String env = Files.readString(SHARED_INFRA.resolve(Path.of("env", "common.env")));

        assertThat(env)
            .contains("HDFS_DEFAULTFS=hdfs://hdfs-namenode:8020")
            .contains("HIVE_METASTORE_URI=thrift://hive-metastore:9083")
            .contains("STARROCKS_JDBC_URL=jdbc:mysql://starrocks-fe:9030/");
        assertThat(env).doesNotContain("STARROCKS_JDBC_URL=jdbc:mysql://starrocks:9030/");
    }

    private Map<String, Object> service(Map<String, Object> services, String name) {
        assertThat(services).containsKey(name);
        return map(services.get(name));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> map(Object value) {
        return (Map<String, Object>) value;
    }

    private List<Object> list(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return List.of();
        }
        return list(value);
    }

    @SuppressWarnings("unchecked")
    private List<Object> list(Object value) {
        return (List<Object>) value;
    }

    private List<String> stringList(Map<String, Object> map, String key) {
        return list(map, key).stream()
            .map(Object::toString)
            .toList();
    }

    private List<String> dependencyNames(Map<String, Object> service) {
        Object dependsOn = service.get("depends_on");
        if (dependsOn == null) {
            return List.of();
        }
        if (dependsOn instanceof Map<?, ?> dependsOnMap) {
            return dependsOnMap.keySet().stream()
                .map(Object::toString)
                .toList();
        }
        return list(dependsOn).stream()
            .map(Object::toString)
            .toList();
    }

    private String dependencyCondition(Map<String, Object> service, String dependency) {
        Object dependsOn = service.get("depends_on");
        assertThat(dependsOn).isInstanceOf(Map.class);
        Map<String, Object> dependencies = map(dependsOn);
        assertThat(dependencies).containsKey(dependency);
        return map(dependencies.get(dependency)).get("condition").toString();
    }
}

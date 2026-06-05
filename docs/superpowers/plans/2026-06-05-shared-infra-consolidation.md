# Shared Infra Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move benchmark infrastructure services into `../shared-data-infra`, update shared services to the benchmark-compatible stack, and make the benchmark runner control shared infra through configurable Docker Compose files.

**Architecture:** `shared-data-infra` owns HDFS, Hive Metastore, `hdfs-init`, HiveServer2, Spark, and split StarRocks FE/BE. `data-benchmark` owns only the runner container and application code. Java lifecycle control builds Docker Compose commands from a small configuration object instead of hard-coding `docker-compose.yml`.

**Tech Stack:** Docker Compose, Java 17, JUnit 5, AssertJ, Jackson YAML, Maven, Apache Spark 3.5.8 Java 17 image, Apache Hive 4.0.0 image, StarRocks 3.3 FE/BE images.

---

## File Map

### `../shared-data-infra`

- Modify `compose.lakehouse.yaml`: add shared `hdfs-init`; convert `hive-server` to explicit command; convert `spark` to long-running executable service.
- Modify `compose.starrocks.yaml`: replace all-in-one `starrocks` with `starrocks-fe` and `starrocks-be`.
- Modify `env/common.env`: change HDFS and StarRocks endpoints to shared benchmark-compatible service names.
- Modify `README.md`: document unified benchmark-compatible profiles, endpoints, startup commands, and changed Spark/StarRocks usage.
- Modify `AGENTS.md`: keep shared infrastructure ownership rule clear after unifying services.

### `data-benchmark`

- Create `src/test/java/com/example/databenchmark/SharedInfraTopologyTest.java`: validates `../shared-data-infra` compose files from the benchmark repo test suite.
- Modify `src/test/java/com/example/databenchmark/ComposeTopologyTest.java`: assert local compose only owns `benchmark-runner` and external shared network.
- Modify `docker-compose.yml`: remove local infrastructure services and keep only `benchmark-runner`.
- Modify `src/test/java/com/example/databenchmark/runner/ComposeServiceControllerTest.java`: update command assertions for shared infra compose targeting and env/config parsing.
- Modify `src/main/java/com/example/databenchmark/runner/ComposeServiceController.java`: add configurable Docker Compose target and use it in restart/readiness commands.
- Modify `README.md`: document shared infra prerequisite, compose commands, runner overrides, and resource ownership.
- Modify `AGENTS.md`: remove old project-local infra exceptions.

## Task 1: Add Failing Shared Infra Topology Test

**Files:**
- Create: `src/test/java/com/example/databenchmark/SharedInfraTopologyTest.java`

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/example/databenchmark/SharedInfraTopologyTest.java` with this content:

```java
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
            .contains("hdfs dfs -fs hdfs://hdfs-namenode:8020 -mkdir -p /warehouse/iceberg")
            .contains("hdfs dfs -fs hdfs://hdfs-namenode:8020 -chmod -R 777 /warehouse");
        assertThat(dependencyCondition(hdfsInit, "datanode")).isEqualTo("service_healthy");

        Map<String, Object> hiveServer = service(services, "hive-server");
        assertThat(hiveServer.get("image")).isEqualTo("apache/hive:4.0.0");
        assertThat(stringList(hiveServer, "entrypoint")).containsExactly("bash", "-lc");
        assertThat(map(hiveServer.get("environment")))
            .containsEntry("IS_RESUME", "true")
            .containsEntry("SKIP_SCHEMA_INIT", "true")
            .doesNotContainEntry("SERVICE_NAME", "hiveserver2");
        assertThat(String.join("\n", stringList(hiveServer, "command")))
            .contains("exec /opt/hive/bin/hive --skiphadoopversion --skiphbasecp --service hiveserver2")
            .contains("--hiveconf hive.metastore.uris=thrift://hive-metastore:9083")
            .contains("--hiveconf hive.server2.thrift.bind.host=0.0.0.0")
            .contains("--hiveconf hive.server2.thrift.port=10000");
        assertThat(dependencyCondition(hiveServer, "hdfs-init")).isEqualTo("service_completed_successfully");

        Map<String, Object> spark = service(services, "spark");
        assertThat(spark.get("image")).isEqualTo("apache/spark:3.5.8-java17");
        assertThat(spark.get("hostname")).isEqualTo("spark");
        assertThat(spark.get("working_dir")).isEqualTo("/workspace");
        assertThat(stringList(spark, "command")).containsExactly("sleep", "infinity");
        assertThat(stringList(spark, "volumes"))
            .contains("${BENCHMARK_WORKSPACE:-../data-benchmark}:/workspace");
        assertThat(dependencyCondition(spark, "hdfs-init")).isEqualTo("service_completed_successfully");
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
        assertThat(stringList(be, "command")).containsExactly("/opt/starrocks/be_entrypoint.sh", "starrocks-fe");
        assertThat(stringList(be, "ports")).contains("${STARROCKS_BE_HTTP_PORT:-8040}:8040");
        assertThat(dependencyNames(be)).contains("starrocks-fe");
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```sh
mvn -Dtest=SharedInfraTopologyTest test
```

Expected: FAIL because current shared infra still has all-in-one `starrocks`, Spark `apache/spark:3.5.4` with `entrypoint`, no `hdfs-init`, and `STARROCKS_JDBC_URL=jdbc:mysql://starrocks:9030/`.

- [ ] **Step 3: Commit the failing test**

```sh
git add src/test/java/com/example/databenchmark/SharedInfraTopologyTest.java
git commit -m "test: lock shared infra benchmark topology"
```

## Task 2: Convert Shared Infra Compose Services

**Files:**
- Modify: `../shared-data-infra/compose.lakehouse.yaml`
- Modify: `../shared-data-infra/compose.starrocks.yaml`
- Modify: `../shared-data-infra/env/common.env`

- [ ] **Step 1: Replace shared StarRocks all-in-one with FE/BE split**

Replace the full contents of `../shared-data-infra/compose.starrocks.yaml` with:

```yaml
services:
  starrocks-fe:
    image: starrocks/fe-ubuntu:3.3-latest
    hostname: starrocks-fe-0
    profiles: [starrocks]
    command:
      - bash
      - -lc
      - |
        sed -i '/^JAVA_OPTS=/d' /opt/starrocks/fe/conf/fe.conf
        printf '%s\n' 'JAVA_OPTS="-Dlog4j2.formatMsgNoLookups=true -Xmx1536m -XX:+UseG1GC -Djava.security.policy=/opt/starrocks/fe/conf/udf_security.policy"' >> /opt/starrocks/fe/conf/fe.conf
        exec /opt/starrocks/fe_entrypoint.sh starrocks-fe
    environment:
      LOG_CONSOLE: "1"
    ports:
      - "${STARROCKS_HTTP_PORT:-8030}:8030"
      - "${STARROCKS_MYSQL_PORT:-9030}:9030"
    deploy:
      resources:
        limits:
          cpus: "2"
          memory: 2GB

  starrocks-be:
    image: starrocks/be-ubuntu:3.3-latest
    hostname: starrocks-be-0
    profiles: [starrocks]
    command: ["/opt/starrocks/be_entrypoint.sh", "starrocks-fe"]
    environment:
      LOG_CONSOLE: "1"
    depends_on:
      - starrocks-fe
    ports:
      - "${STARROCKS_BE_HTTP_PORT:-8040}:8040"
    deploy:
      resources:
        limits:
          cpus: "6"
          memory: 5GB
```

- [ ] **Step 2: Add `hdfs-init` to shared lakehouse**

In `../shared-data-infra/compose.lakehouse.yaml`, insert this service after `hive-metastore`:

```yaml
  hdfs-init:
    image: apache/hadoop:3.3.6
    profiles: [lakehouse]
    command:
      - bash
      - -lc
      - |
        for i in $(seq 1 60); do
          hdfs dfs -fs hdfs://hdfs-namenode:8020 -mkdir -p /warehouse/iceberg && \
          hdfs dfs -fs hdfs://hdfs-namenode:8020 -chmod -R 777 /warehouse && exit 0
          sleep 2
        done
        exit 1
    environment:
      HADOOP_HOME: "/opt/hadoop"
      HADOOP_CONF_DIR: "/opt/hadoop/etc/hadoop"
      CORE-SITE.XML_fs.defaultFS: "hdfs://hdfs-namenode:8020"
    depends_on:
      datanode:
        condition: service_healthy
    networks:
      - default
```

- [ ] **Step 3: Replace shared `hive-server` service**

In `../shared-data-infra/compose.lakehouse.yaml`, replace the existing `hive-server` service with:

```yaml
  hive-server:
    image: apache/hive:4.0.0
    hostname: hive-server
    profiles: [lakehouse-tools]
    entrypoint: ["bash", "-lc"]
    command:
      - |
        export HIVE_CONF_DIR="$${HIVE_CONF_DIR:-$$HIVE_HOME/conf}"
        export IS_RESUME=true
        export SKIP_SCHEMA_INIT=true
        export HADOOP_CLIENT_OPTS="$$HADOOP_CLIENT_OPTS -Xmx1G $$SERVICE_OPTS"
        export HADOOP_CLASSPATH="$$TEZ_HOME/*:$$TEZ_HOME/lib/*:$$HADOOP_CLASSPATH"
        exec /opt/hive/bin/hive --skiphadoopversion --skiphbasecp --service hiveserver2 \
          --hiveconf hive.metastore.uris=thrift://hive-metastore:9083 \
          --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
          --hiveconf hive.server2.thrift.port=10000
    environment:
      IS_RESUME: "true"
      SKIP_SCHEMA_INIT: "true"
      SERVICE_OPTS: "-Dhive.metastore.uris=thrift://hive-metastore:9083 -Dhive.server2.thrift.bind.host=0.0.0.0 -Dhive.server2.thrift.port=10000"
    ports:
      - "${HIVE_SERVER_PORT:-10000}:10000"
    volumes:
      - ./docker/hive-conf/hive-site.xml:/opt/hive/conf/hive-site.xml:ro
      - ./docker/hadoop-conf/core-site.xml:/opt/hive/conf/core-site.xml:ro
      - ./docker/hadoop-conf/hdfs-site.xml:/opt/hive/conf/hdfs-site.xml:ro
    depends_on:
      hdfs-init:
        condition: service_completed_successfully
    healthcheck:
      test: ["CMD-SHELL", "bash -c 'exec 3<>/dev/tcp/localhost/10000' || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 30
      start_period: 60s
```

- [ ] **Step 4: Replace shared `spark` service**

In `../shared-data-infra/compose.lakehouse.yaml`, replace the existing `spark` service with:

```yaml
  spark:
    image: apache/spark:3.5.8-java17
    hostname: spark
    profiles: [spark-tools]
    working_dir: /workspace
    command: ["sleep", "infinity"]
    environment:
      SPARK_MODE: master
      HADOOP_CONF_DIR: /etc/hadoop
      HIVE_CONF_DIR: /etc/hive/conf
    volumes:
      - ${BENCHMARK_WORKSPACE:-../data-benchmark}:/workspace
      - ./docker/hadoop-conf/core-site.xml:/etc/hadoop/core-site.xml:ro
      - ./docker/hadoop-conf/hdfs-site.xml:/etc/hadoop/hdfs-site.xml:ro
      - ./docker/hive-conf/hive-site.xml:/etc/hive/conf/hive-site.xml:ro
    depends_on:
      hdfs-init:
        condition: service_completed_successfully
    deploy:
      resources:
        limits:
          cpus: "6"
          memory: 3GB
```

- [ ] **Step 5: Update shared common env endpoints**

Change `../shared-data-infra/env/common.env` to:

```dotenv
HDFS_DEFAULTFS=hdfs://hdfs-namenode:8020
HIVE_METASTORE_URI=thrift://hive-metastore:9083
YARN_RM_URL=http://resourcemanager:8088
KAFKA_BOOTSTRAP=kafka:9092
STARROCKS_JDBC_URL=jdbc:mysql://starrocks-fe:9030/
NEO4J_URI=bolt://neo4j:7687
```

- [ ] **Step 6: Run shared topology test**

Run from `D:\agent-code\data-benchmark`:

```sh
mvn -Dtest=SharedInfraTopologyTest test
```

Expected: PASS.

- [ ] **Step 7: Run shared compose config checks**

Run from `D:\agent-code\shared-data-infra`:

```sh
docker compose -f compose.yaml -f compose.lakehouse.yaml --profile lakehouse --profile lakehouse-tools --profile spark-tools config
docker compose -f compose.yaml -f compose.starrocks.yaml --profile starrocks config
```

Expected: both commands exit 0 and print resolved Compose configuration.

- [ ] **Step 8: Commit shared infra compose changes**

Commit in `D:\agent-code\shared-data-infra`:

```sh
git add compose.lakehouse.yaml compose.starrocks.yaml env/common.env
git commit -m "refactor: unify benchmark shared infra services"
```

## Task 3: Shrink Data-Benchmark Compose Topology

**Files:**
- Modify: `src/test/java/com/example/databenchmark/ComposeTopologyTest.java`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Replace local compose topology test expectations**

In `src/test/java/com/example/databenchmark/ComposeTopologyTest.java`, change `EXPECTED_RESOURCE_LIMITS` to:

```java
    private static final Map<String, ResourceLimit> EXPECTED_RESOURCE_LIMITS = Map.of(
        "benchmark-runner", new ResourceLimit("2", "1GB")
    );
```

Inside `composeUsesHdfsWarehouseInsteadOfMinio`, replace the service key assertion with:

```java
        assertThat(services.keySet()).containsExactlyInAnyOrder("benchmark-runner");
```

Replace the removed service assertion with:

```java
        assertThat(services).doesNotContainKeys(
            "minio", REMOVED_METRICS_SERVICE, REMOVED_DASHBOARD_SERVICE,
            "hive-metastore", "hdfs-namenode", "hdfs-datanode",
            "hdfs-init", "hive-server", "spark", "starrocks-fe", "starrocks-be");
```

Replace the runner dependency assertion block with:

```java
        assertThat(dependencyNames(runner)).isEmpty();
```

Keep the runner build, volume, environment, command, Dockerfile, and network assertions.

Delete the old assertion blocks that inspect removed local `starrocks-fe`, `starrocks-be`, `spark`, `hive-server`, and `hdfs-init`.

Replace the default network assertion block with:

```java
        Map<String, Object> networks = map(compose.get("networks"));
        assertThat(networks).doesNotContainKey("default");
        assertThat(map(networks.get("shared-data-infra")))
            .containsEntry("external", true)
            .containsEntry("name", "shared-data-infra");
```

In `readmeDocumentsComposeHiveServerResourceLimitsAndEmbeddedReportData`, replace the final README assertions with:

```java
        assertThat(readme)
            .contains("docker compose -f docker-compose.yml build benchmark-runner")
            .contains("mvn package")
            .contains("/var/run/docker.sock")
            .contains("BENCHMARK_INFRA_COMPOSE_FILES")
            .contains("BENCHMARK_INFRA_PROJECT")
            .contains("BENCHMARK_WORKSPACE")
            .contains("shared-data-infra");
        assertThat(readme)
            .doesNotContain("This project still keeps `spark`, `hive-server`, and split `starrocks-fe`/`starrocks-be` services local")
            .doesNotContain("172.20.0.10")
            .doesNotContain("172.20.0.11")
            .doesNotContain("172.20.0.0/24");
```

- [ ] **Step 2: Run local compose topology test and verify it fails**

Run:

```sh
mvn -Dtest=ComposeTopologyTest test
```

Expected: FAIL because `docker-compose.yml` still includes local infrastructure services and README still describes local services.

- [ ] **Step 3: Replace `data-benchmark/docker-compose.yml`**

Replace `docker-compose.yml` with:

```yaml
name: databenchmark

services:
  benchmark-runner:
    build:
      context: ./docker/benchmark-runner
      dockerfile: Dockerfile
    working_dir: /workspace
    volumes:
      - .:/workspace
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      BENCHMARK_COMPOSE_IN_CONTAINER: "true"
      BENCHMARK_INFRA_PROJECT: "${BENCHMARK_INFRA_PROJECT:-shared-data-infra}"
      BENCHMARK_INFRA_COMPOSE_FILES: "${BENCHMARK_INFRA_COMPOSE_FILES:-../shared-data-infra/compose.yaml;../shared-data-infra/compose.lakehouse.yaml;../shared-data-infra/compose.starrocks.yaml}"
      STARROCKS_JDBC_URL: "jdbc:mysql://starrocks-fe:9030/?useSSL=false&allowPublicKeyRetrieval=true&allowMultiQueries=true"
      STARROCKS_STREAM_LOAD_URL: "http://starrocks-be:8040/api/sr_internal/cell_kpi_1min/_stream_load"
    command: ["java", "-jar", "target/data-benchmark-0.1.0-SNAPSHOT.jar", "run", "--mode", "compose", "--run-id", "compose-smoke"]
    networks:
      - shared-data-infra
    deploy:
      resources:
        limits:
          cpus: "2"
          memory: 1GB

networks:
  shared-data-infra:
    external: true
    name: shared-data-infra
```

- [ ] **Step 4: Run compose config**

Run:

```sh
docker compose -f docker-compose.yml config
```

Expected: PASS and resolved config contains only service `benchmark-runner`.

- [ ] **Step 5: Commit data-benchmark compose shrink**

Do not commit README yet because it is handled in a later documentation task.

```sh
git add docker-compose.yml src/test/java/com/example/databenchmark/ComposeTopologyTest.java
git commit -m "refactor: keep only benchmark runner compose service"
```

## Task 4: Make Runner Compose Target Configurable

**Files:**
- Modify: `src/test/java/com/example/databenchmark/runner/ComposeServiceControllerTest.java`
- Modify: `src/main/java/com/example/databenchmark/runner/ComposeServiceController.java`

- [ ] **Step 1: Add failing tests for shared compose command prefix**

In `ComposeServiceControllerTest`, add this test near the existing restart command tests:

```java
    @Test
    void restartCommandsUseConfiguredSharedInfraComposeTarget() {
        FakeCommandRunner commandRunner = new FakeCommandRunner();
        ComposeServiceController controller = new ComposeServiceController(
            commandRunner,
            Path.of("."),
            Duration.ofSeconds(30),
            3,
            Duration.ZERO,
            ignored -> { },
            new ComposeServiceController.InfraComposeTarget(
                "infra-project",
                List.of("../shared-data-infra/compose.yaml", "../shared-data-infra/compose.starrocks.yaml")
            )
        );

        assertThat(controller.restartCommands(BenchmarkRoute.STARROCKS_INTERNAL))
            .containsExactly(
                List.of("docker", "compose", "-p", "infra-project",
                    "-f", "../shared-data-infra/compose.yaml",
                    "-f", "../shared-data-infra/compose.starrocks.yaml",
                    "stop", "starrocks-be"),
                List.of("docker", "compose", "-p", "infra-project",
                    "-f", "../shared-data-infra/compose.yaml",
                    "-f", "../shared-data-infra/compose.starrocks.yaml",
                    "stop", "starrocks-fe"),
                List.of("docker", "compose", "-p", "infra-project",
                    "-f", "../shared-data-infra/compose.yaml",
                    "-f", "../shared-data-infra/compose.starrocks.yaml",
                    "start", "starrocks-fe"),
                List.of("docker", "compose", "-p", "infra-project",
                    "-f", "../shared-data-infra/compose.yaml",
                    "-f", "../shared-data-infra/compose.starrocks.yaml",
                    "start", "starrocks-be")
            );
    }
```

Add this test for default env parsing:

```java
    @Test
    void infraComposeTargetDefaultsToSharedInfraFiles() {
        ComposeServiceController.InfraComposeTarget target =
            ComposeServiceController.InfraComposeTarget.fromEnvironment(Map.of());

        assertThat(target.project()).isEqualTo("shared-data-infra");
        assertThat(target.files()).containsExactly(
            "../shared-data-infra/compose.yaml",
            "../shared-data-infra/compose.lakehouse.yaml",
            "../shared-data-infra/compose.starrocks.yaml"
        );
    }
```

Add this test for override parsing:

```java
    @Test
    void infraComposeTargetParsesSemicolonAndCommaSeparatedFiles() {
        ComposeServiceController.InfraComposeTarget semicolon =
            ComposeServiceController.InfraComposeTarget.fromEnvironment(Map.of(
                "BENCHMARK_INFRA_PROJECT", "custom-project",
                "BENCHMARK_INFRA_COMPOSE_FILES", "a.yaml;b.yaml"
            ));
        ComposeServiceController.InfraComposeTarget comma =
            ComposeServiceController.InfraComposeTarget.fromEnvironment(Map.of(
                "BENCHMARK_INFRA_COMPOSE_FILES", "c.yaml,d.yaml"
            ));

        assertThat(semicolon.project()).isEqualTo("custom-project");
        assertThat(semicolon.files()).containsExactly("a.yaml", "b.yaml");
        assertThat(comma.files()).containsExactly("c.yaml", "d.yaml");
    }
```

- [ ] **Step 2: Update existing command assertions to include default shared prefix**

Replace expected commands in existing tests so the prefix is:

```java
List.of("docker", "compose", "-p", "shared-data-infra",
    "-f", "../shared-data-infra/compose.yaml",
    "-f", "../shared-data-infra/compose.lakehouse.yaml",
    "-f", "../shared-data-infra/compose.starrocks.yaml",
    ...
)
```

For example, Spark restart should become:

```java
List.of("docker", "compose", "-p", "shared-data-infra",
    "-f", "../shared-data-infra/compose.yaml",
    "-f", "../shared-data-infra/compose.lakehouse.yaml",
    "-f", "../shared-data-infra/compose.starrocks.yaml",
    "restart", "spark")
```

Update error message assertions from:

```java
.hasMessageContaining("docker compose -f docker-compose.yml restart spark")
```

to:

```java
.hasMessageContaining("docker compose -p shared-data-infra -f ../shared-data-infra/compose.yaml -f ../shared-data-infra/compose.lakehouse.yaml -f ../shared-data-infra/compose.starrocks.yaml restart spark")
```

- [ ] **Step 3: Run controller tests and verify they fail**

Run:

```sh
mvn -Dtest=ComposeServiceControllerTest test
```

Expected: FAIL because `InfraComposeTarget` does not exist and commands are still hard-coded.

- [ ] **Step 4: Implement configurable compose target**

In `ComposeServiceController.java`, add `Map` import:

```java
import java.util.Map;
```

Add a field:

```java
    private final InfraComposeTarget infraComposeTarget;
```

Update the constructor that accepts all test dependencies to delegate with a target:

```java
    ComposeServiceController(
        CommandRunner commandRunner,
        Path workingDirectory,
        Duration timeout,
        int readinessAttempts,
        Duration readinessDelay,
        Sleeper sleeper
    ) {
        this(
            commandRunner,
            workingDirectory,
            timeout,
            readinessAttempts,
            readinessDelay,
            sleeper,
            InfraComposeTarget.fromEnvironment(System.getenv())
        );
    }

    ComposeServiceController(
        CommandRunner commandRunner,
        Path workingDirectory,
        Duration timeout,
        int readinessAttempts,
        Duration readinessDelay,
        Sleeper sleeper,
        InfraComposeTarget infraComposeTarget
    ) {
        this.commandRunner = commandRunner;
        this.workingDirectory = workingDirectory;
        this.timeout = timeout;
        this.readinessAttempts = Math.max(1, readinessAttempts);
        this.readinessDelay = readinessDelay;
        this.sleeper = sleeper;
        this.infraComposeTarget = infraComposeTarget;
    }
```

Remove the old assignments from the six-argument constructor after adding the delegating call.

Add this helper method inside the class:

```java
    private List<String> composeCommand(String... args) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("compose");
        command.add("-p");
        command.add(infraComposeTarget.project());
        for (String file : infraComposeTarget.files()) {
            command.add("-f");
            command.add(file);
        }
        command.addAll(List.of(args));
        return command;
    }
```

Replace every `List.of("docker", "compose", "-f", "docker-compose.yml", ...)` command construction in `restartCommands`, `readinessCommands`, and `starRocksMysqlCommand` with `composeCommand(...)`.

For example:

```java
            case SPARK_ICEBERG -> List.of(composeCommand("restart", "spark"));
```

and:

```java
        return composeCommand(
            "exec",
            "-T",
            "starrocks-fe",
            "mysql",
            "-h",
            "127.0.0.1",
            "-P",
            "9030",
            "-uroot",
            "-e",
            sql
        );
```

Add this nested record near the bottom of `ComposeServiceController` before `Sleeper`:

```java
    record InfraComposeTarget(String project, List<String> files) {
        private static final String DEFAULT_PROJECT = "shared-data-infra";
        private static final List<String> DEFAULT_FILES = List.of(
            "../shared-data-infra/compose.yaml",
            "../shared-data-infra/compose.lakehouse.yaml",
            "../shared-data-infra/compose.starrocks.yaml"
        );

        static InfraComposeTarget fromEnvironment(Map<String, String> environment) {
            String project = valueOrDefault(environment.get("BENCHMARK_INFRA_PROJECT"), DEFAULT_PROJECT);
            String filesValue = environment.get("BENCHMARK_INFRA_COMPOSE_FILES");
            List<String> files = filesValue == null || filesValue.isBlank()
                ? DEFAULT_FILES
                : splitFiles(filesValue);
            return new InfraComposeTarget(project, files);
        }

        private static List<String> splitFiles(String value) {
            return List.of(value.split("[;,]")).stream()
                .map(String::trim)
                .filter(part -> !part.isEmpty())
                .toList();
        }

        private static String valueOrDefault(String value, String fallback) {
            return value == null || value.isBlank() ? fallback : value.trim();
        }
    }
```

- [ ] **Step 5: Run controller tests**

Run:

```sh
mvn -Dtest=ComposeServiceControllerTest test
```

Expected: PASS.

- [ ] **Step 6: Commit runner configurable compose target**

```sh
git add src/main/java/com/example/databenchmark/runner/ComposeServiceController.java src/test/java/com/example/databenchmark/runner/ComposeServiceControllerTest.java
git commit -m "feat: target shared infra compose services"
```

## Task 5: Update Documentation and Agent Rules

**Files:**
- Modify: `../shared-data-infra/README.md`
- Modify: `../shared-data-infra/AGENTS.md`
- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `src/test/java/com/example/databenchmark/ComposeTopologyTest.java` only if README assertions need final string adjustments

- [ ] **Step 1: Update shared infra README**

In `../shared-data-infra/README.md`, update the profile list to say:

```markdown
- `lakehouse`: HDFS, Hive Metastore, and HDFS warehouse initialization.
- `lakehouse-tools`: long-running HiveServer2 for Beeline/JDBC clients.
- `spark-tools`: long-running Spark SQL tool service.
- `starrocks`: split StarRocks FE/BE services.
```

Replace StarRocks section text with:

```markdown
## StarRocks

The StarRocks overlay provides split FE/BE services for shared local development and benchmark cold restart control.

Container endpoints:

- FE MySQL: `starrocks-fe:9030`
- FE HTTP: `starrocks-fe:8030`
- BE HTTP: `starrocks-be:8040`

Host ports can be overridden with `STARROCKS_HTTP_PORT`, `STARROCKS_MYSQL_PORT`, and `STARROCKS_BE_HTTP_PORT`.

Validate the StarRocks profile:

```bash
docker compose -f compose.yaml -f compose.starrocks.yaml --profile starrocks config
```

Start StarRocks:

```bash
docker compose -f compose.yaml -f compose.starrocks.yaml --profile starrocks up -d
```
```

Add benchmark-compatible startup text under Lakehouse:

```markdown
Start the benchmark-compatible shared stack:

```bash
docker compose -f compose.yaml -f compose.lakehouse.yaml -f compose.starrocks.yaml \
  --profile lakehouse --profile lakehouse-tools --profile spark-tools --profile starrocks \
  up -d
```

Set `BENCHMARK_WORKSPACE` when `../data-benchmark` is not the correct path from this repository:

```bash
BENCHMARK_WORKSPACE=/absolute/path/to/data-benchmark docker compose -f compose.yaml -f compose.lakehouse.yaml \
  --profile lakehouse --profile spark-tools up -d spark
```
```

- [ ] **Step 2: Update shared infra AGENTS**

Replace `../shared-data-infra/AGENTS.md` with UTF-8 readable content:

```markdown
# AGENTS.md

## 公共基础设施约束

- 新增或修改 Docker Compose 基础设施前，必须先检查本仓库是否已经定义同类服务或 profile。
- 只被一个能力域使用的服务，放在对应 overlay。
- 被两个或以上能力域使用的服务，上移到 `compose.yaml`。
- 已有公共服务应优先整改到当前共享技术体系下，而不是新增平行重复服务。
- 需要强隔离、版本不同、生命周期不同的服务，允许拆成独立服务，但必须在 README 或 AGENTS.md 说明原因。
- 修改基础设施后，至少运行相关 `docker compose ... config` 校验。
```

- [ ] **Step 3: Update data-benchmark AGENTS**

Replace `AGENTS.md` with:

```markdown
# AGENTS.md

## 公共基础设施约束

- 新增或修改 Docker Compose 基础设施前，必须先检查 `../shared-data-infra` 是否已经定义同类服务或 profile。
- 如果 `../shared-data-infra` 已定义 HDFS、Hive、Spark、YARN、Kafka、ZooKeeper、StarRocks、Prometheus、Grafana 等能力，不要在本工程重复新增；通过 external network、环境变量和项目级命名空间复用。
- 公共服务有冲突时，优先把 `../shared-data-infra` 整改到本工程需要的共享技术体系下，而不是在本工程保留重复服务。
- 只有 benchmark 隔离、性能对比或现有 runner 兼容性明确需要时，才允许保留 project-local 基础设施，并在 compose 或 README 中写明原因。
- 修改基础设施后，至少运行 `docker compose -f docker-compose.yml config`；如果修改了 `../shared-data-infra`，也要运行对应 shared infra 的 `docker compose ... config`。

## 当前状态

- HDFS、Hive Metastore、HDFS 初始化、HiveServer2、Spark、StarRocks FE/BE 由 `../shared-data-infra` 提供。
- 本工程 `docker-compose.yml` 只保留 `benchmark-runner`，runner 通过 Docker socket 和可配置 compose 文件控制 shared infra 服务生命周期。
```

- [ ] **Step 4: Update data-benchmark README compose section**

Replace the `## HDFS Compose Benchmark` section through the HiveServer2/FE explanation paragraphs with content that includes:

```markdown
## Shared Infra Compose Benchmark

HDFS, Hive Metastore, HDFS initialization, HiveServer2, Spark, and StarRocks FE/BE are provided by `D:\agent-code\shared-data-infra`.

Start the shared benchmark-compatible stack first:

```sh
cd ..\shared-data-infra
docker compose -f compose.yaml -f compose.lakehouse.yaml -f compose.starrocks.yaml --profile lakehouse --profile lakehouse-tools --profile spark-tools --profile starrocks up -d
```

The stable internal endpoints are:

| Service | Endpoint |
| --- | --- |
| HDFS | `hdfs://hdfs-namenode:8020` |
| Hive Metastore | `thrift://hive-metastore:9083` |
| HiveServer2 | `jdbc:hive2://hive-server:10000/default` |
| Spark exec service | `spark` |
| StarRocks FE MySQL | `starrocks-fe:9030` |
| StarRocks BE HTTP | `starrocks-be:8040` |

The runner controls shared infra with:

- `BENCHMARK_INFRA_PROJECT`, default `shared-data-infra`
- `BENCHMARK_INFRA_COMPOSE_FILES`, default `../shared-data-infra/compose.yaml;../shared-data-infra/compose.lakehouse.yaml;../shared-data-infra/compose.starrocks.yaml`
- `BENCHMARK_WORKSPACE`, used by shared Spark to mount this repository at `/workspace`

Run the fast 10k Compose smoke validation:

```sh
mvn package
docker compose -f docker-compose.yml build benchmark-runner
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode compose --config configs/benchmark-compose-smoke.yml --run-id compose-smoke
```

Run through the runner container:

```sh
mvn package
docker compose -f docker-compose.yml build benchmark-runner
docker compose -f docker-compose.yml up benchmark-runner
```

Current Docker Compose resource ownership:

| Service | Owner |
| --- | --- |
| benchmark-runner | `data-benchmark/docker-compose.yml` |
| hdfs-init, hive-server, spark, starrocks-fe, starrocks-be | `../shared-data-infra` |
```

Keep existing report and run-id documentation after this section.

- [ ] **Step 5: Run README-backed topology test**

Run:

```sh
mvn -Dtest=ComposeTopologyTest test
```

Expected: PASS.

- [ ] **Step 6: Commit docs and rules**

Commit shared repo docs first:

```sh
cd ../shared-data-infra
git add README.md AGENTS.md
git commit -m "docs: document unified shared infra stack"
```

Commit benchmark docs:

```sh
cd ../data-benchmark
git add README.md AGENTS.md src/test/java/com/example/databenchmark/ComposeTopologyTest.java
git commit -m "docs: use shared infra for compose benchmark"
```

## Task 6: Final Verification

**Files:**
- No code changes expected.

- [ ] **Step 1: Verify shared infra compose configs**

Run:

```sh
cd ../shared-data-infra
docker compose -f compose.yaml -f compose.lakehouse.yaml --profile lakehouse --profile lakehouse-tools --profile spark-tools config
docker compose -f compose.yaml -f compose.starrocks.yaml --profile starrocks config
docker compose -f compose.yaml -f compose.lakehouse.yaml -f compose.starrocks.yaml --profile lakehouse --profile lakehouse-tools --profile spark-tools --profile starrocks config
```

Expected: all commands exit 0.

- [ ] **Step 2: Verify benchmark compose config**

Run:

```sh
cd ../data-benchmark
docker compose -f docker-compose.yml config
```

Expected: exits 0 and output contains only `benchmark-runner` under services.

- [ ] **Step 3: Run benchmark tests**

Run:

```sh
mvn test
```

Expected: PASS.

- [ ] **Step 4: Optional smoke run when resources allow**

Run:

```sh
cd ../shared-data-infra
docker compose -f compose.yaml -f compose.lakehouse.yaml -f compose.starrocks.yaml --profile lakehouse --profile lakehouse-tools --profile spark-tools --profile starrocks up -d

cd ../data-benchmark
mvn package
docker compose -f docker-compose.yml build benchmark-runner
docker compose -f docker-compose.yml up benchmark-runner
```

Expected: `benchmark-runner` exits 0 and writes a report under `reports/runs/compose-smoke/`.

- [ ] **Step 5: Capture final status**

Run in both repositories:

```sh
git status --short
```

Expected in `data-benchmark`: only pre-existing untracked local artifacts may remain, such as `.github/.idea/`, `.superpowers/`, `data/`, `reports/`, `metastore_db/`, or `derby.log`.

Expected in `shared-data-infra`: no uncommitted changes from this implementation.

## Self-Review Notes

- Spec coverage: tasks cover shared StarRocks split, shared Spark long-running service, shared HiveServer2 explicit startup, shared `hdfs-init`, local compose shrink, configurable runner compose target, documentation, AGENTS rules, and verification.
- Placeholder scan: no task relies on future unspecified work; every code-changing step includes the concrete target content or exact replacement snippet.
- Type consistency: `ComposeServiceController.InfraComposeTarget` is introduced before tests reference it as a nested record; command generation uses `composeCommand(String... args)` consistently across restart and readiness paths.

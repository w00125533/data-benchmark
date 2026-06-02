# data-benchmark

Benchmark design and deployment assets for the StarRocks and Iceberg data benchmark project.

## Staging Deployment

See [docs/deployment/staging.md](docs/deployment/staging.md) for the staging deployment workflow.

## Java 17 Local Benchmark MVP

Build and test:

```powershell
mvn test
```

Package the runner:

```powershell
mvn package
```

Run the CI-sized local smoke workflow:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --run-id local-smoke
```

Generate data only:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar generate --cells 10 --days 1 --row-cap 100
```

The default smoke config is [configs/benchmark-smoke.yml](configs/benchmark-smoke.yml). It preserves the spec values for `10,000` cells and `1` day, and uses `rowCap: 10000` so local verification does not accidentally generate all `14,400,000` rows.

Docker Compose uses the packaged runner jar from `target/`, so run `mvn package` before starting Compose:

```powershell
docker compose -f docker-compose.yml up benchmark-runner
```

The current generator is a deterministic text skeleton written to a `.parquet` path. A follow-up Parquet writer is still needed before these files are real Parquet datasets.

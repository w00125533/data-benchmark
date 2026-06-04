import type { WebBenchmarkReport } from '../types/report';

export const sampleReport: WebBenchmarkReport = {
  schemaVersion: 1,
  run: {
    runId: 'sample-run',
    profile: 'tpch-smoke',
    suite: 'tpch',
    querySet: 'smoke',
    status: 'SUCCESS',
    startedAt: '2026-06-04T00:00:00Z',
    endedAt: '2026-06-04T00:00:12Z',
    durationSeconds: 12,
    fullProfile: false
  },
  dataset: {
    cells: 10000,
    days: 1,
    rows: 899,
    columns: 0,
    bytesWritten: 123456
  },
  loads: [
    {
      engine: 'spark_iceberg',
      tableShape: 'tpch_iceberg',
      stage: 'LOAD',
      rows: 899,
      bytes: 123456,
      durationSeconds: 1.2,
      success: true,
      error: ''
    }
  ],
  queries: [
    {
      engine: 'spark_iceberg',
      tableShape: 'tpch_iceberg',
      queryName: 'q01_pricing_summary_report',
      p50Ms: 120,
      p95Ms: 120,
      p99Ms: 120,
      rows: 4,
      failures: 0,
      success: true,
      error: ''
    }
  ],
  charts: {
    loadDurationByEngine: [
      {
        engine: 'spark_iceberg',
        tableShape: 'tpch_iceberg',
        stage: 'LOAD',
        durationSeconds: 1.2,
        success: true
      }
    ],
    queryLatencyByEngine: [
      {
        engine: 'spark_iceberg',
        tableShape: 'tpch_iceberg',
        queryName: 'q01_pricing_summary_report',
        metric: 'p95',
        latencyMs: 120,
        success: true
      }
    ],
    queryRowsByEngine: [
      {
        engine: 'spark_iceberg',
        queryName: 'q01_pricing_summary_report',
        rows: 4,
        success: true
      }
    ],
    failureSummary: [
      {
        stage: 'LOAD',
        engine: 'spark_iceberg',
        failures: 0
      }
    ]
  },
  notices: ['TPC-H smoke data is compatible test data, not an official TPC-H benchmark result.']
};

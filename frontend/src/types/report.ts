export type RunStatus = 'SUCCESS' | 'DEGRADED';
export type RouteStatus = 'SUCCESS' | 'FAILED' | 'SKIPPED';
export type RouteKey =
  | 'spark_native_parquet'
  | 'spark_iceberg'
  | 'starrocks_internal'
  | 'starrocks_external_iceberg'
  | 'hive_hdfs_parquet';

export interface WebBenchmarkReport {
  schemaVersion: 3;
  run: RunInfo;
  dataset: DatasetInfo;
  loads: LoadSummary[];
  queries: QuerySummary[];
  performanceMatrix: PerformanceMatrixRow[];
  notices: string[];
}

export interface RunInfo {
  runId: string;
  profile: string;
  suite: string;
  querySet: string;
  status: RunStatus;
  startedAt: string;
  endedAt: string;
  durationSeconds: number;
  fullProfile: boolean;
}

export interface DatasetInfo {
  cells: number;
  days: number;
  rows: number;
  columns: number;
  bytesWritten: number;
}

export interface LoadSummary {
  engine: string;
  tableShape: string;
  stage: string;
  rows: number;
  bytes: number;
  durationSeconds: number;
  success: boolean;
  error: string;
}

export interface QuerySummary {
  datasetId: string;
  datasetName: string;
  querySet: string;
  engine: string;
  tableShape: string;
  queryName: string;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  rows: number;
  status: RouteStatus;
  error: string;
}

export interface PerformanceMatrixRow {
  datasetId: string;
  datasetName: string;
  querySet: string;
  queryName: string;
  routes: Record<RouteKey, RouteResult>;
  bestRoute: RouteKey | '';
  bestRouteHotMs: number;
}

export interface RouteResult {
  status: RouteStatus;
  coldMs: number;
  warmMs: number;
  hotMs: number;
  coldStatus: RouteStatus;
  warmStatus: RouteStatus;
  hotStatus: RouteStatus;
  rows: number;
  error: string;
}

declare global {
  interface Window {
    __BENCHMARK_REPORT__?: WebBenchmarkReport;
  }
}

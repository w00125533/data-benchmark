export type RunStatus = 'SUCCESS' | 'DEGRADED';

export interface WebBenchmarkReport {
  schemaVersion: 1;
  run: RunInfo;
  dataset: DatasetInfo;
  loads: LoadSummary[];
  queries: QuerySummary[];
  charts: ChartData;
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
  engine: string;
  tableShape: string;
  queryName: string;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  rows: number;
  failures: number;
  success: boolean;
  error: string;
}

export interface ChartData {
  loadDurationByEngine: LoadDurationPoint[];
  queryLatencyByEngine: QueryLatencyPoint[];
  queryRowsByEngine: QueryRowsPoint[];
  failureSummary: FailureSummaryPoint[];
}

export interface LoadDurationPoint {
  engine: string;
  tableShape: string;
  stage: string;
  durationSeconds: number;
  success: boolean;
}

export interface QueryLatencyPoint {
  engine: string;
  tableShape: string;
  queryName: string;
  metric: 'p50' | 'p95' | 'p99';
  latencyMs: number;
  success: boolean;
}

export interface QueryRowsPoint {
  engine: string;
  queryName: string;
  rows: number;
  success: boolean;
}

export interface FailureSummaryPoint {
  stage: string;
  engine: string;
  failures: number;
}

declare global {
  interface Window {
    __BENCHMARK_REPORT__?: WebBenchmarkReport;
  }
}

import type { WebBenchmarkReport } from '../types/report';
import { sampleReport } from './sampleReport';

export async function loadReport(): Promise<WebBenchmarkReport> {
  if (window.__BENCHMARK_REPORT__) {
    return validateReport(window.__BENCHMARK_REPORT__);
  }

  if (import.meta.env.DEV) {
    return sampleReport;
  }

  const response = await fetch('./report.json');
  if (!response.ok) {
    throw new Error(`Failed to load report.json: HTTP ${response.status}`);
  }
  return validateReport(await response.json());
}

export function validateReport(value: unknown): WebBenchmarkReport {
  if (!value || typeof value !== 'object') {
    throw new Error('Report data is missing');
  }
  const report = value as Partial<WebBenchmarkReport>;
  if (report.schemaVersion !== 1) {
    throw new Error(`Unsupported report schemaVersion: ${String(report.schemaVersion)}`);
  }
  if (!report.run?.runId || !report.dataset || !report.charts) {
    throw new Error('Report data is incomplete');
  }
  return report as WebBenchmarkReport;
}

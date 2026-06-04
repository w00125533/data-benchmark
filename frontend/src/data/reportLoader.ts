import type { WebBenchmarkReport } from '../types/report';

export async function loadReport(): Promise<WebBenchmarkReport> {
  const embedded = window.__BENCHMARK_REPORT__ as (WebBenchmarkReport & { schemaVersion: number }) | undefined;
  if (!embedded) {
    throw new Error('Missing embedded report data: window.__BENCHMARK_REPORT__');
  }
  if (embedded.schemaVersion !== 3) {
    throw new Error(`Unsupported report schema version: ${embedded.schemaVersion}`);
  }
  return embedded;
}

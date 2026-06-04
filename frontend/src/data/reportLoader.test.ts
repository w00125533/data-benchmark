import { afterEach, describe, expect, it, test, vi } from 'vitest';
import { sampleReport } from './sampleReport';
import { loadReport } from './reportLoader';

describe('loadReport', () => {
  function setEmbeddedReportPayload(payload: unknown) {
    Object.defineProperty(window, '__BENCHMARK_REPORT__', {
      value: payload,
      configurable: true,
      writable: true,
    });
  }

  afterEach(() => {
    vi.unstubAllGlobals();
    window.__BENCHMARK_REPORT__ = undefined;
  });

  it('loads embedded report without fetching report.json', async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);
    window.__BENCHMARK_REPORT__ = sampleReport;

    const report = await loadReport();

    expect(report.run.runId).toBe(sampleReport.run.runId);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  test('rejects missing embedded report data', async () => {
    await expect(loadReport()).rejects.toThrow(
      'Missing embedded report data: window.__BENCHMARK_REPORT__'
    );
  });

  test('rejects unsupported schema version', async () => {
    setEmbeddedReportPayload({ ...sampleReport, schemaVersion: 99 });

    await expect(loadReport()).rejects.toThrow(
      'Unsupported report schema version: 99'
    );
  });

  test('rejects schema version 2 reports', async () => {
    setEmbeddedReportPayload({ ...sampleReport, schemaVersion: 2 });

    await expect(loadReport()).rejects.toThrow(
      'Unsupported report schema version: 2'
    );
  });

  test('accepts schema version 3 reports', async () => {
    const schemaV3Report = { ...sampleReport, schemaVersion: 3 } as unknown;
    setEmbeddedReportPayload(schemaV3Report);

    const report = await loadReport();

    expect(report.schemaVersion).toBe(3);
    expect(report.run.runId).toBe(sampleReport.run.runId);
  });
});

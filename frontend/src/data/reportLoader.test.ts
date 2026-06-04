import { describe, expect, test } from 'vitest';
import { sampleReport } from './sampleReport';
import { validateReport } from './reportLoader';

describe('validateReport', () => {
  test('accepts schema version 1 report', () => {
    expect(validateReport(sampleReport).run.runId).toBe('sample-run');
  });

  test('rejects unsupported schema version', () => {
    expect(() => validateReport({ ...sampleReport, schemaVersion: 99 })).toThrow(
      'Unsupported report schemaVersion'
    );
  });

  test('rejects incomplete report data', () => {
    expect(() => validateReport({ schemaVersion: 1 })).toThrow('Report data is incomplete');
  });
});

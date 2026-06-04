import '@testing-library/jest-dom/vitest';
import { render, screen } from '@testing-library/react';
import { beforeEach, expect, test } from 'vitest';
import App from './App';
import { sampleReport } from './data/sampleReport';

beforeEach(() => {
  window.__BENCHMARK_REPORT__ = sampleReport;
});

test('renders embedded report summary', async () => {
  render(<App />);

  expect(await screen.findByText('Data Benchmark Report')).toBeInTheDocument();
  expect(screen.getByText('Run ID: sample-run')).toBeInTheDocument();
  expect(screen.getByText('Suite: tpch')).toBeInTheDocument();
});

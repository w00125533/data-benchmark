import '@testing-library/jest-dom/vitest';
import { render, screen } from '@testing-library/react';
import { beforeEach, expect, test } from 'vitest';
import App from './App';
import { sampleReport } from './data/sampleReport';
import type { WebBenchmarkReport } from './types/report';

beforeEach(() => {
  window.__BENCHMARK_REPORT__ = sampleReport;
});

test('renders embedded report summary', async () => {
  render(<App />);

  expect(await screen.findByText('Data Benchmark Report')).toBeInTheDocument();
  expect(screen.getByText('运行状态: SUCCESS')).toBeInTheDocument();
  expect(screen.getByText('Run ID: sample-run')).toBeInTheDocument();
  expect(screen.getByText('Suite: tpch')).toBeInTheDocument();
});

test('renders localized load failure message', async () => {
  window.__BENCHMARK_REPORT__ = {} as unknown as WebBenchmarkReport;

  render(<App />);

  expect(await screen.findByText('报告数据加载失败')).toBeInTheDocument();
  expect(screen.getByText('Unsupported report schemaVersion: undefined')).toBeInTheDocument();
});

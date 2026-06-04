import '@testing-library/jest-dom/vitest';
import { render, screen } from '@testing-library/react';
import { beforeAll, beforeEach, expect, test, vi } from 'vitest';
import App from './App';
import { sampleReport } from './data/sampleReport';
import type { WebBenchmarkReport } from './types/report';

beforeAll(() => {
  const getComputedStyle = window.getComputedStyle;
  window.getComputedStyle = ((element: Element) => getComputedStyle(element)) as typeof window.getComputedStyle;

  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: vi.fn().mockImplementation((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })),
  });
});

beforeEach(() => {
  window.__BENCHMARK_REPORT__ = sampleReport;
});

test('renders run summary, performance matrix, and detail tables', async () => {
  render(<App />);

  expect(await screen.findByText('Data Benchmark Report')).toBeInTheDocument();
  expect(screen.getByText('Run ID')).toBeInTheDocument();
  expect(screen.getByText('性能矩阵')).toBeInTheDocument();
  expect(screen.getByText('Load 明细')).toBeInTheDocument();
  expect(screen.getByText('Query 明细')).toBeInTheDocument();
  expect(
    screen.getByText('TPC-H smoke data is compatible test data, not an official TPC-H benchmark result.'),
  ).toBeInTheDocument();
});

test('renders performance matrix with route statuses and best route', async () => {
  render(<App />);

  expect(await screen.findByText('性能矩阵')).toBeInTheDocument();
  expect(screen.getByText('q03_shipping_priority')).toBeInTheDocument();
  expect(screen.getByText('top_region_sales')).toBeInTheDocument();
  expect(screen.getByText('datasetId tpch')).toBeInTheDocument();
  expect(screen.getAllByText('rows 60,000').length).toBeGreaterThan(0);
  expect(screen.getAllByText('cells 10,000 / days 1').length).toBeGreaterThan(0);
  expect(screen.getAllByText('StarRocks Internal').length).toBeGreaterThan(0);
  expect(screen.getByText('p95 760 ms')).toBeInTheDocument();
  expect(screen.getByText('best p95 760 ms')).toBeInTheDocument();
  expect(screen.getByText('catalog timeout')).toBeInTheDocument();
  expect(screen.getByText('SKIPPED')).toBeInTheDocument();
});

test('keeps load and query detail sections visible', async () => {
  render(<App />);

  expect(await screen.findByText('Load 明细')).toBeInTheDocument();
  expect(screen.getByText('Query 明细')).toBeInTheDocument();
  expect(screen.getAllByText('表形态').length).toBeGreaterThan(0);
  expect(screen.getAllByText('阶段').length).toBeGreaterThan(0);
  expect(screen.getAllByText('耗时 秒').length).toBeGreaterThan(0);
  expect(screen.getAllByText('q01_pricing_summary_report').length).toBeGreaterThan(0);
});

test('renders localized load failure message with readable Chinese', async () => {
  window.__BENCHMARK_REPORT__ = {} as unknown as WebBenchmarkReport;

  render(<App />);

  expect(await screen.findByText('报告数据加载失败')).toBeInTheDocument();
  expect(screen.getByText('Unsupported report schema version: undefined')).toBeInTheDocument();
});

test('renders degraded alert and failed query row', async () => {
  window.__BENCHMARK_REPORT__ = {
    ...sampleReport,
    run: { ...sampleReport.run, status: 'DEGRADED' },
    queries: [
      {
        datasetId: 'tpch',
        datasetName: 'TPC-H SF 0.01',
        querySet: 'smoke',
        engine: 'starrocks_internal',
        tableShape: 'sr_internal_tpch',
        queryName: 'q01_pricing_summary_report',
        p50Ms: 0,
        p95Ms: 0,
        p99Ms: 0,
        rows: 0,
        status: 'FAILED',
        error: 'query failed',
      },
    ],
  };

  render(<App />);

  expect(await screen.findByText('本次运行存在失败阶段，请查看明细错误。')).toBeInTheDocument();
  expect(screen.getAllByText('FAILED').length).toBeGreaterThan(0);
  expect(screen.getAllByText('query failed').length).toBeGreaterThan(0);
});

test('explains when a local smoke run has no comparable route matrix', async () => {
  window.__BENCHMARK_REPORT__ = {
    ...sampleReport,
    performanceMatrix: [],
    queries: [
      {
        datasetId: 'kpi',
        datasetName: 'KPI smoke',
        querySet: 'smoke',
        engine: 'local',
        tableShape: 'generated_parquet',
        queryName: 'catalog_render_check',
        p50Ms: 0,
        p95Ms: 0,
        p99Ms: 0,
        rows: 0,
        status: 'SUCCESS',
        error: '',
      },
    ],
  };

  render(<App />);

  expect(await screen.findByText('本次运行没有三技术路线性能对比数据。')).toBeInTheDocument();
  expect(screen.getAllByText(/local smoke 只生成本地数据/).length).toBeGreaterThan(0);
  expect(screen.queryByText('datasetId kpi')).not.toBeInTheDocument();
  expect(screen.getByText('catalog_render_check')).toBeInTheDocument();
});

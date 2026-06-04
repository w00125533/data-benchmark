import '@testing-library/jest-dom/vitest';
import { render, screen } from '@testing-library/react';
import { beforeAll, beforeEach, expect, test, vi } from 'vitest';
import App from './App';
import { sampleReport } from './data/sampleReport';
import type { WebBenchmarkReport } from './types/report';

vi.mock('@ant-design/plots', () => ({
  Bar: ({ data }: { data: unknown[] }) => <div data-testid="bar-chart">{data.length}</div>,
  Column: ({ data }: { data: unknown[] }) => <div data-testid="column-chart">{data.length}</div>
}));

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
      dispatchEvent: vi.fn()
    }))
  });
});

beforeEach(() => {
  window.__BENCHMARK_REPORT__ = sampleReport;
});

test('renders run summary, chart sections, stage flow, and detail tables', async () => {
  render(<App />);

  expect(await screen.findByText('Data Benchmark Report')).toBeInTheDocument();
  expect(screen.getByText('运行摘要')).toBeInTheDocument();
  expect(screen.getByText('加载耗时对比')).toBeInTheDocument();
  expect(screen.getByText('查询 P95 延迟对比')).toBeInTheDocument();
  expect(screen.getByText('阶段状态流')).toBeInTheDocument();
  expect(screen.getByText('Load 明细')).toBeInTheDocument();
  expect(screen.getByText('Query 明细')).toBeInTheDocument();
  expect(screen.getAllByText('q01_pricing_summary_report').length).toBeGreaterThan(0);
  expect(
    screen.getByText('TPC-H smoke data is compatible test data, not an official TPC-H benchmark result.')
  ).toBeInTheDocument();
});

test('renders localized load failure message with readable Chinese', async () => {
  window.__BENCHMARK_REPORT__ = {} as unknown as WebBenchmarkReport;

  render(<App />);

  expect(await screen.findByText('报告数据加载失败')).toBeInTheDocument();
  expect(screen.getByText('Unsupported report schemaVersion: undefined')).toBeInTheDocument();
});

test('renders degraded alert and failed query row', async () => {
  window.__BENCHMARK_REPORT__ = {
    ...sampleReport,
    run: { ...sampleReport.run, status: 'DEGRADED' },
    queries: [
      {
        ...sampleReport.queries[0],
        success: false,
        failures: 1,
        error: 'query failed'
      }
    ]
  };

  render(<App />);

  expect(await screen.findByText('本次运行存在失败阶段，请查看明细错误。')).toBeInTheDocument();
  expect(screen.getAllByText('FAILED').length).toBeGreaterThan(0);
  expect(screen.getAllByText('query failed').length).toBeGreaterThan(0);
});

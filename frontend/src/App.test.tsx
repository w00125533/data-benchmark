import '@testing-library/jest-dom/vitest';
import { render, screen, within } from '@testing-library/react';
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

test('renders run summary, performance matrix, and detail sections', async () => {
  render(<App />);

  expect(await screen.findByText('Data Benchmark Report')).toBeInTheDocument();
  expect(screen.getByText('Run ID')).toBeInTheDocument();
  expect(screen.getByText('Performance Matrix')).toBeInTheDocument();
  expect(screen.getByText('Load Details')).toBeInTheDocument();
  expect(screen.getByText('Table Metadata')).toBeInTheDocument();
  expect(screen.getByText('Query Details')).toBeInTheDocument();
  expect(
    screen.getByText('TPC-H smoke data is compatible test data, not an official TPC-H benchmark result.'),
  ).toBeInTheDocument();
});

test('renders performance matrix as SQL blocks with split cold warm hot rows and charts', async () => {
  const { container } = render(<App />);

  expect(await screen.findByText('Performance Matrix')).toBeInTheDocument();
  expect(screen.getByText('q03_shipping_priority')).toBeInTheDocument();
  expect(screen.getByText('top_region_sales')).toBeInTheDocument();
  expect(screen.getByText('Actual SQL sent by route')).toBeInTheDocument();
  expect(container.textContent).toContain('FROM sr_internal_lineitem');
  expect(container.querySelector('details.matrix-sql-details')).not.toHaveAttribute('open');
  expect(screen.getByText('dataset TPC-H SF 0.01')).toBeInTheDocument();
  expect(screen.getAllByText('Spark SQL Native Parquet').length).toBeGreaterThan(0);
  expect(screen.getAllByText('StarRocks Internal').length).toBeGreaterThan(0);
  expect(screen.getAllByText('Hive HDFS Parquet').length).toBeGreaterThan(0);
  expect(screen.getAllByText('Cold').length).toBeGreaterThan(0);
  expect(screen.getAllByText('Warm').length).toBeGreaterThan(0);
  expect(screen.getAllByText('Hot').length).toBeGreaterThan(0);
  expect(screen.getAllByText('500.000 ms').length).toBeGreaterThan(0);
  expect(screen.getAllByText('90.000 ms').length).toBeGreaterThan(0);
  expect(screen.getAllByText('60.000 ms').length).toBeGreaterThan(0);
  expect(screen.getAllByText('Cold query').length).toBeGreaterThan(0);
  expect(screen.getAllByText('Warm query').length).toBeGreaterThan(0);
  expect(screen.getAllByText('Hot query').length).toBeGreaterThan(0);
  expect(screen.getAllByText(/linear scale, max/).length).toBeGreaterThan(0);
  expect(screen.getAllByText('Best hot route').length).toBeGreaterThan(0);
  expect(screen.getAllByText('catalog timeout').length).toBeGreaterThan(0);
  expect(screen.getAllByText('SKIPPED').length).toBeGreaterThan(0);
});

test('renders distinct cold and hot best routes in matrix phase rows', async () => {
  window.__BENCHMARK_REPORT__ = {
    ...sampleReport,
    performanceMatrix: [
      {
        ...sampleReport.performanceMatrix[0],
        routes: {
          ...sampleReport.performanceMatrix[0].routes,
          spark_iceberg: {
            ...sampleReport.performanceMatrix[0].routes.spark_iceberg,
            coldMs: 420,
            coldStatus: 'SUCCESS',
            hotMs: 2200,
            hotStatus: 'SUCCESS',
          },
          starrocks_internal: {
            ...sampleReport.performanceMatrix[0].routes.starrocks_internal,
            coldMs: 900,
            coldStatus: 'SUCCESS',
            hotMs: 60,
            hotStatus: 'SUCCESS',
          },
        },
        bestRoute: 'starrocks_internal',
        bestRouteHotMs: 60,
      },
    ],
  };

  const { container } = render(<App />);

  expect(await screen.findByText('Performance Matrix')).toBeInTheDocument();
  const phaseTable = container.querySelector('.matrix-phase-table');
  expect(phaseTable).toBeTruthy();

  const phaseRows = within(phaseTable as HTMLElement).getAllByRole('row').slice(1);
  const coldRow = phaseRows.find((row) => row.querySelector('.phase-name')?.textContent === 'Cold');
  const hotRow = phaseRows.find((row) => row.querySelector('.phase-name')?.textContent === 'Hot');

  expect(coldRow).toBeTruthy();
  expect(hotRow).toBeTruthy();

  const coldCells = within(coldRow as HTMLElement).getAllByRole('cell');
  const hotCells = within(hotRow as HTMLElement).getAllByRole('cell');
  const coldBestCell = coldCells[coldCells.length - 1];
  const hotBestCell = hotCells[hotCells.length - 1];

  expect(coldBestCell).toHaveTextContent('Spark Iceberg');
  expect(coldBestCell).toHaveTextContent('420.000 ms');
  expect(hotBestCell).toHaveTextContent('StarRocks Internal');
  expect(hotBestCell).toHaveTextContent('60.000 ms');
});

test('ignores invalid route durations for matrix best and chart bars', async () => {
  window.__BENCHMARK_REPORT__ = {
    ...sampleReport,
    performanceMatrix: [
      {
        ...sampleReport.performanceMatrix[0],
        routes: {
          spark_native_parquet: {
            ...sampleReport.performanceMatrix[0].routes.spark_native_parquet,
            coldMs: 25,
            warmMs: 25,
            coldStatus: 'SKIPPED',
            warmStatus: 'SKIPPED',
          },
          spark_iceberg: {
            ...sampleReport.performanceMatrix[0].routes.spark_iceberg,
            coldMs: Number.NaN,
            warmMs: 250,
            coldStatus: 'SUCCESS',
            warmStatus: 'SUCCESS',
          },
          starrocks_internal: {
            ...sampleReport.performanceMatrix[0].routes.starrocks_internal,
            coldMs: 10,
            warmMs: 10,
            coldStatus: 'FAILED',
            warmStatus: 'FAILED',
          },
          starrocks_external_iceberg: {
            ...sampleReport.performanceMatrix[0].routes.starrocks_external_iceberg,
            coldMs: 0,
            warmMs: 0,
            coldStatus: 'SUCCESS',
            warmStatus: 'SUCCESS',
          },
          hive_hdfs_parquet: {
            ...sampleReport.performanceMatrix[0].routes.hive_hdfs_parquet,
            coldMs: Number.POSITIVE_INFINITY,
            warmMs: Number.POSITIVE_INFINITY,
            coldStatus: 'SUCCESS',
            warmStatus: 'SUCCESS',
          },
        },
        bestRoute: 'starrocks_internal',
        bestRouteHotMs: 60,
      },
    ],
  };

  const { container } = render(<App />);

  expect(await screen.findByText('Performance Matrix')).toBeInTheDocument();
  const phaseTable = container.querySelector('.matrix-phase-table');
  expect(phaseTable).toBeTruthy();

  const phaseRows = within(phaseTable as HTMLElement).getAllByRole('row').slice(1);
  const coldRow = phaseRows.find((row) => row.querySelector('.phase-name')?.textContent === 'Cold');
  const warmRow = phaseRows.find((row) => row.querySelector('.phase-name')?.textContent === 'Warm');

  expect(coldRow).toBeTruthy();
  expect(warmRow).toBeTruthy();

  const coldCells = within(coldRow as HTMLElement).getAllByRole('cell');
  const warmCells = within(warmRow as HTMLElement).getAllByRole('cell');
  expect(coldCells[coldCells.length - 1]).toHaveTextContent('-');
  const warmBestCell = warmCells[warmCells.length - 1];
  expect(warmBestCell).toHaveTextContent('Spark Iceberg');
  expect(warmBestCell).toHaveTextContent('250.000 ms');

  const barStyles = [...container.querySelectorAll<HTMLElement>('.matrix-bar')].map(
    (bar) => bar.getAttribute('style') ?? '',
  );
  expect(barStyles.join(' ')).not.toMatch(/NaN|Infinity/);
});

test('keeps load, table metadata, and query detail sections visible', async () => {
  const { container } = render(<App />);

  expect(await screen.findByText('Load Details')).toBeInTheDocument();
  expect(screen.getByText('Table Metadata')).toBeInTheDocument();
  expect(screen.getByText('Query Details')).toBeInTheDocument();
  expect(screen.getAllByText('q01_pricing_summary_report').length).toBeGreaterThan(0);
  expect(screen.getAllByText('Phase').length).toBeGreaterThan(0);
  expect(screen.getAllByText('Duration ms').length).toBeGreaterThan(0);
  expect(screen.queryByText('P50 ms')).not.toBeInTheDocument();
  expect(screen.queryByText('P95 ms')).not.toBeInTheDocument();
  expect(screen.queryByText('P99 ms')).not.toBeInTheDocument();
  expect(screen.getAllByText('Warm').length).toBeGreaterThan(0);
  expect(screen.queryByText('Table Info')).not.toBeInTheDocument();
  expect(screen.queryByText('Table runtime metadata')).not.toBeInTheDocument();
  expect(screen.getByText('cell_kpi_1min')).toBeInTheDocument();
  expect(screen.getAllByText('iceberg_catalog.iceberg_db.cell_kpi_1min').length).toBeGreaterThan(0);
  expect(screen.getAllByText('hive_db.cell_kpi_1min').length).toBeGreaterThan(0);
  expect(screen.getAllByText('Spark Iceberg').length).toBeGreaterThan(0);
  expect(screen.getAllByText('Hive HDFS Parquet').length).toBeGreaterThan(0);
  expect(screen.getByText('SHOW CREATE TABLE output')).toBeInTheDocument();
  expect(container.querySelector('td[rowspan="3"]')).toBeTruthy();
});

test('keeps grouped query detail rows visible beyond ten rows', async () => {
  window.__BENCHMARK_REPORT__ = {
    ...sampleReport,
    performanceMatrix: [],
    queries: Array.from({ length: 12 }, (_, index) => ({
      ...sampleReport.queries[0],
      phase: ['COLD', 'WARM', 'HOT'][index % 3],
      durationMs: 1000 + index,
    })),
  };

  const { container } = render(<App />);

  expect(await screen.findByText('Query Details')).toBeInTheDocument();
  expect(screen.getByText('1011.000 ms')).toBeInTheDocument();
  expect(container.querySelector('td[rowspan="12"]')).toBeTruthy();
});

test('keeps query detail groups separated by dataset and query set', async () => {
  window.__BENCHMARK_REPORT__ = {
    ...sampleReport,
    tableRuntimeInfos: [],
    performanceMatrix: [],
    queries: [
      {
        ...sampleReport.queries[0],
        datasetId: 'dataset-a',
        querySet: 'smoke',
        phase: 'HOT',
      },
      {
        ...sampleReport.queries[0],
        datasetId: 'dataset-b',
        querySet: 'daily',
        phase: 'HOT',
      },
    ],
  };

  const { container } = render(<App />);

  expect(await screen.findByText('Query Details')).toBeInTheDocument();
  expect(container.querySelector('td[rowspan="2"]')).toBeNull();
});

test('renders empty table metadata for older reports without table runtime infos', async () => {
  window.__BENCHMARK_REPORT__ = {
    ...sampleReport,
    tableRuntimeInfos: [],
    performanceMatrix: [],
    queries: [
      {
        datasetId: 'legacy',
        datasetName: 'Legacy report',
        querySet: 'smoke',
        engine: 'spark_iceberg',
        tableShape: 'iceberg_catalog.tpch',
        queryName: 'legacy_query',
        phase: 'HOT',
        p50Ms: 0,
        p95Ms: 0,
        p99Ms: 0,
        durationMs: 0,
        rows: 0,
        status: 'SUCCESS',
        error: '',
      },
    ],
  };

  render(<App />);

  expect(await screen.findByText('Table Metadata')).toBeInTheDocument();
  expect(await screen.findByText('Query Details')).toBeInTheDocument();
  expect(screen.getByText('No table runtime metadata was collected for this run.')).toBeInTheDocument();
  expect(screen.queryByText('metadata unavailable')).not.toBeInTheDocument();
});

test('renders report loading failure', async () => {
  window.__BENCHMARK_REPORT__ = {} as unknown as WebBenchmarkReport;

  render(<App />);

  expect(await screen.findByText('Report data failed to load')).toBeInTheDocument();
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
        phase: 'HOT',
        p50Ms: 0,
        p95Ms: 0,
        p99Ms: 0,
        durationMs: 0,
        rows: 0,
        status: 'FAILED',
        error: 'query failed',
      },
    ],
  };

  render(<App />);

  expect(await screen.findByText('This run has failed stages. Check details for errors.')).toBeInTheDocument();
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
        phase: 'HOT',
        p50Ms: 0,
        p95Ms: 0,
        p99Ms: 0,
        durationMs: 0,
        rows: 0,
        status: 'SUCCESS',
        error: '',
      },
    ],
  };

  render(<App />);

  expect(await screen.findByText('No comparable route performance data in this run.')).toBeInTheDocument();
  expect(screen.getAllByText(/Spark SQL Native Parquet/).length).toBeGreaterThan(0);
  expect(screen.getAllByText(/Spark Iceberg/).length).toBeGreaterThan(0);
  expect(screen.getAllByText(/StarRocks Internal/).length).toBeGreaterThan(0);
  expect(screen.getAllByText(/StarRocks External Iceberg/).length).toBeGreaterThan(0);
  expect(screen.getAllByText(/Hive HDFS Parquet/).length).toBeGreaterThan(0);
  expect(screen.getAllByText(/Local smoke runs only generate local data/).length).toBeGreaterThan(0);
  expect(screen.queryByText('dataset KPI smoke')).not.toBeInTheDocument();
  expect(screen.getByText('catalog_render_check')).toBeInTheDocument();
});

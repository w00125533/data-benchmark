import '@testing-library/jest-dom/vitest';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { beforeAll, expect, test, vi } from 'vitest';
import TableMetadataTable from './TableMetadataTable';
import type { TableRuntimeInfo } from '../types/report';

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

test('renders table metadata with normalized qualified name and status as the final column', () => {
  const longQualifiedName = 'iceberg_catalog.iceberg_db.cell_kpi_1min_with_a_very_long_runtime_identifier';
  const rows: TableRuntimeInfo[] = [
    {
      route: 'spark_iceberg',
      displayName: 'Spark Iceberg',
      tableShape: 'spark_iceberg',
      tableIdentifier: longQualifiedName,
      storageType: 'Iceberg',
      location: 'hdfs://hdfs-namenode:8020/warehouse/iceberg/iceberg_db/cell_kpi_1min',
      format: 'Parquet',
      columns: 50,
      partitioning: 'days(event_time)',
      bucketingOrDistribution: '',
      indexes: '',
      snapshotOrVersion: 'snapshot=1865824109249660975',
      fileCount: 9,
      totalBytes: 4_567_890,
      rawDetails: 'DESCRIBE EXTENDED output',
      success: true,
      error: '',
    },
  ];

  const { container } = render(<TableMetadataTable rows={rows} />);

  const headers = [...container.querySelectorAll('thead th')]
    .map((header) => header.textContent?.trim())
    .filter(Boolean);
  expect(headers).toEqual([
    'Data Table',
    'Technology Direction',
    'Qualified Name',
    'Storage',
    'Partitioning',
    'Columns',
    'Files / Objects',
    'Bytes',
    'Snapshot',
    'Detail',
    'Status',
  ]);
  const clippedCell = screen
    .getAllByText(longQualifiedName)
    .find((element) => element.getAttribute('title') === longQualifiedName);
  expect(clippedCell).toBeTruthy();

  const colWidths = [...container.querySelectorAll('col')].map((col) => col.getAttribute('style') ?? '');
  expect(colWidths).toContain('width: 220px;');
  expect(colWidths).toContain('width: 160px;');
  expect(colWidths).toContain('width: 120px;');
  expect(colWidths).toContain('width: 220px;');
});

test('allows table metadata column widths to be resized from the header handle', () => {
  const rows: TableRuntimeInfo[] = [
    {
      route: 'spark_iceberg',
      displayName: 'Spark Iceberg',
      tableShape: 'spark_iceberg',
      tableIdentifier: 'iceberg_catalog.iceberg_db.cell_kpi_1min',
      storageType: 'Iceberg',
      location: 'hdfs://hdfs-namenode:8020/warehouse/iceberg/iceberg_db/cell_kpi_1min',
      format: 'Parquet',
      columns: 50,
      partitioning: 'days(event_time)',
      bucketingOrDistribution: '',
      indexes: '',
      snapshotOrVersion: 'snapshot=1865824109249660975',
      fileCount: 9,
      totalBytes: 4_567_890,
      rawDetails: 'DESCRIBE EXTENDED output',
      success: true,
      error: '',
    },
  ];

  const { container } = render(<TableMetadataTable rows={rows} />);
  const qualifiedNameResizeHandle = screen.getAllByLabelText('Resize Qualified Name column')[0];

  fireEvent.mouseDown(qualifiedNameResizeHandle, { clientX: 100 });
  fireEvent.mouseMove(window, { clientX: 150 });
  fireEvent.mouseUp(window);

  const colWidths = [...container.querySelectorAll('col')].map((col) => col.getAttribute('style') ?? '');
  expect(colWidths).toContain('width: 270px;');
});

test('summarizes performance-related metadata in row details', () => {
  const rows: TableRuntimeInfo[] = [
    {
      route: 'spark_iceberg',
      displayName: 'Spark Iceberg',
      tableShape: 'spark_iceberg',
      tableIdentifier: 'iceberg_catalog.iceberg_db.cell_kpi_1min',
      storageType: 'Iceberg',
      location: 'hdfs://hdfs-namenode:8020/warehouse/iceberg/iceberg_db/cell_kpi_1min',
      format: 'Parquet',
      columns: 50,
      partitioning: 'days(event_time)',
      bucketingOrDistribution: '',
      indexes: '',
      snapshotOrVersion: 'snapshot=1865824109249660975',
      fileCount: 9,
      totalBytes: 4_567_890,
      rawDetails:
        '# Detailed Table Information\n'
        + 'Type\tMANAGED\n'
        + 'Provider\ticeberg\n'
        + 'Table Properties\t[current-snapshot-id=1865824109249660975,format=iceberg/parquet,format-version=2,write.parquet.compression-codec=zstd]\n',
      success: true,
      error: '',
    },
  ];

  render(<TableMetadataTable rows={rows} />);
  const detail = screen.getByText('Details').closest('details')!;

  expect(within(detail).getByText('Performance Properties')).toBeInTheDocument();
  expect(within(detail).getByText('Table Type')).toBeInTheDocument();
  expect(within(detail).getByText('MANAGED')).toBeInTheDocument();
  expect(within(detail).getByText('Provider')).toBeInTheDocument();
  expect(within(detail).getAllByText('iceberg').length).toBeGreaterThan(0);
  expect(within(detail).getByText('Compression')).toBeInTheDocument();
  expect(within(detail).getByText('zstd')).toBeInTheDocument();
  expect(within(detail).getByText('Format Version')).toBeInTheDocument();
  expect(within(detail).getByText('2')).toBeInTheDocument();
  expect(within(detail).getByText('Files')).toBeInTheDocument();
  expect(within(detail).getByText('9')).toBeInTheDocument();
  expect(within(detail).getByText('Size')).toBeInTheDocument();
  expect(within(detail).getByText('4.4 MB')).toBeInTheDocument();
});

test('summarizes starrocks distribution and storage metadata in row details', () => {
  const rows: TableRuntimeInfo[] = [
    {
      route: 'starrocks_internal',
      displayName: 'StarRocks Internal',
      tableShape: 'starrocks_internal',
      tableIdentifier: 'sr_internal.cell_kpi_1min',
      storageType: 'StarRocks Internal',
      location: '',
      format: '',
      columns: 50,
      partitioning: '',
      bucketingOrDistribution: 'HASH(cell_id)',
      indexes: '',
      snapshotOrVersion: '',
      fileCount: 0,
      tabletCount: 2,
      rowsetCount: 2,
      segmentCount: 26,
      totalBytes: 13_107_200,
      rawDetails:
        'SHOW CREATE TABLE\n'
        + 'Create Table: CREATE TABLE cell_kpi_1min (event_time DATETIME, cell_id VARCHAR(64)) '
        + 'DUPLICATE KEY(event_time, cell_id) DISTRIBUTED BY HASH(cell_id) BUCKETS 24 '
        + 'PROPERTIES ("replication_num" = "1", "compression" = "LZ4")\n\n'
        + 'SHOW PARTITIONS\n'
        + 'PartitionName: cell_kpi_1min\n'
        + 'DataSize: 12.5MB\n'
        + 'RowCount: 0\n\n'
        + 'SHOW TABLET SUMMARY\n'
        + 'tablet_count: 2\n'
        + 'rowset_count: 2\n'
        + 'segment_count: 26\n'
        + 'row_count: 14400\n',
      success: true,
      error: '',
    },
  ];

  render(<TableMetadataTable rows={rows} />);
  const detail = screen.getByText('Details').closest('details')!;

  expect(within(detail).getByText('Engine')).toBeInTheDocument();
  expect(within(detail).getByText('StarRocks')).toBeInTheDocument();
  expect(within(detail).getByText('Key')).toBeInTheDocument();
  expect(within(detail).getByText('DUPLICATE KEY(event_time, cell_id)')).toBeInTheDocument();
  expect(within(detail).getByText('Distribution')).toBeInTheDocument();
  expect(within(detail).getByText('HASH(cell_id)')).toBeInTheDocument();
  expect(within(detail).getByText('Buckets')).toBeInTheDocument();
  expect(within(detail).getByText('24')).toBeInTheDocument();
  expect(within(detail).getByText('Replication')).toBeInTheDocument();
  expect(within(detail).getByText('1')).toBeInTheDocument();
  expect(within(detail).getByText('Rows')).toBeInTheDocument();
  expect(within(detail).getByText('14400')).toBeInTheDocument();
  expect(within(detail).queryByText('Files')).not.toBeInTheDocument();
  expectDetailEntry(detail, 'Tablets', '2');
  expectDetailEntry(detail, 'Rowsets', '2');
  expectDetailEntry(detail, 'Segments', '26');
  expect(within(detail).getByText('Compression')).toBeInTheDocument();
  expect(within(detail).getByText('LZ4')).toBeInTheDocument();
  expect(screen.getByText('26 segments')).toBeInTheDocument();
});

test('does not surface stale Hive metastore numRows zero as a performance row count', () => {
  const rows: TableRuntimeInfo[] = [
    {
      route: 'hive_hdfs_parquet',
      displayName: 'Hive HDFS Parquet',
      tableShape: 'hive_hdfs_parquet',
      tableIdentifier: 'hive_hdfs_parquet.cell_kpi_1min',
      storageType: 'Hive External Parquet',
      location: 'hdfs://hdfs-namenode:8020/services/data-benchmark/generated/kpi/kpi-1b',
      format: 'Parquet',
      columns: 50,
      partitioning: 'event_date STRING',
      bucketingOrDistribution: '',
      indexes: '',
      snapshotOrVersion: '',
      fileCount: 1093,
      totalBytes: 125_905_830_707,
      rawDetails:
        '# Detailed Table Information\n'
        + 'Table Parameters:\n'
        + 'numRows 0\n'
        + 'rawDataSize 0\n'
        + 'totalSize 125905830707\n',
      success: true,
      error: '',
    },
  ];

  render(<TableMetadataTable rows={rows} />);
  const detail = screen.getByText('Details').closest('details')!;

  expect(within(detail).queryByText('Rows')).not.toBeInTheDocument();
});

function expectDetailEntry(detail: HTMLElement, label: string, value: string): void {
  const labelElement = [...detail.querySelectorAll('dt')].find((element) => element.textContent?.trim() === label);
  expect(labelElement).toBeTruthy();
  expect(within(labelElement!.parentElement!).getByText(value)).toBeInTheDocument();
}

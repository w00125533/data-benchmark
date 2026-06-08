import { Empty, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { MouseEvent as ReactMouseEvent } from 'react';
import { useMemo, useState } from 'react';
import type { TableRuntimeInfo } from '../types/report';

type TableMetadataRow = TableRuntimeInfo & {
  logicalTable: string;
  rowSpan: number;
  originalIndex: number;
};

type ColumnKey =
  | 'logicalTable'
  | 'route'
  | 'qualifiedName'
  | 'storage'
  | 'partitioning'
  | 'columns'
  | 'files'
  | 'bytes'
  | 'snapshot'
  | 'detail'
  | 'status';

type ColumnWidths = Record<ColumnKey, number>;

type DetailProperty = {
  label: string;
  value: string;
};

const DEFAULT_COLUMN_WIDTHS: ColumnWidths = {
  logicalTable: 200,
  route: 200,
  qualifiedName: 220,
  storage: 170,
  partitioning: 160,
  columns: 90,
  files: 80,
  bytes: 100,
  snapshot: 120,
  detail: 220,
  status: 100,
};

const MIN_COLUMN_WIDTHS: ColumnWidths = {
  logicalTable: 140,
  route: 150,
  qualifiedName: 160,
  storage: 120,
  partitioning: 120,
  columns: 70,
  files: 70,
  bytes: 80,
  snapshot: 100,
  detail: 160,
  status: 90,
};

function routeLabel(info: TableRuntimeInfo) {
  return info.displayName || info.route || '-';
}

function logicalTableName(info: TableRuntimeInfo) {
  if (info.tableIdentifier) {
    const identifierSegments = info.tableIdentifier.split('.').filter(Boolean);
    return identifierSegments[identifierSegments.length - 1] || info.tableIdentifier;
  }
  const location = info.location || 'unknown table';
  const pathSegments = location.replace(/\\/g, '/').split('/').filter(Boolean);
  return pathSegments[pathSegments.length - 1] || location;
}

function formatBytes(bytes: number) {
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return '-';
  }
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(value >= 10 || unit === 0 ? 0 : 1)} ${units[unit]}`;
}

function formatCount(count: number) {
  return Number.isFinite(count) && count > 0 ? count : '-';
}

function positiveCount(count?: number | null) {
  return Number.isFinite(count) && Number(count) > 0 ? Number(count) : 0;
}

function storageObjectCount(info: TableRuntimeInfo) {
  const segments = positiveCount(info.segmentCount);
  if (info.route === 'starrocks_internal' && segments > 0) {
    return `${segments.toLocaleString()} segments`;
  }
  return formatCount(info.fileCount);
}

function clippedText(value: string, maxWidth = 260) {
  if (!value) {
    return '-';
  }
  return (
    <Typography.Text ellipsis title={value} style={{ display: 'inline-block', maxWidth }}>
      {value}
    </Typography.Text>
  );
}

function cleanedValue(value?: string | number | null) {
  if (value === undefined || value === null) {
    return '';
  }
  return String(value).trim().replace(/\s+/g, ' ');
}

function visibleValue(value?: string | number | null) {
  const cleaned = cleanedValue(value);
  if (!cleaned || cleaned === '-' || cleaned.toLowerCase() === 'none') {
    return '';
  }
  return cleaned;
}

function addProperty(properties: DetailProperty[], label: string, value?: string | number | null) {
  const cleaned = visibleValue(value);
  if (!cleaned || properties.some((property) => property.label === label)) {
    return;
  }
  properties.push({ label, value: cleaned });
}

function addPositiveProperty(properties: DetailProperty[], label: string, value?: string | number | null) {
  const cleaned = visibleValue(value);
  if (!cleaned || !/^\d+$/.test(cleaned) || Number(cleaned) <= 0) {
    return;
  }
  addProperty(properties, label, cleaned);
}

function firstMatch(text: string, patterns: RegExp[]) {
  for (const pattern of patterns) {
    const match = pattern.exec(text);
    if (match?.[1]) {
      return cleanedValue(match[1]);
    }
  }
  return '';
}

function firstPositiveMatch(text: string, patterns: RegExp[]) {
  for (const pattern of patterns) {
    const value = firstMatch(text, [pattern]);
    if (/^\d+$/.test(value) && Number(value) > 0) {
      return value;
    }
  }
  return '';
}

function metadataValue(rawDetails: string, labels: string[]) {
  for (const label of labels) {
    const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const direct = firstMatch(rawDetails, [
      new RegExp(`(?:^|\\n)\\s*${escaped}\\s*(?:\\t|:|=)\\s*([^\\r\\n|]+)`, 'i'),
      new RegExp(`\\|\\s*${escaped}\\s*:?\\s*\\|\\s*([^|\\r\\n]+)`, 'i'),
    ]);
    if (direct) {
      return direct;
    }
  }
  return '';
}

function tableProperty(rawDetails: string, names: string[]) {
  for (const name of names) {
    const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const value = firstMatch(rawDetails, [
      new RegExp(`${escaped}\\s*=\\s*([^,\\]\\s]+)`, 'i'),
      new RegExp(`"${escaped}"\\s*=\\s*"([^"]+)"`, 'i'),
      new RegExp(`'${escaped}'\\s*=\\s*'([^']+)'`, 'i'),
    ]);
    if (value) {
      return value.replace(/^["']|["']$/g, '');
    }
  }
  return '';
}

function engineValue(info: TableRuntimeInfo) {
  if (info.route.startsWith('starrocks')) {
    return 'StarRocks';
  }
  if (info.route.startsWith('spark')) {
    return 'Spark SQL';
  }
  if (info.route.startsWith('hive')) {
    return 'Hive';
  }
  return firstMatch(info.rawDetails || '', [/ENGINE\s*=\s*([A-Z_]+)/i]);
}

function performanceProperties(info: TableRuntimeInfo) {
  const rawDetails = info.rawDetails || '';
  const properties: DetailProperty[] = [];
  const provider = metadataValue(rawDetails, ['Provider']);
  const tableType = metadataValue(rawDetails, ['Type', 'Table Type']);
  const compression = firstMatch(rawDetails, [
    /write\.parquet\.compression-codec\s*=\s*([^,\]\s]+)/i,
    /"compression"\s*=\s*"([^"]+)"/i,
    /'compression'\s*=\s*'([^']+)'/i,
    /compression\s*=\s*([^,\]\s]+)/i,
    /Compressed\s*:\s*([^\r\n|]+)/i,
  ]);
  const formatVersion = tableProperty(rawDetails, ['format-version']);
  const key = firstMatch(rawDetails, [
    /((?:DUPLICATE|PRIMARY|UNIQUE|AGGREGATE)\s+KEY\s*\([^)]*\))/i,
  ]);
  const buckets = firstMatch(rawDetails, [
    /\bBUCKETS\s+(\d+)/i,
    /Num Buckets\s*(?:\t|:|=|\|)\s*([^|\r\n]+)/i,
    /Buckets\s*(?:\t|:|=|\|)\s*([^|\r\n]+)/i,
  ]);
  const replication = firstMatch(rawDetails, [
    /"replication_num"\s*=\s*"([^"]+)"/i,
    /'replication_num'\s*=\s*'([^']+)'/i,
    /ReplicationNum\s*:\s*([^\r\n|]+)/i,
  ]);
  const rowCount = firstPositiveMatch(rawDetails, [
    /RowCount\s*:\s*(\d+)/i,
    /row_count\s*:\s*(\d+)/i,
    /numRows\s*(?:\t|:|=|\|)\s*(\d+)/i,
  ]);
  const inputFormat = metadataValue(rawDetails, ['InputFormat', 'Input Format']);
  const outputFormat = metadataValue(rawDetails, ['OutputFormat', 'Output Format']);
  const serde = metadataValue(rawDetails, ['Serde Library', 'SerDe Library']);

  addProperty(properties, 'Engine', engineValue(info));
  addProperty(properties, 'Table Type', tableType);
  addProperty(properties, 'Provider', provider);
  addProperty(properties, 'Format', info.format || tableProperty(rawDetails, ['format']));
  addProperty(properties, 'Compression', compression);
  addProperty(properties, 'Format Version', formatVersion);
  addProperty(properties, 'Partitioning', info.partitioning);
  addProperty(properties, 'Distribution', info.bucketingOrDistribution);
  addProperty(properties, 'Key', key);
  addProperty(properties, 'Buckets', buckets);
  addProperty(properties, 'Replication', replication);
  if (!(info.route === 'starrocks_internal' && positiveCount(info.segmentCount) > 0)) {
    addProperty(properties, 'Files', formatCount(info.fileCount));
  }
  addPositiveProperty(properties, 'Tablets', info.tabletCount);
  addPositiveProperty(properties, 'Rowsets', info.rowsetCount);
  addPositiveProperty(properties, 'Segments', info.segmentCount);
  addProperty(properties, 'Size', formatBytes(info.totalBytes));
  addProperty(properties, 'Snapshot', info.snapshotOrVersion);
  addPositiveProperty(properties, 'Rows', rowCount);
  addProperty(properties, 'Input Format', inputFormat);
  addProperty(properties, 'Output Format', outputFormat);
  addProperty(properties, 'Serde', serde);

  return properties;
}

function ResizableTitle({
  title,
  columnKey,
  width,
  onResize,
}: {
  title: string;
  columnKey: ColumnKey;
  width: number;
  onResize: (columnKey: ColumnKey, nextWidth: number) => void;
}) {
  function startResize(event: ReactMouseEvent<HTMLButtonElement>) {
    event.preventDefault();
    event.stopPropagation();

    const startX = event.clientX;
    const startWidth = width;

    function move(nextEvent: MouseEvent) {
      const nextWidth = Math.max(MIN_COLUMN_WIDTHS[columnKey], startWidth + nextEvent.clientX - startX);
      onResize(columnKey, nextWidth);
    }

    function stop() {
      window.removeEventListener('mousemove', move);
      window.removeEventListener('mouseup', stop);
    }

    window.addEventListener('mousemove', move);
    window.addEventListener('mouseup', stop);
  }

  return (
    <span style={{ display: 'flex', alignItems: 'center', gap: 6, minWidth: 0 }}>
      <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{title}</span>
      <button
        aria-label={`Resize ${title} column`}
        onMouseDown={startResize}
        style={{
          alignSelf: 'stretch',
          background: 'transparent',
          border: 0,
          borderLeft: '1px solid #d9d9d9',
          cursor: 'col-resize',
          marginLeft: 'auto',
          padding: '0 3px',
          width: 8,
        }}
        type="button"
      />
    </span>
  );
}

function details(info: TableRuntimeInfo) {
  const properties = performanceProperties(info);
  return (
    <details>
      <summary>Details</summary>
      {properties.length > 0 ? (
        <>
          <Typography.Title level={5} style={{ margin: '8px 0 4px' }}>
            Performance Properties
          </Typography.Title>
          <dl>
            {properties.map((property) => (
              <div key={property.label}>
                <dt>{property.label}</dt>
                <dd>{property.value}</dd>
              </div>
            ))}
          </dl>
        </>
      ) : null}
      <dl>
        <dt>Qualified Name</dt>
        <dd>{info.tableIdentifier || '-'}</dd>
        <dt>Location</dt>
        <dd>{info.location || '-'}</dd>
        <dt>Indexes</dt>
        <dd>{info.indexes || '-'}</dd>
      </dl>
      {info.rawDetails ? <pre style={{ margin: '8px 0 0', whiteSpace: 'pre-wrap' }}>{info.rawDetails}</pre> : null}
      {info.error ? <Typography.Text type="danger">{info.error}</Typography.Text> : null}
    </details>
  );
}

function buildRows(infos: TableRuntimeInfo[]): TableMetadataRow[] {
  const sorted = infos
    .map((info, originalIndex) => ({
      ...info,
      logicalTable: logicalTableName(info),
      rowSpan: 1,
      originalIndex,
    }))
    .sort((left, right) => {
      const table = left.logicalTable.localeCompare(right.logicalTable);
      if (table !== 0) return table;
      return routeLabel(left).localeCompare(routeLabel(right));
    });

  return sorted.map((row, index, allRows) => {
    if (index > 0 && allRows[index - 1].logicalTable === row.logicalTable) {
      return { ...row, rowSpan: 0 };
    }

    let rowSpan = 1;
    while (index + rowSpan < allRows.length && allRows[index + rowSpan].logicalTable === row.logicalTable) {
      rowSpan += 1;
    }
    return { ...row, rowSpan };
  });
}

export default function TableMetadataTable({ rows }: { rows?: TableRuntimeInfo[] }) {
  const [columnWidths, setColumnWidths] = useState<ColumnWidths>(DEFAULT_COLUMN_WIDTHS);
  const tableRows = useMemo(() => buildRows(rows ?? []), [rows]);
  const resizeColumn = (columnKey: ColumnKey, nextWidth: number) => {
    setColumnWidths((current) => ({ ...current, [columnKey]: nextWidth }));
  };
  const columnTitle = (title: string, columnKey: ColumnKey) => (
    <ResizableTitle title={title} columnKey={columnKey} width={columnWidths[columnKey]} onResize={resizeColumn} />
  );
  const columns: ColumnsType<TableMetadataRow> = useMemo(
    () => [
      {
        title: columnTitle('Data Table', 'logicalTable'),
        dataIndex: 'logicalTable',
        width: columnWidths.logicalTable,
        onCell: (row) => ({ rowSpan: row.rowSpan }),
      },
      {
        title: columnTitle('Technology Direction', 'route'),
        width: columnWidths.route,
        render: (_, row) => routeLabel(row),
      },
      {
        title: columnTitle('Qualified Name', 'qualifiedName'),
        dataIndex: 'tableIdentifier',
        width: columnWidths.qualifiedName,
        render: (identifier: string) => clippedText(identifier, columnWidths.qualifiedName - 24),
      },
      {
        title: columnTitle('Storage', 'storage'),
        width: columnWidths.storage,
        render: (_, row) => row.storageType || row.format || '-',
      },
      {
        title: columnTitle('Partitioning', 'partitioning'),
        dataIndex: 'partitioning',
        width: columnWidths.partitioning,
        render: (partitioning: string) => clippedText(partitioning || 'none', columnWidths.partitioning - 24),
      },
      {
        title: columnTitle('Columns', 'columns'),
        dataIndex: 'columns',
        align: 'right',
        width: columnWidths.columns,
      },
      {
        title: columnTitle('Files / Objects', 'files'),
        dataIndex: 'fileCount',
        align: 'right',
        width: columnWidths.files,
        render: (_, row) => storageObjectCount(row),
      },
      {
        title: columnTitle('Bytes', 'bytes'),
        dataIndex: 'totalBytes',
        align: 'right',
        width: columnWidths.bytes,
        render: (bytes: number) => formatBytes(bytes),
      },
      {
        title: columnTitle('Snapshot', 'snapshot'),
        dataIndex: 'snapshotOrVersion',
        width: columnWidths.snapshot,
        render: (snapshot: string) => clippedText(snapshot || '-', columnWidths.snapshot - 24),
      },
      {
        title: columnTitle('Detail', 'detail'),
        width: columnWidths.detail,
        render: (_, row) => details(row),
      },
      {
        title: columnTitle('Status', 'status'),
        width: columnWidths.status,
        render: (_, row) => <Tag color={row.success ? 'green' : 'red'}>{row.success ? 'SUCCESS' : 'FAILED'}</Tag>,
      },
    ],
    [columnWidths],
  );
  const scrollX = Object.values(columnWidths).reduce((sum, width) => sum + width, 0);

  if (tableRows.length === 0) {
    return <Empty description="No table runtime metadata was collected for this run." />;
  }

  return (
    <Table
      rowKey={(row) => `${row.route}-${row.tableIdentifier}-${row.originalIndex}`}
      size="small"
      columns={columns}
      dataSource={tableRows}
      pagination={false}
      scroll={{ x: scrollX }}
    />
  );
}

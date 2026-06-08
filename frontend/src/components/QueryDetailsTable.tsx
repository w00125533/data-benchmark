import { Badge, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useMemo } from 'react';
import type { QuerySummary } from '../types/report';

type QueryDetailsRow = QuerySummary & {
  groupKey: string;
  groupRowSpan: number;
  originalIndex: number;
};

const phaseRank: Record<string, number> = {
  COLD: 0,
  WARM: 1,
  HOT: 2,
};

function compareText(left: string, right: string) {
  return left.localeCompare(right);
}

function comparePhase(left: string, right: string) {
  return (phaseRank[left] ?? Number.MAX_SAFE_INTEGER) - (phaseRank[right] ?? Number.MAX_SAFE_INTEGER);
}

function groupKey(row: QuerySummary) {
  return [row.datasetId, row.querySet, row.engine, row.tableShape, row.queryName].join('\u0000');
}

function rowKey(row: QueryDetailsRow) {
  return [row.datasetId, row.querySet, row.engine, row.tableShape, row.queryName, row.phase, row.originalIndex].join('\u0000');
}

function formatPhase(phase: string) {
  return phase ? phase.charAt(0).toUpperCase() + phase.slice(1).toLowerCase() : '-';
}

function exactMs(value: number) {
  return `${value.toFixed(3)} ms`;
}

function buildRows(rows: QuerySummary[]): QueryDetailsRow[] {
  const sortedRows = rows.map((row, originalIndex) => ({ ...row, originalIndex })).sort((left, right) => {
    const engine = compareText(left.engine, right.engine);
    if (engine !== 0) return engine;

    const tableShape = compareText(left.tableShape, right.tableShape);
    if (tableShape !== 0) return tableShape;

    const queryName = compareText(left.queryName, right.queryName);
    if (queryName !== 0) return queryName;

    const datasetId = compareText(left.datasetId, right.datasetId);
    if (datasetId !== 0) return datasetId;

    const querySet = compareText(left.querySet, right.querySet);
    if (querySet !== 0) return querySet;

    return comparePhase(left.phase, right.phase);
  });

  return sortedRows.map((row, index, allRows) => {
    const key = groupKey(row);
    if (index > 0 && groupKey(allRows[index - 1]) === key) {
      return { ...row, groupKey: key, groupRowSpan: 0 };
    }

    let groupRowSpan = 1;
    while (index + groupRowSpan < allRows.length && groupKey(allRows[index + groupRowSpan]) === key) {
      groupRowSpan += 1;
    }

    return { ...row, groupKey: key, groupRowSpan };
  });
}

function groupedCell(row: QueryDetailsRow) {
  return { rowSpan: row.groupRowSpan };
}

const columns: ColumnsType<QueryDetailsRow> = [
  {
    title: 'Engine',
    dataIndex: 'engine',
    width: 180,
    onCell: groupedCell,
  },
  {
    title: 'Query',
    dataIndex: 'queryName',
    width: 260,
    onCell: groupedCell,
  },
  {
    title: 'Phase',
    dataIndex: 'phase',
    width: 110,
    render: (phase: string) => formatPhase(phase),
  },
  {
    title: 'Duration ms',
    dataIndex: 'durationMs',
    align: 'right',
    width: 140,
    render: (durationMs: number) => exactMs(durationMs),
  },
  { title: 'Result Rows', dataIndex: 'rows', align: 'right' },
  {
    title: 'Status',
    render: (_, row) => {
      const isSuccess = row.status === 'SUCCESS';
      return <Badge status={isSuccess ? 'success' : 'error'} text={row.status} />;
    },
  },
  {
    title: 'Error',
    dataIndex: 'error',
    render: (error: string) => (error ? <Typography.Text type="danger">{error}</Typography.Text> : '-'),
  },
];

export default function QueryDetailsTable({ rows }: { rows: QuerySummary[] }) {
  const tableRows = useMemo(() => buildRows(rows), [rows]);

  return (
    <Table
      rowKey={rowKey}
      size="small"
      columns={columns}
      dataSource={tableRows}
      pagination={false}
      scroll={{ x: 1300 }}
    />
  );
}

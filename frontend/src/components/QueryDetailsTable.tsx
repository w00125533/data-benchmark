import { Badge, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { QuerySummary } from '../types/report';

const columns: ColumnsType<QuerySummary> = [
  { title: 'Engine', dataIndex: 'engine', width: 180 },
  { title: 'Table Shape', dataIndex: 'tableShape', width: 220 },
  { title: 'Query', dataIndex: 'queryName', width: 260 },
  { title: 'P50 ms', dataIndex: 'p50Ms', align: 'right' },
  { title: 'P95 ms', dataIndex: 'p95Ms', align: 'right' },
  { title: 'P99 ms', dataIndex: 'p99Ms', align: 'right' },
  { title: 'Rows', dataIndex: 'rows', align: 'right' },
  { title: 'Failures', dataIndex: 'failures', align: 'right' },
  {
    title: 'Status',
    render: (_, row) => {
      const ok = row.success && row.failures === 0;
      return <Badge status={ok ? 'success' : 'error'} text={ok ? 'SUCCESS' : 'FAILED'} />;
    }
  },
  {
    title: 'Error',
    dataIndex: 'error',
    render: (error: string) => (error ? <Typography.Text type="danger">{error}</Typography.Text> : '-')
  }
];

export default function QueryDetailsTable({ rows }: { rows: QuerySummary[] }) {
  return (
    <Table
      rowKey={(row) => `${row.engine}-${row.tableShape}-${row.queryName}`}
      size="small"
      columns={columns}
      dataSource={rows}
      pagination={{ pageSize: 10 }}
    />
  );
}

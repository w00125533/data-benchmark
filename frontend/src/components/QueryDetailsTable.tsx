import { Badge, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { QuerySummary } from '../types/report';

const columns: ColumnsType<QuerySummary> = [
  { title: '引擎', dataIndex: 'engine', width: 180 },
  { title: '表形态', dataIndex: 'tableShape', width: 220 },
  { title: '查询', dataIndex: 'queryName', width: 260 },
  { title: 'P50 ms', dataIndex: 'p50Ms', align: 'right' },
  { title: 'P95 ms', dataIndex: 'p95Ms', align: 'right' },
  { title: 'P99 ms', dataIndex: 'p99Ms', align: 'right' },
  { title: '返回行数', dataIndex: 'rows', align: 'right' },
  {
    title: '运行状态',
    render: (_, row) => {
      const isSuccess = row.status === 'SUCCESS';
      return <Badge status={isSuccess ? 'success' : 'error'} text={row.status} />;
    },
  },
  {
    title: '错误',
    dataIndex: 'error',
    render: (error: string) => (error ? <Typography.Text type="danger">{error}</Typography.Text> : '-'),
  },
];

export default function QueryDetailsTable({ rows }: { rows: QuerySummary[] }) {
  return (
    <Table
      rowKey={(row) => `${row.engine}-${row.tableShape}-${row.queryName}`}
      size="small"
      columns={columns}
      dataSource={rows}
      pagination={{ pageSize: 10 }}
      scroll={{ x: 1300 }}
    />
  );
}

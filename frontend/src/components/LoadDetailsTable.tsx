import { Badge, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { LoadSummary } from '../types/report';

const columns: ColumnsType<LoadSummary> = [
  { title: 'Engine', dataIndex: 'engine', width: 180 },
  { title: 'Table Shape', dataIndex: 'tableShape', width: 220 },
  { title: 'Stage', dataIndex: 'stage', width: 160 },
  { title: 'Rows', dataIndex: 'rows', align: 'right' },
  { title: 'Bytes', dataIndex: 'bytes', align: 'right' },
  { title: 'Duration Seconds', dataIndex: 'durationSeconds', align: 'right' },
  {
    title: 'Status',
    dataIndex: 'success',
    render: (success: boolean) => (
      <Badge status={success ? 'success' : 'error'} text={success ? 'SUCCESS' : 'FAILED'} />
    )
  },
  {
    title: 'Error',
    dataIndex: 'error',
    render: (error: string) => (error ? <Typography.Text type="danger">{error}</Typography.Text> : '-')
  }
];

export default function LoadDetailsTable({ rows }: { rows: LoadSummary[] }) {
  return (
    <Table
      rowKey={(row) => `${row.engine}-${row.tableShape}-${row.stage}`}
      size="small"
      columns={columns}
      dataSource={rows}
      pagination={{ pageSize: 10 }}
    />
  );
}

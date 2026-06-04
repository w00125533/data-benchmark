import { Badge, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { LoadSummary } from '../types/report';

const columns: ColumnsType<LoadSummary> = [
  { title: '引擎', dataIndex: 'engine', width: 180 },
  { title: '表形态', dataIndex: 'tableShape', width: 220 },
  { title: '阶段', dataIndex: 'stage', width: 160 },
  { title: '行数', dataIndex: 'rows', align: 'right' },
  { title: '字节', dataIndex: 'bytes', align: 'right' },
  { title: '耗时 秒', dataIndex: 'durationSeconds', align: 'right' },
  {
    title: '运行状态',
    dataIndex: 'success',
    render: (success: boolean) => (
      <Badge status={success ? 'success' : 'error'} text={success ? 'SUCCESS' : 'FAILED'} />
    )
  },
  {
    title: '错误',
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
      scroll={{ x: 1000 }}
    />
  );
}

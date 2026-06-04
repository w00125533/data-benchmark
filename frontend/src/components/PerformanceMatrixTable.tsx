import { Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { PerformanceMatrixRow, RouteKey, RouteResult } from '../types/report';

const routeLabels: Record<RouteKey, string> = {
  spark_iceberg: 'Spark Iceberg',
  starrocks_internal: 'StarRocks Internal',
  starrocks_external_iceberg: 'StarRocks External Iceberg',
};

const statusColor: Record<RouteResult['status'], string> = {
  SUCCESS: 'green',
  FAILED: 'red',
  SKIPPED: 'default',
};

function formatMs(value: number) {
  return `${Number.isFinite(value) ? value : 0} ms`;
}

function RouteCell({ result }: { result: RouteResult }) {
  if (result.status === 'SKIPPED') {
    return <Tag color={statusColor.SKIPPED}>SKIPPED</Tag>;
  }

  if (result.status === 'FAILED') {
    return (
      <div>
        <Tag color={statusColor.FAILED}>FAILED</Tag>
        <Typography.Text type="danger">{result.error || '-'}</Typography.Text>
      </div>
    );
  }

  return (
    <div>
      <Tag color={statusColor.SUCCESS}>SUCCESS</Tag>
      <div>
        <strong>p95 {formatMs(result.p95Ms)}</strong>
      </div>
      <div>
        p50 {formatMs(result.p50Ms)} / p99 {formatMs(result.p99Ms)}
      </div>
      <div>rows {result.rows.toLocaleString()}</div>
    </div>
  );
}

function BestRouteCell({ row }: { row: PerformanceMatrixRow }) {
  if (!row.bestRoute) {
    return '-';
  }

  return (
    <div>
      <strong>{routeLabels[row.bestRoute]}</strong>
      <div>best p95 {formatMs(row.bestRouteP95Ms)}</div>
    </div>
  );
}

export default function PerformanceMatrixTable({ rows }: { rows: PerformanceMatrixRow[] }) {
  const columns: ColumnsType<PerformanceMatrixRow> = [
    { title: '数据集', dataIndex: 'datasetName', key: 'datasetName', width: 160 },
    { title: 'Query Set', dataIndex: 'querySet', key: 'querySet', width: 120 },
    { title: 'SQL', dataIndex: 'queryName', key: 'queryName', width: 220 },
    {
      title: routeLabels.spark_iceberg,
      key: 'spark_iceberg',
      render: (_, row) => <RouteCell result={row.routes.spark_iceberg} />,
    },
    {
      title: routeLabels.starrocks_internal,
      key: 'starrocks_internal',
      render: (_, row) => <RouteCell result={row.routes.starrocks_internal} />,
    },
    {
      title: routeLabels.starrocks_external_iceberg,
      key: 'starrocks_external_iceberg',
      render: (_, row) => <RouteCell result={row.routes.starrocks_external_iceberg} />,
    },
    {
      title: '最优路线',
      key: 'bestRoute',
      render: (_, row) => <BestRouteCell row={row} />,
      width: 180,
    },
  ];

  return (
    <Table
      size="small"
      rowKey={(row) => `${row.datasetId}:${row.querySet}:${row.queryName}`}
      columns={columns}
      dataSource={[...rows].sort((a, b) =>
        `${a.datasetName}:${a.querySet}:${a.queryName}`.localeCompare(
          `${b.datasetName}:${b.querySet}:${b.queryName}`,
        ),
      )}
      pagination={false}
      scroll={{ x: 1200 }}
    />
  );
}

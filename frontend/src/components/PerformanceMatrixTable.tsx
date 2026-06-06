import { Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { DatasetInfo, PerformanceMatrixRow, RouteKey, RouteResult } from '../types/report';

const routeLabels: Record<RouteKey, string> = {
  spark_native_parquet: 'Spark SQL Native Parquet',
  spark_iceberg: 'Spark Iceberg',
  starrocks_internal: 'StarRocks Internal',
  starrocks_external_iceberg: 'StarRocks External Iceberg',
  hive_hdfs_parquet: 'Hive HDFS Parquet',
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
    return (
      <div>
        <Tag color={statusColor.SKIPPED}>SKIPPED</Tag>
        <div>cold {formatMs(result.coldMs)}</div>
        <div>warm {formatMs(result.warmMs)}</div>
        <div>hot {formatMs(result.hotMs)}</div>
      </div>
    );
  }

  if (result.status === 'FAILED') {
    return (
      <div>
        <Tag color={statusColor.FAILED}>FAILED</Tag>
        <div>cold {formatMs(result.coldMs)}</div>
        <div>warm {formatMs(result.warmMs)}</div>
        <div>hot {formatMs(result.hotMs)}</div>
        <Typography.Text type="danger">{result.error || '-'}</Typography.Text>
      </div>
    );
  }

  return (
    <div>
      <Tag color={statusColor.SUCCESS}>SUCCESS</Tag>
      <div>cold {formatMs(result.coldMs)}</div>
      <div>warm {formatMs(result.warmMs)}</div>
      <div>
        <strong>hot {formatMs(result.hotMs)}</strong>
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
      <div>best hot {formatMs(row.bestRouteHotMs)}</div>
    </div>
  );
}

function DatasetCell({ row, dataset }: { row: PerformanceMatrixRow; dataset: DatasetInfo }) {
  return (
    <div>
      <strong>{row.datasetName}</strong>
      <div>datasetId {row.datasetId}</div>
      <div>rows {dataset.rows.toLocaleString()}</div>
      <div>
        cells {dataset.cells.toLocaleString()} / days {dataset.days.toLocaleString()}
      </div>
    </div>
  );
}

export default function PerformanceMatrixTable({
  rows,
  dataset,
}: {
  rows: PerformanceMatrixRow[];
  dataset: DatasetInfo;
}) {
  const columns: ColumnsType<PerformanceMatrixRow> = [
    {
      title: '数据集',
      key: 'dataset',
      width: 220,
      render: (_, row) => <DatasetCell row={row} dataset={dataset} />,
    },
    { title: 'Query Set', dataIndex: 'querySet', key: 'querySet', width: 120 },
    { title: 'SQL', dataIndex: 'queryName', key: 'queryName', width: 220 },
    {
      title: routeLabels.spark_native_parquet,
      key: 'spark_native_parquet',
      render: (_, row) => <RouteCell result={row.routes.spark_native_parquet} />,
    },
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
      title: routeLabels.hive_hdfs_parquet,
      key: 'hive_hdfs_parquet',
      render: (_, row) => <RouteCell result={row.routes.hive_hdfs_parquet} />,
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
      locale={{
        emptyText:
          'No comparable route query results. Run compose mode to compare Spark SQL Native Parquet, Spark Iceberg, StarRocks Internal, StarRocks External Iceberg, and Hive HDFS Parquet.',
      }}
    />
  );
}

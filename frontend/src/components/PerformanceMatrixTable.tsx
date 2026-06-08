import { Empty, Tag, Typography } from 'antd';
import type { DatasetInfo, PerformanceMatrixRow, RouteKey, RouteResult } from '../types/report';

const routeLabels: Record<RouteKey, string> = {
  spark_native_parquet: 'Spark SQL Native Parquet',
  spark_iceberg: 'Spark Iceberg',
  starrocks_internal: 'StarRocks Internal',
  starrocks_external_iceberg: 'StarRocks External Iceberg',
  hive_hdfs_parquet: 'Hive HDFS Parquet',
};

const routeColors: Record<RouteKey, string> = {
  spark_native_parquet: '#2563eb',
  spark_iceberg: '#0891b2',
  starrocks_internal: '#dc2626',
  starrocks_external_iceberg: '#f97316',
  hive_hdfs_parquet: '#16a34a',
};

const routeKeys = Object.keys(routeLabels) as RouteKey[];

const phases = [
  { key: 'coldMs', statusKey: 'coldStatus', label: 'Cold' },
  { key: 'warmMs', statusKey: 'warmStatus', label: 'Warm' },
  { key: 'hotMs', statusKey: 'hotStatus', label: 'Hot' },
] as const;

const statusColor: Record<RouteResult['status'], string> = {
  SUCCESS: 'green',
  FAILED: 'red',
  SKIPPED: 'default',
};

function formatMs(value: number) {
  if (!Number.isFinite(value)) {
    return '0 ms';
  }
  if (value >= 1000) {
    return `${(value / 1000).toFixed(value >= 10000 ? 1 : 2)} s`;
  }
  return `${value.toFixed(value >= 100 ? 1 : 2)} ms`;
}

function exactMs(value: number) {
  return `${Number.isFinite(value) ? value.toFixed(3) : '0.000'} ms`;
}

function validPhaseDuration(result: RouteResult, phase: (typeof phases)[number]) {
  const value = result[phase.key];
  return result[phase.statusKey] === 'SUCCESS' && Number.isFinite(value) && value > 0 ? value : 0;
}

function bestRouteForPhase(row: PerformanceMatrixRow, phase: (typeof phases)[number]) {
  return routeKeys
    .map((route) => ({ route, result: row.routes[route], value: validPhaseDuration(row.routes[route], phase) }))
    .filter(({ value }) => value > 0)
    .sort((a, b) => a.value - b.value)[0];
}

function DatasetSummary({ row, dataset }: { row: PerformanceMatrixRow; dataset: DatasetInfo }) {
  return (
    <div style={{ color: '#4b5563', fontSize: 12 }}>
      <span>dataset {row.datasetName}</span>
      <span style={{ marginLeft: 12 }}>query set {row.querySet}</span>
      <span style={{ marginLeft: 12 }}>rows {dataset.rows.toLocaleString()}</span>
      <span style={{ marginLeft: 12 }}>
        cells {dataset.cells.toLocaleString()} / days {dataset.days.toLocaleString()}
      </span>
    </div>
  );
}

function SqlDetails({ row }: { row: PerformanceMatrixRow }) {
  const sqlByRoute = row.sqlByRoute ?? {};
  const entries = routeKeys
    .map((route) => ({ route, sql: sqlByRoute[route]?.trim() ?? '' }))
    .filter((entry) => entry.sql.length > 0);

  if (entries.length === 0) {
    return null;
  }

  return (
    <details className="matrix-sql-details">
      <summary>Actual SQL sent by route</summary>
      {entries.map((entry) => (
        <details className="matrix-route-sql" key={entry.route}>
          <summary>{routeLabels[entry.route]}</summary>
          <pre>
            <code>{entry.sql}</code>
          </pre>
        </details>
      ))}
    </details>
  );
}

function PhaseRows({ row }: { row: PerformanceMatrixRow }) {
  return (
    <table className="matrix-phase-table">
      <thead>
        <tr>
          <th>Phase</th>
          {routeKeys.map((route) => (
            <th key={route}>{routeLabels[route]}</th>
          ))}
          <th>Best</th>
        </tr>
      </thead>
      <tbody>
        {phases.map((phase) => {
          const best = bestRouteForPhase(row, phase);

          return (
            <tr key={phase.key}>
              <td className="phase-name">{phase.label}</td>
              {routeKeys.map((route) => {
                const result = row.routes[route];
                const status = result[phase.statusKey];
                return (
                  <td key={route}>
                    <Tag color={statusColor[status]}>{status}</Tag>
                    <div className="phase-ms">{exactMs(result[phase.key])}</div>
                    <div className="phase-rows">rows {result.rows.toLocaleString()}</div>
                    {result.error ? <Typography.Text type="danger">{result.error}</Typography.Text> : null}
                  </td>
                );
              })}
              <td>
                {best ? (
                  <>
                    <strong>{routeLabels[best.route]}</strong>
                    <div className="phase-ms">{exactMs(best.result[phase.key])}</div>
                  </>
                ) : (
                  '-'
                )}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function PhaseChart({ row, phase }: { row: PerformanceMatrixRow; phase: (typeof phases)[number] }) {
  const values = routeKeys.map((route) => ({
    route,
    label: routeLabels[route],
    color: routeColors[route],
    value: validPhaseDuration(row.routes[route], phase),
  }));
  const max = Math.max(1, ...values.map((value) => value.value));

  return (
    <div className="matrix-chart">
      <div className="matrix-chart-title">
        {phase.label} query
        <span>linear scale, max {formatMs(max)}</span>
      </div>
      {values.map((value) => {
        const width = value.value <= 0 ? 0 : (value.value / max) * 100;
        return (
          <div className="matrix-bar-row" key={value.route}>
            <div className="matrix-bar-label">{value.label}</div>
            <div className="matrix-track">
              <div
                className="matrix-bar"
                style={{
                  width: `${width.toFixed(4)}%`,
                  background: value.color,
                }}
              />
            </div>
            <strong>{formatMs(value.value)}</strong>
          </div>
        );
      })}
    </div>
  );
}

function QueryBlock({ row, dataset }: { row: PerformanceMatrixRow; dataset: DatasetInfo }) {
  return (
    <section className="matrix-query-block">
      <div className="matrix-query-header">
        <div>
          <div className="matrix-eyebrow">SQL</div>
          <h3>{row.queryName}</h3>
          <SqlDetails row={row} />
          <DatasetSummary row={row} dataset={dataset} />
        </div>
        <div className="matrix-best">
          <span>Best hot route</span>
          <strong>{row.bestRoute ? routeLabels[row.bestRoute] : '-'}</strong>
          <div>{row.bestRoute ? exactMs(row.bestRouteHotMs) : '-'}</div>
        </div>
      </div>
      <PhaseRows row={row} />
      <div className="matrix-chart-grid">
        {phases.map((phase) => (
          <PhaseChart key={phase.key} row={row} phase={phase} />
        ))}
      </div>
    </section>
  );
}

export default function PerformanceMatrixTable({
  rows,
  dataset,
}: {
  rows: PerformanceMatrixRow[];
  dataset: DatasetInfo;
}) {
  if (rows.length === 0) {
    return (
      <Empty
        description="No comparable route query results. Run compose mode to compare Spark SQL Native Parquet, Spark Iceberg, StarRocks Internal, StarRocks External Iceberg, and Hive HDFS Parquet."
      />
    );
  }

  return (
    <div className="matrix-list">
      <style>{`
        .matrix-list {
          display: grid;
          gap: 16px;
        }
        .matrix-query-block {
          border: 1px solid #e5e7eb;
          border-radius: 8px;
          background: #fff;
          padding: 16px;
        }
        .matrix-query-header {
          display: flex;
          justify-content: space-between;
          gap: 16px;
          align-items: flex-start;
          margin-bottom: 12px;
        }
        .matrix-eyebrow {
          color: #6b7280;
          font-size: 12px;
          font-weight: 700;
          text-transform: uppercase;
        }
        .matrix-query-header h3 {
          margin: 2px 0 6px;
          font-size: 18px;
        }
        .matrix-sql-details {
          margin: 0 0 8px;
          color: #374151;
          font-size: 12px;
        }
        .matrix-sql-details summary,
        .matrix-route-sql summary {
          cursor: pointer;
          font-weight: 700;
        }
        .matrix-route-sql {
          margin-top: 8px;
        }
        .matrix-route-sql pre {
          max-width: min(980px, calc(100vw - 120px));
          max-height: 280px;
          margin: 6px 0 0;
          overflow: auto;
          border: 1px solid #e5e7eb;
          border-radius: 6px;
          background: #111827;
          color: #f9fafb;
          padding: 10px;
          font-size: 12px;
          line-height: 1.5;
          white-space: pre;
        }
        .matrix-best {
          min-width: 220px;
          text-align: right;
          color: #374151;
          font-size: 12px;
        }
        .matrix-best span {
          display: block;
          color: #6b7280;
        }
        .matrix-best strong {
          display: block;
          color: #111827;
          font-size: 13px;
        }
        .matrix-phase-table {
          width: 100%;
          border-collapse: collapse;
          font-size: 12px;
        }
        .matrix-phase-table th,
        .matrix-phase-table td {
          border-top: 1px solid #e5e7eb;
          padding: 8px;
          text-align: left;
          vertical-align: top;
        }
        .matrix-phase-table th {
          background: #f9fafb;
          color: #374151;
          font-weight: 700;
        }
        .phase-name,
        .phase-ms {
          font-weight: 700;
        }
        .phase-rows {
          color: #6b7280;
        }
        .matrix-chart-grid {
          display: grid;
          grid-template-columns: repeat(3, minmax(280px, 1fr));
          gap: 12px;
          margin-top: 14px;
        }
        .matrix-chart {
          border: 1px solid #e5e7eb;
          border-radius: 6px;
          background: #fbfdff;
          padding: 10px;
        }
        .matrix-chart-title {
          display: flex;
          justify-content: space-between;
          gap: 8px;
          font-weight: 700;
          margin-bottom: 8px;
        }
        .matrix-chart-title span {
          color: #6b7280;
          font-size: 11px;
          font-weight: 400;
        }
        .matrix-bar-row {
          display: grid;
          grid-template-columns: 170px 1fr 68px;
          gap: 8px;
          align-items: center;
          margin: 7px 0;
          font-size: 12px;
        }
        .matrix-bar-label {
          color: #374151;
        }
        .matrix-track {
          height: 18px;
          background: #eef2f7;
          border-radius: 3px;
          overflow: hidden;
        }
        .matrix-bar {
          height: 100%;
        }
        @media (max-width: 1300px) {
          .matrix-query-header {
            display: block;
          }
          .matrix-best {
            text-align: left;
            margin-top: 10px;
          }
          .matrix-chart-grid {
            grid-template-columns: 1fr;
          }
          .matrix-phase-table {
            display: block;
            overflow-x: auto;
            white-space: nowrap;
          }
        }
      `}</style>
      {[...rows]
        .sort((a, b) =>
          `${a.datasetName}:${a.querySet}:${a.queryName}`.localeCompare(
            `${b.datasetName}:${b.querySet}:${b.queryName}`,
          ),
        )
        .map((row) => (
          <QueryBlock key={`${row.datasetId}:${row.querySet}:${row.queryName}`} row={row} dataset={dataset} />
        ))}
    </div>
  );
}

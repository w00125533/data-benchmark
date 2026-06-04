import { Card, Tag, Timeline } from 'antd';
import type { LoadSummary, QuerySummary, WebBenchmarkReport } from '../types/report';

interface StageItem {
  stage: string;
  engine: string;
  tableShape: string;
  duration: string;
  success: boolean;
  error: string;
}

export default function StageTimeline({ report }: { report: WebBenchmarkReport }) {
  const stages = [...report.loads.map(toLoadStage), ...report.queries.map(toQueryStage)];

  return (
    <Card size="small" title="阶段状态流">
      <Timeline
        items={stages.map((stage) => ({
          color: stage.success ? 'green' : 'red',
          children: (
            <>
              <Tag color={stage.success ? 'green' : 'red'}>{stage.success ? 'SUCCESS' : 'FAILED'}</Tag>
              <strong>{stage.stage}</strong> {stage.engine} / {stage.tableShape}
              <span style={{ marginLeft: 8 }}>{stage.duration}</span>
              {stage.error ? <div style={{ color: '#cf1322', marginTop: 4 }}>{stage.error}</div> : null}
            </>
          )
        }))}
      />
    </Card>
  );
}

function toLoadStage(load: LoadSummary): StageItem {
  return {
    stage: load.stage,
    engine: load.engine,
    tableShape: load.tableShape,
    duration: `${formatNumber(load.durationSeconds)}s`,
    success: load.success,
    error: load.error
  };
}

function toQueryStage(query: QuerySummary): StageItem {
  return {
    stage: query.queryName,
    engine: query.engine,
    tableShape: query.tableShape,
    duration: `${formatNumber(query.p95Ms)}ms`,
    success: query.success && query.failures === 0,
    error: query.error
  };
}

function formatNumber(value: number) {
  return Number.isFinite(value) ? value.toFixed(3).replace(/\.?0+$/, '') : '-';
}

import { Bar, Column } from '@ant-design/plots';
import { Card, Col, Empty, Row } from 'antd';
import type { WebBenchmarkReport } from '../types/report';

export default function EngineComparison({ report }: { report: WebBenchmarkReport }) {
  const p95Latency = report.charts.queryLatencyByEngine.filter((item) => item.metric === 'p95');

  return (
    <Row gutter={[16, 16]}>
      <Col xs={24} xl={12}>
        <Card size="small" title="加载耗时对比">
          {report.charts.loadDurationByEngine.length === 0 ? (
            <Empty description="暂无加载数据" />
          ) : (
            <Column
              height={280}
              data={report.charts.loadDurationByEngine}
              xField="engine"
              yField="durationSeconds"
              seriesField="tableShape"
              colorField="tableShape"
            />
          )}
        </Card>
      </Col>
      <Col xs={24} xl={12}>
        <Card size="small" title="查询 P95 延迟对比">
          {p95Latency.length === 0 ? (
            <Empty description="暂无查询数据" />
          ) : (
            <Column
              height={280}
              data={p95Latency}
              xField="queryName"
              yField="latencyMs"
              seriesField="engine"
              colorField="engine"
            />
          )}
        </Card>
      </Col>
      <Col xs={24} xl={12}>
        <Card size="small" title="查询返回行数">
          {report.charts.queryRowsByEngine.length === 0 ? (
            <Empty description="暂无行数数据" />
          ) : (
            <Bar
              height={280}
              data={report.charts.queryRowsByEngine}
              xField="rows"
              yField="queryName"
              seriesField="engine"
              colorField="engine"
            />
          )}
        </Card>
      </Col>
      <Col xs={24} xl={12}>
        <Card size="small" title="失败分布">
          {report.charts.failureSummary.length === 0 ? (
            <Empty description="暂无失败数据" />
          ) : (
            <Column
              height={280}
              data={report.charts.failureSummary}
              xField="stage"
              yField="failures"
              seriesField="engine"
              colorField="engine"
            />
          )}
        </Card>
      </Col>
    </Row>
  );
}

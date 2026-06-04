import { Alert, Card, Col, Descriptions, Row, Statistic, Tag } from 'antd';
import type { WebBenchmarkReport } from '../types/report';

export default function RunSummary({ report }: { report: WebBenchmarkReport }) {
  return (
    <Row gutter={[16, 16]}>
      <Col xs={24} lg={16}>
        <Card size="small" title="运行摘要">
          <Descriptions size="small" bordered column={{ xs: 1, md: 2 }}>
            <Descriptions.Item label="Run ID">{report.run.runId}</Descriptions.Item>
            <Descriptions.Item label="状态">
              <Tag color={report.run.status === 'SUCCESS' ? 'green' : 'orange'}>{report.run.status}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="Profile">{report.run.profile}</Descriptions.Item>
            <Descriptions.Item label="Suite">{report.run.suite}</Descriptions.Item>
            <Descriptions.Item label="Query Set">{report.run.querySet}</Descriptions.Item>
            <Descriptions.Item label="耗时">{formatNumber(report.run.durationSeconds)}s</Descriptions.Item>
            <Descriptions.Item label="开始时间">{formatTime(report.run.startedAt)}</Descriptions.Item>
            <Descriptions.Item label="结束时间">{formatTime(report.run.endedAt)}</Descriptions.Item>
          </Descriptions>
          {report.notices.map((notice) => (
            <Alert key={notice} style={{ marginTop: 12 }} type="info" message={notice} showIcon />
          ))}
        </Card>
      </Col>
      <Col xs={24} lg={8}>
        <Row gutter={[16, 16]}>
          <Col span={12}>
            <Card size="small">
              <Statistic title="Rows" value={report.dataset.rows} />
            </Card>
          </Col>
          <Col span={12}>
            <Card size="small">
              <Statistic title="Bytes" value={report.dataset.bytesWritten} />
            </Card>
          </Col>
          <Col span={12}>
            <Card size="small">
              <Statistic title="Cells" value={report.dataset.cells} />
            </Card>
          </Col>
          <Col span={12}>
            <Card size="small">
              <Statistic title="Days" value={report.dataset.days} />
            </Card>
          </Col>
        </Row>
      </Col>
    </Row>
  );
}

function formatTime(value: string) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function formatNumber(value: number) {
  return Number.isFinite(value) ? value.toFixed(3).replace(/\.?0+$/, '') : '-';
}

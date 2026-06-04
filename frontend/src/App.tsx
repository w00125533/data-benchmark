import { Alert, Card, Layout, Space, Spin, Typography } from 'antd';
import { useEffect, useState } from 'react';
import LoadDetailsTable from './components/LoadDetailsTable';
import PerformanceMatrixTable from './components/PerformanceMatrixTable';
import QueryDetailsTable from './components/QueryDetailsTable';
import RunSummary from './components/RunSummary';
import { loadReport } from './data/reportLoader';
import type { WebBenchmarkReport } from './types/report';

export default function App() {
  const [report, setReport] = useState<WebBenchmarkReport>();
  const [error, setError] = useState<string>();

  useEffect(() => {
    let active = true;
    loadReport()
      .then((nextReport) => {
        if (active) {
          setReport(nextReport);
          setError(undefined);
        }
      })
      .catch((err: Error) => {
        if (active) {
          setError(err.message);
        }
      });
    return () => {
      active = false;
    };
  }, []);

  if (error) {
    return (
      <Layout style={{ minHeight: '100vh', background: '#f5f7fb' }}>
        <Layout.Content style={{ padding: 24 }}>
          <Alert type="error" message="报告数据加载失败" description={error} />
        </Layout.Content>
      </Layout>
    );
  }

  if (!report) {
    return (
      <Layout style={{ minHeight: '100vh', background: '#f5f7fb' }}>
        <Layout.Content style={{ padding: 24 }}>
          <Spin />
        </Layout.Content>
      </Layout>
    );
  }

  return (
    <Layout style={{ minHeight: '100vh', background: '#f5f7fb' }}>
      <Layout.Content style={{ padding: 24 }}>
        <Space direction="vertical" size={16} style={{ width: '100%' }}>
          <Typography.Title level={3} style={{ margin: 0 }}>
            Data Benchmark Report
          </Typography.Title>
          {report.run.status === 'DEGRADED' ? (
            <Alert type="warning" showIcon message="本次运行存在失败阶段，请查看明细错误。" />
          ) : null}
          <RunSummary report={report} />
          <Card size="small" title="性能矩阵">
            {report.performanceMatrix.length === 0 ? (
              <Alert
                type="info"
                showIcon
                style={{ marginBottom: 12 }}
                message="本次运行没有三技术路线性能对比数据。"
                description="local smoke 只生成本地数据并验证静态报告渲染，不会执行 Spark Iceberg、StarRocks Internal、StarRocks External Iceberg 查询。请使用 compose 模式生成真实对比报告。"
              />
            ) : null}
            <PerformanceMatrixTable rows={report.performanceMatrix} dataset={report.dataset} />
          </Card>
          <Card size="small" title="Load 明细">
            <LoadDetailsTable rows={report.loads} />
          </Card>
          <Card size="small" title="Query 明细">
            <QueryDetailsTable rows={report.queries} />
          </Card>
        </Space>
      </Layout.Content>
    </Layout>
  );
}

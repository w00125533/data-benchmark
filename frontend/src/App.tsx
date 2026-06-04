import { Alert, Layout, Space, Spin, Typography } from 'antd';
import { useEffect, useState } from 'react';
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
          <Alert
            type={report.run.status === 'SUCCESS' ? 'success' : 'warning'}
            message={`运行状态: ${report.run.status}`}
            showIcon
          />
          <Typography.Text>Run ID: {report.run.runId}</Typography.Text>
          <Typography.Text>Suite: {report.run.suite}</Typography.Text>
        </Space>
      </Layout.Content>
    </Layout>
  );
}

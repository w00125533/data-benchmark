import '@testing-library/jest-dom/vitest';
import { render, screen, within } from '@testing-library/react';
import { beforeAll, expect, test, vi } from 'vitest';
import LoadDetailsTable from './LoadDetailsTable';
import type { LoadSummary } from '../types/report';

beforeAll(() => {
  const getComputedStyle = window.getComputedStyle;
  window.getComputedStyle = ((element: Element) => getComputedStyle(element)) as typeof window.getComputedStyle;

  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: vi.fn().mockImplementation((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })),
  });
});

test('labels load bytes as input bytes to distinguish source size from table storage size', () => {
  const rows: LoadSummary[] = [
    {
      engine: 'starrocks',
      tableShape: 'starrocks_internal',
      stage: 'STARROCKS_INTERNAL_LOAD',
      rows: 1_000_000_000,
      bytes: 125_905_830_707,
      durationSeconds: 1220.005,
      success: true,
      error: '',
    },
  ];

  const { container } = render(<LoadDetailsTable rows={rows} />);

  expect(within(container.querySelector('thead')!).getByText('Input Bytes')).toBeInTheDocument();
  expect(screen.queryByRole('columnheader', { name: 'Bytes' })).not.toBeInTheDocument();
});

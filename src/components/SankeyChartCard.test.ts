import { describe, expect, it } from 'vitest';

import { escapeTooltipHtml } from './SankeyChartCard';

describe('escapeTooltipHtml', () => {
  it('escapes source-provided HTML before rendering a chart tooltip', () => {
    expect(escapeTooltipHtml('<img src=x onerror="alert(1)"> & O\'Brien')).toBe(
      '&lt;img src=x onerror=&quot;alert(1)&quot;&gt; &amp; O&#39;Brien',
    );
  });
});

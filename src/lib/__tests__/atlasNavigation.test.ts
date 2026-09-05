import { describe, expect, it } from 'vitest';

import {
  atlasBackLabel,
  backAtlasView,
  defaultAtlasYear,
  pageAtlasView,
  preserveAtlasViewForYear,
  pushAtlasView,
  type AtlasDrilldownState,
} from '../atlasNavigation';

describe('atlasNavigation', () => {
  it('preserves the previous scoped view when paging the current drilldown', () => {
    const schoolView: AtlasDrilldownState = {
      scope: 'school',
      nodeId: 'region:Středočeský',
      label: 'Středočeský',
      offset: 25,
    };
    const healthView: AtlasDrilldownState = {
      scope: 'health',
      nodeId: 'health:specialty:hp31|Hlavní město Praha|všeobecné praktické lékařství',
      label: 'všeobecné praktické lékařství',
      offset: 0,
    };

    const stack = pushAtlasView([schoolView], healthView);
    const paged = pageAtlasView(stack, 28);

    expect(paged[0]).toEqual(schoolView);
    expect(paged[1]).toEqual({ ...healthView, offset: 28 });
  });

  it('restores the exact previous view on back', () => {
    const stack: AtlasDrilldownState[] = [
      { scope: 'school', nodeId: 'founder:abc', label: 'Kraj', offset: 20 },
      { scope: 'mv', nodeId: 'security:police', label: 'Policie CR', offset: 0 },
    ];

    expect(backAtlasView(stack)).toEqual([{ scope: 'school', nodeId: 'founder:abc', label: 'Kraj', offset: 20 }]);
    expect(atlasBackLabel(stack, 'Sjednoceny Sankey')).toBe('Kraj');
  });

  it('defaults to 2025 and otherwise uses the newest advertised year', () => {
    expect(defaultAtlasYear([2024, 2025, 2026])).toBe(2025);
    expect(defaultAtlasYear([2023, 2024])).toBe(2024);
    expect(defaultAtlasYear([])).toBeNull();
  });

  it('keeps semantic drilldown IDs but resets pagination when the year changes', () => {
    const stack: AtlasDrilldownState[] = [
      { scope: 'school', nodeId: 'founder:00063941', label: 'Praha 10', offset: 28 },
      { scope: 'school', nodeId: 'school:600040925', label: 'Mateřská škola', offset: 0 },
    ];

    expect(preserveAtlasViewForYear(stack)).toEqual([
      { ...stack[0], offset: 0 },
      stack[1],
    ]);
  });
});

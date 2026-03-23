export type AtlasDrilldownState =
  | { scope: 'school'; nodeId: string; label: string; offset: number }
  | { scope: 'health'; nodeId: string | null; label: string; offset: number }
  | { scope: 'mv'; nodeId: string | null; label: string; offset: number }
  | { scope: 'transport'; nodeId: string | null; label: string; offset: number }
  | { scope: 'agriculture'; nodeId: string | null; label: string; offset: number }
  | { scope: 'justice'; nodeId: string | null; label: string; offset: number };

export function pushAtlasView(
  stack: AtlasDrilldownState[],
  view: AtlasDrilldownState,
): AtlasDrilldownState[] {
  return [...stack, view];
}

export function pageAtlasView(
  stack: AtlasDrilldownState[],
  delta: number,
): AtlasDrilldownState[] {
  const current = stack.at(-1);
  if (!current) return stack;
  return [
    ...stack.slice(0, -1),
    { ...current, offset: Math.max(0, current.offset + delta) },
  ];
}

export function backAtlasView(stack: AtlasDrilldownState[]): AtlasDrilldownState[] {
  return stack.slice(0, -1);
}

export function atlasBackLabel(stack: AtlasDrilldownState[], rootLabel: string): string {
  return stack.length > 1 ? stack.at(-2)!.label : rootLabel;
}

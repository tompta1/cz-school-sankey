export function formatCzk(value: number): string {
  return new Intl.NumberFormat('cs-CZ', {
    style: 'currency',
    currency: 'CZK',
    maximumFractionDigits: 0
  }).format(value);
}

export function formatCompactCzk(value: number): string {
  const absolute = Math.abs(value);

  if (absolute >= 1_000_000_000) {
    return `${(value / 1_000_000_000).toFixed(1)} mld. Kč`;
  }

  if (absolute >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1)} mil. Kč`;
  }

  if (absolute >= 1_000) {
    return `${(value / 1_000).toFixed(1)} tis. Kč`;
  }

  return formatCzk(value);
}

export function formatPerPupil(czk: number): string {
  const rounded = Math.round(czk);
  return `${new Intl.NumberFormat('cs-CZ', { maximumFractionDigits: 0 }).format(rounded)} Kč/žák/rok`;
}

export function formatPerUnit(czk: number, unitLabel: string): string {
  const rounded = Math.round(czk);
  return `${new Intl.NumberFormat('cs-CZ', { maximumFractionDigits: 0 }).format(rounded)} Kč/${unitLabel}`;
}

export function formatInteger(value: number): string {
  return new Intl.NumberFormat('cs-CZ', { maximumFractionDigits: 0 }).format(value);
}

export function formatPercent(value: number): string {
  return `${new Intl.NumberFormat('cs-CZ', {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  }).format(value)} %`;
}

export function titleCase(input: string): string {
  return input
    .replaceAll('_', ' ')
    .split(' ')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

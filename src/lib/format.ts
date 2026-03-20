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
    return `${(value / 1_000_000_000).toFixed(1)} bn CZK`;
  }

  if (absolute >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1)} m CZK`;
  }

  if (absolute >= 1_000) {
    return `${(value / 1_000).toFixed(1)} k CZK`;
  }

  return formatCzk(value);
}

export function formatPerPupil(czk: number): string {
  const rounded = Math.round(czk);
  return `${new Intl.NumberFormat('cs-CZ', { maximumFractionDigits: 0 }).format(rounded)} Kč/pupil`;
}

export function titleCase(input: string): string {
  return input
    .replaceAll('_', ' ')
    .split(' ')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

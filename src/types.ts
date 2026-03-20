export type NodeCategory =
  | 'state'
  | 'ministry'
  | 'region'
  | 'municipality'
  | 'eu_programme'
  | 'eu_project'
  | 'school_entity'
  | 'cost_bucket'
  | 'other';

export type FlowCertainty = 'observed' | 'inferred';
export type FlowBasis = 'allocated' | 'budgeted' | 'realized';

export interface SankeyNode {
  id: string;
  name: string;
  category: NodeCategory;
  level: number;
  ico?: string;
  founderType?: string;
  metadata?: Record<string, string | number | boolean | null>;
}

export interface SankeyLink {
  source: string;
  target: string;
  value: number;
  amountCzk: number;
  year: number;
  flowType: string;
  basis: FlowBasis;
  certainty: FlowCertainty;
  sourceDataset: string;
  sourceUrl?: string;
  institutionId?: string;
  note?: string;
}

export interface DataSourceNote {
  id: string;
  label: string;
  url?: string;
  coverage: string;
  confidence: 'high' | 'medium' | 'low';
}

export interface InstitutionSummary {
  id: string;
  name: string;
  ico?: string;
  founderName?: string;
  founderType?: string;
  municipality?: string;
  region?: string;
  capacity?: number;
}

export interface YearDataset {
  year: number;
  currency: 'CZK';
  title: string;
  subtitle?: string;
  nodes: SankeyNode[];
  links: SankeyLink[];
  institutions: InstitutionSummary[];
  sources: DataSourceNote[];
}

export interface ManifestEntry {
  year: number;
  title: string;
  file: string;
  status: 'demo' | 'pilot' | 'production';
}

export interface Manifest {
  dataset: string;
  years: ManifestEntry[];
}

export interface Filters {
  year: number;
  certainty: 'all' | FlowCertainty;
  thresholdCzk: number;
  institutionId: string | 'all';
  flowView: 'all' | 'funding' | 'spending';
}

export interface HoverInfo {
  label: string;
  amount: number | null;
  isLink: boolean;
}

export interface DrilldownEntry {
  nodeId: string;
  label: string;
  offset?: number;
}

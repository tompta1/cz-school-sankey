export type NodeCategory =
  | 'state'
  | 'ministry'
  | 'region'
  | 'municipality'
  | 'health_system'
  | 'health_provider'
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

export interface ApiGraph {
  year: number;
  nodes: SankeyNode[];
  links: SankeyLink[];
}

export interface ApiYearInfo {
  year: number;
  directSchoolFinanceRows: number;
  schoolExpenditureRows: number;
}

export interface ApiYearsResponse {
  domain: 'school';
  years: ApiYearInfo[];
}

export interface HealthYearsResponse {
  domain: 'health';
  years: number[];
}

export interface HealthSummaryResponse {
  years: number[];
  counts: {
    providers: number;
    facilities: number;
    hospitalLikeFacilities: number;
    insurers: number;
    providerClaimRows: number;
    payerClaimRows: number;
  };
  sources: Array<{
    datasetCode: string;
    snapshotLabel: string;
    rowCount: number;
    status: string;
  }>;
}

export interface HealthProviderDirectoryEntry {
  providerIco: string | null;
  zzId: number | null;
  zzKod: string | null;
  facilityName: string | null;
  facilityTypeName: string | null;
  providerName: string | null;
  providerType: string | null;
  providerLegalFormName: string | null;
  regionName: string | null;
  municipality: string | null;
  careField: string | null;
  careForm: string | null;
  careKind: string | null;
  founderType: string | null;
}

export interface HealthProviderActivityRow {
  year: number;
  providerIco: string;
  providerName: string | null;
  providerType: string | null;
  providerLegalFormName: string | null;
  regionName: string | null;
  hospitalLike: boolean;
  publicHealthLike: boolean;
  totalQuantity: number;
  patientCount: number;
  contactCount: number;
}

export interface HealthPayerActivityRow {
  year: number;
  month: number;
  payerCode: string;
  payerName: string | null;
  totalQuantity: number;
}

export interface AtlasYearInfo {
  year: number;
  school: boolean;
  healthActivity: boolean;
  healthFinancePilot: boolean;
}

export interface AtlasYearsResponse {
  domain: 'atlas';
  years: AtlasYearInfo[];
}

export interface AtlasSearchHit {
  id: string;
  name: string;
  domain: 'school' | 'health' | 'atlas';
  region?: string;
  municipality?: string;
  providerType?: string;
  available?: boolean;
  reason?: string;
  // For domain === 'atlas':
  scope?: string;
  nodeId?: string | null;
  context?: string;
}

export interface AtlasSearchResponse {
  year: number;
  q: string;
  results: AtlasSearchHit[];
}

export interface AtlasProviderFinanceRow {
  providerIco: string;
  providerName: string;
  providerType: string | null;
  regionName: string | null;
  founderType?: string | null;
  focus: 'hospital' | 'public_health' | 'zzs' | 'other';
  ownerBranch?: 'region' | 'municipality' | 'central_state' | null;
  revenues: number;
  costs: number;
  result: number;
  assets: number;
  liabilities: number;
  receivables: number;
  patientCount: number;
  contactCount: number;
  totalQuantity: number;
  costPerPatient: number | null;
  costPerContact: number | null;
  marginPct: number | null;
  pressureIndex: number | null;
  pressureLabel: string | null;
}

export interface AtlasOverviewResponse {
  year: number;
  metricMode?: 'revenue' | 'cost';
  years: AtlasYearInfo[];
  nodes: SankeyNode[];
  links: SankeyLink[];
  schoolSummary: {
    institutions: number;
    directSchoolFinance: number;
    founderSupport: number;
    euProjectSupport: number;
    schoolExpenditure: number;
  };
  healthFinance: {
    matchedProviders: number;
    visibleProviders: number;
    totalRevenues: number;
    totalCosts: number;
    totalResult: number;
    totalPatients: number;
    totalContacts: number;
    totalQuantity: number;
  };
  healthProviders: AtlasProviderFinanceRow[];
  notes: string[];
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

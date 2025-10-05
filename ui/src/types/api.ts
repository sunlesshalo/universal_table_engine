export interface SourceMetadata {
  filename: string;
  client_id: string | null;
  detected_format: string;
  sheet: string | null;
}

export interface SchemaMetadata {
  columns: string[];
  types: Record<string, string>;
  aliases: Record<string, string>;
  dataset_type: string;
}

export interface PIIMetadata {
  email: boolean;
  phone: boolean;
}

export interface ParseResponse {
  status: string;
  confidence: number;
  source: SourceMetadata;
  schema: SchemaMetadata;
  data: Array<Record<string, unknown>>;
  notes: string[];
  pii_detected: PIIMetadata;
  adapter_results?: Array<Record<string, unknown>> | null;
}

export interface WebhookReceipt {
  intake_id: string;
  client_id: string | null;
  preset_id: string | null;
  idempotency_key: string;
  status: string;
  processing: boolean;
  duplicate: boolean;
  sync: boolean;
  received_at: string;
  filename: string | null;
  notes: string[];
  parse?: ParseResponse | null;
  artifacts: Record<string, string>;
  results_url?: string | null;
}

export interface DeliverySummary {
  intake_id: string;
  client_id: string | null;
  preset_id: string | null;
  status: string;
  confidence?: number | null;
  received_at: string;
  filename?: string | null;
  rule_applied?: string | null;
  notes: string[];
}

export interface PresetDefinition {
  client_id: string;
  preset_id: string;
  defaults: Record<string, unknown>;
}

export interface AdminSettingsSnapshot {
  environment: string;
  webhook: {
    enable: boolean;
    require_auth: boolean;
    async_default: boolean;
    clock_skew_seconds: number;
  };
  adapters: {
    json: boolean;
    sheets: boolean;
    bigquery: boolean;
  };
  limits: {
    parse_max_mb: number;
    webhook_max_mb: number;
  };
  api_base_url: string;
}

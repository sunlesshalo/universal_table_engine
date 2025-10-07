import React from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { FileDropZone } from "@/components/FileDropZone";
import { ClientSelector } from "@/components/ClientSelector";
import { PresetSelector } from "@/components/PresetSelector";
import { ParseOptionsForm, defaultParseOptions } from "@/components/ParseOptionsForm";
import { SummaryCard } from "@/components/SummaryCard";
import { SchemaTable } from "@/components/SchemaTable";
import { NotesList } from "@/components/NotesList";
import { ArtifactsList } from "@/components/ArtifactsList";
import { useToast } from "@/components/hooks/useToast";
import { API_BASE_URL } from "@/lib/utils";
import { AdapterResult, DeliverySummary, ParseResponse, PresetDefinition } from "@/types/api";

interface ParseResultState {
  response: ParseResponse;
  rows: number;
  cols: number;
  durationMs?: number;
  ruleApplied?: string | null;
  adapterResults: AdapterResult[];
}

export const UploadPage: React.FC = () => {
  const { push } = useToast();
  const [file, setFile] = React.useState<File | null>(null);
  const [clientId, setClientId] = React.useState<string>("");
  const [presetId, setPresetId] = React.useState<string | null>(null);
  const [options, setOptions] = React.useState(defaultParseOptions);
  const [parseResult, setParseResult] = React.useState<ParseResultState | null>(null);

  const deliveriesQuery = useQuery({
    queryKey: ["deliveries", clientId],
    queryFn: async () => {
      const params = new URLSearchParams({ limit: "100" });
      if (clientId) params.set("client_id", clientId);
      const response = await fetch(`${API_BASE_URL}/admin/deliveries?${params.toString()}`);
      if (!response.ok) throw new Error("Failed to load deliveries");
      return (await response.json()) as DeliverySummary[];
    }
  });

  const presetsQuery = useQuery({
    queryKey: ["presets", clientId],
    enabled: Boolean(clientId),
    queryFn: async () => {
      const response = await fetch(`${API_BASE_URL}/admin/presets?client_id=${clientId}`);
      if (!response.ok) throw new Error("Failed to load presets");
      return (await response.json()) as PresetDefinition[];
    }
  });

  const parseMutation = useMutation({
    mutationFn: async (adapterOverride?: string) => {
      if (!file) throw new Error("No file selected");
      const formData = new FormData();
      formData.append("file", file);
      const params = new URLSearchParams();
      if (clientId) params.set("client_id", clientId);
      if (presetId) params.set("preset_id", presetId);
      if (options.source_hint) params.set("source_hint", options.source_hint);
      if (options.sheet_name) params.set("sheet_name", options.sheet_name);
      if (options.enable_llm) params.set("enable_llm", String(options.enable_llm));
      if (options.header_row) params.set("header_row", options.header_row);
      params.set("dayfirst", String(options.dayfirst));
      params.set("decimal_style", options.decimal_style);
      if (options.dry_run) params.set("dry_run", "true");
      params.set("adapter", adapterOverride ?? options.adapter);
      if (options.load_mode) params.set("load_mode", options.load_mode);
      if (options.use_ndjson_file) params.set("use_ndjson_file", String(options.use_ndjson_file));
      const response = await fetch(`${API_BASE_URL}/parse?${params.toString()}`, {
        method: "POST",
        body: formData
      });
      if (!response.ok) {
        const detail = await response.text();
        throw new Error(detail || "Parse failed");
      }
      return (await response.json()) as ParseResponse;
    },
    onSuccess: (data) => {
      setParseResult({
        response: data,
        rows: data.data.length,
        cols: data.schema.columns.length,
        ruleApplied: data.notes.find((note) => note.startsWith("rule_applied="))?.split("=", 1)[1] ?? null,
        adapterResults: data.adapter_results ?? []
      });
      push({ title: "Parse complete", description: `Status: ${data.status}`, variant: "success" });
    },
    onError: (error: Error) => {
      push({ title: "Parse failed", description: error.message, variant: "error" });
    }
  });

  const clientSuggestions = deliveriesQuery.data?.map((d) => d.client_id ?? "default") ?? [];
  const presetOptions = (presetsQuery.data ?? []).map((preset) => ({
    id: preset.preset_id,
    label: preset.preset_id
  }));

  const handleParse = (adapter?: string) => {
    if (!clientId) {
      push({ title: "Client required", description: "Please provide a client identifier", variant: "warning" });
      return;
    }
    parseMutation.mutate(adapter);
  };

  const copyCurl = () => {
    if (!file) return;
    const params = new URLSearchParams();
    if (clientId) params.set("client_id", clientId);
    if (presetId) params.set("preset_id", presetId);
    if (options.source_hint) params.set("source_hint", options.source_hint);
    if (options.sheet_name) params.set("sheet_name", options.sheet_name);
    if (options.enable_llm) params.set("enable_llm", String(options.enable_llm));
    if (options.header_row) params.set("header_row", options.header_row);
    params.set("dayfirst", String(options.dayfirst));
    params.set("decimal_style", options.decimal_style);
    if (options.dry_run) params.set("dry_run", "true");
    params.set("adapter", options.adapter);
    if (options.load_mode) params.set("load_mode", options.load_mode);
    if (options.use_ndjson_file) params.set("use_ndjson_file", String(options.use_ndjson_file));
    const curl = [
      "curl",
      "-X",
      "POST",
      `'${API_BASE_URL}/parse?${params.toString()}'`,
      "-F",
      `'file=@${file.name}'`
    ].join(" ");
    navigator.clipboard.writeText(curl);
    push({ title: "cURL copied", description: "Command copied to clipboard", variant: "success" });
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="text-base font-semibold">Manual upload</CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          <FileDropZone onFile={setFile}>
            {file ? (
              <div className="text-sm">
                <p className="font-medium">Selected file</p>
                <p className="text-muted-foreground">{file.name}</p>
              </div>
            ) : undefined}
          </FileDropZone>

          <div className="grid gap-4 md:grid-cols-2">
            <ClientSelector value={clientId} onChange={setClientId} suggestions={clientSuggestions} />
            <PresetSelector value={presetId} presets={presetOptions} onChange={setPresetId} />
          </div>

          <ParseOptionsForm value={options} onChange={setOptions} />

          <div className="flex flex-wrap gap-3">
            <Button onClick={() => handleParse()} disabled={!file || parseMutation.isLoading}>
              Parse
            </Button>
            <Button
              variant="outline"
              onClick={() => handleParse("sheets")}
              disabled={!file || parseMutation.isLoading}
            >
              Parse &amp; Sheets
            </Button>
            <Button
              variant="outline"
              onClick={() => handleParse("bigquery")}
              disabled={!file || parseMutation.isLoading}
            >
              Parse &amp; BigQuery
            </Button>
            <Button variant="outline" onClick={copyCurl} disabled={!file}>
              Copy cURL
            </Button>
          </div>
        </CardContent>
      </Card>

      {parseResult ? (
        <div className="grid gap-6 lg:grid-cols-2">
          <div className="space-y-4">
            <SummaryCard
              result={parseResult.response}
              rows={parseResult.rows}
              cols={parseResult.cols}
              durationMs={parseResult.durationMs}
              ruleApplied={parseResult.ruleApplied}
            />
            <NotesList notes={parseResult.response.notes} />
            <ArtifactsList adapterResults={parseResult.adapterResults} />
          </div>
          <SchemaTable schema={parseResult.response.schema} />
        </div>
      ) : null}
    </div>
  );
};

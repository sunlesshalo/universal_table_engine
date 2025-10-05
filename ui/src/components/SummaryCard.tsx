import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ParseResponse } from "@/types/api";
import { formatConfidence } from "@/lib/utils";

interface SummaryCardProps {
  result: ParseResponse;
  rows: number;
  cols: number;
  durationMs?: number;
  ruleApplied?: string | null;
}

const statusVariant: Record<string, "success" | "warning" | "danger" | "default"> = {
  ok: "success",
  parsed_with_low_confidence: "warning",
  needs_rulefile: "warning",
  failed: "danger"
};

export const SummaryCard: React.FC<SummaryCardProps> = ({ result, rows, cols, durationMs, ruleApplied }) => {
  const variant = statusVariant[result.status] ?? "default";
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="text-base font-semibold">Parse summary</CardTitle>
          <Badge variant={variant}>{result.status}</Badge>
        </div>
      </CardHeader>
      <CardContent className="grid gap-3 md:grid-cols-2">
        <SummaryField label="Confidence" value={`${formatConfidence(result.confidence)}`} />
        <SummaryField label="Rows" value={rows.toString()} />
        <SummaryField label="Columns" value={cols.toString()} />
        <SummaryField label="Detected format" value={result.source.detected_format} />
        {ruleApplied ? <SummaryField label="Rule applied" value={ruleApplied} /> : null}
        {durationMs != null ? <SummaryField label="Duration" value={`${durationMs.toFixed(1)} ms`} /> : null}
      </CardContent>
    </Card>
  );
};

const SummaryField: React.FC<{ label: string; value: string }> = ({ label, value }) => (
  <div className="flex flex-col">
    <span className="text-xs font-medium uppercase text-muted-foreground">{label}</span>
    <span className="text-sm">{value}</span>
  </div>
);

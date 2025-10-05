import React from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { API_BASE_URL } from "@/lib/utils";
import { AdminSettingsSnapshot } from "@/types/api";

export const SettingsPage: React.FC = () => {
  const { data } = useQuery({
    queryKey: ["admin-settings"],
    queryFn: async () => {
      const response = await fetch(`${API_BASE_URL}/admin/settings`);
      if (!response.ok) throw new Error("Failed to load settings");
      return (await response.json()) as AdminSettingsSnapshot;
    }
  });

  if (!data) {
    return <p className="text-sm text-muted-foreground">Loading settings…</p>;
  }

  return (
    <div className="grid gap-4 md:grid-cols-2">
      <Card>
        <CardHeader>
          <CardTitle className="text-base font-semibold">Environment</CardTitle>
        </CardHeader>
        <CardContent className="space-y-1 text-sm">
          <InfoRow label="Environment" value={data.environment} />
          <InfoRow label="API base" value={data.api_base_url} />
          <InfoRow label="Webhook enabled" value={String(data.webhook.enable)} />
          <InfoRow label="Webhook auth required" value={String(data.webhook.require_auth)} />
          <InfoRow label="Clock skew (s)" value={String(data.webhook.clock_skew_seconds)} />
          <InfoRow label="Async default" value={String(data.webhook.async_default)} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base font-semibold">Adapters & limits</CardTitle>
        </CardHeader>
        <CardContent className="space-y-1 text-sm">
          <InfoRow label="JSON adapter" value={String(data.adapters.json)} />
          <InfoRow label="Sheets adapter" value={String(data.adapters.sheets)} />
          <InfoRow label="BigQuery adapter" value={String(data.adapters.bigquery)} />
          <InfoRow label="Parse max (MB)" value={String(data.limits.parse_max_mb)} />
          <InfoRow label="Webhook max (MB)" value={String(data.limits.webhook_max_mb)} />
        </CardContent>
      </Card>
    </div>
  );
};

const InfoRow: React.FC<{ label: string; value: string }> = ({ label, value }) => (
  <div className="flex justify-between text-sm">
    <span className="text-muted-foreground">{label}</span>
    <span className="font-medium text-foreground">{value}</span>
  </div>
);

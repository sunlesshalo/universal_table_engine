import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { API_BASE_URL } from "@/lib/utils";

interface WebhookEndpointPanelProps {
  clientId?: string;
  presetId?: string;
}

export const WebhookEndpointPanel: React.FC<WebhookEndpointPanelProps> = ({ clientId, presetId }) => {
  const base = `${API_BASE_URL}/webhook/v1/intake`;
  const endpoints = [base];
  if (clientId) {
    endpoints.push(`${base}/${clientId}`);
    if (presetId) {
      endpoints.push(`${base}/${clientId}/${presetId}`);
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base font-semibold">Endpoint URLs</CardTitle>
      </CardHeader>
      <CardContent className="space-y-2 text-sm">
        {endpoints.map((url) => (
          <div key={url} className="flex items-center justify-between gap-3">
            <code className="break-all rounded bg-muted px-2 py-1 text-xs">{url}</code>
            <Button variant="ghost" size="sm" onClick={() => navigator.clipboard.writeText(url)}>
              Copy
            </Button>
          </div>
        ))}
      </CardContent>
    </Card>
  );
};

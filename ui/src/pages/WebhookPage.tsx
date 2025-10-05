import React from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { FileDropZone } from "@/components/FileDropZone";
import { ClientSelector } from "@/components/ClientSelector";
import { PresetSelector } from "@/components/PresetSelector";
import { Button } from "@/components/ui/button";
import { HeadersBuilder } from "@/components/HeadersBuilder";
import { WebhookEndpointPanel } from "@/components/WebhookEndpointPanel";
import { ReceiptCard } from "@/components/ReceiptCard";
import { useToast } from "@/components/hooks/useToast";
import { API_BASE_URL } from "@/lib/utils";
import { PresetDefinition, WebhookReceipt } from "@/types/api";

export const WebhookPage: React.FC = () => {
  const { push } = useToast();
  const [clientId, setClientId] = React.useState("demo");
  const [presetId, setPresetId] = React.useState<string | null>(null);
  const [file, setFile] = React.useState<File | null>(null);
  const [receipt, setReceipt] = React.useState<WebhookReceipt | null>(null);
  const [idempotencyKey, setIdempotencyKey] = React.useState<string>(crypto.randomUUID());

  const presetsQuery = useQuery({
    queryKey: ["presets", clientId],
    enabled: Boolean(clientId),
    queryFn: async () => {
      const response = await fetch(`${API_BASE_URL}/admin/presets?client_id=${clientId}`);
      if (!response.ok) throw new Error("Failed to load presets");
      return (await response.json()) as PresetDefinition[];
    }
  });

  const intakeMutation = useMutation({
    mutationFn: async () => {
      if (!file) throw new Error("Select a file first");
      const formData = new FormData();
      formData.append("file", file);
      const endpoint = presetId ? `/webhook/v1/intake/${clientId}/${presetId}` : clientId ? `/webhook/v1/intake/${clientId}` : "/webhook/v1/intake";
      const response = await fetch(`${API_BASE_URL}${endpoint}?sync=true`, {
        method: "POST",
        body: formData,
        headers: {
          "X-UTE-Idempotency-Key": idempotencyKey
        }
      });
      if (!response.ok) {
        const detail = await response.text();
        throw new Error(detail || "Webhook call failed");
      }
      return (await response.json()) as WebhookReceipt;
    },
    onSuccess: (data) => {
      setReceipt(data);
      setIdempotencyKey(crypto.randomUUID());
      push({ title: "Webhook processed", description: `Status: ${data.status}`, variant: "success" });
    },
    onError: (error: Error) => {
      push({ title: "Webhook error", description: error.message, variant: "error" });
    }
  });

  return (
    <div className="grid gap-6 lg:grid-cols-[2fr_1fr]">
      <div className="space-y-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-base font-semibold">Webhook test</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <FileDropZone onFile={setFile}>
              {file ? (
                <div className="text-sm">
                  <p className="font-medium">Selected file</p>
                  <p className="text-muted-foreground">{file.name}</p>
                </div>
              ) : undefined}
            </FileDropZone>

            <ClientSelector value={clientId} onChange={setClientId} />
            <PresetSelector
              value={presetId}
              presets={(presetsQuery.data ?? []).map((preset) => ({
                id: preset.preset_id,
                label: preset.preset_id
              }))}
              onChange={setPresetId}
              placeholder="Optional preset"
            />

            <div className="flex flex-col gap-2">
              <label className="text-sm font-medium">Idempotency key</label>
              <div className="flex gap-2">
                <input
                  className="flex-1 rounded-lg border border-input px-3 py-2 text-sm"
                  value={idempotencyKey}
                  onChange={(event) => setIdempotencyKey(event.target.value)}
                />
                <Button variant="outline" onClick={() => setIdempotencyKey(crypto.randomUUID())}>
                  Regenerate
                </Button>
              </div>
            </div>

            <div className="flex gap-3">
              <Button disabled={!file || intakeMutation.isLoading} onClick={() => intakeMutation.mutate()}>Send test</Button>
            </div>
          </CardContent>
        </Card>

        {receipt ? <ReceiptCard receipt={receipt} /> : null}
      </div>

      <div className="space-y-6">
        <WebhookEndpointPanel clientId={clientId} presetId={presetId || undefined} />
        <HeadersBuilder
          clientId={clientId}
          presetId={presetId || undefined}
          endpoint={presetId ? `/webhook/v1/intake/${clientId}/${presetId}` : clientId ? `/webhook/v1/intake/${clientId}` : "/webhook/v1/intake"}
          apiKeyEnabled={true}
          hmacEnabled={true}
        />
        <Card>
          <CardHeader>
            <CardTitle className="text-base font-semibold">Platform tips</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4 text-sm text-muted-foreground">
            <div>
              <p className="font-semibold text-foreground">Zapier</p>
              <ul className="list-disc pl-4">
                <li>Use Webhooks by Zapier → Custom Request</li>
                <li>Method POST, data type form-data with field <code>file</code></li>
                <li>Add required headers from the builder above</li>
              </ul>
            </div>
            <div>
              <p className="font-semibold text-foreground">Make.com</p>
              <ul className="list-disc pl-4">
                <li>Add HTTP module → Make a request</li>
                <li>Set method POST, body type multipart with file field</li>
                <li>Manually set each header</li>
              </ul>
            </div>
            <div>
              <p className="font-semibold text-foreground">n8n</p>
              <ul className="list-disc pl-4">
                <li>HTTP Request node → POST</li>
                <li>Binary data mode with property name <code>file</code></li>
                <li>Use headers from the builder</li>
              </ul>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

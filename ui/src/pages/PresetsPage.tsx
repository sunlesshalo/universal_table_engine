import React from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useToast } from "@/components/hooks/useToast";
import { API_BASE_URL } from "@/lib/utils";
import { PresetDefinition } from "@/types/api";

interface PresetFormState {
  client_id: string;
  preset_id: string;
  adapter: string;
  source_hint: string;
  dayfirst: boolean;
  decimal_style: "auto" | "comma" | "dot";
  enable_llm: boolean;
  header_row: string;
}

const emptyState: PresetFormState = {
  client_id: "",
  preset_id: "",
  adapter: "json",
  source_hint: "",
  dayfirst: true,
  decimal_style: "auto",
  enable_llm: false,
  header_row: "",
};

export const PresetsPage: React.FC = () => {
  const { push } = useToast();
  const queryClient = useQueryClient();
  const [form, setForm] = React.useState<PresetFormState>(emptyState);

  const presetsQuery = useQuery({
    queryKey: ["presets"],
    queryFn: async () => {
      const response = await fetch(`${API_BASE_URL}/admin/presets`);
      if (!response.ok) throw new Error("Failed to load presets");
      return (await response.json()) as PresetDefinition[];
    }
  });

  const saveMutation = useMutation({
    mutationFn: async (payload: PresetFormState) => {
      const defaults: Record<string, unknown> = {
        adapter: payload.adapter,
        source_hint: payload.source_hint,
        dayfirst: payload.dayfirst,
        decimal_style: payload.decimal_style,
        enable_llm: payload.enable_llm,
      };
      if (payload.header_row) {
        defaults.header_row = Number(payload.header_row);
      }
      const response = await fetch(`${API_BASE_URL}/admin/presets`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          client_id: payload.client_id,
          preset_id: payload.preset_id,
          defaults,
        })
      });
      if (!response.ok) {
        const detail = await response.text();
        throw new Error(detail || "Failed to save preset");
      }
      return response.json();
    },
    onSuccess: () => {
      push({ title: "Preset saved", description: "Preset file updated", variant: "success" });
      setForm(emptyState);
      queryClient.invalidateQueries({ queryKey: ["presets"] });
    },
    onError: (error: Error) => push({ title: "Save error", description: error.message, variant: "error" })
  });

  const deleteMutation = useMutation({
    mutationFn: async (preset: PresetDefinition) => {
      const response = await fetch(`${API_BASE_URL}/admin/presets/${preset.client_id}/${preset.preset_id}`, {
        method: "DELETE"
      });
      if (!response.ok) {
        const detail = await response.text();
        throw new Error(detail || "Failed to delete preset");
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["presets"] });
      push({ title: "Preset deleted", variant: "success" });
    },
    onError: (error: Error) => push({ title: "Delete error", description: error.message, variant: "error" })
  });

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    saveMutation.mutate(form);
  };

  return (
    <div className="grid gap-6 lg:grid-cols-[2fr_3fr]">
      <Card>
        <CardHeader>
          <CardTitle className="text-base font-semibold">Create or update preset</CardTitle>
        </CardHeader>
        <CardContent>
          <form className="space-y-4" onSubmit={handleSubmit}>
            <div className="space-y-2">
              <Label htmlFor="preset-client">Client ID</Label>
              <Input
                id="preset-client"
                required
                value={form.client_id}
                onChange={(event) => setForm((state) => ({ ...state, client_id: event.target.value }))}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="preset-id">Preset ID</Label>
              <Input
                id="preset-id"
                required
                value={form.preset_id}
                onChange={(event) => setForm((state) => ({ ...state, preset_id: event.target.value }))}
              />
            </div>
            <div className="space-y-2">
              <Label>Adapter</Label>
              <Select value={form.adapter} onValueChange={(adapter) => setForm((state) => ({ ...state, adapter }))}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="json">json</SelectItem>
                  <SelectItem value="sheets">sheets</SelectItem>
                  <SelectItem value="bigquery">bigquery</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="preset-source">Source hint</Label>
              <Input
                id="preset-source"
                value={form.source_hint}
                onChange={(event) => setForm((state) => ({ ...state, source_hint: event.target.value }))}
              />
            </div>
            <div className="flex items-center justify-between rounded-lg border border-border px-4 py-3">
              <div>
                <Label className="text-sm">Day first</Label>
                <p className="text-xs text-muted-foreground">Prefer DD/MM/YYYY</p>
              </div>
              <Switch
                checked={form.dayfirst}
                onCheckedChange={(checked) => setForm((state) => ({ ...state, dayfirst: checked }))}
              />
            </div>
            <div className="space-y-2">
              <Label>Decimal style</Label>
              <Select
                value={form.decimal_style}
                onValueChange={(decimal_style) => setForm((state) => ({ ...state, decimal_style: decimal_style as PresetFormState["decimal_style"] }))}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="auto">auto</SelectItem>
                  <SelectItem value="comma">comma</SelectItem>
                  <SelectItem value="dot">dot</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="preset-header-row">Header row</Label>
              <Input
                id="preset-header-row"
                type="number"
                min={0}
                placeholder="Auto"
                value={form.header_row}
                onChange={(event) => setForm((state) => ({ ...state, header_row: event.target.value }))}
              />
            </div>
            <div className="flex items-center justify-between rounded-lg border border-border px-4 py-3">
              <div>
                <Label className="text-sm">Enable LLM</Label>
                <p className="text-xs text-muted-foreground">Toggle LLM suggestions for this preset</p>
              </div>
              <Switch
                checked={form.enable_llm}
                onCheckedChange={(checked) => setForm((state) => ({ ...state, enable_llm: checked }))}
              />
            </div>
            <Button type="submit" disabled={saveMutation.isLoading}>
              Save preset
            </Button>
          </form>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base font-semibold">Existing presets</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3 text-sm">
          {(presetsQuery.data ?? []).map((preset) => (
            <div key={`${preset.client_id}:${preset.preset_id}`} className="flex items-center justify-between gap-3 rounded-lg border border-border bg-white px-4 py-3">
              <div>
                <p className="font-medium">
                  {preset.client_id} / {preset.preset_id}
                </p>
                <p className="text-xs text-muted-foreground">
                  Adapter: {String(preset.defaults.adapter ?? "json")} · Source hint: {String(preset.defaults.source_hint ?? "—")} · Header row: {
                    preset.defaults.header_row ?? "auto"
                  }
                </p>
              </div>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() =>
                    setForm({
                      client_id: preset.client_id,
                      preset_id: preset.preset_id,
                      adapter: String(preset.defaults.adapter ?? "json"),
                      source_hint: String(preset.defaults.source_hint ?? ""),
                      dayfirst: Boolean(preset.defaults.dayfirst ?? true),
                      decimal_style: (preset.defaults.decimal_style as PresetFormState["decimal_style"]) ?? "auto",
                      enable_llm: Boolean(preset.defaults.enable_llm ?? false),
                      header_row:
                        preset.defaults.header_row !== undefined && preset.defaults.header_row !== null
                          ? String(preset.defaults.header_row)
                          : "",
                    })
                  }
                >
                  Edit
                </Button>
                <Button variant="outline" size="sm" onClick={() => deleteMutation.mutate(preset)}>
                  Delete
                </Button>
              </div>
            </div>
          ))}
          {(presetsQuery.data ?? []).length === 0 ? (
            <p className="text-sm text-muted-foreground">No presets yet. Create one on the left.</p>
          ) : null}
        </CardContent>
      </Card>
    </div>
  );
};

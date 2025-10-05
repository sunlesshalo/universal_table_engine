import React from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { DeliveriesTable } from "@/components/DeliveriesTable";
import { ReceiptCard } from "@/components/ReceiptCard";
import { ReplayModal } from "@/components/ReplayModal";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { useToast } from "@/components/hooks/useToast";
import { API_BASE_URL } from "@/lib/utils";
import { DeliverySummary, WebhookReceipt } from "@/types/api";

export const DeliveriesPage: React.FC = () => {
  const { push } = useToast();
  const queryClient = useQueryClient();
  const [clientId, setClientId] = React.useState<string>("");
  const STATUS_ALL = "__all__";
  const [status, setStatus] = React.useState<string | null>(null);
  const [search, setSearch] = React.useState<string>("");
  const [selected, setSelected] = React.useState<WebhookReceipt | null>(null);
  const [replayId, setReplayId] = React.useState<string | null>(null);

  const deliveriesQuery = useQuery({
    queryKey: ["deliveries", clientId, status, search],
    queryFn: async () => {
      const params = new URLSearchParams({ limit: "100" });
      if (clientId) params.set("client_id", clientId);
      if (status) params.set("status_filter", status);
      if (search) params.set("search", search);
      const response = await fetch(`${API_BASE_URL}/admin/deliveries?${params.toString()}`);
      if (!response.ok) throw new Error("Failed to load deliveries");
      return (await response.json()) as DeliverySummary[];
    }
  });

  const replayMutation = useMutation({
    mutationFn: async ({ intakeId, overrides }: { intakeId: string; overrides: Record<string, unknown> }) => {
      const response = await fetch(`${API_BASE_URL}/admin/deliveries/${intakeId}/replay`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(overrides)
      });
      if (!response.ok) {
        const detail = await response.text();
        throw new Error(detail || "Replay failed");
      }
      return (await response.json()) as WebhookReceipt;
    },
    onSuccess: (data) => {
      push({ title: "Replay complete", description: `New intake ${data.intake_id}`, variant: "success" });
      queryClient.invalidateQueries({ queryKey: ["deliveries"] });
      setSelected(data);
    },
    onError: (error: Error) => {
      push({ title: "Replay error", description: error.message, variant: "error" });
    }
  });

  const handleView = async (intakeId: string) => {
    try {
      const params = new URLSearchParams();
      if (clientId) params.set("client_id", clientId);
      const response = await fetch(`${API_BASE_URL}/admin/deliveries/${intakeId}?${params.toString()}`);
      if (!response.ok) throw new Error("Failed to load receipt");
      const data = (await response.json()) as WebhookReceipt;
      setSelected(data);
    } catch (error) {
      push({ title: "Load error", description: (error as Error).message, variant: "error" });
    }
  };

  const handleReplay = (intakeId: string) => {
    setReplayId(intakeId);
  };

  return (
    <div className="grid gap-6 lg:grid-cols-[3fr_2fr]">
      <div className="space-y-4">
        <Card>
          <CardHeader>
            <CardTitle className="text-base font-semibold">Filters</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-4 md:grid-cols-3">
            <div className="md:col-span-1">
              <Input placeholder="Client" value={clientId} onChange={(event) => setClientId(event.target.value)} />
            </div>
            <div className="md:col-span-1">
              <Select
                value={status ?? STATUS_ALL}
                onValueChange={(value) => setStatus(value === STATUS_ALL ? null : value)}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Status" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={STATUS_ALL}>All</SelectItem>
                  <SelectItem value="ok">ok</SelectItem>
                  <SelectItem value="parsed_with_low_confidence">parsed_with_low_confidence</SelectItem>
                  <SelectItem value="queued">queued</SelectItem>
                  <SelectItem value="failed">failed</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="md:col-span-1">
              <Input placeholder="Search" value={search} onChange={(event) => setSearch(event.target.value)} />
            </div>
          </CardContent>
        </Card>

        <DeliveriesTable
          items={deliveriesQuery.data ?? []}
          onView={handleView}
          onReplay={handleReplay}
        />
      </div>

      <div className="space-y-4">
        {selected ? (
          <ReceiptCard receipt={selected} />
        ) : (
          <Card>
            <CardHeader>
              <CardTitle className="text-base font-semibold">Receipt</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground">Select a delivery to inspect its details.</p>
            </CardContent>
          </Card>
        )}
      </div>

      <ReplayModal
        open={Boolean(replayId)}
        onOpenChange={(open) => !open && setReplayId(null)}
        onConfirm={(overrides) => {
          if (!replayId) return;
          replayMutation.mutate({ intakeId: replayId, overrides });
          setReplayId(null);
        }}
      />
    </div>
  );
};

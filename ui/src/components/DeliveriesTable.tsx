import React from "react";
import { DeliverySummary } from "@/types/api";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { formatConfidence } from "@/lib/utils";

interface DeliveriesTableProps {
  items: DeliverySummary[];
  onView: (intakeId: string) => void;
  onReplay: (intakeId: string) => void;
}

const statusVariant: Record<string, "success" | "warning" | "default" | "danger"> = {
  ok: "success",
  parsed_with_low_confidence: "warning",
  queued: "default",
  failed: "danger"
};

export const DeliveriesTable: React.FC<DeliveriesTableProps> = ({ items, onView, onReplay }) => {
  if (items.length === 0) {
    return <p className="text-sm text-muted-foreground">No deliveries yet. Hit the webhook or upload a file.</p>;
  }

  return (
    <div className="overflow-x-auto rounded-lg border border-border bg-white">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Received</TableHead>
            <TableHead>Client</TableHead>
            <TableHead>Preset</TableHead>
            <TableHead>Status</TableHead>
            <TableHead>Confidence</TableHead>
            <TableHead>Rule</TableHead>
            <TableHead>Filename</TableHead>
            <TableHead className="w-48">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {items.map((item) => (
            <TableRow key={item.intake_id}>
              <TableCell>{new Date(item.received_at).toLocaleString()}</TableCell>
              <TableCell>{item.client_id ?? "default"}</TableCell>
              <TableCell>{item.preset_id ?? "—"}</TableCell>
              <TableCell>
                <Badge variant={statusVariant[item.status] ?? "default"}>{item.status}</Badge>
              </TableCell>
              <TableCell>{item.confidence != null ? formatConfidence(item.confidence) : "—"}</TableCell>
              <TableCell>{item.rule_applied ?? "—"}</TableCell>
              <TableCell>{item.filename ?? "—"}</TableCell>
              <TableCell className="flex gap-2">
                <Button size="sm" variant="outline" onClick={() => onView(item.intake_id)}>
                  View
                </Button>
                <Button size="sm" onClick={() => onReplay(item.intake_id)}>
                  Replay
                </Button>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => window.open(`/admin/deliveries/${item.intake_id}/artifacts.zip`, "_blank")}
                >
                  Download
                </Button>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
};

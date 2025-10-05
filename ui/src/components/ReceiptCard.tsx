import React from "react";
import { WebhookReceipt } from "@/types/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { formatConfidence } from "@/lib/utils";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

interface ReceiptCardProps {
  receipt: WebhookReceipt;
}

export const ReceiptCard: React.FC<ReceiptCardProps> = ({ receipt }) => {
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <div>
          <CardTitle className="text-base font-semibold">Webhook Receipt</CardTitle>
          <p className="text-xs text-muted-foreground">Intake ID: {receipt.intake_id}</p>
        </div>
        <Badge>{receipt.status}</Badge>
      </CardHeader>
      <CardContent className="space-y-4 text-sm">
        <div className="grid gap-2 md:grid-cols-2">
          <InfoRow label="Client" value={receipt.client_id ?? "default"} />
          <InfoRow label="Preset" value={receipt.preset_id ?? "—"} />
          <InfoRow label="Filename" value={receipt.filename ?? "—"} />
          <InfoRow label="Received" value={new Date(receipt.received_at).toLocaleString()} />
          <InfoRow label="Idempotency" value={receipt.idempotency_key} />
          <InfoRow label="Processing" value={receipt.processing ? "Yes" : "No"} />
          <InfoRow label="Duplicate" value={receipt.duplicate ? "Yes" : "No"} />
          <InfoRow label="Results" value={receipt.results_url ?? "—"} />
        </div>
        {receipt.notes.length > 0 ? (
          <div>
            <p className="text-xs font-semibold uppercase text-muted-foreground">Notes</p>
            <ul className="list-disc space-y-1 pl-4">
              {receipt.notes.map((note) => (
                <li key={note}>{note}</li>
              ))}
            </ul>
          </div>
        ) : null}
        {receipt.parse ? (
          <div className="space-y-2">
            <p className="text-xs font-semibold uppercase text-muted-foreground">Parse summary</p>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Status</TableHead>
                  <TableHead>Confidence</TableHead>
                  <TableHead>Rows</TableHead>
                  <TableHead>Columns</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                <TableRow>
                  <TableCell>{receipt.parse.status}</TableCell>
                  <TableCell>{formatConfidence(receipt.parse.confidence)}</TableCell>
                  <TableCell>{receipt.parse.data.length}</TableCell>
                  <TableCell>{receipt.parse.schema.columns.length}</TableCell>
                </TableRow>
              </TableBody>
            </Table>
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
};

const InfoRow: React.FC<{ label: string; value: string }> = ({ label, value }) => (
  <div className="flex flex-col">
    <span className="text-xs font-semibold uppercase text-muted-foreground">{label}</span>
    <span>{value}</span>
  </div>
);

import React from "react";
import * as Dialog from "@radix-ui/react-dialog";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";

interface ReplayModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onConfirm: (overrides: Record<string, unknown>) => void;
}

export const ReplayModal: React.FC<ReplayModalProps> = ({ open, onOpenChange, onConfirm }) => {
  const [adapter, setAdapter] = React.useState<string>("");
  const [sourceHint, setSourceHint] = React.useState<string>("");
  const [enableLLM, setEnableLLM] = React.useState<boolean>(false);
  const [dryRun, setDryRun] = React.useState<boolean>(false);

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 bg-black/40" />
        <Dialog.Content className="fixed left-1/2 top-1/2 w-full max-w-md -translate-x-1/2 -translate-y-1/2 rounded-xl bg-white p-6 shadow-xl">
          <Dialog.Title className="text-lg font-semibold">Replay intake</Dialog.Title>
          <Dialog.Description className="mb-4 text-sm text-muted-foreground">
            Override any options and press replay to run the parser again.
          </Dialog.Description>
          <div className="space-y-3">
            <div className="flex flex-col gap-2">
              <Label htmlFor="replay-adapter">Adapter override</Label>
              <Input
                id="replay-adapter"
                placeholder="json | sheets | bigquery"
                value={adapter}
                onChange={(event) => setAdapter(event.target.value)}
              />
            </div>
            <div className="flex flex-col gap-2">
              <Label htmlFor="replay-source-hint">Source hint</Label>
              <Input
                id="replay-source-hint"
                placeholder="Optional"
                value={sourceHint}
                onChange={(event) => setSourceHint(event.target.value)}
              />
            </div>
            <div className="flex items-center justify-between rounded-lg border border-border px-4 py-3">
              <div>
                <Label className="text-sm">Enable LLM</Label>
                <p className="text-xs text-muted-foreground">Overrides server default only for this replay</p>
              </div>
              <Switch checked={enableLLM} onCheckedChange={setEnableLLM} />
            </div>
            <div className="flex items-center justify-between rounded-lg border border-border px-4 py-3">
              <div>
                <Label className="text-sm">Dry run</Label>
                <p className="text-xs text-muted-foreground">Skip downstream adapters</p>
              </div>
              <Switch checked={dryRun} onCheckedChange={setDryRun} />
            </div>
          </div>
          <div className="mt-6 flex justify-end gap-2">
            <Button variant="outline" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button
              onClick={() => {
                onConfirm({ adapter, source_hint: sourceHint, enable_llm: enableLLM, dry_run: dryRun });
                onOpenChange(false);
              }}
            >
              Replay
            </Button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
};

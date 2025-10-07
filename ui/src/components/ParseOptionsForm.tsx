import React from "react";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";

export interface ParseOptions {
  adapter: string;
  source_hint: string;
  sheet_name: string;
  enable_llm: boolean;
  dayfirst: boolean;
  decimal_style: "auto" | "comma" | "dot";
  dry_run: boolean;
  header_row: string;
  use_ndjson_file: boolean;
  load_mode: "stream" | "file";
}

interface ParseOptionsFormProps {
  value: ParseOptions;
  onChange: (next: ParseOptions) => void;
}

export const defaultParseOptions: ParseOptions = {
  adapter: "json",
  source_hint: "",
  sheet_name: "",
  enable_llm: false,
  dayfirst: true,
  decimal_style: "auto",
  dry_run: false,
  header_row: "",
  use_ndjson_file: false,
  load_mode: "stream",
};

export const ParseOptionsForm: React.FC<ParseOptionsFormProps> = ({ value, onChange }) => {
  const update = (partial: Partial<ParseOptions>) => onChange({ ...value, ...partial });

  return (
    <div className="grid gap-4 md:grid-cols-2">
      <div className="flex flex-col gap-2">
        <Label>Adapter</Label>
        <Select
          value={value.adapter}
          onValueChange={(adapter) =>
            update({
              adapter,
              load_mode: adapter === "bigquery" ? value.load_mode : "stream",
              use_ndjson_file: adapter === "bigquery" ? value.use_ndjson_file : false,
            })
          }
        >
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="json">JSON (local)</SelectItem>
            <SelectItem value="sheets">Google Sheets</SelectItem>
            <SelectItem value="bigquery">BigQuery</SelectItem>
            <SelectItem value="none">None</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div className="flex flex-col gap-2">
        <Label htmlFor="source_hint">Source hint</Label>
        <Input
          id="source_hint"
          placeholder="e.g. metoric"
          value={value.source_hint}
          onChange={(event) => update({ source_hint: event.target.value })}
        />
      </div>

      <div className="flex flex-col gap-2">
        <Label htmlFor="sheet_name">Sheet name</Label>
        <Input
          id="sheet_name"
          placeholder="Optional sheet name"
          value={value.sheet_name}
          onChange={(event) => update({ sheet_name: event.target.value })}
        />
      </div>

      <div className="flex flex-col gap-2">
        <Label htmlFor="header_row">Header row</Label>
        <Input
          id="header_row"
          type="number"
          min={0}
          placeholder="Auto detect"
          value={value.header_row}
          onChange={(event) => update({ header_row: event.target.value })}
        />
      </div>

      <div className="flex items-center justify-between gap-4 rounded-lg border border-border bg-white px-4 py-3">
        <div>
          <Label className="text-sm">Enable LLM</Label>
          <p className="text-xs text-muted-foreground">Use LLM assisted header/alias detection (if enabled server-side)</p>
        </div>
        <Switch checked={value.enable_llm} onCheckedChange={(state) => update({ enable_llm: state })} />
      </div>

      <div className="flex items-center justify-between gap-4 rounded-lg border border-border bg-white px-4 py-3">
        <div>
          <Label className="text-sm">Day-first dates</Label>
          <p className="text-xs text-muted-foreground">Interpret ambiguous dates as DD/MM/YYYY</p>
        </div>
        <Switch checked={value.dayfirst} onCheckedChange={(state) => update({ dayfirst: state })} />
      </div>

      <div className="flex flex-col gap-2">
        <Label>Decimal style</Label>
        <Select value={value.decimal_style} onValueChange={(decimal_style) => update({ decimal_style: decimal_style as ParseOptions["decimal_style"] })}>
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="auto">Auto detect</SelectItem>
            <SelectItem value="comma">Comma (1.234,56)</SelectItem>
            <SelectItem value="dot">Dot (1,234.56)</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div className="flex items-center justify-between gap-4 rounded-lg border border-border bg-white px-4 py-3">
        <div>
          <Label className="text-sm">Dry run</Label>
          <p className="text-xs text-muted-foreground">Skip downstream adapters (JSON/SHEETS/BQ)</p>
        </div>
        <Switch checked={value.dry_run} onCheckedChange={(state) => update({ dry_run: state })} />
      </div>

      {value.adapter === "bigquery" ? (
        <>
          <div className="flex flex-col gap-2">
            <Label>BigQuery load mode</Label>
            <Select
              value={value.load_mode}
              onValueChange={(mode) =>
                update({
                  load_mode: mode as ParseOptions["load_mode"],
                  use_ndjson_file: mode === "file" ? true : value.use_ndjson_file,
                })
              }
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="stream">Stream (DataFrame)</SelectItem>
                <SelectItem value="file">File load (NDJSON)</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="flex items-center justify-between gap-4 rounded-lg border border-border bg-white px-4 py-3">
            <div>
              <Label className="text-sm">Use NDJSON sidecar</Label>
              <p className="text-xs text-muted-foreground">Persist NDJSON and load via file when available</p>
            </div>
            <Switch
              checked={value.use_ndjson_file}
              onCheckedChange={(state) => update({ use_ndjson_file: state })}
            />
          </div>
        </>
      ) : null}
    </div>
  );
};

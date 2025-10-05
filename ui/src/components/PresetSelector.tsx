import React from "react";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

const NONE_VALUE = "__none__";

interface PresetSelectorProps {
  value: string | null;
  presets: Array<{ id: string; label: string }>;
  onChange: (value: string | null) => void;
  placeholder?: string;
}

export const PresetSelector: React.FC<PresetSelectorProps> = ({ value, presets, onChange, placeholder = "Select preset" }) => {
  const selectedValue = value ?? NONE_VALUE;

  return (
    <div className="flex flex-col gap-2">
      <Label>Preset</Label>
      <Select value={selectedValue} onValueChange={(next) => onChange(next === NONE_VALUE ? null : next)}>
        <SelectTrigger>
          <SelectValue placeholder={placeholder} />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={NONE_VALUE}>None</SelectItem>
          {presets.map((preset) => (
            <SelectItem key={preset.id} value={preset.id}>
              {preset.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
};

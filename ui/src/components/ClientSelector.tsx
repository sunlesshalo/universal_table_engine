import React from "react";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

interface ClientSelectorProps {
  value: string;
  onChange: (value: string) => void;
  suggestions?: string[];
}

export const ClientSelector: React.FC<ClientSelectorProps> = ({ value, onChange, suggestions = [] }) => {
  const uniqueSuggestions = Array.from(new Set(suggestions.filter(Boolean))).slice(0, 20);

  return (
    <div className="flex flex-col gap-2">
      <Label htmlFor="client">Client</Label>
      <Input
        list="client-suggestions"
        id="client"
        placeholder="e.g. acme"
        value={value}
        onChange={(event) => onChange(event.target.value)}
        required
      />
      <datalist id="client-suggestions">
        {uniqueSuggestions.map((client) => (
          <option key={client} value={client} />
        ))}
      </datalist>
    </div>
  );
};

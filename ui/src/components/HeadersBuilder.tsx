import React from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useToast } from "@/components/hooks/useToast";
import { API_BASE_URL } from "@/lib/utils";

const PROFILES = {
  multipart: {
    label: "Multipart",
    description: "Send file via form-data",
    body: "-F \"file=@sample.csv\""
  },
  jsonUrl: {
    label: "JSON URL",
    description: "Send JSON with file_url",
    body: `-H 'Content-Type: application/json' -d '{"file_url": "https://example.com/export.csv"}'`
  },
  base64: {
    label: "Base64",
    description: "Send JSON with file_b64",
    body: `-H 'Content-Type: application/json' -d '{"file_b64": "..."}'`
  }
} as const;

interface HeadersBuilderProps {
  clientId: string;
  presetId?: string;
  endpoint: string;
  apiKeyEnabled: boolean;
  hmacEnabled: boolean;
}

export const HeadersBuilder: React.FC<HeadersBuilderProps> = ({ clientId, presetId, endpoint, apiKeyEnabled, hmacEnabled }) => {
  const [profile, setProfile] = React.useState<keyof typeof PROFILES>("multipart");
  const { push } = useToast();

  const headers: Array<[string, string]> = [];
  if (hmacEnabled) {
    headers.push(["X-UTE-Signature", "sha256=<computed>"]);
    headers.push(["X-UTE-Timestamp", "<epoch-seconds>"]);
  }
  if (apiKeyEnabled) {
    headers.push(["Authorization", "Bearer <api-key>"]);
  }
  headers.push(["X-UTE-Idempotency-Key", "<unique-id>"]);

  const profileData = PROFILES[profile];
  const targetUrl = `${API_BASE_URL}${endpoint}`;

  const curl = [
    "curl",
    "-X",
    "POST",
    `'${targetUrl}'`
  ]
    .concat(headers.map(([name, value]) => `-H '${name}: ${value}'`))
    .concat(profileData.body)
    .join(" ");

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="text-base font-semibold">Headers & cURL</CardTitle>
          <div className="w-48">
            <Select value={profile} onValueChange={(value: keyof typeof PROFILES) => setProfile(value)}>
              <SelectTrigger>
                <SelectValue placeholder="Profile" />
              </SelectTrigger>
              <SelectContent>
                {Object.entries(PROFILES).map(([key, item]) => (
                  <SelectItem key={key} value={key}>
                    {item.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
        <p className="text-xs text-muted-foreground">{profileData.description}</p>
      </CardHeader>
      <CardContent className="space-y-3 text-sm">
        <div className="space-y-2">
          {headers.map(([name, value]) => (
            <div key={name} className="flex items-center justify-between gap-4">
              <span className="font-medium">{name}</span>
              <code className="rounded bg-muted px-2 py-1">{value}</code>
            </div>
          ))}
        </div>
        <div className="rounded-lg bg-slate-900 p-4 text-xs text-slate-100">
          <code className="break-words whitespace-pre-line">{curl}</code>
        </div>
        <Button
          variant="outline"
          onClick={() => {
            navigator.clipboard.writeText(curl);
            push({ title: "cURL copied", description: "Command copied to clipboard", variant: "success" });
          }}
        >
          Copy cURL
        </Button>
      </CardContent>
    </Card>
  );
};

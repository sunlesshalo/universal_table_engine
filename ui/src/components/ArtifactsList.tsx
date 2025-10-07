import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { AdapterResult, ArtifactDescriptor } from "@/types/api";

interface ArtifactsListProps {
  adapterResults: AdapterResult[];
}

interface ArtifactEntry {
  label: string;
  path: string;
}

const buildBqLoadCommand = (table: string, path: string, gzip: boolean, location?: string): string => {
  const parts = ["bq", "load", "--source_format=NEWLINE_DELIMITED_JSON", "--autodetect"];
  if (gzip) parts.push("--compression=GZIP");
  if (location) parts.push(`--location=${location}`);
  parts.push(table, `'${path}'`);
  return parts.join(" ");
};

const firstArtifact = (result: AdapterResult | undefined): ArtifactDescriptor | undefined => {
  if (!result || !result.artifacts || result.artifacts.length === 0) return undefined;
  return result.artifacts[0];
};

export const ArtifactsList: React.FC<ArtifactsListProps> = ({ adapterResults }) => {
  if (!adapterResults || adapterResults.length === 0) return null;

  const jsonResult = adapterResults.find((item) => item.adapter === "json");
  const ndjsonResult = adapterResults.find((item) => item.adapter === "ndjson");
  const bigQueryResult = adapterResults.find((item) => item.adapter === "bigquery");

  const jsonArtifact = firstArtifact(jsonResult);
  const ndjsonArtifact = firstArtifact(ndjsonResult);

  const artifactEntries: ArtifactEntry[] = [];
  if (jsonArtifact?.path) {
    artifactEntries.push({ label: "JSON envelope", path: jsonArtifact.path });
  }
  if (ndjsonArtifact?.path) {
    const meta = (ndjsonArtifact.meta ?? {}) as Record<string, unknown>;
    const gzipHint = meta["gzip"] ? " (gzip)" : "";
    artifactEntries.push({ label: `NDJSON${gzipHint}`, path: ndjsonArtifact.path });
  }

  adapterResults
    .filter((item) => !["json", "ndjson", "bigquery"].includes(item.adapter))
    .forEach((result) => {
      (result.artifacts ?? []).forEach((artifact) => {
        if (artifact.path) {
          artifactEntries.push({
            label: `${artifact.name.replace(/_/g, " ")} (${result.adapter})`,
            path: artifact.path,
          });
        }
      });
    });

  const ndjsonMeta = (ndjsonArtifact?.meta ?? {}) as Record<string, unknown>;
  const ndjsonPath = typeof ndjsonArtifact?.path === "string" ? ndjsonArtifact.path : undefined;
  const ndjsonGzip = Boolean(ndjsonMeta["gzip"]);

  const bqLocation =
    typeof bigQueryResult?.details?.location === "string" && bigQueryResult?.details?.location
      ? (bigQueryResult.details.location as string)
      : undefined;
  const bqTable = typeof bigQueryResult?.table === "string" ? bigQueryResult.table : undefined;
  const bqJob = typeof bigQueryResult?.job_id === "string" ? bigQueryResult.job_id : undefined;
  const bqMode = bigQueryResult?.mode ?? "stream";
  const loadCommand =
    bqTable && ndjsonPath && bigQueryResult?.status === "ok"
      ? buildBqLoadCommand(bqTable, ndjsonPath, ndjsonGzip, bqLocation)
      : null;

  if (artifactEntries.length === 0 && !bigQueryResult) return null;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base font-semibold">Artifacts</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4 text-sm">
        {artifactEntries.length ? (
          <div className="space-y-2">
            {artifactEntries.map((entry) => (
              <div key={`${entry.label}-${entry.path}`} className="flex items-center justify-between gap-4">
                <span className="font-medium">{entry.label}</span>
                <Button variant="ghost" size="sm" onClick={() => navigator.clipboard.writeText(entry.path)}>
                  Copy path
                </Button>
              </div>
            ))}
          </div>
        ) : null}

        {bigQueryResult ? (
          <div className="rounded-lg border border-border p-3">
            <div className="flex items-center justify-between gap-4">
              <span className="font-medium">BigQuery</span>
              <span className="text-xs uppercase text-muted-foreground">{bigQueryResult.status}</span>
            </div>
            <dl className="mt-2 space-y-1 text-xs">
              {bqTable ? (
                <div className="flex items-center justify-between gap-4">
                  <dt className="text-muted-foreground">Table</dt>
                  <dd className="font-mono">{bqTable}</dd>
                </div>
              ) : null}
              {bqJob ? (
                <div className="flex items-center justify-between gap-4">
                  <dt className="text-muted-foreground">Job ID</dt>
                  <dd className="font-mono">{bqJob}</dd>
                </div>
              ) : null}
              <div className="flex items-center justify-between gap-4">
                <dt className="text-muted-foreground">Mode</dt>
                <dd className="capitalize">{bqMode}</dd>
              </div>
              {bqLocation ? (
                <div className="flex items-center justify-between gap-4">
                  <dt className="text-muted-foreground">Location</dt>
                  <dd>{bqLocation}</dd>
                </div>
              ) : null}
            </dl>
            {loadCommand ? (
              <Button
                variant="outline"
                size="sm"
                className="mt-3"
                onClick={() => navigator.clipboard.writeText(loadCommand)}
              >
                Copy bq load command
              </Button>
            ) : null}
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
};

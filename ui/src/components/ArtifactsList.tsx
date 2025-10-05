import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";

interface ArtifactsListProps {
  artifacts: Record<string, string>;
}

export const ArtifactsList: React.FC<ArtifactsListProps> = ({ artifacts }) => {
  const entries = Object.entries(artifacts ?? {});
  if (entries.length === 0) return null;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base font-semibold">Artifacts</CardTitle>
      </CardHeader>
      <CardContent className="space-y-2 text-sm">
        {entries.map(([name, value]) => (
          <div key={name} className="flex items-center justify-between gap-4">
            <span className="font-medium capitalize">{name.replace(/_/g, " ")}</span>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => navigator.clipboard.writeText(value)}
            >
              Copy path
            </Button>
          </div>
        ))}
      </CardContent>
    </Card>
  );
};

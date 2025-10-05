import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { SchemaMetadata } from "@/types/api";
import { Badge } from "@/components/ui/badge";

const typeVariant: Record<string, "success" | "warning" | "default" | "danger"> = {
  date: "success",
  number: "warning",
  boolean: "default",
  string: "default"
};

interface SchemaTableProps {
  schema: SchemaMetadata;
}

export const SchemaTable: React.FC<SchemaTableProps> = ({ schema }) => {
  const rows = schema.columns.map((column) => ({
    column,
    type: schema.types[column] ?? "string",
    alias: schema.aliases[column] ?? "—"
  }));

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base font-semibold">Schema</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Column</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Alias</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((row) => (
                <TableRow key={row.column}>
                  <TableCell className="font-medium">{row.column}</TableCell>
                  <TableCell>
                    <Badge variant={typeVariant[row.type] ?? "default"}>{row.type}</Badge>
                  </TableCell>
                  <TableCell>{row.alias}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
};

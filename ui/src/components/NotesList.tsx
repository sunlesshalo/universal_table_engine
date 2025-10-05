import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

interface NotesListProps {
  notes: string[];
}

export const NotesList: React.FC<NotesListProps> = ({ notes }) => {
  if (notes.length === 0) return null;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base font-semibold">Notes</CardTitle>
      </CardHeader>
      <CardContent>
        <ul className="list-disc space-y-1 pl-5 text-sm text-muted-foreground">
          {notes.slice(0, 10).map((note) => (
            <li key={note}>{note}</li>
          ))}
        </ul>
      </CardContent>
    </Card>
  );
};

import * as React from "react";
import { Label as RadixLabel } from "@radix-ui/react-label";
import { cn } from "@/lib/utils";

export const Label = React.forwardRef<React.ElementRef<typeof RadixLabel>, React.ComponentPropsWithoutRef<typeof RadixLabel>>(
  ({ className, ...props }, ref) => (
    <RadixLabel ref={ref} className={cn("text-sm font-medium text-foreground", className)} {...props} />
  )
);
Label.displayName = RadixLabel.displayName;

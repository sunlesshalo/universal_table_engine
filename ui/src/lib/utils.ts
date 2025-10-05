import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || window.location.origin;

export function formatConfidence(value?: number | null) {
  if (value == null) {
    return "–";
  }
  return `${Math.round(value * 100) / 100}`;
}

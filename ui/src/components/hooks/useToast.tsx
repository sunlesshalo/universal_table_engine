import React from "react";
import { cn } from "@/lib/utils";
import { X } from "lucide-react";

export type ToastVariant = "default" | "success" | "warning" | "error";

export interface ToastPayload {
  title: string;
  description?: string;
  variant?: ToastVariant;
  id?: string;
  duration?: number;
}

interface ToastContextValue {
  push: (toast: ToastPayload) => void;
}

const ToastContext = React.createContext<ToastContextValue | undefined>(undefined);

const variantClass: Record<ToastVariant, string> = {
  default: "bg-foreground text-white",
  success: "bg-emerald-600 text-white",
  warning: "bg-amber-500 text-white",
  error: "bg-red-600 text-white"
};

export const ToastProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [toasts, setToasts] = React.useState<ToastPayload[]>([]);

  const push = React.useCallback((toast: ToastPayload) => {
    const id = toast.id ?? crypto.randomUUID();
    const payload = { ...toast, id };
    setToasts((current) => [...current, payload]);
    const duration = toast.duration ?? 4000;
    if (duration > 0) {
      window.setTimeout(() => {
        setToasts((current) => current.filter((item) => item.id !== id));
      }, duration);
    }
  }, []);

  const remove = React.useCallback((id: string) => {
    setToasts((current) => current.filter((item) => item.id !== id));
  }, []);

  return (
    <ToastContext.Provider value={{ push }}>
      {children}
      <div className="fixed bottom-4 right-4 z-50 flex w-80 flex-col gap-2">
        {toasts.map((toast) => (
          <div
            key={toast.id}
            className={cn(
              "flex items-start gap-3 rounded-xl px-4 py-3 shadow-lg",
              variantClass[toast.variant ?? "default"]
            )}
          >
            <div className="flex-1">
              <p className="text-sm font-semibold">{toast.title}</p>
              {toast.description ? <p className="text-sm opacity-90">{toast.description}</p> : null}
            </div>
            <button
              type="button"
              onClick={() => toast.id && remove(toast.id)}
              className="rounded-full bg-white/20 p-1 text-sm text-white hover:bg-white/30"
            >
              <X className="h-4 w-4" />
            </button>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
};

export function useToast() {
  const ctx = React.useContext(ToastContext);
  if (!ctx) {
    throw new Error("useToast must be used within ToastProvider");
  }
  return ctx;
}

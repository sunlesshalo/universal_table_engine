import React from "react";
import { NavLink, Route, Routes, useLocation } from "react-router-dom";
import { UploadPage } from "@/pages/UploadPage";
import { WebhookPage } from "@/pages/WebhookPage";
import { DeliveriesPage } from "@/pages/DeliveriesPage";
import { PresetsPage } from "@/pages/PresetsPage";
import { SettingsPage } from "@/pages/SettingsPage";
import { ToastProvider } from "@/components/hooks/useToast";
import { Badge } from "@/components/ui/badge";
import { useQuery } from "@tanstack/react-query";
import { API_BASE_URL, cn } from "@/lib/utils";
import { Loader2 } from "lucide-react";

const NAV_ITEMS = [
  { to: "/upload", label: "Upload" },
  { to: "/webhook", label: "Webhook" },
  { to: "/deliveries", label: "Deliveries" },
  { to: "/presets", label: "Presets" },
  { to: "/settings", label: "Settings" }
];

export const App: React.FC = () => {
  const { data, isLoading } = useQuery({
    queryKey: ["health"],
    queryFn: async () => {
      const response = await fetch(`${API_BASE_URL}/health`);
      if (!response.ok) {
        throw new Error("Failed to load health status");
      }
      return response.json() as Promise<{ environment?: string; status: string }>;
    }
  });
  const location = useLocation();

  const environment = (() => {
    if (data?.environment) return data.environment;
    if (import.meta.env.MODE === "production") return "prod";
    if (import.meta.env.MODE === "staging") return "staging";
    return "dev";
  })();

  return (
    <ToastProvider>
      <div className="min-h-screen bg-slate-50">
        <header className="border-b border-border bg-white">
          <div className="mx-auto flex max-w-6xl items-center justify-between px-6 py-4">
            <div className="flex items-center gap-4">
              <span className="text-lg font-semibold">Universal Table Engine</span>
              <Badge variant="outline">Admin</Badge>
              <Badge variant={environment === "prod" ? "danger" : environment === "staging" ? "warning" : "success"}>
                {environment}
              </Badge>
            </div>
            <nav className="flex items-center gap-2 text-sm font-medium">
              {NAV_ITEMS.map((item) => (
                <NavLink
                  key={item.to}
                  to={item.to}
                  className={({ isActive }) =>
                    cn(
                      "rounded-lg px-3 py-2 transition-colors",
                      isActive ? "bg-primary text-white" : "text-muted-foreground hover:bg-muted"
                    )
                  }
                >
                  {item.label}
                </NavLink>
              ))}
            </nav>
          </div>
        </header>
        <main className="mx-auto flex max-w-6xl flex-col gap-6 px-6 py-8">
          {isLoading ? (
            <div className="flex items-center gap-2 text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              <span>Loading environment…</span>
            </div>
          ) : null}
          <Routes>
            <Route path="/" element={<UploadPage />} />
            <Route path="/upload" element={<UploadPage />} />
            <Route path="/webhook" element={<WebhookPage />} />
            <Route path="/deliveries" element={<DeliveriesPage />} />
            <Route path="/presets" element={<PresetsPage />} />
            <Route path="/settings" element={<SettingsPage />} />
            <Route path="*" element={<NotFound />} />
          </Routes>
        </main>
      </div>
    </ToastProvider>
  );
};

const NotFound: React.FC = () => (
  <div className="rounded-lg border border-border bg-white p-6 text-center">
    <h2 className="text-lg font-semibold">Page not found</h2>
    <p className="mt-2 text-sm text-muted-foreground">The path you requested does not exist: {window.location.pathname}</p>
  </div>
);

export default App;

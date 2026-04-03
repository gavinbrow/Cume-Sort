import { useEffect, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import {
  ArrowLeft,
  Play,
  Download,
  FolderSync,
  ScanSearch,
  RefreshCw,
  Loader2,
  CheckCircle2,
  XCircle,
  Clock,
  Lock,
  LogOut,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api";
import type { PipelineStatus, Stats } from "@/lib/api";

type StepKey = "pipeline" | "download" | "index" | "ocr" | "fts";

interface StepConfig {
  key: StepKey;
  label: string;
  description: string;
  icon: React.ReactNode;
  action: () => Promise<unknown>;
  background?: boolean;
}

const Admin = () => {
  const queryClient = useQueryClient();
  const [authenticated, setAuthenticated] = useState(!!api.getAdminToken());
  const [password, setPassword] = useState("");
  const [loginError, setLoginError] = useState("");
  const [loggingIn, setLoggingIn] = useState(false);

  const [runningStep, setRunningStep] = useState<StepKey | null>(null);
  const [stepResult, setStepResult] = useState<{
    key: StepKey;
    ok: boolean;
    message: string;
  } | null>(null);

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoggingIn(true);
    setLoginError("");
    try {
      const res = await api.adminLogin(password);
      if (res.ok) {
        setAuthenticated(true);
        setPassword("");
      } else {
        setLoginError("Wrong password");
      }
    } catch {
      setLoginError("Failed to connect");
    } finally {
      setLoggingIn(false);
    }
  };

  const handleLogout = () => {
    api.adminLogout();
    setAuthenticated(false);
    setStepResult(null);
  };

  const { data: status, refetch: refetchStatus } = useQuery<PipelineStatus>({
    queryKey: ["pipelineStatus"],
    queryFn: () => api.getPipelineStatus(),
    refetchInterval: (query) =>
      query.state.data?.running ? 2000 : false,
    enabled: authenticated,
  });

  const { data: stats } = useQuery<Stats>({
    queryKey: ["stats"],
    queryFn: () => api.getStats(),
    enabled: authenticated,
  });

  const steps: StepConfig[] = [
    {
      key: "pipeline",
      label: "Full Pipeline",
      description:
        "Run everything: download new exams, index metadata, OCR pages, and rebuild search index.",
      icon: <Play className="h-5 w-5" />,
      action: () => api.runPipeline(),
      background: true,
    },
    {
      key: "download",
      label: "Download Exams",
      description:
        "Crawl the UARK LibGuides pages and download any new PDF exams found.",
      icon: <Download className="h-5 w-5" />,
      action: () => api.runDownload(),
      background: true,
    },
    {
      key: "index",
      label: "Index Metadata",
      description:
        "Scan the exams folder and extract metadata (department, year, author, dates) into the database.",
      icon: <FolderSync className="h-5 w-5" />,
      action: () => api.runIndex(),
      background: true,
    },
    {
      key: "ocr",
      label: "Run OCR",
      description:
        "Render PDF pages as images and run Tesseract OCR to extract searchable text.",
      icon: <ScanSearch className="h-5 w-5" />,
      action: () => api.runOcr(),
      background: true,
    },
    {
      key: "fts",
      label: "Rebuild Search Index",
      description:
        "Drop and recreate the full-text search index from OCR'd page text.",
      icon: <RefreshCw className="h-5 w-5" />,
      action: () => api.runFtsRebuild(),
      background: true,
    },
  ];

  const handleRun = async (step: StepConfig) => {
    setRunningStep(step.key);
    setStepResult(null);
    try {
      await step.action();
      setStepResult({
        key: step.key,
        ok: true,
        message: `${step.label} started. Status will update automatically.`,
      });
      refetchStatus();
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : "Unknown error";
      if (msg.includes("Unauthorized")) {
        setAuthenticated(false);
        api.adminLogout();
      }
      setStepResult({ key: step.key, ok: false, message: msg });
    } finally {
      setRunningStep(null);
    }
  };

  const pipelineRunning = status?.running ?? false;
  const prevRunningRef = useRef(false);
  useEffect(() => {
    if (prevRunningRef.current && !pipelineRunning) {
      queryClient.invalidateQueries({ queryKey: ["stats"] });
    }
    prevRunningRef.current = pipelineRunning;
  }, [pipelineRunning, queryClient]);

  // ---- Login screen ----
  if (!authenticated) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center px-4">
        <Card className="p-8 w-full max-w-sm">
          <div className="flex flex-col items-center gap-4 mb-6">
            <div className="p-3 bg-accent rounded-full">
              <Lock className="h-8 w-8 text-accent-foreground" />
            </div>
            <h1 className="text-2xl font-bold">Admin Login</h1>
            <p className="text-sm text-muted-foreground text-center">
              Enter the admin password to access pipeline controls.
            </p>
          </div>
          <form onSubmit={handleLogin} className="space-y-4">
            <Input
              type="password"
              placeholder="Password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              autoFocus
            />
            {loginError && (
              <p className="text-sm text-red-600">{loginError}</p>
            )}
            <Button type="submit" className="w-full" disabled={loggingIn || !password}>
              {loggingIn ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : null}
              Log in
            </Button>
          </form>
          <Link
            to="/"
            className="block text-center text-sm text-muted-foreground mt-4 hover:underline"
          >
            Back to search
          </Link>
        </Card>
      </div>
    );
  }

  // ---- Authenticated admin panel ----
  return (
    <div className="min-h-screen bg-background">
      <header className="bg-hero-gradient text-white py-10 px-4">
        <div className="container mx-auto max-w-4xl">
          <div className="flex items-center justify-between mb-4">
            <Link
              to="/"
              className="inline-flex items-center gap-1 text-white/80 hover:text-white text-sm"
            >
              <ArrowLeft className="h-4 w-4" /> Back to search
            </Link>
            <Button
              variant="ghost"
              size="sm"
              onClick={handleLogout}
              className="text-white/70 hover:text-white hover:bg-white/10"
            >
              <LogOut className="h-4 w-4 mr-1" /> Logout
            </Button>
          </div>
          <h1 className="text-3xl md:text-4xl font-bold">Admin Panel</h1>
          <p className="text-white/80 mt-2">
            Manually trigger pipeline steps or check status.
          </p>
        </div>
      </header>

      <main className="container mx-auto max-w-4xl px-4 py-8 space-y-6">
        {/* Status card */}
        <Card className="p-6">
          <h2 className="text-xl font-semibold mb-4 flex items-center gap-2">
            Pipeline Status
            <Button
              variant="ghost"
              size="icon"
              onClick={() => refetchStatus()}
              title="Refresh status"
            >
              <RefreshCw className="h-4 w-4" />
            </Button>
          </h2>

          <div className="grid gap-3 sm:grid-cols-2 md:grid-cols-4">
            <div className="flex items-center gap-2">
              {pipelineRunning ? (
                <Loader2 className="h-5 w-5 animate-spin text-blue-500" />
              ) : (
                <CheckCircle2 className="h-5 w-5 text-green-500" />
              )}
              <span className="text-sm font-medium">
                {pipelineRunning ? `Running: ${status?.step}` : "Idle"}
              </span>
            </div>

            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Clock className="h-4 w-4" />
              {status?.last_run
                ? `Last run: ${new Date(status.last_run).toLocaleString()}`
                : "Never run"}
            </div>

            {stats && (
              <>
                <div className="text-sm">
                  <span className="font-semibold">{stats.documents.toLocaleString()}</span> documents
                </div>
                <div className="text-sm">
                  <span className="font-semibold">{stats.page_fts_rows.toLocaleString()}</span> searchable pages
                </div>
              </>
            )}
          </div>

          {status?.error && (
            <div className="mt-3 p-3 bg-red-50 dark:bg-red-950 rounded text-sm text-red-700 dark:text-red-300 flex items-start gap-2">
              <XCircle className="h-4 w-4 mt-0.5 flex-shrink-0" />
              <span>{status.error}</span>
            </div>
          )}
        </Card>

        {/* Step cards */}
        <div className="space-y-4">
          {steps.map((step) => {
            const isRunning = runningStep === step.key || (step.key === "pipeline" && pipelineRunning);
            const result = stepResult?.key === step.key ? stepResult : null;

            return (
              <Card key={step.key} className="p-5">
                <div className="flex items-start gap-4">
                  <div className="flex-shrink-0 p-2 bg-accent rounded-lg mt-0.5">
                    {step.icon}
                  </div>
                  <div className="flex-1 min-w-0">
                    <h3 className="font-semibold text-lg">{step.label}</h3>
                    <p className="text-sm text-muted-foreground mt-1">
                      {step.description}
                    </p>

                    {result && (
                      <div
                        className={`mt-3 p-3 rounded text-sm ${
                          result.ok
                            ? "bg-green-50 dark:bg-green-950 text-green-700 dark:text-green-300"
                            : "bg-red-50 dark:bg-red-950 text-red-700 dark:text-red-300"
                        }`}
                      >
                        <pre className="whitespace-pre-wrap font-mono text-xs">
                          {result.message}
                        </pre>
                      </div>
                    )}
                  </div>
                  <Button
                    onClick={() => handleRun(step)}
                    disabled={isRunning || pipelineRunning}
                    className="flex-shrink-0"
                  >
                    {isRunning ? (
                      <Loader2 className="h-4 w-4 animate-spin mr-2" />
                    ) : (
                      <Play className="h-4 w-4 mr-2" />
                    )}
                    {isRunning ? "Running..." : "Run"}
                  </Button>
                </div>
              </Card>
            );
          })}
        </div>
      </main>
    </div>
  );
};

export default Admin;

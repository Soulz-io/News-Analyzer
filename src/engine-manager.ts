import http from "node:http";

interface PluginLogger {
  info: (msg: string) => void;
  warn: (msg: string) => void;
}

interface EngineManagerParams {
  logger?: PluginLogger;
  engineRoot: string;
  enginePort: number;
  pythonPath: string;
}

const HEALTH_CHECK_INTERVAL_MS = 30_000; // 30s — just monitoring, not managing
const CONSECUTIVE_FAILURES_WARN = 3;

/**
 * Lightweight health monitor for the externally-managed News Analyzer engine.
 *
 * The engine is managed by systemd (openclaw-news-analyzer.service).
 * This class only monitors health via HTTP polling — it does NOT spawn
 * or restart the engine process. Systemd handles all process lifecycle.
 *
 * Previous architecture:
 *   EngineManager spawned uvicorn → caused orphans & port conflicts.
 *
 * Current architecture:
 *   systemd (openclaw-news-analyzer.service) → manages engine on port 9121
 *   EngineManager → health-check polling only, logs warnings on failure
 */
export class EngineManager {
  private readonly logger?: PluginLogger;
  private readonly enginePort: number;

  private healthCheckTimer: ReturnType<typeof setInterval> | null = null;
  private stopped = false;
  private consecutiveFailures = 0;

  constructor(params: EngineManagerParams) {
    this.logger = params.logger;
    this.enginePort = params.enginePort;
  }

  /**
   * Start health-check polling against the systemd-managed engine.
   */
  async start(): Promise<void> {
    if (this.stopped) return;

    this.logger?.info?.(
      `[engine-manager] Proxy mode — monitoring engine on port ${this.enginePort} (managed by systemd)`,
    );
    this.startHealthCheck();
  }

  /**
   * Stop health-check polling.
   */
  stop(): void {
    this.stopped = true;

    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
    }

    this.logger?.info?.("[engine-manager] Health monitor stopped.");
  }

  // ── Health check ───────────────────────────────────────────

  private startHealthCheck(): void {
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
    }

    // Initial check after 5s
    setTimeout(() => this.performHealthCheck(), 5_000);

    this.healthCheckTimer = setInterval(() => {
      this.performHealthCheck();
    }, HEALTH_CHECK_INTERVAL_MS);
  }

  private performHealthCheck(): void {
    if (this.stopped) return;

    const url = `http://127.0.0.1:${this.enginePort}/api/status`;

    const req = http.get(url, { timeout: 5_000 }, (res) => {
      res.resume();
      if (res.statusCode === 200) {
        if (this.consecutiveFailures > 0) {
          this.logger?.info?.(
            `[engine-manager] Engine recovered after ${this.consecutiveFailures} failed checks.`,
          );
        }
        this.consecutiveFailures = 0;
      } else {
        this.consecutiveFailures++;
        this.logger?.warn?.(
          `[engine-manager] Health check returned status ${res.statusCode} (${this.consecutiveFailures} consecutive).`,
        );
      }
    });

    req.on("error", (err) => {
      this.consecutiveFailures++;
      if (this.consecutiveFailures >= CONSECUTIVE_FAILURES_WARN) {
        this.logger?.warn?.(
          `[engine-manager] Engine unreachable (${this.consecutiveFailures} consecutive failures: ${err.message}). ` +
            `Check: systemctl status openclaw-news-analyzer`,
        );
      }
    });

    req.on("timeout", () => {
      req.destroy();
      this.consecutiveFailures++;
      this.logger?.warn?.(
        `[engine-manager] Health check timed out (${this.consecutiveFailures} consecutive).`,
      );
    });
  }
}

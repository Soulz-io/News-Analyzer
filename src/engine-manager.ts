import { spawn, execSync, type ChildProcess } from "node:child_process";
import path from "node:path";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";

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

const HEALTH_CHECK_INTERVAL_MS = 15_000;
const BACKOFF_STEPS_MS = [5_000, 10_000, 30_000, 60_000, 300_000];
const GRACEFUL_SHUTDOWN_TIMEOUT_MS = 5_000;

/**
 * Manages the Python FastAPI engine subprocess.
 *
 * Lifecycle:
 *   1. Ensure virtualenv exists (creates if missing)
 *   2. Install requirements.txt
 *   3. Download spaCy model if missing
 *   4. Download NLTK punkt tokenizer if missing
 *   5. Spawn uvicorn process
 *   6. Health-check polling
 *   7. Restart on crash with exponential backoff
 *   8. Graceful shutdown: SIGTERM -> 5s wait -> SIGKILL
 */
export class EngineManager {
  private readonly logger?: PluginLogger;
  private readonly engineRoot: string;
  private readonly enginePort: number;
  private readonly pythonPath: string;
  private readonly venvPath: string;
  private readonly venvPython: string;
  private readonly venvPip: string;
  private readonly dataDir: string;

  private process: ChildProcess | null = null;
  private healthCheckTimer: ReturnType<typeof setInterval> | null = null;
  private restartTimer: ReturnType<typeof setTimeout> | null = null;
  private crashCount = 0;
  private stopped = false;
  private starting = false;

  constructor(params: EngineManagerParams) {
    this.logger = params.logger;
    this.engineRoot = params.engineRoot;
    this.enginePort = params.enginePort;
    this.pythonPath = params.pythonPath;
    this.venvPath = path.join(this.engineRoot, "venv");

    const isWindows = os.platform() === "win32";
    const binDir = isWindows ? "Scripts" : "bin";
    this.venvPython = path.join(this.venvPath, binDir, "python");
    this.venvPip = path.join(this.venvPath, binDir, "pip");

    this.dataDir = path.join(this.engineRoot, "data");
  }

  /**
   * Starts the engine setup and spawn sequence.
   * Runs environment setup synchronously, then spawns the engine async.
   */
  async start(): Promise<void> {
    if (this.stopped) return;
    if (this.starting) return;
    this.starting = true;

    try {
      this.logger?.info?.("[engine-manager] Preparing Python engine environment...");

      this.ensureVenv();
      this.installRequirements();
      this.ensureSpacyModel();
      this.ensureNltkData();
      this.ensureDataDir();

      this.spawnEngine();
      this.startHealthCheck();

      this.logger?.info?.("[engine-manager] Engine started successfully.");
    } catch (err) {
      this.logger?.warn?.(`[engine-manager] Failed to start engine: ${err}`);
      this.scheduleRestart();
    } finally {
      this.starting = false;
    }
  }

  /**
   * Gracefully stops the engine process and all timers.
   */
  stop(): void {
    this.stopped = true;

    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
    }
    if (this.restartTimer) {
      clearTimeout(this.restartTimer);
      this.restartTimer = null;
    }

    this.killProcess();
  }

  // ── Environment setup ──────────────────────────────────────

  private ensureVenv(): void {
    if (fs.existsSync(this.venvPython)) {
      this.logger?.info?.("[engine-manager] Virtualenv already exists.");
      return;
    }

    this.logger?.info?.("[engine-manager] Creating virtualenv...");
    execSync(`${this.pythonPath} -m venv "${this.venvPath}"`, {
      cwd: this.engineRoot,
      stdio: "pipe",
      timeout: 120_000,
    });
    this.logger?.info?.("[engine-manager] Virtualenv created.");
  }

  private installRequirements(): void {
    const reqFile = path.join(this.engineRoot, "requirements.txt");
    if (!fs.existsSync(reqFile)) {
      this.logger?.warn?.("[engine-manager] requirements.txt not found, skipping install.");
      return;
    }

    this.logger?.info?.("[engine-manager] Installing Python requirements...");
    execSync(
      `"${this.venvPip}" install --upgrade pip && "${this.venvPip}" install -r "${reqFile}"`,
      {
        cwd: this.engineRoot,
        stdio: "pipe",
        timeout: 600_000, // 10 minutes for large packages
        shell: "/bin/bash",
      },
    );
    this.logger?.info?.("[engine-manager] Requirements installed.");
  }

  private ensureSpacyModel(): void {
    try {
      // Check if the model is already installed
      execSync(
        `"${this.venvPython}" -c "import spacy; spacy.load('en_core_web_lg')"`,
        {
          cwd: this.engineRoot,
          stdio: "pipe",
          timeout: 60_000,
        },
      );
      this.logger?.info?.("[engine-manager] spaCy en_core_web_lg already installed.");
    } catch {
      this.logger?.info?.("[engine-manager] Downloading spaCy en_core_web_lg model...");
      execSync(
        `"${this.venvPython}" -m spacy download en_core_web_lg`,
        {
          cwd: this.engineRoot,
          stdio: "pipe",
          timeout: 600_000,
        },
      );
      this.logger?.info?.("[engine-manager] spaCy model downloaded.");
    }
  }

  private ensureNltkData(): void {
    try {
      // Check if punkt tokenizer data is already available
      execSync(
        `"${this.venvPython}" -c "import nltk; nltk.data.find('tokenizers/punkt_tab')"`,
        {
          cwd: this.engineRoot,
          stdio: "pipe",
          timeout: 60_000,
        },
      );
      this.logger?.info?.("[engine-manager] NLTK punkt tokenizer data already present.");
    } catch {
      this.logger?.info?.("[engine-manager] Downloading NLTK punkt tokenizer data...");
      execSync(
        `"${this.venvPython}" -c "import nltk; nltk.download('punkt_tab', quiet=True)"`,
        {
          cwd: this.engineRoot,
          stdio: "pipe",
          timeout: 120_000,
        },
      );
      this.logger?.info?.("[engine-manager] NLTK data downloaded.");
    }
  }

  private ensureDataDir(): void {
    fs.mkdirSync(this.dataDir, { recursive: true });
  }

  // ── Process management ─────────────────────────────────────

  private spawnEngine(): void {
    if (this.stopped) return;

    const args = [
      "-m", "uvicorn",
      "engine:app",
      "--host", "127.0.0.1",
      "--port", String(this.enginePort),
    ];

    this.logger?.info?.(
      `[engine-manager] Spawning: ${this.venvPython} ${args.join(" ")}`,
    );

    this.process = spawn(this.venvPython, args, {
      cwd: this.engineRoot,
      stdio: ["ignore", "pipe", "pipe"],
      env: {
        ...process.env,
        ENGINE_PORT: String(this.enginePort),
        DATA_DIR: this.dataDir,
        VIRTUAL_ENV: this.venvPath,
        PATH: `${path.dirname(this.venvPython)}:${process.env.PATH || ""}`,
      },
    });

    this.process.stdout?.on("data", (chunk: Buffer) => {
      const lines = chunk.toString("utf-8").trim().split("\n");
      for (const line of lines) {
        this.logger?.info?.(`[engine] ${line}`);
      }
    });

    this.process.stderr?.on("data", (chunk: Buffer) => {
      const lines = chunk.toString("utf-8").trim().split("\n");
      for (const line of lines) {
        this.logger?.warn?.(`[engine:err] ${line}`);
      }
    });

    this.process.on("exit", (code, signal) => {
      this.logger?.warn?.(
        `[engine-manager] Engine exited (code=${code}, signal=${signal}).`,
      );
      this.process = null;

      if (!this.stopped) {
        this.scheduleRestart();
      }
    });

    this.process.on("error", (err) => {
      this.logger?.warn?.(`[engine-manager] Engine spawn error: ${err.message}`);
      this.process = null;

      if (!this.stopped) {
        this.scheduleRestart();
      }
    });
  }

  private scheduleRestart(): void {
    if (this.stopped) return;
    if (this.restartTimer) return;

    const backoffIndex = Math.min(this.crashCount, BACKOFF_STEPS_MS.length - 1);
    const delayMs = BACKOFF_STEPS_MS[backoffIndex];
    this.crashCount++;

    this.logger?.info?.(
      `[engine-manager] Scheduling restart in ${delayMs / 1000}s (attempt ${this.crashCount})...`,
    );

    this.restartTimer = setTimeout(() => {
      this.restartTimer = null;
      this.start();
    }, delayMs);
  }

  // ── Health check ───────────────────────────────────────────

  private startHealthCheck(): void {
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
    }

    this.healthCheckTimer = setInterval(() => {
      this.performHealthCheck();
    }, HEALTH_CHECK_INTERVAL_MS);
  }

  private performHealthCheck(): void {
    if (this.stopped) return;

    const url = `http://127.0.0.1:${this.enginePort}/api/status`;

    const req = http.get(url, { timeout: 5_000 }, (res) => {
      // Consume response data to free the socket
      res.resume();
      if (res.statusCode === 200) {
        // Engine is healthy -- reset crash counter
        this.crashCount = 0;
      } else {
        this.logger?.warn?.(
          `[engine-manager] Health check returned status ${res.statusCode}.`,
        );
      }
    });

    req.on("error", (err) => {
      this.logger?.warn?.(
        `[engine-manager] Health check failed: ${err.message}`,
      );
      // If the process is already dead, the exit handler will schedule restart
    });

    req.on("timeout", () => {
      req.destroy();
      this.logger?.warn?.("[engine-manager] Health check timed out.");
    });
  }

  // ── Graceful kill ──────────────────────────────────────────

  private killProcess(): void {
    if (!this.process) return;

    const proc = this.process;
    this.process = null;

    try {
      proc.kill("SIGTERM");
    } catch {
      /* process may already be gone */
    }

    // Wait for graceful shutdown, then force-kill
    const forceKillTimer = setTimeout(() => {
      try {
        proc.kill("SIGKILL");
      } catch {
        /* already gone */
      }
    }, GRACEFUL_SHUTDOWN_TIMEOUT_MS);

    proc.on("exit", () => {
      clearTimeout(forceKillTimer);
    });
  }
}

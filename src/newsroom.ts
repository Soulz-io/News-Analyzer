import http from "node:http";

interface PluginLogger {
  info: (msg: string) => void;
  warn: (msg: string) => void;
}

interface NewsroomParams {
  logger?: PluginLogger;
  enginePort: number;
  fetchIntervalMinutes: number;
  scanIntervalHours: number;
  runupCheckIntervalHours: number;
  probabilityShiftThreshold: number;
  scannerModel: string;
  analystModel: string;
  runtime: any;
}

// ── Agent system prompts ─────────────────────────────────────

const NARRATIVE_SCANNER_PROMPT = `You are a Narrative Scanner agent for a news analysis system.

Your role is to analyze a batch of recently fetched news articles and identify emerging narrative threads.

For each batch of articles you receive, you must:
1. Read every article's title, summary, and extracted entities/keywords.
2. Cluster articles by shared narrative themes (e.g., "US-China trade tensions", "AI regulation debate").
3. For each cluster, produce:
   - A concise narrative label (max 10 words).
   - A one-paragraph narrative summary explaining the thread.
   - Confidence score (0.0-1.0) that this is a coherent, developing narrative.
   - List of key entities (people, organizations, countries) involved.
   - Sentiment direction: escalating, de-escalating, or stable.
4. Flag any narratives that appear to be accelerating (more articles, stronger language).

Output as structured JSON with a "narratives" array.
Do NOT hallucinate articles or facts not present in the input data.`;

const RUNUP_DETECTOR_PROMPT = `You are a Run-Up Detector agent for a news analysis system.

Your role is to analyze narrative timelines and detect "run-ups" -- patterns where media coverage
is building toward a likely significant event (policy announcement, conflict escalation, market move, etc.).

For each narrative timeline you receive, you must:
1. Examine the article frequency over time (is coverage accelerating?).
2. Analyze sentiment trajectory (is language becoming more urgent/extreme?).
3. Look for "signal" patterns:
   - Increasing frequency of official statements or leaks.
   - Shift from background reporting to front-page coverage.
   - Convergence of multiple independent narratives.
   - Appearance of "preparation" language (e.g., "sources say", "expected to", "preparing for").
4. For each detected run-up, produce:
   - The narrative it belongs to.
   - Run-up confidence score (0.0-1.0).
   - Estimated time horizon (days/weeks/months).
   - Key indicators that triggered detection.
   - Potential culminating events (what might happen).

Output as structured JSON with a "runups" array.
Base your analysis ONLY on the data provided. Do not speculate beyond the evidence.`;

const GAME_THEORY_ANALYST_PROMPT = `You are a Game Theory Analyst agent for a news analysis system.

Your role is to build decision trees for detected run-ups, modeling the strategic options
available to key actors and their likely outcomes.

For each run-up you receive, you must:
1. Identify the key actors (nations, organizations, leaders, companies).
2. For each actor, enumerate their realistic strategic options (2-4 per actor).
3. Build a decision tree:
   - Root: Current situation (the run-up state).
   - Branches: Each actor's possible moves.
   - Leaves: Outcome scenarios with probability estimates.
4. For each leaf/outcome:
   - Assign a probability (all leaves must sum to 1.0 per branch).
   - Describe the outcome scenario (2-3 sentences).
   - Rate impact severity: low, medium, high, critical.
   - Identify which actors benefit/lose.
5. Identify the Nash equilibrium (most likely outcome assuming rational actors).
6. Flag any "wild card" scenarios with low probability but high impact.

Output as structured JSON with a "decision_tree" object.
Probabilities must be justified by the evidence in the input data.`;

const TRUTH_ANALYST_PROMPT = `You are a Truth Analyst agent for a news analysis system.

Your role is to evaluate active run-ups daily and assess whether the predicted trajectory
is still on track, needs revision, or has been invalidated.

For each active run-up and its latest articles, you must:
1. Compare today's coverage against the predicted trajectory.
2. Identify confirming signals (events/statements that support the prediction).
3. Identify disconfirming signals (events/statements that contradict the prediction).
4. Check for new actors or unexpected developments.
5. Produce an updated assessment:
   - Status: on_track, accelerating, decelerating, stalled, invalidated.
   - Updated confidence score (0.0-1.0) with explanation of change.
   - Updated time horizon if changed.
   - Any new branches to add to the decision tree.
   - Key events to watch for in the next 24-48 hours.

Output as structured JSON with an "assessment" object.
Be rigorous: flag when your earlier predictions were wrong and explain why.`;

const PROBABILITY_UPDATER_PROMPT = `You are a Probability Updater agent for a news analysis system.

Your role is to recalculate decision tree probabilities when significant new information arrives
that shifts the likelihood of outcomes.

You will receive:
- The current decision tree with existing probabilities.
- New evidence (articles, events) that triggered this update.
- The magnitude of the probability shift that triggered your activation.

You must:
1. Identify which branches/outcomes are affected by the new evidence.
2. Apply Bayesian reasoning to update probabilities:
   - State your prior (existing probability).
   - State the likelihood of observing this evidence given each outcome.
   - Calculate the posterior probability.
3. Ensure all probabilities remain consistent (sum to 1.0 per branch level).
4. Explain each significant probability change (>5% shift) with reasoning.
5. Flag if any outcome has crossed a critical threshold (>70% or <5%).
6. Update the Nash equilibrium assessment if it has changed.

Output as structured JSON with an "updated_tree" object and a "changes" array.
Show your Bayesian math for each significant update.`;

// ── Types ────────────────────────────────────────────────────

interface Narrative {
  id: string;
  label: string;
  summary: string;
  confidence: number;
  sentiment_direction: string;
}

interface RunUp {
  id: string;
  narrative_id: string;
  label: string;
  confidence: number;
  time_horizon: string;
  status: string;
  has_decision_tree: boolean;
}

interface EngineStatus {
  status: string;
  last_fetch?: string;
  article_count?: number;
}

interface ProbabilityShift {
  runup_id: string;
  branch_path: string;
  old_probability: number;
  new_probability: number;
  shift: number;
  evidence_summary: string;
}

// ── Newsroom orchestrator ────────────────────────────────────

/**
 * The Newsroom orchestrates the spawning of analysis agents based on
 * scheduled intervals and event-driven triggers.
 *
 * Scheduled:
 *   - After each fetch cycle: spawn Narrative Scanner
 *   - Every N hours: spawn Run-Up Detector
 *   - Daily per active run-up: spawn Truth Analyst
 *
 * Event-driven:
 *   - New run-up detected: spawn Game Theory Analyst
 *   - Probability shift > threshold: spawn Probability Updater
 */
export class Newsroom {
  private readonly logger?: PluginLogger;
  private readonly enginePort: number;
  private readonly fetchIntervalMinutes: number;
  private readonly scanIntervalHours: number;
  private readonly runupCheckIntervalHours: number;
  private readonly probabilityShiftThreshold: number;
  private readonly scannerModel: string;
  private readonly analystModel: string;
  private readonly runtime: any;

  private fetchCheckTimer: ReturnType<typeof setInterval> | null = null;
  private scanTimer: ReturnType<typeof setInterval> | null = null;
  private runupCheckTimer: ReturnType<typeof setInterval> | null = null;
  private truthAnalystTimer: ReturnType<typeof setInterval> | null = null;
  private probabilityPollTimer: ReturnType<typeof setInterval> | null = null;

  private lastFetchTimestamp: string | null = null;
  private knownRunUpIds: Set<string> = new Set();
  private stopped = false;

  constructor(params: NewsroomParams) {
    this.logger = params.logger;
    this.enginePort = params.enginePort;
    this.fetchIntervalMinutes = params.fetchIntervalMinutes;
    this.scanIntervalHours = params.scanIntervalHours;
    this.runupCheckIntervalHours = params.runupCheckIntervalHours;
    this.probabilityShiftThreshold = params.probabilityShiftThreshold;
    this.scannerModel = params.scannerModel;
    this.analystModel = params.analystModel;
    this.runtime = params.runtime;
  }

  /**
   * Starts all scheduled polling loops.
   * Waits briefly for the engine to be ready before starting.
   */
  start(): void {
    if (this.stopped) return;

    this.logger?.info?.("[newsroom] Starting orchestrator...");

    // Poll for fetch completion to trigger Narrative Scanner
    const fetchCheckMs = this.fetchIntervalMinutes * 60 * 1000;
    this.fetchCheckTimer = setInterval(() => {
      this.checkFetchAndScanNarratives();
    }, fetchCheckMs);

    // Periodic Narrative Scanner (independent of fetch signal)
    const scanMs = this.scanIntervalHours * 60 * 60 * 1000;
    this.scanTimer = setInterval(() => {
      this.spawnNarrativeScanner();
    }, scanMs);

    // Periodic Run-Up Detector
    const runupCheckMs = this.runupCheckIntervalHours * 60 * 60 * 1000;
    this.runupCheckTimer = setInterval(() => {
      this.spawnRunUpDetector();
    }, runupCheckMs);

    // Daily Truth Analyst for active run-ups (every 24 hours)
    const truthMs = 24 * 60 * 60 * 1000;
    this.truthAnalystTimer = setInterval(() => {
      this.spawnTruthAnalystsForActiveRunUps();
    }, truthMs);

    // Poll for probability shifts (every 5 minutes)
    const probPollMs = 5 * 60 * 1000;
    this.probabilityPollTimer = setInterval(() => {
      this.checkProbabilityShifts();
    }, probPollMs);

    // Run initial scan after a 60-second startup delay (allow engine to boot)
    setTimeout(() => {
      if (!this.stopped) {
        this.spawnNarrativeScanner();
        this.initializeKnownRunUps();
      }
    }, 60_000);

    this.logger?.info?.("[newsroom] Orchestrator started.");
  }

  /**
   * Stops all timers and halts agent spawning.
   */
  stop(): void {
    this.stopped = true;

    const timers = [
      this.fetchCheckTimer,
      this.scanTimer,
      this.runupCheckTimer,
      this.truthAnalystTimer,
      this.probabilityPollTimer,
    ];

    for (const timer of timers) {
      if (timer) clearInterval(timer);
    }

    this.fetchCheckTimer = null;
    this.scanTimer = null;
    this.runupCheckTimer = null;
    this.truthAnalystTimer = null;
    this.probabilityPollTimer = null;

    this.logger?.info?.("[newsroom] Orchestrator stopped.");
  }

  // ── Scheduled spawn logic ──────────────────────────────────

  /**
   * Checks if a new fetch cycle completed and triggers narrative scanning.
   */
  private async checkFetchAndScanNarratives(): Promise<void> {
    if (this.stopped) return;

    try {
      const status = await this.engineGet<EngineStatus>("/api/status");
      if (!status || status.status !== "ok") return;

      if (status.last_fetch && status.last_fetch !== this.lastFetchTimestamp) {
        this.lastFetchTimestamp = status.last_fetch;
        this.logger?.info?.("[newsroom] New fetch cycle detected, spawning Narrative Scanner.");
        await this.spawnNarrativeScanner();
      }
    } catch (err) {
      this.logger?.warn?.(`[newsroom] Fetch check error: ${err}`);
    }
  }

  /**
   * Spawns a Narrative Scanner agent with the latest articles batch.
   */
  private async spawnNarrativeScanner(): Promise<void> {
    if (this.stopped) return;

    try {
      const articles = await this.engineGet<unknown[]>("/api/articles/recent");
      if (!articles || articles.length === 0) {
        this.logger?.info?.("[newsroom] No recent articles to scan.");
        return;
      }

      this.logger?.info?.(`[newsroom] Spawning Narrative Scanner for ${articles.length} articles.`);

      await this.spawnAgent({
        label: "Narrative Scanner",
        model: this.scannerModel,
        systemPrompt: NARRATIVE_SCANNER_PROMPT,
        attachments: [
          {
            name: "articles_batch.json",
            content: JSON.stringify(articles, null, 2),
            mimeType: "application/json",
          },
        ],
        callbackEndpoint: "/api/narratives/ingest",
      });
    } catch (err) {
      this.logger?.warn?.(`[newsroom] Failed to spawn Narrative Scanner: ${err}`);
    }
  }

  /**
   * Spawns a Run-Up Detector agent with current narrative timelines.
   */
  private async spawnRunUpDetector(): Promise<void> {
    if (this.stopped) return;

    try {
      const narratives = await this.engineGet<Narrative[]>("/api/narratives");
      if (!narratives || narratives.length === 0) {
        this.logger?.info?.("[newsroom] No narratives to check for run-ups.");
        return;
      }

      const timelines = await this.engineGet<unknown>("/api/narratives/timelines");

      this.logger?.info?.(`[newsroom] Spawning Run-Up Detector for ${narratives.length} narratives.`);

      await this.spawnAgent({
        label: "Run-Up Detector",
        model: this.scannerModel,
        systemPrompt: RUNUP_DETECTOR_PROMPT,
        attachments: [
          {
            name: "narratives.json",
            content: JSON.stringify(narratives, null, 2),
            mimeType: "application/json",
          },
          {
            name: "timelines.json",
            content: JSON.stringify(timelines, null, 2),
            mimeType: "application/json",
          },
        ],
        callbackEndpoint: "/api/runups/ingest",
      });

      // After detection, check for newly discovered run-ups
      setTimeout(() => this.checkForNewRunUps(), 120_000);
    } catch (err) {
      this.logger?.warn?.(`[newsroom] Failed to spawn Run-Up Detector: ${err}`);
    }
  }

  /**
   * Spawns a Truth Analyst agent for each active run-up (daily).
   */
  private async spawnTruthAnalystsForActiveRunUps(): Promise<void> {
    if (this.stopped) return;

    try {
      const runups = await this.engineGet<RunUp[]>("/api/runups");
      if (!runups) return;

      const active = runups.filter(
        (r) => r.status === "active" || r.status === "accelerating",
      );

      if (active.length === 0) {
        this.logger?.info?.("[newsroom] No active run-ups for Truth Analyst.");
        return;
      }

      this.logger?.info?.(`[newsroom] Spawning Truth Analysts for ${active.length} active run-ups.`);

      for (const runup of active) {
        if (this.stopped) break;

        const runupDetail = await this.engineGet<unknown>(
          `/api/runups/${runup.id}`,
        );
        const recentArticles = await this.engineGet<unknown[]>(
          `/api/runups/${runup.id}/articles`,
        );

        await this.spawnAgent({
          label: `Truth Analyst: ${runup.label}`,
          model: this.analystModel,
          systemPrompt: TRUTH_ANALYST_PROMPT,
          attachments: [
            {
              name: "runup_detail.json",
              content: JSON.stringify(runupDetail, null, 2),
              mimeType: "application/json",
            },
            {
              name: "recent_articles.json",
              content: JSON.stringify(recentArticles, null, 2),
              mimeType: "application/json",
            },
          ],
          callbackEndpoint: `/api/runups/${runup.id}/assessment`,
        });
      }
    } catch (err) {
      this.logger?.warn?.(`[newsroom] Failed to spawn Truth Analysts: ${err}`);
    }
  }

  // ── Event-driven spawn logic ───────────────────────────────

  /**
   * Initializes the set of known run-up IDs so we can detect new ones.
   */
  private async initializeKnownRunUps(): Promise<void> {
    try {
      const runups = await this.engineGet<RunUp[]>("/api/runups");
      if (runups) {
        for (const r of runups) {
          this.knownRunUpIds.add(r.id);
        }
        this.logger?.info?.(
          `[newsroom] Initialized with ${this.knownRunUpIds.size} known run-ups.`,
        );
      }
    } catch (err) {
      this.logger?.warn?.(`[newsroom] Failed to initialize known run-ups: ${err}`);
    }
  }

  /**
   * Checks for newly detected run-ups and spawns Game Theory Analyst for each.
   */
  private async checkForNewRunUps(): Promise<void> {
    if (this.stopped) return;

    try {
      const runups = await this.engineGet<RunUp[]>("/api/runups");
      if (!runups) return;

      for (const runup of runups) {
        if (this.knownRunUpIds.has(runup.id)) continue;

        this.knownRunUpIds.add(runup.id);
        this.logger?.info?.(
          `[newsroom] New run-up detected: "${runup.label}", spawning Game Theory Analyst.`,
        );

        await this.spawnGameTheoryAnalyst(runup);
      }
    } catch (err) {
      this.logger?.warn?.(`[newsroom] Failed to check for new run-ups: ${err}`);
    }
  }

  /**
   * Spawns a Game Theory Analyst for a newly detected run-up.
   */
  private async spawnGameTheoryAnalyst(runup: RunUp): Promise<void> {
    if (this.stopped) return;

    try {
      const runupDetail = await this.engineGet<unknown>(`/api/runups/${runup.id}`);
      const narrativeDetail = await this.engineGet<unknown>(
        `/api/narratives/${runup.narrative_id}`,
      );

      await this.spawnAgent({
        label: `Game Theory Analyst: ${runup.label}`,
        model: this.analystModel,
        systemPrompt: GAME_THEORY_ANALYST_PROMPT,
        attachments: [
          {
            name: "runup_detail.json",
            content: JSON.stringify(runupDetail, null, 2),
            mimeType: "application/json",
          },
          {
            name: "narrative_context.json",
            content: JSON.stringify(narrativeDetail, null, 2),
            mimeType: "application/json",
          },
        ],
        callbackEndpoint: `/api/runups/${runup.id}/decision-tree`,
      });
    } catch (err) {
      this.logger?.warn?.(
        `[newsroom] Failed to spawn Game Theory Analyst for ${runup.id}: ${err}`,
      );
    }
  }

  /**
   * Polls the engine for probability shifts and spawns Probability Updater
   * when the shift exceeds the configured threshold.
   */
  private async checkProbabilityShifts(): Promise<void> {
    if (this.stopped) return;

    try {
      const shifts = await this.engineGet<ProbabilityShift[]>(
        "/api/probability-shifts/pending",
      );
      if (!shifts || shifts.length === 0) return;

      const significant = shifts.filter(
        (s) => Math.abs(s.shift) >= this.probabilityShiftThreshold,
      );

      if (significant.length === 0) return;

      this.logger?.info?.(
        `[newsroom] ${significant.length} significant probability shift(s) detected.`,
      );

      // Group shifts by run-up
      const byRunUp = new Map<string, ProbabilityShift[]>();
      for (const shift of significant) {
        const existing = byRunUp.get(shift.runup_id) || [];
        existing.push(shift);
        byRunUp.set(shift.runup_id, existing);
      }

      for (const [runupId, runupShifts] of byRunUp) {
        if (this.stopped) break;

        const tree = await this.engineGet<unknown>(
          `/api/runups/${runupId}/decision-tree`,
        );
        const evidence = await this.engineGet<unknown[]>(
          `/api/runups/${runupId}/recent-evidence`,
        );

        await this.spawnAgent({
          label: `Probability Updater: ${runupId}`,
          model: this.analystModel,
          systemPrompt: PROBABILITY_UPDATER_PROMPT,
          attachments: [
            {
              name: "current_decision_tree.json",
              content: JSON.stringify(tree, null, 2),
              mimeType: "application/json",
            },
            {
              name: "probability_shifts.json",
              content: JSON.stringify(runupShifts, null, 2),
              mimeType: "application/json",
            },
            {
              name: "new_evidence.json",
              content: JSON.stringify(evidence, null, 2),
              mimeType: "application/json",
            },
          ],
          callbackEndpoint: `/api/runups/${runupId}/update-probabilities`,
        });
      }

      // Acknowledge processed shifts
      await this.enginePost("/api/probability-shifts/acknowledge", {
        shift_ids: significant.map((s) => `${s.runup_id}:${s.branch_path}`),
      });
    } catch (err) {
      this.logger?.warn?.(`[newsroom] Probability shift check error: ${err}`);
    }
  }

  // ── Agent spawning ─────────────────────────────────────────

  /**
   * Spawns a sub-agent via the OpenClaw runtime or falls back to posting
   * the task directly to the engine for processing.
   *
   * The agent receives its data as JSON attachments and its role via the
   * system prompt. Results are posted back to the engine's callback endpoint.
   */
  private async spawnAgent(params: {
    label: string;
    model: string;
    systemPrompt: string;
    attachments: Array<{
      name: string;
      content: string;
      mimeType: string;
    }>;
    callbackEndpoint: string;
  }): Promise<void> {
    const { label, model, systemPrompt, attachments, callbackEndpoint } = params;

    this.logger?.info?.(`[newsroom] Spawning agent: ${label}`);

    // Build the task message that includes references to attachments
    const attachmentList = attachments
      .map((a) => `- ${a.name}`)
      .join("\n");
    const taskMessage = [
      `Analyze the attached data and produce your assessment.`,
      ``,
      `Attached files:`,
      attachmentList,
      ``,
      `Return your analysis as valid JSON matching the schema described in your instructions.`,
    ].join("\n");

    // Try to use OpenClaw runtime to spawn a sub-agent
    if (this.runtime && typeof this.runtime.spawnSubagent === "function") {
      try {
        const result = await this.runtime.spawnSubagent({
          label,
          model,
          systemPrompt,
          task: taskMessage,
          attachments: attachments.map((a) => ({
            filename: a.name,
            content: a.content,
            mimeType: a.mimeType,
          })),
        });

        // Post the agent's result back to the engine
        if (result && result.output) {
          await this.enginePost(callbackEndpoint, {
            agent_label: label,
            agent_model: model,
            output: result.output,
            timestamp: new Date().toISOString(),
          });
        }

        this.logger?.info?.(`[newsroom] Agent "${label}" completed via runtime.`);
        return;
      } catch (err) {
        this.logger?.warn?.(
          `[newsroom] Runtime spawn failed for "${label}", falling back to engine: ${err}`,
        );
      }
    }

    // Fallback: post agent task directly to the engine for local processing
    try {
      await this.enginePost("/api/agents/dispatch", {
        label,
        model,
        system_prompt: systemPrompt,
        task: taskMessage,
        attachments: attachments.map((a) => ({
          filename: a.name,
          content: a.content,
          mime_type: a.mimeType,
        })),
        callback_endpoint: callbackEndpoint,
      });

      this.logger?.info?.(`[newsroom] Agent "${label}" dispatched to engine.`);
    } catch (err) {
      this.logger?.warn?.(`[newsroom] Failed to dispatch agent "${label}": ${err}`);
    }
  }

  // ── Engine HTTP helpers ────────────────────────────────────

  /**
   * Performs a GET request to the Python engine API.
   */
  private engineGet<T>(apiPath: string): Promise<T | null> {
    return new Promise((resolve) => {
      const url = `http://127.0.0.1:${this.enginePort}${apiPath}`;

      const req = http.get(url, { timeout: 30_000 }, (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => chunks.push(chunk));
        res.on("end", () => {
          if (res.statusCode !== 200) {
            this.logger?.warn?.(
              `[newsroom] GET ${apiPath} returned ${res.statusCode}`,
            );
            resolve(null);
            return;
          }

          try {
            const body = Buffer.concat(chunks).toString("utf-8");
            resolve(JSON.parse(body) as T);
          } catch (err) {
            this.logger?.warn?.(`[newsroom] Failed to parse response from ${apiPath}: ${err}`);
            resolve(null);
          }
        });
      });

      req.on("error", (err) => {
        this.logger?.warn?.(`[newsroom] GET ${apiPath} error: ${err.message}`);
        resolve(null);
      });

      req.on("timeout", () => {
        req.destroy();
        this.logger?.warn?.(`[newsroom] GET ${apiPath} timed out.`);
        resolve(null);
      });
    });
  }

  /**
   * Performs a POST request to the Python engine API.
   */
  private enginePost<T>(apiPath: string, data: unknown): Promise<T | null> {
    return new Promise((resolve) => {
      const url = new URL(`http://127.0.0.1:${this.enginePort}${apiPath}`);
      const payload = JSON.stringify(data);

      const opts: http.RequestOptions = {
        hostname: url.hostname,
        port: url.port,
        path: url.pathname,
        method: "POST",
        headers: {
          "Content-Type": "application/json; charset=utf-8",
          "Content-Length": Buffer.byteLength(payload),
        },
        timeout: 60_000,
      };

      const req = http.request(opts, (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => chunks.push(chunk));
        res.on("end", () => {
          if (res.statusCode && res.statusCode >= 400) {
            this.logger?.warn?.(
              `[newsroom] POST ${apiPath} returned ${res.statusCode}`,
            );
            resolve(null);
            return;
          }

          try {
            const body = Buffer.concat(chunks).toString("utf-8");
            resolve(body ? (JSON.parse(body) as T) : (null as T));
          } catch {
            resolve(null);
          }
        });
      });

      req.on("error", (err) => {
        this.logger?.warn?.(`[newsroom] POST ${apiPath} error: ${err.message}`);
        resolve(null);
      });

      req.on("timeout", () => {
        req.destroy();
        this.logger?.warn?.(`[newsroom] POST ${apiPath} timed out.`);
        resolve(null);
      });

      req.write(payload);
      req.end();
    });
  }
}

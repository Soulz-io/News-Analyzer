import path from "node:path";
import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { createHttpHandler } from "./src/http-handler.js";
import { registerGatewayMethods } from "./src/gateway-methods.js";
import { setupControlUiPatch } from "./src/tab-injector.js";
import { EngineManager } from "./src/engine-manager.js";
import { Newsroom } from "./src/newsroom.js";

const plugin = {
  id: "openclaw-news-analyzer",
  name: "News Analyzer",
  description:
    "AI newsroom: detects narrative run-ups, builds game theory decision trees, predicts events from global news.",

  register(api: OpenClawPluginApi) {
    const pluginRoot = path.dirname(
      typeof __filename !== "undefined"
        ? __filename
        : new URL(import.meta.url).pathname,
    );
    const uiRoot = path.resolve(pluginRoot, "ui");
    const engineRoot = path.resolve(pluginRoot, "engine");

    // ── Plugin config ──────────────────────────────────────────
    const pluginCfg = (api.pluginConfig || {}) as Record<string, unknown>;
    const fetchIntervalMinutes = (pluginCfg.fetchIntervalMinutes as number) || 30;
    const scanIntervalHours = (pluginCfg.scanIntervalHours as number) || 2;
    const runupCheckIntervalHours = (pluginCfg.runupCheckIntervalHours as number) || 6;
    const probabilityShiftThreshold = (pluginCfg.probabilityShiftThreshold as number) || 0.10;
    const enginePort = (pluginCfg.enginePort as number) || 9120;
    const scannerModel = (pluginCfg.scannerModel as string) || "anthropic/claude-haiku-4-5-20251001";
    const analystModel = (pluginCfg.analystModel as string) || "anthropic/claude-sonnet-4-6";
    const pythonPath = (pluginCfg.pythonPath as string) || "python3";

    // ── Start the Python engine ────────────────────────────────
    const engineManager = new EngineManager({
      logger: api.logger,
      engineRoot,
      enginePort,
      pythonPath,
    });
    engineManager.start();

    // ── HTTP routes: dashboard UI + API proxy ──────────────────
    api.registerHttpRoute({
      path: "/plugins/openclaw-news-analyzer",
      auth: "plugin",
      match: "prefix",
      handler: createHttpHandler({
        logger: api.logger,
        uiRoot,
        enginePort,
      }),
    });

    // ── Gateway RPC methods for WebSocket consumers ────────────
    registerGatewayMethods(api, enginePort);

    // ── Patch Control UI to inject the News Analyzer tab ───────
    setupControlUiPatch({ logger: api.logger });

    // ── Start the Newsroom orchestrator ────────────────────────
    const newsroom = new Newsroom({
      logger: api.logger,
      enginePort,
      fetchIntervalMinutes,
      scanIntervalHours,
      runupCheckIntervalHours,
      probabilityShiftThreshold,
      scannerModel,
      analystModel,
      runtime: api.runtime,
    });
    newsroom.start();

    // ── Graceful shutdown ──────────────────────────────────────
    const shutdown = () => {
      newsroom.stop();
      engineManager.stop();
    };
    process.on("SIGTERM", shutdown);
    process.on("SIGINT", shutdown);

    api.logger.info("News Analyzer plugin registered");
  },
};

export default plugin;

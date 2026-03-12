import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";

interface PluginLogger {
  info: (msg: string) => void;
  warn: (msg: string) => void;
}

interface TabInjectorParams {
  logger?: PluginLogger;
  config?: Record<string, any>;
}

const INJECTOR_TAG =
  '<script src="/plugins/openclaw-news-analyzer/injector.js" defer></script>';

/**
 * Injects the News Analyzer injector script tag into the Control UI
 * index.html **on disk**.  This is safe with other plugins that use
 * the HTTP-handler interception pattern (e.g. openclaw-subagents):
 * when they read the HTML from disk they'll find our tag already
 * present, and they'll add their own tag alongside it.
 *
 * We never copy files or create a patched directory — we edit in-place
 * and only add our tag if it's missing.
 */
export function setupControlUiPatch(params: TabInjectorParams): void {
  const { logger, config } = params;

  try {
    const controlUiRoot = resolveControlUiRoot(config);
    if (!controlUiRoot) {
      logger?.warn?.(
        "[openclaw-news-analyzer] Could not locate control-ui; tab injection skipped.",
      );
      return;
    }

    const indexHtmlPath = path.join(controlUiRoot, "index.html");
    logger?.info?.(
      `[openclaw-news-analyzer] Control-ui root: ${controlUiRoot}`,
    );

    const html = fs.readFileSync(indexHtmlPath, "utf8");

    if (html.includes("openclaw-news-analyzer/injector.js")) {
      logger?.info?.(
        "[openclaw-news-analyzer] Injector tag already present in index.html.",
      );
      return;
    }

    // Insert our script tag before </body>
    const patched = html.replace(
      "</body>",
      `    ${INJECTOR_TAG}\n  </body>`,
    );

    fs.writeFileSync(indexHtmlPath, patched, "utf8");
    logger?.info?.(
      `[openclaw-news-analyzer] Injected tab script into ${indexHtmlPath}`,
    );
  } catch (err) {
    logger?.warn?.(
      `[openclaw-news-analyzer] Failed to patch control-ui: ${err}`,
    );
  }
}

/**
 * Resolve the Control UI root directory.  Tries (in order):
 *   1. gateway.controlUi.root from config
 *   2. require.resolve("openclaw") → dist/control-ui
 *   3. Known filesystem candidate paths (NVM, system, cwd)
 */
function resolveControlUiRoot(
  config?: Record<string, any>,
): string | null {
  // Strategy 1: Config root
  const configRoot = config?.gateway?.controlUi?.root;
  if (typeof configRoot === "string" && configRoot.trim()) {
    const resolved = path.resolve(configRoot.trim());
    const idx = path.join(resolved, "index.html");
    if (fs.existsSync(idx)) return resolved;
  }

  // Strategy 2: require.resolve
  try {
    const require_ = createRequire(import.meta.url);
    const openclawMain = require_.resolve("openclaw");
    const openclawDir = path.dirname(openclawMain);
    for (const rel of [
      "control-ui",
      "../control-ui",
      "dist/control-ui",
      "../dist/control-ui",
    ]) {
      const dir = path.join(openclawDir, rel);
      if (fs.existsSync(path.join(dir, "index.html"))) return dir;
    }
  } catch {
    /* require.resolve not available */
  }

  // Strategy 3: Known candidates
  const candidates: string[] = [];

  // NVM global
  if (process.env.NVM_BIN) {
    candidates.push(
      path.resolve(
        process.env.NVM_BIN,
        "../lib/node_modules/openclaw/dist/control-ui",
      ),
    );
  }

  // Home-based nvm (fallback)
  const home = process.env.HOME || "/root";
  try {
    const nvmDir = path.join(home, ".nvm/versions/node");
    if (fs.existsSync(nvmDir)) {
      for (const v of fs.readdirSync(nvmDir)) {
        candidates.push(
          path.join(
            nvmDir,
            v,
            "lib/node_modules/openclaw/dist/control-ui",
          ),
        );
      }
    }
  } catch {
    /* nvm not available */
  }

  // System-wide
  candidates.push("/usr/lib/node_modules/openclaw/dist/control-ui");

  // cwd workspace
  candidates.push(
    path.resolve(process.cwd(), "node_modules/openclaw/dist/control-ui"),
  );

  for (const dir of candidates) {
    try {
      if (fs.existsSync(path.join(dir, "index.html"))) return dir;
    } catch {
      /* skip */
    }
  }

  return null;
}

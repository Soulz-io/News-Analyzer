import fs from "node:fs";
import path from "node:path";

interface PluginLogger {
  info: (msg: string) => void;
  warn: (msg: string) => void;
}

/**
 * Patches the Control UI index.html to include the News Analyzer dashboard
 * injector script. Files are COPIED (never symlinked) because the gateway
 * rejects symlinks in custom controlUi.root directories.
 *
 * Safe with other plugins: only adds its own script tag if missing.
 */
export function setupControlUiPatch(params: { logger?: PluginLogger }): void {
  const { logger } = params;

  try {
    const originalDir = resolveOriginalControlUiDir();
    if (!originalDir) {
      logger?.warn?.(
        "Could not locate original control-ui; tab injection skipped.",
      );
      return;
    }

    logger?.info?.(`[openclaw-news-analyzer] Original control-ui: ${originalDir}`);

    const patchedDir = path.resolve(__dirname, "../control-ui-patched");
    fs.mkdirSync(patchedDir, { recursive: true });

    // Copy all files except index.html (we patch that separately)
    copyDirSync(originalDir, patchedDir, ["index.html"]);

    const injectorTag =
      '<script src="/plugins/openclaw-news-analyzer/injector.js" defer></script>';

    // Read from patched dir first (may already have other plugins' tags),
    // fall back to original
    const htmlSource = fs.existsSync(path.join(patchedDir, "index.html"))
      ? path.join(patchedDir, "index.html")
      : path.join(originalDir, "index.html");

    const currentHtml = fs.readFileSync(htmlSource, "utf8");

    if (currentHtml.includes(injectorTag)) {
      // Our tag already present -- just ensure the file is in the patched dir
      if (htmlSource !== path.join(patchedDir, "index.html")) {
        fs.writeFileSync(path.join(patchedDir, "index.html"), currentHtml);
      }
      logger?.info?.("[openclaw-news-analyzer] Injector tag already present.");
      return;
    }

    const patchedHtml = currentHtml.replace(
      "</body>",
      `    ${injectorTag}\n  </body>`,
    );
    fs.writeFileSync(path.join(patchedDir, "index.html"), patchedHtml);

    logger?.info?.(
      `[openclaw-news-analyzer] Patched control-ui at ${patchedDir}`,
    );
    logger?.info?.(
      `Set gateway.controlUi.root to "${patchedDir}" in openclaw.json.`,
    );
  } catch (err) {
    logger?.warn?.(`[openclaw-news-analyzer] Failed to patch control-ui: ${err}`);
  }
}

function copyDirSync(
  src: string,
  dest: string,
  exclude: string[] = [],
): void {
  fs.mkdirSync(dest, { recursive: true });
  for (const entry of fs.readdirSync(src)) {
    if (exclude.includes(entry)) continue;
    const srcPath = path.join(src, entry);
    const destPath = path.join(dest, entry);
    const stat = fs.statSync(srcPath);
    if (stat.isDirectory()) {
      copyDirSync(srcPath, destPath);
    } else {
      fs.copyFileSync(srcPath, destPath);
    }
  }
}

function resolveOriginalControlUiDir(): string | null {
  const candidates = [
    // System-wide install first (daemon always uses this)
    "/usr/lib/node_modules/openclaw/dist/control-ui",
    // nvm global
    ...(process.env.NVM_BIN
      ? [
          path.resolve(
            process.env.NVM_BIN,
            "../lib/node_modules/openclaw/dist/control-ui",
          ),
        ]
      : []),
    // workspace-level
    path.resolve(process.cwd(), "node_modules/openclaw/dist/control-ui"),
    // relative to plugin
    path.resolve(__dirname, "../../../../node_modules/openclaw/dist/control-ui"),
  ];

  for (const dir of candidates) {
    try {
      if (fs.existsSync(path.join(dir, "index.html"))) return dir;
    } catch {
      /* skip */
    }
  }
  return null;
}

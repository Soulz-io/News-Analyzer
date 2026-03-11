import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import type { IncomingMessage, ServerResponse } from "node:http";

interface PluginLogger {
  info: (msg: string) => void;
  warn: (msg: string) => void;
}

interface HttpHandlerParams {
  logger?: PluginLogger;
  uiRoot: string;
  enginePort: number;
}

const PREFIX = "/plugins/openclaw-news-analyzer";
const API_PREFIX = `${PREFIX}/api/`;

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
};

/**
 * Serves the News Analyzer dashboard UI and proxies API requests
 * to the Python engine.
 *
 * API routes:
 *   /api/*  -- proxied to http://localhost:{enginePort}/api/*
 *
 * Static routes:
 *   /injector.js  -- the tab injector script
 *   /*            -- static UI files with SPA fallback
 */
export function createHttpHandler(params: HttpHandlerParams) {
  const { logger, uiRoot, enginePort } = params;

  return async function handler(
    req: IncomingMessage,
    res: ServerResponse,
  ): Promise<boolean> {
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
    const pathname = url.pathname;

    // ── API proxy: forward to Python engine ──────────────────
    if (pathname.startsWith(API_PREFIX)) {
      const enginePath = "/api/" + pathname.slice(API_PREFIX.length);
      const engineUrl = `http://127.0.0.1:${enginePort}${enginePath}${url.search}`;
      return proxyToEngine(req, res, engineUrl, logger);
    }

    // ── Injector script (served from ui/) ─────────────────────
    if (pathname === `${PREFIX}/injector.js`) {
      return serveFile(path.join(uiRoot, "injector.js"), ".js", res);
    }

    // ── Static UI files ───────────────────────────────────────
    let relPath = pathname.slice(PREFIX.length) || "/";
    if (relPath === "/") relPath = "/index.html";

    // Prevent path traversal
    const resolved = path.resolve(uiRoot, relPath.slice(1));
    if (!resolved.startsWith(path.resolve(uiRoot))) {
      res.statusCode = 403;
      res.end("Forbidden");
      return true;
    }

    const ext = path.extname(resolved);
    if (fs.existsSync(resolved) && fs.statSync(resolved).isFile()) {
      return serveFile(resolved, ext, res);
    }

    // SPA fallback
    return serveFile(path.join(uiRoot, "index.html"), ".html", res);
  };
}

/**
 * Proxies an incoming request to the Python engine and pipes the response back.
 */
function proxyToEngine(
  req: IncomingMessage,
  res: ServerResponse,
  engineUrl: string,
  logger?: PluginLogger,
): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const parsed = new URL(engineUrl);

    const proxyOpts: http.RequestOptions = {
      hostname: parsed.hostname,
      port: parsed.port,
      path: parsed.pathname + parsed.search,
      method: req.method || "GET",
      headers: {
        ...req.headers,
        host: `${parsed.hostname}:${parsed.port}`,
      },
    };

    const proxyReq = http.request(proxyOpts, (proxyRes) => {
      res.statusCode = proxyRes.statusCode || 502;
      // Forward response headers
      const proxyHeaders = proxyRes.headers;
      for (const [key, value] of Object.entries(proxyHeaders)) {
        if (value !== undefined) {
          res.setHeader(key, value);
        }
      }
      proxyRes.pipe(res);
      proxyRes.on("end", () => resolve(true));
    });

    proxyReq.on("error", (err) => {
      logger?.warn?.(`[openclaw-news-analyzer] Engine proxy error: ${err.message}`);
      res.statusCode = 502;
      res.setHeader("Content-Type", "application/json; charset=utf-8");
      res.end(JSON.stringify({
        error: "Engine unavailable",
        detail: err.message,
      }));
      resolve(true);
    });

    // Pipe the request body to the engine
    req.pipe(proxyReq);
  });
}

function serveFile(
  filePath: string,
  ext: string,
  res: ServerResponse,
): boolean {
  try {
    const content = fs.readFileSync(filePath);
    res.statusCode = 200;
    res.setHeader(
      "Content-Type",
      MIME[ext] || "application/octet-stream",
    );
    res.end(content);
    return true;
  } catch {
    res.statusCode = 404;
    res.end("Not found");
    return true;
  }
}

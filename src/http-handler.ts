import fs from "node:fs";
import path from "node:path";
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

/**
 * Gateway only supports exact-path matching for plugin routes.
 * This handler serves on the EXACT path `/plugins/openclaw-news-analyzer`:
 *   - No query params → self-contained HTML bundle (inline JS+CSS)
 *   - ?_api=overview   → proxy GET /api/dashboard/overview
 *   - ?_api=status     → proxy GET /api/status
 *   - ?_api=tree&id=N  → proxy GET /api/dashboard/tree/{N}
 *   - ?_api=feeds      → proxy GET/POST /api/feeds
 *   - ?_api=feeds&id=N → proxy DELETE /api/feeds/{N}
 *   - ?_api=budget     → proxy GET/PUT /api/budget
 *   - ?_api=apikey     → proxy GET/PUT /api/settings/api-key
 */
export function createHttpHandler(params: HttpHandlerParams) {
  const { logger, uiRoot, enginePort } = params;
  const ENGINE_BASE = `http://127.0.0.1:${enginePort}`;

  let bundledHtml: string | null = null;
  let lastBundleTime = 0;

  function buildBundle(): string {
    const cssPath = path.join(uiRoot, "app.css");
    const jsPath = path.join(uiRoot, "app.js");

    const css = fs.existsSync(cssPath) ? fs.readFileSync(cssPath, "utf8") : "";
    let js = fs.existsSync(jsPath) ? fs.readFileSync(jsPath, "utf8") : "";

    // Rewrite API_BASE to use query-parameter dispatch on the same URL
    js = js.replace(
      /const API_BASE\s*=\s*["'][^"']*["']/,
      `const API_BASE = "${PREFIX}"`,
    );

    return `<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>News Analyzer</title>
  <style>${css}</style>
  <script src="https://unpkg.com/lightweight-charts@4/dist/lightweight-charts.standalone.production.js"></script>
  <script src="https://unpkg.com/dagre@0.8/dist/dagre.min.js"></script>
  <script src="https://unpkg.com/cytoscape@3/dist/cytoscape.min.js"></script>
  <script src="https://unpkg.com/cytoscape-dagre@2/cytoscape-dagre.js"></script>
</head>
<body>
  <div id="app"></div>
  <script type="module">${js}</script>
</body>
</html>`;
  }

  async function proxyToEngine(
    enginePath: string,
    req: IncomingMessage,
    res: ServerResponse,
  ): Promise<boolean> {
    try {
      const url = `${ENGINE_BASE}${enginePath}`;
      const headers: Record<string, string> = { "Content-Type": "application/json" };
      const method = req.method || "GET";

      const fetchOpts: RequestInit = { method, headers };

      if (method !== "GET" && method !== "HEAD") {
        const chunks: Buffer[] = [];
        for await (const chunk of req) chunks.push(chunk as Buffer);
        const body = Buffer.concat(chunks).toString("utf-8");
        if (body) fetchOpts.body = body;
      }

      const engineRes = await fetch(url, fetchOpts);
      const data = await engineRes.text();

      res.writeHead(engineRes.status, {
        "Content-Type": engineRes.headers.get("content-type") || "application/json; charset=utf-8",
        "Cache-Control": "no-store",
      });
      res.end(data);
    } catch (err) {
      logger?.warn?.(`[openclaw-news-analyzer] Proxy error: ${err}`);
      res.writeHead(502, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: `Engine proxy error: ${err}` }));
    }
    return true;
  }

  return async function handler(
    req: IncomingMessage,
    res: ServerResponse,
  ): Promise<boolean> {
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
    const pathname = url.pathname;

    if (pathname !== PREFIX) return false;

    // ── API dispatch via _api query param ────────────────────
    const apiAction = url.searchParams.get("_api");

    if (apiAction === "overview" && req.method === "GET") {
      return proxyToEngine("/api/dashboard/overview", req, res);
    }
    if (apiAction === "status" && req.method === "GET") {
      return proxyToEngine("/api/status", req, res);
    }
    if (apiAction === "tree" && req.method === "GET") {
      const id = url.searchParams.get("id") || "";
      return proxyToEngine(`/api/dashboard/tree/${encodeURIComponent(id)}`, req, res);
    }
    if (apiAction === "feeds") {
      if (req.method === "GET") return proxyToEngine("/api/feeds", req, res);
      if (req.method === "POST") return proxyToEngine("/api/feeds", req, res);
      if (req.method === "DELETE") {
        const id = url.searchParams.get("id") || "";
        return proxyToEngine(`/api/feeds/${encodeURIComponent(id)}`, req, res);
      }
    }
    if (apiAction === "budget") {
      if (req.method === "GET") return proxyToEngine("/api/budget", req, res);
      if (req.method === "PUT") return proxyToEngine("/api/budget", req, res);
    }
    if (apiAction === "apikey") {
      if (req.method === "GET") return proxyToEngine("/api/settings/api-key", req, res);
      if (req.method === "PUT") return proxyToEngine("/api/settings/api-key", req, res);
    }
    if (apiAction === "polymarket" && req.method === "GET") {
      const id = url.searchParams.get("id") || "";
      return proxyToEngine(`/api/polymarket/${encodeURIComponent(id)}`, req, res);
    }
    if (apiAction === "polymarket-refresh" && req.method === "POST") {
      return proxyToEngine("/api/polymarket/refresh", req, res);
    }
    if (apiAction === "analysis" && req.method === "GET") {
      return proxyToEngine("/api/analysis/latest", req, res);
    }
    if (apiAction === "analysis-run" && req.method === "POST") {
      return proxyToEngine("/api/analysis/run", req, res);
    }
    if (apiAction === "prediction-score" && req.method === "POST") {
      return proxyToEngine("/api/predictions/score", req, res);
    }
    if (apiAction === "signals" && req.method === "GET") {
      return proxyToEngine("/api/signals", req, res);
    }
    if (apiAction === "signals-history" && req.method === "GET") {
      return proxyToEngine("/api/signals/history", req, res);
    }
    if (apiAction === "signals-refresh" && req.method === "POST") {
      return proxyToEngine("/api/signals/refresh", req, res);
    }
    if (apiAction === "price" && req.method === "GET") {
      const ticker = url.searchParams.get("ticker") || "";
      return proxyToEngine(`/api/price/${encodeURIComponent(ticker)}`, req, res);
    }
    if (apiAction === "price-chart" && req.method === "GET") {
      const ticker = url.searchParams.get("ticker") || "";
      const period = url.searchParams.get("period") || "3mo";
      return proxyToEngine(`/api/price/${encodeURIComponent(ticker)}/chart?period=${encodeURIComponent(period)}`, req, res);
    }
    if (apiAction === "indicators" && req.method === "GET") {
      return proxyToEngine("/api/indicators", req, res);
    }

    // ── HTML bundle ──────────────────────────────────────────
    const now = Date.now();
    if (!bundledHtml || now - lastBundleTime > 5000) {
      try {
        bundledHtml = buildBundle();
        lastBundleTime = now;
      } catch (err) {
        logger?.warn?.(`[openclaw-news-analyzer] Bundle build error: ${err}`);
        res.statusCode = 500;
        res.end("Failed to build UI bundle");
        return true;
      }
    }

    res.statusCode = 200;
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.end(bundledHtml);
    return true;
  };
}

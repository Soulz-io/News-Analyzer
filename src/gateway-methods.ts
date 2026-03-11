import http from "node:http";
import type { OpenClawPluginApi } from "openclaw/plugin-sdk";

/**
 * Registers gateway RPC methods so WebSocket clients (CLI, other plugins)
 * can query news analysis data directly.
 *
 * Each method proxies to the Python engine API running on localhost.
 *
 * Methods:
 *   news.pulse   — latest pulse: article counts, top topics, sentiment
 *   news.topics  — active narrative topics with clustering
 *   news.runups  — detected run-ups and their statuses
 *   news.tree    — decision tree for a specific run-up
 *   news.status  — engine health and processing status
 */
export function registerGatewayMethods(
  api: OpenClawPluginApi,
  enginePort: number,
): void {
  api.registerGatewayMethod("news.pulse", (opts: any) => {
    proxyToEngine(enginePort, "/api/pulse", opts, api);
  });

  api.registerGatewayMethod("news.topics", (opts: any) => {
    proxyToEngine(enginePort, "/api/topics", opts, api);
  });

  api.registerGatewayMethod("news.runups", (opts: any) => {
    proxyToEngine(enginePort, "/api/runups", opts, api);
  });

  api.registerGatewayMethod("news.tree", (opts: any) => {
    const runupId =
      opts.params?.runup_id || opts.params?.runupId || opts.params?.id;
    if (!runupId) {
      opts.respond(false, undefined, {
        code: "MISSING_PARAM",
        message: "Required parameter: runup_id",
      });
      return;
    }
    proxyToEngine(
      enginePort,
      `/api/runups/${encodeURIComponent(runupId)}/decision-tree`,
      opts,
      api,
    );
  });

  api.registerGatewayMethod("news.status", (opts: any) => {
    proxyToEngine(enginePort, "/api/status", opts, api);
  });
}

/**
 * Makes a GET request to the Python engine and responds via the gateway callback.
 */
function proxyToEngine(
  enginePort: number,
  apiPath: string,
  opts: any,
  api: OpenClawPluginApi,
): void {
  const url = `http://127.0.0.1:${enginePort}${apiPath}`;

  const req = http.get(url, { timeout: 15_000 }, (res) => {
    const chunks: Buffer[] = [];
    res.on("data", (chunk: Buffer) => chunks.push(chunk));
    res.on("end", () => {
      if (res.statusCode !== 200) {
        opts.respond(false, undefined, {
          code: "ENGINE_ERROR",
          message: `Engine returned status ${res.statusCode}`,
        });
        return;
      }

      try {
        const body = Buffer.concat(chunks).toString("utf-8");
        const data = JSON.parse(body);
        opts.respond(true, data);
      } catch (err) {
        opts.respond(false, undefined, {
          code: "PARSE_ERROR",
          message: `Failed to parse engine response: ${err}`,
        });
      }
    });
  });

  req.on("error", (err) => {
    api.logger?.warn?.(
      `[openclaw-news-analyzer] Gateway proxy error for ${apiPath}: ${err.message}`,
    );
    opts.respond(false, undefined, {
      code: "ENGINE_UNAVAILABLE",
      message: `Engine unavailable: ${err.message}`,
    });
  });

  req.on("timeout", () => {
    req.destroy();
    opts.respond(false, undefined, {
      code: "ENGINE_TIMEOUT",
      message: "Engine request timed out",
    });
  });
}

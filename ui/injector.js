/**
 * News Analyzer Dashboard — Tab Injector for OpenClaw Control UI
 *
 * Injects a "News Analyzer" tab into the Control UI sidebar (Shadow DOM).
 * Detects if a "Monitoring" nav-group already exists (e.g. from the subagents
 * plugin) and appends to it rather than creating a duplicate group.
 *
 * Uses MutationObserver on childList only (NO attributes) to avoid
 * infinite mutation loops that freeze the page.
 */
(function () {
  "use strict";

  const PLUGIN_URL = "/plugins/openclaw-news-analyzer/";
  const TAB_HASH = "#/news-analyzer";
  const INJECT_ATTR = "data-news-analyzer-dash";

  /* Newspaper / chart icon */
  const ICON_SVG = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 22h16a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2H8a2 2 0 0 0-2 2v16a2 2 0 0 1-2 2Zm0 0a2 2 0 0 1-2-2v-9c0-1.1.9-2 2-2h2"/><path d="M18 14h-8"/><path d="M15 18h-5"/><path d="M10 6h8v4h-8z"/></svg>`;

  let active = false;
  let iframeBox = null;
  let mutationPending = false;
  let _root = null;

  /* ── Helpers ────────────────────────────────────────────────── */

  function getRoot(app) {
    return app.shadowRoot || app;
  }

  function waitForApp(cb) {
    let n = 0;
    const poll = () => {
      n++;
      const app = document.querySelector("openclaw-app");
      if (!app) { if (n < 200) setTimeout(poll, 50); return; }
      const root = getRoot(app);
      const nav = root.querySelector("aside.nav, aside, .nav");
      if (nav) cb(app, root, nav);
      else if (n < 200) setTimeout(poll, 50);
    };
    poll();
  }

  /* ── Tab injection ─────────────────────────────────────────── */

  function injectTab(nav) {
    // Already injected?
    if (nav.querySelector(`[${INJECT_ATTR}]`)) return;

    // Check if a "Monitoring" nav-group already exists (e.g. from subagents plugin)
    let monitoringGroup = null;
    const existingGroups = nav.querySelectorAll(".nav-group");
    for (const grp of existingGroups) {
      const labelText = grp.querySelector(".nav-label__text");
      if (labelText && labelText.textContent.trim() === "Monitoring") {
        monitoringGroup = grp;
        break;
      }
    }

    if (monitoringGroup) {
      // Append our tab into the existing Monitoring group
      const itemsContainer = monitoringGroup.querySelector(".nav-group__items");
      if (itemsContainer) {
        const link = document.createElement("a");
        link.href = TAB_HASH;
        link.className = "nav-item";
        link.title = "News Analyzer Dashboard";
        link.setAttribute("data-news-analyzer-tab", "");
        link.setAttribute(INJECT_ATTR, "");
        link.innerHTML = `
          <span class="nav-item__icon" aria-hidden="true">${ICON_SVG}</span>
          <span class="nav-item__text">News Analyzer</span>`;
        itemsContainer.appendChild(link);

        link.addEventListener("click", (e) => {
          e.preventDefault();
          e.stopPropagation();
          activate();
        });
      }
    } else {
      // Create a new Monitoring nav-group
      const group = document.createElement("div");
      group.className = "nav-group";
      group.setAttribute(INJECT_ATTR, "");
      group.innerHTML = `
        <button class="nav-label" aria-expanded="true">
          <span class="nav-label__text">Monitoring</span>
          <span class="nav-label__chevron">\u2212</span>
        </button>
        <div class="nav-group__items">
          <a href="${TAB_HASH}" class="nav-item" title="News Analyzer Dashboard"
             data-news-analyzer-tab ${INJECT_ATTR}>
            <span class="nav-item__icon" aria-hidden="true">${ICON_SVG}</span>
            <span class="nav-item__text">News Analyzer</span>
          </a>
        </div>`;

      const links = nav.querySelector(".nav-group--links");
      if (links) nav.insertBefore(group, links);
      else nav.appendChild(group);

      // Collapse toggle
      const label = group.querySelector(".nav-label");
      const chevron = group.querySelector(".nav-label__chevron");
      const items = group.querySelector(".nav-group__items");
      label.addEventListener("click", (e) => {
        e.stopPropagation();
        const collapsed = items.style.display === "none";
        items.style.display = collapsed ? "" : "none";
        chevron.textContent = collapsed ? "\u2212" : "+";
      });

      // Tab click
      group.querySelector("[data-news-analyzer-tab]").addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        activate();
      });
    }
  }

  /* ── Iframe management ─────────────────────────────────────── */

  function ensureIframe() {
    if (iframeBox || !_root) return;
    const main = _root.querySelector("main.content, main, .content");
    if (!main) return;

    iframeBox = document.createElement("div");
    iframeBox.setAttribute(INJECT_ATTR, "iframe");
    iframeBox.style.cssText =
      "display:none;position:absolute;inset:0;z-index:50;background:var(--bg,#12141a);";

    const iframe = document.createElement("iframe");
    iframe.src = PLUGIN_URL;
    iframe.style.cssText = "width:100%;height:100%;border:none;background:var(--bg,#12141a);";
    iframe.setAttribute("allow", "clipboard-write");
    iframe.setAttribute("title", "News Analyzer Dashboard");
    iframeBox.appendChild(iframe);

    if (window.getComputedStyle(main).position === "static") {
      main.style.position = "relative";
    }
    main.appendChild(iframeBox);
  }

  /* ── Activate / Deactivate ─────────────────────────────────── */

  function activate() {
    if (active || !_root) return;
    active = true;
    ensureIframe();

    const main = _root.querySelector("main.content, main, .content");
    if (main) {
      for (const ch of main.children) {
        if (ch.getAttribute(INJECT_ATTR) === "iframe") ch.style.display = "block";
        else { ch.dataset._naPrev = ch.style.display; ch.style.display = "none"; }
      }
    }

    const nav = _root.querySelector("aside.nav, aside, .nav");
    if (nav) {
      nav.querySelectorAll(".nav-item").forEach((el) => {
        if (el.hasAttribute(INJECT_ATTR)) el.classList.add("active");
        else el.classList.remove("active");
      });
    }
    history.pushState(null, "", TAB_HASH);
  }

  function deactivate() {
    if (!active || !_root) return;
    active = false;
    if (iframeBox) iframeBox.style.display = "none";

    const main = _root.querySelector("main.content, main, .content");
    if (main) {
      for (const ch of main.children) {
        if (ch.getAttribute(INJECT_ATTR) !== "iframe" && ch.dataset._naPrev !== undefined) {
          ch.style.display = ch.dataset._naPrev;
          delete ch.dataset._naPrev;
        }
      }
    }
    const tab = _root.querySelector("[data-news-analyzer-tab]");
    if (tab) tab.classList.remove("active");
  }

  /* ── Bootstrap ─────────────────────────────────────────────── */

  waitForApp(function (app, root, nav) {
    _root = root;
    injectTab(nav);

    // Observe nav only, childList only — no attributes to avoid infinite loops
    const observer = new MutationObserver(() => {
      if (mutationPending) return;
      mutationPending = true;
      requestAnimationFrame(() => {
        mutationPending = false;
        const cur = root.querySelector("aside.nav, aside, .nav");
        if (!cur) return;
        if (!cur.querySelector(`[${INJECT_ATTR}]`)) injectTab(cur);
        if (active) {
          const other = cur.querySelector(".nav-item.active:not([data-news-analyzer-tab])");
          if (other) deactivate();
        }
      });
    });
    observer.observe(nav, { childList: true, subtree: true });

    // Watch for nav replacement by Lit
    const navParent = nav.parentElement;
    if (navParent) {
      new MutationObserver(() => {
        const newNav = root.querySelector("aside.nav, aside, .nav");
        if (newNav && !newNav.querySelector(`[${INJECT_ATTR}]`)) {
          injectTab(newNav);
          observer.disconnect();
          observer.observe(newNav, { childList: true, subtree: true });
        }
      }).observe(navParent, { childList: true });
    }

    if (typeof app.setTab === "function") {
      const orig = app.setTab.bind(app);
      app.setTab = function (t) { deactivate(); return orig(t); };
    }

    window.addEventListener("popstate", () => {
      if (location.hash === TAB_HASH) activate();
      else if (active) deactivate();
    });

    if (location.hash === TAB_HASH) setTimeout(activate, 150);
  });
})();

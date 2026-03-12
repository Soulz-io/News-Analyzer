/**
 * News Analyzer Dashboard — Tab Injector for OpenClaw Control UI
 *
 * Injects a "News Analyzer" tab into the "Apps" nav-group in the
 * Control UI sidebar (Shadow DOM). Uses the shared coordinator
 * (window.__ocPluginTabs__) for cross-plugin tab coordination.
 */
(function () {
  "use strict";

  const PLUGIN_ID = "news-analyzer";
  const PLUGIN_URL = "/plugins/openclaw-news-analyzer";
  const TAB_HASH = "#/news-analyzer";
  const INJECT_ATTR = "data-news-analyzer-dash";
  const TAB_ATTR = "data-news-analyzer-tab";

  /* Newspaper / chart icon */
  const ICON_SVG = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 22h16a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2H8a2 2 0 0 0-2 2v16a2 2 0 0 1-2 2Zm0 0a2 2 0 0 1-2-2v-9c0-1.1.9-2 2-2h2"/><path d="M18 14h-8"/><path d="M15 18h-5"/><path d="M10 6h8v4h-8z"/></svg>`;

  let iframeBox = null;
  let _root = null;
  let mutationPending = false;

  /* ── Shared coordinator ────────────────────────────────────── */

  function ensureCoordinator(root) {
    if (window.__ocPluginTabs__ && window.__ocPluginTabs__._v === 1) {
      if (root && !window.__ocPluginTabs__._root) {
        window.__ocPluginTabs__._root = root;
      }
      return window.__ocPluginTabs__;
    }

    const coord = {
      _v: 1,
      _plugins: {},
      _activeId: null,
      _setTabWrapped: false,
      _popstateAttached: false,
      _root: root || null,

      register(id, opts) {
        this._plugins[id] = opts;
      },

      activate(id) {
        if (this._activeId === id) return;
        if (!this._plugins[id]) return;
        if (this._activeId && this._plugins[this._activeId]) {
          this._plugins[this._activeId].deactivate();
        }
        this._restoreMainContent();
        this._hideMainContent(this._plugins[id].injectAttr);
        this._activeId = id;
        this._plugins[id].activate();
        const hash = this._plugins[id].hash;
        if (location.hash !== hash) history.pushState(null, "", hash);
      },

      deactivateAll() {
        if (!this._activeId) return;
        if (this._plugins[this._activeId]) {
          this._plugins[this._activeId].deactivate();
        }
        this._activeId = null;
        this._restoreMainContent();
      },

      getActive() { return this._activeId; },

      _hideMainContent(injectAttr) {
        const r = this._root; if (!r) return;
        const main = r.querySelector("main.content, main, .content");
        if (!main) return;
        for (const ch of main.children) {
          if (ch.hasAttribute("data-oc-plugin-iframe")) {
            ch.style.display = ch.getAttribute(injectAttr) === "iframe" ? "block" : "none";
          } else {
            if (ch.dataset._ocPrev === undefined) ch.dataset._ocPrev = ch.style.display;
            ch.style.display = "none";
          }
        }
      },

      _restoreMainContent() {
        const r = this._root; if (!r) return;
        const main = r.querySelector("main.content, main, .content");
        if (!main) return;
        for (const ch of main.children) {
          if (ch.hasAttribute("data-oc-plugin-iframe")) {
            ch.style.display = "none";
          } else if (ch.dataset._ocPrev !== undefined) {
            ch.style.display = ch.dataset._ocPrev;
            delete ch.dataset._ocPrev;
          }
        }
      },

      _wrapSetTab(app) {
        if (this._setTabWrapped) return;
        if (typeof app.setTab !== "function") return;
        this._setTabWrapped = true;
        const c = this;
        const orig = app.setTab.bind(app);
        app.setTab = function (t) { c.deactivateAll(); return orig(t); };
      },

      _attachPopstate() {
        if (this._popstateAttached) return;
        this._popstateAttached = true;
        const c = this;
        window.addEventListener("popstate", () => {
          const hash = location.hash;
          let matched = false;
          for (const [id, opts] of Object.entries(c._plugins)) {
            if (opts.hash === hash) { c.activate(id); matched = true; break; }
          }
          if (!matched && c._activeId) c.deactivateAll();
        });
      },
    };

    window.__ocPluginTabs__ = coord;
    return coord;
  }

  /* ── Helpers ────────────────────────────────────────────────── */

  function getRoot(app) { return app.shadowRoot || app; }

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

  function injectTab(nav, coord) {
    if (nav.querySelector(`[${INJECT_ATTR}]`)) return;

    // Look for existing "Apps" nav-group
    let appsGroup = null;
    for (const grp of nav.querySelectorAll(".nav-group")) {
      const lt = grp.querySelector(".nav-label__text");
      if (lt && lt.textContent.trim() === "Apps") { appsGroup = grp; break; }
    }

    if (appsGroup) {
      const items = appsGroup.querySelector(".nav-group__items");
      if (items) {
        const link = document.createElement("a");
        link.href = TAB_HASH;
        link.className = "nav-item";
        link.title = "News Analyzer Dashboard";
        link.setAttribute(TAB_ATTR, "");
        link.setAttribute(INJECT_ATTR, "");
        link.innerHTML = `
          <span class="nav-item__icon" aria-hidden="true">${ICON_SVG}</span>
          <span class="nav-item__text">News Analyzer</span>`;
        items.appendChild(link);
        link.addEventListener("click", (e) => {
          e.preventDefault(); e.stopPropagation();
          coord.activate(PLUGIN_ID);
        });
      }
    } else {
      // Fallback: create "Apps" nav-group
      const group = document.createElement("div");
      group.className = "nav-group";
      group.setAttribute(INJECT_ATTR, "");
      group.innerHTML = `
        <button class="nav-label" aria-expanded="true">
          <span class="nav-label__text">Apps</span>
          <span class="nav-label__chevron">\u2212</span>
        </button>
        <div class="nav-group__items">
          <a href="${TAB_HASH}" class="nav-item" title="News Analyzer Dashboard"
             ${TAB_ATTR} ${INJECT_ATTR}>
            <span class="nav-item__icon" aria-hidden="true">${ICON_SVG}</span>
            <span class="nav-item__text">News Analyzer</span>
          </a>
        </div>`;

      const links = nav.querySelector(".nav-group--links");
      if (links) nav.insertBefore(group, links);
      else nav.appendChild(group);

      const label = group.querySelector(".nav-label");
      const chevron = group.querySelector(".nav-label__chevron");
      const items = group.querySelector(".nav-group__items");
      label.addEventListener("click", (e) => {
        e.stopPropagation();
        const collapsed = items.style.display === "none";
        items.style.display = collapsed ? "" : "none";
        chevron.textContent = collapsed ? "\u2212" : "+";
      });

      group.querySelector(`[${TAB_ATTR}]`).addEventListener("click", (e) => {
        e.preventDefault(); e.stopPropagation();
        coord.activate(PLUGIN_ID);
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
    iframeBox.setAttribute("data-oc-plugin-iframe", "");
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

  /* ── Plugin-local activate / deactivate ────────────────────── */

  function activateSelf() {
    ensureIframe();
    if (iframeBox) iframeBox.style.display = "block";
    const nav = _root && _root.querySelector("aside.nav, aside, .nav");
    if (nav) {
      nav.querySelectorAll(".nav-item").forEach((el) => {
        el.classList.toggle("active", el.hasAttribute(TAB_ATTR));
      });
    }
  }

  function deactivateSelf() {
    if (iframeBox) iframeBox.style.display = "none";
    const tab = _root && _root.querySelector(`[${TAB_ATTR}]`);
    if (tab) tab.classList.remove("active");
  }

  /* ── Bootstrap ─────────────────────────────────────────────── */

  waitForApp(function (app, root, nav) {
    _root = root;
    const coord = ensureCoordinator(root);

    coord.register(PLUGIN_ID, {
      activate: activateSelf,
      deactivate: deactivateSelf,
      hash: TAB_HASH,
      injectAttr: INJECT_ATTR,
    });

    injectTab(nav, coord);

    const observer = new MutationObserver(() => {
      if (mutationPending) return;
      mutationPending = true;
      requestAnimationFrame(() => {
        mutationPending = false;
        const cur = root.querySelector("aside.nav, aside, .nav");
        if (cur && !cur.querySelector(`[${INJECT_ATTR}]`)) injectTab(cur, coord);
      });
    });
    observer.observe(nav, { childList: true, subtree: true });

    const navParent = nav.parentElement;
    if (navParent) {
      new MutationObserver(() => {
        const newNav = root.querySelector("aside.nav, aside, .nav");
        if (newNav && !newNav.querySelector(`[${INJECT_ATTR}]`)) {
          injectTab(newNav, coord);
          observer.disconnect();
          observer.observe(newNav, { childList: true, subtree: true });
        }
      }).observe(navParent, { childList: true });
    }

    coord._wrapSetTab(app);
    coord._attachPopstate();

    if (location.hash === TAB_HASH) setTimeout(() => coord.activate(PLUGIN_ID), 150);
  });
})();

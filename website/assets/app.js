/* 官网的全部行为：语言切换、导航折叠、滚动进场、首屏数字滚动。
 *
 * 四个都刻意用最朴素的方式写，因为这个站的硬约束是零构建、零依赖、
 * file:// 下也要能用。没有框架，没有模块（ES module 在 file:// 下会被 CORS 拦）。
 */
(function () {
  "use strict";

  var I18N = window.RUSTORAGE_I18N || { zh: {}, en: {} };
  var LANGS = ["zh", "en"];
  var STORE_KEY = "rustorage-lang";
  var html = document.documentElement;

  // 进场动效的初始隐藏态是 `html.js [data-reveal]`。这个类必须在首帧之前打上，
  // 否则会出现「先显示、再隐藏、再淡入」的闪烁。脚本在 head 里同步执行就是为了这个。
  html.classList.add("js");

  var reduceMotion = false;
  try {
    reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  } catch (e) { /* 老浏览器没有 matchMedia，按「不减少」处理 */ }

  var current = "zh";

  /* ── 语言 ───────────────────────────────────────────────── */

  function storedLang() {
    try {
      return window.localStorage.getItem(STORE_KEY);
    } catch (e) {
      // 隐私模式下 localStorage 可能直接抛，那就当作没有偏好
      return null;
    }
  }

  function rememberLang(lang) {
    try {
      window.localStorage.setItem(STORE_KEY, lang);
    } catch (e) { /* 记不住就记不住，不影响这次切换 */ }
  }

  function detectLang() {
    var saved = storedLang();
    if (saved && LANGS.indexOf(saved) >= 0) return saved;
    // zh-CN / zh-Hans / zh 都算中文，其余一律英文
    return /^zh\b/i.test(navigator.language || "") ? "zh" : "en";
  }

  function setMeta(tree) {
    var pairs = [
      ['meta[name="description"]', "meta.description"],
      ['meta[property="og:title"]', "meta.ogTitle"],
      ['meta[property="og:description"]', "meta.ogDescription"]
    ];
    for (var i = 0; i < pairs.length; i++) {
      var el = document.querySelector(pairs[i][0]);
      var val = tree[pairs[i][1]];
      if (el && val) el.setAttribute("content", val);
    }
  }

  function applyLang(lang) {
    var tree = I18N[lang];
    if (!tree) return;

    var nodes = document.querySelectorAll("[data-i18n]");
    for (var i = 0; i < nodes.length; i++) {
      var key = nodes[i].getAttribute("data-i18n");
      var val = tree[key];
      if (val === undefined) {
        // 缺键不静默：控制台点出来，website/check.sh 会在提交前拦住
        console.warn("[i18n] 缺键: " + key + "（语言 " + lang + "）");
        continue;
      }
      nodes[i].textContent = val;
    }

    html.lang = lang === "zh" ? "zh-CN" : "en";
    if (tree["meta.title"]) document.title = tree["meta.title"];
    setMeta(tree);

    var btns = document.querySelectorAll(".lang-btn");
    for (var j = 0; j < btns.length; j++) {
      var on = btns[j].getAttribute("data-lang") === lang;
      btns[j].setAttribute("aria-pressed", on ? "true" : "false");
      btns[j].classList.toggle("is-on", on);
    }

    current = lang;
  }

  function setupLang() {
    var btns = document.querySelectorAll(".lang-btn");
    for (var i = 0; i < btns.length; i++) {
      btns[i].addEventListener("click", function () {
        var lang = this.getAttribute("data-lang");
        if (LANGS.indexOf(lang) < 0 || lang === current) return;
        rememberLang(lang);
        applyLang(lang);
      });
    }
  }

  /* ── 导航折叠 ────────────────────────────────────────────── */

  function setupNav() {
    var btn = document.getElementById("navToggle");
    var nav = document.getElementById("siteNav");
    if (!btn || !nav) return;

    btn.addEventListener("click", function () {
      var open = btn.getAttribute("aria-expanded") === "true";
      btn.setAttribute("aria-expanded", open ? "false" : "true");
      nav.classList.toggle("is-open", !open);
    });

    nav.addEventListener("click", function (e) {
      if (e.target.tagName !== "A") return;
      btn.setAttribute("aria-expanded", "false");
      nav.classList.remove("is-open");
    });
  }

  /* ── 滚动进场 ────────────────────────────────────────────── */

  function revealAll() {
    var els = document.querySelectorAll("[data-reveal]");
    for (var i = 0; i < els.length; i++) els[i].classList.add("is-in");
  }

  function setupReveal() {
    // 关掉动效、或浏览器没有 IntersectionObserver：直接给终态，
    // 绝不让内容因为「动画没跑」而一直藏着。
    if (reduceMotion || !("IntersectionObserver" in window)) {
      revealAll();
      return;
    }
    var io = new IntersectionObserver(function (entries) {
      for (var i = 0; i < entries.length; i++) {
        if (!entries[i].isIntersecting) continue;
        entries[i].target.classList.add("is-in");
        io.unobserve(entries[i].target);
      }
    }, { rootMargin: "0px 0px -60px 0px", threshold: 0 });
    var els = document.querySelectorAll("[data-reveal]");
    for (var i = 0; i < els.length; i++) io.observe(els[i]);
  }

  /* ── 首屏数字 ────────────────────────────────────────────── */

  function countTo(el, to) {
    if (reduceMotion || to === 0) {
      el.textContent = String(to);
      return;
    }
    var DURATION = 900;
    var start = null;
    el.textContent = "0";
    function step(ts) {
      if (start === null) start = ts;
      var p = Math.min(1, (ts - start) / DURATION);
      var eased = 1 - Math.pow(1 - p, 3); // easeOutCubic
      el.textContent = String(Math.round(to * eased));
      if (p < 1) window.requestAnimationFrame(step);
      else el.textContent = String(to);
    }
    window.requestAnimationFrame(step);
  }

  function setupCounters() {
    var els = document.querySelectorAll("[data-count-to]");
    for (var i = 0; i < els.length; i++) {
      var to = parseInt(els[i].getAttribute("data-count-to"), 10);
      if (isNaN(to)) continue;
      countTo(els[i], to);
    }
  }

  /* ── 启动 ────────────────────────────────────────────────── */

  function init() {
    applyLang(detectLang());
    setupLang();
    setupNav();
    setupReveal();
    setupCounters();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

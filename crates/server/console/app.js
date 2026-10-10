// 面板的视图层：hash 路由 + 侧栏 + 四个视图（登录 / 桶列表 / 对象浏览 / 服务状态）。
//
// **一律用 `textContent` 或 `h()` 建节点，绝不拼 innerHTML**：对象 key 与服务端错误
// 消息都是外部输入，拼 HTML 会让 `a<b>.txt` 这种 key 变成注入点。
//
// **样式一律走 style.css 的类名，不写内联 `style`**：CSP 是 `default-src 'self'`，
// 内联样式会被浏览器直接拒掉。

import {
  ConsoleError, clearCredentials, fetchMetricsText, fetchReady, getObjectBlob,
  hasCredentials, headObject, listBuckets, listObjects, setCredentials,
} from './s3api.js';
import { formatValue, labelText, parseMetrics } from './metrics.js';

const view = document.getElementById('view');
const nav = document.getElementById('nav');
const bucketsNav = document.getElementById('buckets');
const conn = document.getElementById('conn');
const logoutBtn = document.getElementById('logout');

const NAV_ITEMS = [['概览', '#/'], ['服务状态', '#/status']];

/// 侧栏缓存下来的桶名。`null` = 还没取过：登录成功与退出登录都要置回 `null`，
/// 否则会拿别人的凭据看着上一个人的桶列表。
let bucketNames = null;
/// 侧栏要点亮的项，由各视图在渲染时设置。
let currentNav = '#/';
let activeBucket = null;

// ---- 建节点 --------------------------------------------------------------

/// 建元素。`class` / `text` / `on*` 有特殊含义，其余当属性写入；
/// 值为 `null` / `false` 的属性跳过（`setAttribute(k, null)` 会写字面量 "null"）。
function h(tag, props = {}, ...kids) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) {
    if (v == null || v === false) continue;
    if (k === 'class') node.className = v;
    else if (k === 'text') node.textContent = v;
    else if (k.startsWith('on')) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  for (const kid of kids) if (kid != null) node.append(kid);
  return node;
}

function fmtSize(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / 1024 / 1024).toFixed(2)} MiB`;
}

/// 统一的错误呈现（设计 §7）：原样显示服务端的 `Code` 与 `Message`，不美化；
/// 503 单独翻译成「服务启动中」，因为那是唯一一种「等一下就好」的错误。
function errorBox(err) {
  if (err instanceof ConsoleError && err.status === 503) {
    return h('p', { class: 'err', text: '服务启动中，请稍候……（未就绪）' });
  }
  if (err instanceof ConsoleError && err.status === 403) {
    return h('p', { class: 'err', text: '凭据错误，或该凭据无权执行此操作。' });
  }
  const detail = err instanceof ConsoleError
    ? `${err.code ? err.code + ': ' : ''}${err.message}`
    : String(err && err.message ? err.message : err);
  return h('p', { class: 'err', text: detail });
}

// ---- 侧栏 ----------------------------------------------------------------

/// 侧栏的两块：主导航与桶列表。都由这一个函数画，谁改了状态就调它一次。
function paintSidebar() {
  nav.replaceChildren();
  for (const [label, href] of NAV_ITEMS) {
    const on = href === currentNav;
    nav.append(h('a', {
      href,
      class: on ? 'nav-item on' : 'nav-item',
      'aria-current': on ? 'page' : null,
      text: label,
    }));
  }

  bucketsNav.replaceChildren();
  if (bucketNames === null) {
    bucketsNav.append(h('p', { class: 'hint', text: '加载中……' }));
  } else if (bucketNames.length === 0) {
    bucketsNav.append(h('p', { class: 'hint', text: '还没有桶。' }));
  } else {
    for (const name of bucketNames) {
      const on = name === activeBucket;
      bucketsNav.append(h('a', {
        href: `#/b/${encodeURIComponent(name)}/`,
        class: on ? 'bucket-link on' : 'bucket-link',
        title: name,
        'aria-current': on ? 'page' : null,
        text: name,
      }));
    }
  }
}

/// 侧栏取不到桶**不该**把主视图也带走：主视图自己会显示服务端返回的错误，
/// 而这里失败只意味着侧栏暂时是空的。
async function ensureBuckets() {
  if (bucketNames === null) {
    try {
      bucketNames = await listBuckets();
    } catch {
      bucketNames = [];
    }
    paintSidebar();
  }
  return bucketNames;
}

function setSidebar(navHref, bucket = null) {
  currentNav = navHref;
  activeBucket = bucket;
  paintSidebar();
}

/// 连接状态只在**真实调用成功之后**才说「已连接」——把它写在发请求之前，
/// 凭据是错的时候页面会一边报 403 一边显示「已连接」。
function setConn(text, bad = false) {
  conn.textContent = text;
  conn.classList.toggle('bad', bad);
}

logoutBtn.addEventListener('click', () => {
  clearCredentials();
  bucketNames = null;
  location.hash = '#/';
  render();
});

function go(hash) {
  location.hash = hash;
}

// ---- 视图 ----------------------------------------------------------------

function renderLogin(err) {
  const access = h('input', { type: 'text', autocomplete: 'off' });
  const secret = h('input', { type: 'password', autocomplete: 'off' });
  const form = h('form', {
    onsubmit: async (e) => {
      e.preventDefault();
      setCredentials({ accessKey: access.value, secretKey: secret.value });
      try {
        await listBuckets(); // 用一次真实调用验凭据，签名错自然 403
        bucketNames = null;  // 换了凭据，侧栏的旧快照作废
        go('#/');
        render();
      } catch (e2) {
        clearCredentials();
        renderLogin(e2);
      }
    },
  },
    h('label', { text: 'Access Key' }), access,
    h('label', { text: 'Secret Key' }), secret,
    h('p', {}, h('button', { type: 'submit', text: '登录' })),
  );
  view.replaceChildren(h('div', { class: 'login' },
    h('h1', { text: '登录 Rustorage' }),
    h('p', { class: 'hint', text: '用与 aws-cli / mc 相同的 S3 凭据。' }),
    h('div', { class: 'card' }, form, err ? errorBox(err) : null),
    h('p', {
      class: 'hint',
      text: '凭据只存在本标签页的 sessionStorage 里，关闭标签页即失效。',
    }),
  ));
}

async function renderBuckets() {
  setSidebar('#/', null);
  setConn('连接中……');
  view.replaceChildren(h('h1', { text: '概览' }), h('p', { class: 'hint', text: '加载中……' }));
  try {
    const names = await listBuckets();
    bucketNames = names;
    paintSidebar();
    setConn('已连接');
    const rows = names.map((name) => h('tr', {
      class: 'clickable',
      onclick: () => go(`#/b/${encodeURIComponent(name)}/`),
    }, h('td', { class: 'mono', text: name })));
    view.replaceChildren(
      h('h1', { text: '概览' }),
      h('p', { class: 'hint', text: `${names.length} 个桶` }),
      names.length === 0
        ? h('p', { class: 'hint', text: '还没有桶。' })
        : h('table', {}, h('thead', {}, h('tr', {}, h('th', { text: '名称' }))), h('tbody', {}, ...rows)),
      // 服务端不存桶的创建时间（`bucket.meta` 内容就是 `{}`），所以这一列**不存在**，
      // 而不是显示空白或假值（设计 §5.4）。
    );
  } catch (err) {
    setConn('连接异常', true);
    view.replaceChildren(h('h1', { text: '概览' }), h('div', { class: 'card' }, errorBox(err)));
  }
}

async function renderBrowse(bucket, prefix) {
  setSidebar('#/', bucket);
  setConn('连接中……');
  ensureBuckets(); // 从侧栏直接点进来的，侧栏可能还没取过桶
  const crumb = h('div', { class: 'crumb' });
  crumb.append(h('a', { href: '#/', text: '概览' }), h('span', { text: '/' }));
  crumb.append(h('a', { href: `#/b/${encodeURIComponent(bucket)}/`, text: bucket }));
  if (prefix) {
    const segs = prefix.replace(/\/$/, '').split('/');
    segs.forEach((seg, i) => {
      crumb.append(h('span', { text: '/' }));
      crumb.append(h('a', {
        href: `#/b/${encodeURIComponent(bucket)}/${segs.slice(0, i + 1).join('/')}/`,
        text: seg,
      }));
    });
  }
  view.replaceChildren(h('h1', { class: 'mono', text: bucket }), crumb, h('p', { class: 'hint', text: '加载中……' }));
  try {
    const page = await listObjects(bucket, { prefix });
    setConn('已连接');
    const rows = [
      // 前缀（「文件夹」）排在对象前面，与各家控制台的习惯一致。
      ...page.prefixes.map((p) => h('tr', {
        class: 'clickable row-prefix',
        onclick: () => go(`#/b/${encodeURIComponent(bucket)}/${p}`),
      }, h('td', { class: 'mono', text: p }), h('td', {}), h('td', {}))),
      ...page.keys.map((o) => {
        const rel = prefix ? o.key.slice(prefix.length) : o.key;
        return h('tr', { class: 'clickable', onclick: () => showDetail(bucket, o.key) },
          h('td', { class: 'mono', text: rel }),
          h('td', { class: 'num', text: fmtSize(o.size) }),
          h('td', { class: 'hint', text: o.lastModified }));
      }),
    ];
    view.replaceChildren(
      h('h1', { class: 'mono', text: bucket }),
      crumb,
      rows.length === 0
        ? h('p', { class: 'hint', text: '这个前缀下没有对象。' })
        : h('table', {},
            h('thead', {}, h('tr', {},
              h('th', { text: '键' }), h('th', { class: 'num', text: '大小' }), h('th', { text: '修改时间' }))),
            h('tbody', {}, ...rows)),
      page.truncated
        ? h('p', {}, h('button', {
            text: '加载更多',
            onclick: async (e) => {
              e.target.disabled = true;
              e.target.textContent = '加载中……';
              const more = await listObjects(bucket, { prefix, token: page.nextToken });
              e.target.closest('p').replaceWith(h('p', {
                class: 'hint',
                text: `还有更多（下一页 ${more.keys.length} 项），本版未做连续翻页。`,
              }));
            },
          }))
        : null,
      // LIST 是**全盘遍历**（README §7）：大桶上会很慢，界面上说清楚，
      // 免得用户以为是面板卡了。
      h('p', { class: 'hint', text: `前缀：${prefix || '（根）'}　·　服务端无索引，大桶下列表较慢` }),
    );
  } catch (err) {
    setConn('连接异常', true);
    view.replaceChildren(h('h1', { class: 'mono', text: bucket }), crumb, h('div', { class: 'card' }, errorBox(err)));
  }
}

/// 对象详情：用 HEAD 取元数据，再挂一个下载按钮。
///
/// **不显示 Content-Type / 自定义元数据 / 存储类 / 版本**：服务端根本不存这些
/// （设计 §5.4），显示出来只会是空白或假值。
async function showDetail(bucket, key) {
  const panel = h('div', { class: 'card' },
    h('h2', { text: '对象详情' }), h('p', { class: 'hint', text: '加载中……' }));
  view.append(panel);
  try {
    const info = await headObject(bucket, key);
    panel.replaceChildren(
      h('h2', { text: '对象详情' }),
      h('table', {}, h('tbody', {},
        h('tr', {}, h('th', { text: '键' }), h('td', { class: 'mono', text: key })),
        h('tr', {}, h('th', { text: '大小' }), h('td', { class: 'num', text: fmtSize(info.size) })),
        h('tr', {}, h('th', { text: 'ETag' }), h('td', { class: 'mono', text: info.etag })),
        h('tr', {}, h('th', { text: '修改时间' }), h('td', { text: info.lastModified })))),
      h('p', {}, h('button', {
        text: '下载',
        onclick: async (e) => {
          e.target.disabled = true;
          try {
            const blob = await getObjectBlob(bucket, key);
            const url = URL.createObjectURL(blob);
            const a = h('a', { href: url, download: key.split('/').pop() });
            a.click();
            URL.revokeObjectURL(url);
          } catch (err) {
            panel.append(errorBox(err));
          } finally {
            e.target.disabled = false;
          }
        },
      })),
    );
  } catch (err) {
    panel.replaceChildren(h('h2', { text: '对象详情' }), errorBox(err));
  }
}

async function renderStatus() {
  setSidebar('#/status');
  setConn('连接中……');
  ensureBuckets();
  const ready = h('div', { class: 'card' },
    h('h2', { text: '就绪探针 /ready' }), h('p', { class: 'hint', text: '查询中……' }));
  const metrics = h('div', { class: 'card' },
    h('h2', { text: '指标 /metrics' }), h('p', { class: 'hint', text: '查询中……' }));
  view.replaceChildren(h('h1', { text: '服务状态' }), ready, metrics);

  const r = await fetchReady();
  setConn(r.ok ? '已连接' : '未就绪', !r.ok);
  ready.replaceChildren(
    h('h2', { text: '就绪探针 /ready' }),
    r.ok
      ? h('p', { text: '已就绪（200）' })
      : h('p', { class: 'err', text: `未就绪（503），Retry-After: ${r.retryAfter || '—'}` }));

  const text = await fetchMetricsText();
  const rows = parseMetrics(text);
  if (rows.length === 0) {
    // 服务端在 `--metrics` 关闭时返回空串（见 metrics.rs 的 render）。如实说明，
    // 而不是显示一张空表让人以为「计数器都是 0」。
    metrics.replaceChildren(
      h('h2', { text: '指标 /metrics' }),
      h('p', { class: 'hint', text: '没有指标输出——服务端可能未以 --metrics 启动。' }));
    return;
  }
  metrics.replaceChildren(
    h('h2', { text: '指标 /metrics' }),
    h('table', {},
      h('thead', {}, h('tr', {},
        h('th', { text: '指标' }), h('th', { text: '标签' }), h('th', { class: 'num', text: '值' }))),
      h('tbody', {}, ...rows.map((row) => h('tr', {},
        h('td', { class: 'mono', text: row.name }),
        h('td', { class: 'hint', text: labelText(row.labels) }),
        h('td', { class: 'num', text: formatValue(row.name, row.value) }))))),
    h('p', { class: 'hint', text: '本版每次进入本页刷新一次快照，未做轮询与折线。' }));
}

// ---- 路由 ----------------------------------------------------------------

function render() {
  // 登录与否决定侧栏收不收起来（样式在 style.css 里按 data-state 分支）。
  const authed = hasCredentials();
  document.body.dataset.state = authed ? 'authed' : 'anon';
  if (!authed) {
    bucketNames = null;
    setConn('');
    renderLogin(null);
    return;
  }
  const hash = location.hash || '#/';
  const parts = hash.replace(/^#\//, '').split('/').filter((s) => s !== '');
  const head = parts[0] ? decodeURIComponent(parts[0]) : '';
  if (head === 'status') return renderStatus();
  if (head === 'b' && parts[1]) {
    const bucket = decodeURIComponent(parts[1]);
    const prefix = parts.slice(2).map(decodeURIComponent).join('/');
    return renderBrowse(bucket, prefix);
  }
  return renderBuckets();
}

window.addEventListener('hashchange', render);
render();

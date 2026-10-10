// 面板的视图层：hash 路由 + 侧栏 + 四个视图（登录 / 概览 / 对象浏览 / 服务状态）。
//
// **一律用 `textContent` 或 `h()` 建节点，绝不拼 innerHTML**：对象 key 与服务端错误
// 消息都是外部输入，拼 HTML 会让 `a<b>.txt` 这种 key 变成注入点。
//
// **样式一律走 style.css 的类名，不写内联 `style`**：CSP 是 `default-src 'self'`，
// 内联样式会被浏览器直接拒掉。DOM 原语（`h` / `icon` / toast / 抽屉 / 骨架 / 空态）
// 都在 ui.js，本文件只负责「哪个视图、取什么数据、渲染成什么」。
//
// **取元素一律用 `getElementById` 加单引号**：`console.rs` 的守卫是按「函数名 + 单引号」
// 这个字面串切分来抠 id 名的，换成双引号会让它静默退化成一条永远绿的假测试。
// （连**注释里**都不能出现那三个字符的连写——守卫不做语法分析，它只切字符串。）

import {
  ConsoleError, clearCredentials, fetchMetricsText, fetchReady, getObjectBlob,
  hasCredentials, headObject, listBuckets, listObjects, setCredentials,
} from './s3api.js';
import { formatValue, labelText, parseMetrics } from './metrics.js';
import {
  closeDrawer, copyRow, copyText, emptyState, h, icon, kvList, openDrawer,
  skeletonRows, toast,
} from './ui.js';

const view = document.getElementById('view');
const nav = document.getElementById('nav');
const bucketsNav = document.getElementById('buckets');
const bucketCount = document.getElementById('bucket-count');
const conn = document.getElementById('conn');
const logoutBtn = document.getElementById('logout');

const NAV_ITEMS = [
  ['概览', '#/', 'grid'],
  ['服务状态', '#/status', 'status'],
];

/// 侧栏缓存下来的桶名。`null` = 还没取过：登录成功与退出登录都要置回 `null`，
/// 否则会拿别人的凭据看着上一个人的桶列表。
let bucketNames = null;
/// 侧栏要点亮的项，由各视图在渲染时设置。
let currentNav = '#/';
let activeBucket = null;

// ---- 小工具 --------------------------------------------------------------

function fmtSize(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / 1024 / 1024).toFixed(2)} MiB`;
}

/// 把错误压成一行文字。`ConsoleError` 自带 S3 的 `Code`，带上它才有得查。
function errText(err) {
  if (err instanceof ConsoleError) {
    return `${err.code ? err.code + ': ' : ''}${err.message}`;
  }
  return String(err && err.message ? err.message : err);
}

/// 统一的错误呈现（设计 §7）：原样显示服务端的 `Code` 与 `Message`，不美化；
/// 503 与 403 单独翻译成人话，因为那是两种「看代码看不出来、但用户要知道怎么办」的情况。
function errorBox(err) {
  if (err instanceof ConsoleError && err.status === 503) {
    return h('p', { class: 'err', text: '服务启动中，请稍候……（未就绪）' });
  }
  if (err instanceof ConsoleError && err.status === 403) {
    return h('p', { class: 'err', text: '凭据错误，或该凭据无权执行此操作。' });
  }
  return h('p', { class: 'err', text: errText(err) });
}

/// 对象 key 相对当前前缀的显示名——列表里显示的应该是「当前层看到的名字」，
/// 而不是把整条 `a/b/c/d.txt` 重复一遍。
const relName = (prefix, key) => (prefix && key.startsWith(prefix) ? key.slice(prefix.length) : key);

/// 页头 + 页体的固定骨架。页头是**粘的**（`position: sticky`，见 style.css），
/// 所以滚到表格中段时标题、面包屑、搜索框都还在。
/// `title` 传字符串，或 `{ text, mono }`——桶名与 key 要等宽。
function page({ crumb = null, title, sub = null, toolbar = null }, ...body) {
  const t = typeof title === 'string' ? { text: title, mono: false } : title;
  return [
    h('header', { class: 'page-head' },
      crumb,
      h('div', { class: 'head-row' },
        h('div', {},
          h('h1', { class: t.mono ? 'page-title mono' : 'page-title', text: t.text }),
          sub),
        toolbar)),
    h('div', { class: 'page-body' }, ...body),
  ];
}

/// 工具条上的搜索框：对**已加载**的行做客户端过滤，不重新发请求——
/// 服务端 LIST 是全盘遍历（README §7），每敲一个字打一次太贵。
/// 返回输入框本身，因为调用方要在数据到位后才启用它。
function searchBox(placeholder, onInput) {
  const input = h('input', {
    type: 'search', autocomplete: 'off', placeholder, 'aria-label': placeholder,
  });
  input.addEventListener('input', () => onInput(input.value.trim().toLowerCase()));
  return { box: h('div', { class: 'search' }, icon('search', { size: 15 }), input), input };
}

/// 行内的小图标按钮（复制 / 下载）。`onclick` 由调用方负责
/// `stopPropagation`——否则点击会同时触发整行的「打开详情」，一次点出两件事。
function actionBtn(name, label, onclick) {
  return h('button', {
    class: 'btn-icon', type: 'button', title: label, 'aria-label': label, onclick,
  }, icon(name, { size: 14 }));
}

/// 「刷新」按钮：重跑当前视图（`render` 按 hash 重新分发），而不是重载整页。
function refreshBtn() {
  return h('button', {
    class: 'btn btn-ghost', type: 'button', title: '刷新', onclick: () => render(),
  }, icon('refresh', { size: 15 }), '刷新');
}

/// 表格里放空态的那一格。空态塞进 `td` 而不是替换整张表：表头留着，
/// 「这里是空的」与「这一列是什么」于是能一起读，表格结构也不必二选一。
function emptyRow(colspan, opts) {
  return h('tr', {}, h('td', { colspan }, emptyState(opts)));
}

// ---- 侧栏 ----------------------------------------------------------------

/// 侧栏的两块：主导航与桶列表。都由这一个函数画，谁改了状态就调它一次。
function paintSidebar() {
  nav.replaceChildren();
  for (const [label, href, ico] of NAV_ITEMS) {
    const on = href === currentNav;
    nav.append(h('a', {
      href,
      class: on ? 'nav-item on' : 'nav-item',
      'aria-current': on ? 'page' : null,
    }, icon(ico, { size: 16 }), h('span', { text: label })));
  }

  // 计数由这里统一更新——侧栏的桶列表就是从 `bucketNames` 画的，
  // 让它跟列表各写一处，迟早会出现「写着 3 个、列出来 4 个」。
  bucketCount.textContent = bucketNames && bucketNames.length ? String(bucketNames.length) : '';

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
      }, icon('bucket', { size: 14 }), h('span', { text: name })));
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
/// `bad` 为 `null` 表示「进行中」：圆点保持灰色，不冒充任何一种结论。
function setConn(text, bad = null) {
  conn.textContent = text;
  conn.classList.toggle('ok', bad === false);
  conn.classList.toggle('bad', bad === true);
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

// ---- 视图：登录 ----------------------------------------------------------

function renderLogin(err) {
  const access = h('input', { id: 'login-ak', type: 'text', autocomplete: 'off' });
  const secret = h('input', { id: 'login-sk', type: 'password', autocomplete: 'off' });
  const form = h('form', {
    onsubmit: async (e) => {
      e.preventDefault();
      setCredentials({ accessKey: access.value, secretKey: secret.value });
      try {
        await listBuckets(); // 用一次真实调用验凭据，签名错自然 403
        bucketNames = null; // 换了凭据，侧栏的旧快照作废
        go('#/');
        render();
      } catch (e2) {
        clearCredentials();
        renderLogin(e2);
      }
    },
  },
    h('div', { class: 'field' },
      h('label', { class: 'field-label', for: 'login-ak', text: 'Access Key' }), access),
    h('div', { class: 'field' },
      h('label', { class: 'field-label', for: 'login-sk', text: 'Secret Key' }), secret),
    h('button', { class: 'btn btn-primary btn-block', type: 'submit', text: '登录' }),
  );
  view.replaceChildren(h('div', { class: 'login' },
    h('div', { class: 'login-mark', 'aria-hidden': 'true', text: 'RS' }),
    h('h1', { text: '登录 Rustorage' }),
    h('p', { class: 'hint', text: '用与 aws-cli / mc 相同的 S3 凭据。' }),
    h('div', { class: 'card' }, form, err ? errorBox(err) : null),
    // 这句话必须与实现一致：凭据是 `s3api.js` 里的一个模块级变量，刷新即丢。
    // 原文案写的是「存在 sessionStorage 里」——全仓没有这个调用，那是句假话，
    // 而它恰好是一句关于**凭据去哪了**的话，错在这里最不该。
    h('p', { class: 'hint', text: '凭据只存在本页内存中，刷新页面需重新登录。' }),
  ));
  // 进来就能直接打字，不必先用鼠标点一下输入框。
  access.focus();
}

// ---- 视图：概览（桶列表） -------------------------------------------------

async function renderBuckets() {
  setSidebar('#/', null);
  setConn('连接中……');

  const sub = h('p', { class: 'page-sub', text: '加载中……' });
  const host = h('div');
  const tbody = h('tbody');
  tbody.append(...skeletonRows(2));
  const table = h('div', { class: 'panel' },
    h('div', { class: 'tbl-wrap' },
      h('table', {},
        h('thead', {}, h('tr', {},
          h('th', { text: '名称' }),
          h('th', { class: 'num', text: '操作' }))),
        tbody)));
  host.append(table);

  let all = [];
  const { box: search, input: searchInput } = searchBox('搜索桶', (q) => paintRows(q));
  searchInput.disabled = true;

  view.replaceChildren(...page({
    title: '概览',
    sub,
    toolbar: h('div', { class: 'toolbar' }, search, refreshBtn()),
  }, host));

  /// 只换 `tbody`，**不碰搜索框**：重画整页会让输入框丢焦点，一个字都打不下去。
  function paintRows(q) {
    const names = q ? all.filter((n) => n.toLowerCase().includes(q)) : all;
    if (names.length === 0) {
      tbody.replaceChildren(emptyRow(2, all.length === 0
        ? { iconName: 'bucket', title: '还没有桶', hint: '用 aws-cli 或 mc 建一个桶，点「刷新」就会出现在这里。' }
        : { iconName: 'search', title: '没有匹配的桶', hint: `没有名字含「${q}」的桶。` }));
      return;
    }
    tbody.replaceChildren(...names.map((name) => {
      const open = () => go(`#/b/${encodeURIComponent(name)}/`);
      return h('tr', {
        class: 'clickable', tabindex: '0', onclick: open,
        onkeydown: (e) => { if (e.key === 'Enter') { e.preventDefault(); open(); } },
      },
        h('td', {}, h('div', { class: 'cell-icon' },
          icon('bucket', { size: 15 }), h('span', { class: 'mono', text: name }))),
        h('td', { class: 'num' }, h('div', { class: 'row-actions' },
          actionBtn('copy', `复制桶名 ${name}`, (e) => { e.stopPropagation(); copyText(name); }))));
    }));
  }

  try {
    all = await listBuckets();
    bucketNames = all;
    paintSidebar();
    setConn('已连接', false);
    sub.textContent = `${all.length} 个桶`;
    searchInput.disabled = all.length === 0;
    paintRows('');
    // 服务端不存桶的创建时间（`bucket.meta` 内容就是 `{}`），所以这一列**不存在**，
    // 而不是显示空白或假值（设计 §5.4）。
  } catch (err) {
    setConn('连接异常', true);
    sub.textContent = '';
    searchInput.disabled = true;
    host.replaceChildren(h('div', { class: 'card' }, errorBox(err)));
  }
}

// ---- 视图：对象浏览 -------------------------------------------------------

async function renderBrowse(bucket, prefix) {
  setSidebar('#/', bucket);
  setConn('连接中……');
  ensureBuckets(); // 从侧栏直接点进来的，侧栏可能还没取过桶

  const crumb = h('div', { class: 'crumb' });
  crumb.append(h('a', { href: '#/', text: '概览' }), h('span', { class: 'sep', text: '/' }));
  crumb.append(h('a', { href: `#/b/${encodeURIComponent(bucket)}/`, text: bucket }));
  if (prefix) {
    const segs = prefix.replace(/\/$/, '').split('/');
    segs.forEach((seg, i) => {
      crumb.append(h('span', { class: 'sep', text: '/' }));
      crumb.append(h('a', {
        href: `#/b/${encodeURIComponent(bucket)}/${segs.slice(0, i + 1).join('/')}/`,
        text: seg,
      }));
    });
  }

  const sub = h('p', { class: 'page-sub', text: '加载中……' });
  const host = h('div');
  const tbody = h('tbody');
  tbody.append(...skeletonRows(4));
  const foot = h('span', { class: 'hint', text: '加载中……' });
  const more = h('button', {
    class: 'btn', type: 'button', text: '加载更多', hidden: true, onclick: loadMore,
  });
  const panel = h('div', { class: 'panel' },
    h('div', { class: 'tbl-wrap' },
      h('table', {},
        h('thead', {}, h('tr', {},
          h('th', { text: '键' }),
          h('th', { class: 'num', text: '大小' }),
          h('th', { text: '修改时间' }),
          h('th', { class: 'num', text: '操作' }))),
        tbody)),
    h('div', { class: 'panel-foot' }, foot, more));
  host.append(panel);

  const { box: search, input: searchInput } = searchBox('搜索当前层', (q) => { query = q; paint(); });
  searchInput.disabled = true;

  view.replaceChildren(...page({
    crumb,
    title: { text: bucket, mono: true },
    sub,
    toolbar: h('div', { class: 'toolbar' }, search, refreshBtn()),
  }, host));

  // 累积已加载的行：翻页是**追加**，不是替换——替换会把用户已经看过的内容抹掉。
  const dirs = [];   // 前缀（「文件夹」）的完整路径
  const objs = [];   // 对象
  let token = null;
  let truncated = false;
  let loading = false;
  let query = '';

  /// 能不能继续翻。**必须同时看 token**：只有 `IsTruncated` 而没有
  /// `NextContinuationToken` 时再点一次会重新取回第一页，把行**重复追加**一遍。
  const canMore = () => truncated && token !== null;

  function footText() {
    const n = dirs.length + objs.length;
    if (truncated && !token) return `已加载 ${n} 项 · 服务端说还有更多，但没给继续令牌`;
    // LIST 是**全盘遍历**（README §7）：大桶上会很慢，界面上说清楚，
    // 免得用户以为是面板卡了。
    return `已加载 ${n} 项${truncated ? '（还有更多）' : ''} · 服务端无索引，大桶下列表较慢`;
  }

  function objRow(o) {
    const open = () => showDetail(bucket, o.key);
    return h('tr', {
      class: 'clickable', tabindex: '0', onclick: open,
      onkeydown: (e) => { if (e.key === 'Enter') { e.preventDefault(); open(); } },
    },
      h('td', {}, h('div', { class: 'cell-icon' },
        icon('file', { size: 15 }),
        h('span', { class: 'mono', text: relName(prefix, o.key) }))),
      h('td', { class: 'num', text: fmtSize(o.size) }),
      h('td', { class: 'hint', text: o.lastModified }),
      h('td', { class: 'num' }, h('div', { class: 'row-actions' },
        actionBtn('copy', `复制键 ${o.key}`, (e) => { e.stopPropagation(); copyText(o.key); }),
        actionBtn('download', `下载 ${o.key}`, (e) => {
          e.stopPropagation();
          downloadObject(bucket, o.key, e.currentTarget);
        }))));
  }

  function prefixRow(p) {
    const open = () => go(`#/b/${encodeURIComponent(bucket)}/${p}`);
    // 前缀排在对象前面，与各家控制台的习惯一致。
    return h('tr', {
      class: 'clickable row-prefix', tabindex: '0', onclick: open,
      onkeydown: (e) => { if (e.key === 'Enter') { e.preventDefault(); open(); } },
    },
      h('td', {}, h('div', { class: 'cell-icon is-dir' },
        icon('folder', { size: 15 }), h('span', { class: 'mono', text: relName(prefix, p) }))),
      h('td', {}), h('td', {}), h('td', {}));
  }

  /// 只换 `tbody`，不碰搜索框（同 `renderBuckets`）。
  function paint() {
    const hit = (s) => !query || s.toLowerCase().includes(query);
    const rows = [
      ...dirs.filter((p) => hit(relName(prefix, p))).map(prefixRow),
      ...objs.filter((o) => hit(relName(prefix, o.key))).map(objRow),
    ];
    if (rows.length === 0) {
      tbody.replaceChildren(emptyRow(4, dirs.length + objs.length === 0
        ? { iconName: 'folder', title: '这里没有对象', hint: '换个前缀，或点面包屑回上一层。' }
        : { iconName: 'search', title: '没有匹配的项', hint: `已加载的项里没有含「${query}」的。` }));
    } else {
      tbody.replaceChildren(...rows);
    }
    foot.textContent = footText();
    more.hidden = !canMore();
  }

  /// 把一页结果并进累积区。
  function absorb(p) {
    dirs.push(...p.prefixes);
    objs.push(...p.keys);
    token = p.nextToken;
    truncated = p.truncated;
  }

  async function loadMore() {
    if (loading || !canMore()) return;
    loading = true;
    more.disabled = true;
    more.textContent = '加载中……';
    const want = token;
    try {
      absorb(await listObjects(bucket, { prefix, token: want }));
      setConn('已连接', false);
      paint();
    } catch (err) {
      // 翻页失败**不清空**已加载的内容：把用户看得好好的行拿走，
      // 只为了报告"下一页没取到"，是拿更糟的结果换一个更响的提示。
      toast(`加载下一页失败：${errText(err)}`, 'bad');
    } finally {
      loading = false;
      more.disabled = false;
      more.textContent = '加载更多';
    }
  }

  try {
    absorb(await listObjects(bucket, { prefix }));
    setConn('已连接', false);
    sub.textContent = `前缀：${prefix || '（根）'}`;
    searchInput.disabled = dirs.length + objs.length === 0;
    paint();
  } catch (err) {
    setConn('连接异常', true);
    sub.textContent = '';
    searchInput.disabled = true;
    host.replaceChildren(h('div', { class: 'card' }, errorBox(err)));
  }
}

// ---- 视图：对象详情（右侧抽屉） -------------------------------------------

/// 用 HEAD 取元数据，进右侧抽屉。
///
/// **不显示 Content-Type / 自定义元数据 / 存储类 / 版本**：服务端根本不存这些
/// （设计 §5.4），显示出来只会是空白或假值。
///
/// 先开抽屉显示「加载中」，拿到结果再填——两次 `openDrawer`，第二次只换内容
/// 不抢焦点（`ui.js` 里按 `data-open` 判断）。
async function showDetail(bucket, key) {
  const name = key.split('/').pop() || key;
  const close = h('button', {
    class: 'btn btn-ghost btn-block', type: 'button', onclick: closeDrawer, text: '关闭',
  });
  const acts = h('div', { class: 'drawer-acts' },
    h('button', {
      class: 'btn btn-primary btn-block', type: 'button',
      onclick: (e) => downloadObject(bucket, key, e.currentTarget),
    }, icon('download', { size: 15 }), '下载'),
    close);

  openDrawer({ title: name, body: h('p', { class: 'hint', text: '加载中……' }) });
  try {
    const info = await headObject(bucket, key);
    openDrawer({
      title: name,
      body: h('div', {},
        kvList([
          ['键', copyRow(key)],
          ['大小', h('span', { text: fmtSize(info.size) })],
          ['ETag', info.etag ? copyRow(info.etag) : h('span', { class: 'hint', text: '—' })],
          ['修改时间', h('span', { text: info.lastModified || '—' })],
        ]),
        acts),
    });
  } catch (err) {
    openDrawer({ title: name, body: h('div', {}, errorBox(err), acts) });
  }
}

/// 下载：取回整份字节、造一个临时 URL、点一下隐藏的 `<a download>`。
///
/// 撤销 URL 用 `setTimeout` 而不是紧随 `click()`：某些浏览器上立刻撤销会
/// 把还没开始的下载掐掉，而现象是「有时能下、有时不能」。
async function downloadObject(bucket, key, btn) {
  const name = key.split('/').pop() || 'download';
  btn.disabled = true;
  try {
    const blob = await getObjectBlob(bucket, key);
    const url = URL.createObjectURL(blob);
    const a = h('a', { href: url, download: name });
    document.body.append(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 10000);
    toast(`已开始下载 ${name}`);
  } catch (err) {
    toast(`下载失败：${errText(err)}`, 'bad');
  } finally {
    btn.disabled = false;
  }
}

// ---- 视图：服务状态 -------------------------------------------------------

function stat(label, value, hint) {
  return h('div', { class: 'stat' },
    h('div', { class: 'stat-label', text: label }),
    h('div', { class: 'stat-value' }, value),
    hint ? h('div', { class: 'stat-hint', text: hint }) : null);
}

function badge(ok, text) {
  return h('span', { class: ok ? 'badge badge-ok' : 'badge badge-bad' },
    icon(ok ? 'check' : 'alert', { size: 13 }), text);
}

async function renderStatus() {
  setSidebar('#/status');
  setConn('连接中……');
  ensureBuckets();

  const sub = h('p', { class: 'page-sub', text: '每次进入本页取一次快照，未做轮询' });
  const stats = h('div', { class: 'stat-grid' },
    stat('就绪探针 /ready', h('span', { class: 'hint', text: '查询中……' }), null),
    stat('指标 /metrics', h('span', { class: 'hint', text: '查询中……' }), null));
  const host = h('div');
  view.replaceChildren(...page({
    title: '服务状态',
    sub,
    toolbar: h('div', { class: 'toolbar' }, refreshBtn()),
  }, stats, host));

  try {
    // 两个端点都不是 S3 面（不签名、不受 ready 门控制，设计 §2.2），并行取。
    const [r, text] = await Promise.all([fetchReady(), fetchMetricsText()]);
    const rows = parseMetrics(text);
    setConn(r.ok ? '已连接' : '未就绪', !r.ok);

    stats.replaceChildren(
      stat('就绪探针 /ready',
        badge(r.ok, r.ok ? '已就绪' : '未就绪'),
        r.ok ? 'HTTP 200' : `HTTP 503 · Retry-After: ${r.retryAfter || '—'}`),
      stat('指标 /metrics',
        rows.length === 0 ? h('span', { class: 'hint', text: '无输出' }) : String(rows.length),
        rows.length === 0
          ? '服务端可能未以 --metrics 启动'
          : '条已采集的指标'));

    if (rows.length === 0) {
      // 服务端在 `--metrics` 关闭时返回空串（见 metrics.rs 的 render）。如实说明，
      // 而不是显示一张空表让人以为「计数器都是 0」。
      host.replaceChildren(h('div', { class: 'panel' },
        h('div', { class: 'tbl-wrap' },
          h('table', {},
            h('thead', {}, h('tr', {},
              h('th', { text: '指标' }),
              h('th', { text: '标签' }),
              h('th', { class: 'num', text: '值' }))),
            h('tbody', {}, emptyRow(3, {
              iconName: 'status',
              title: '没有指标输出',
              hint: '服务端可能未以 --metrics 启动，重启时加上这个开关。',
            }))))));
      return;
    }
    host.replaceChildren(h('div', { class: 'panel' },
      h('div', { class: 'tbl-wrap' },
        h('table', {},
          h('thead', {}, h('tr', {},
            h('th', { text: '指标' }),
            h('th', { text: '标签' }),
            h('th', { class: 'num', text: '值' }))),
          h('tbody', {}, ...rows.map((row) => h('tr', {},
            h('td', { class: 'mono', text: row.name }),
            h('td', { class: 'hint', text: labelText(row.labels) }),
            h('td', { class: 'num', text: formatValue(row.name, row.value) }))))))));
  } catch (err) {
    setConn('连接异常', true);
    sub.textContent = '';
    // 用 `hidden` 而不是清空子节点：空着的 `.stat-grid` 还留着 margin-bottom，
    // 会在错误卡上方留一道莫名其妙的空隙。
    stats.hidden = true;
    host.replaceChildren(h('div', { class: 'card' }, errorBox(err)));
  }
}

// ---- 路由 ----------------------------------------------------------------

function render() {
  // 登录与否决定侧栏收不收起来（样式在 style.css 里按 data-state 分支）。
  const authed = hasCredentials();
  document.body.dataset.state = authed ? 'authed' : 'anon';
  // 换视图先把抽屉收掉：它显示的是**上一个视图**里的对象，换页后还开着就是张冠李戴。
  closeDrawer();

  if (!authed) {
    bucketNames = null;
    paintSidebar();
    setConn('');
    renderLogin(null);
    return;
  }
  const hash = location.hash || '#/';
  // **不要 `filter(s => s !== '')`**：那样会连**尾斜杠**一起丢掉，而前缀的尾斜杠
  // 是有意义的。`#/b/photos/a/` 的前缀必须是 `a/`——丢成 `a` 之后，S3 的
  // `delimiter=/` 会把 `a/one.txt`、`a/two.txt` 聚成一个名为 `a/` 的前缀返回，
  // 页面于是自己指向自己：点进去只看到一行 `a/`，再点还是它，走不出去。
  const parts = hash.replace(/^#\//, '').split('/').map(decodeURIComponent);
  const head = parts[0];
  if (head === 'status') return renderStatus();
  if (head === 'b' && parts[1]) {
    return renderBrowse(parts[1], parts.slice(2).join('/'));
  }
  return renderBuckets();
}

window.addEventListener('hashchange', render);
render();

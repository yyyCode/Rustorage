// 面板的 DOM 原语层：建节点、图标、toast、抽屉、剪贴板、空态与骨架。
//
// 与 app.js 的分工：app.js 只关心「哪个视图、取什么数据、渲染成什么」，
// 这里只关心「怎么把它造出来」。
//
// **本文件不调 `getElementById`**：`console.rs` 的
// `app_looks_up_ids_that_index_actually_provides` 守卫只扫 app.js，新文件里的
// id 查找会落在护栏之外——「取了 index.html 里不存在的 id」正是白屏的头号原因，
// 不该有一处是没人盯的。这里需要挂载点时（toast 区、抽屉）一律**自建并 append 到 body**。

const SVG_NS = 'http://www.w3.org/2000/svg';

// ---- 建节点 --------------------------------------------------------------

/// 建 HTML 元素。`class` / `text` / `on*` 有特殊含义，其余当属性写入；
/// 值为 `null` / `false` 的属性跳过（`setAttribute(k, null)` 会写字面量 "null"）。
export function h(tag, props = {}, ...kids) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) {
    if (v == null || v === false) continue;
    // 内联样式会被 CSP（`default-src 'self'`）**静默**拒掉——页面看着只是"没生效"，
    // 报错只在浏览器控制台里。在这里直接抛，把它变成一次响亮的失败。
    if (k === 'style') {
      throw new Error('内联样式会被 CSP 拒掉：请往 style.css 加类，不要写 style 属性');
    }
    if (k === 'class') node.className = v;
    else if (k === 'text') node.textContent = v;
    else if (k.startsWith('on')) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  for (const kid of kids) if (kid != null) node.append(kid);
  return node;
}

/// 建 SVG 元素。`h()` 用的是 `createElement`，造不出 SVG 命名空间的节点
/// （浏览器会当成 HTMLUnknownElement，图形不渲染），所以单开一个。
function s(tag, attrs = {}) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) if (v != null) node.setAttribute(k, v);
  return node;
}

// ---- 图标 ----------------------------------------------------------------

/// 每个图标是一串子元素声明 `[tag, attrs][]`。统一 24×24 视窗、`currentColor`
/// 描边——颜色由 CSS 的 `color` 决定，所以同一个图标在侧栏、按钮、空态里
/// 各自跟随所在处的文字色，不需要为每种场合复制一份。
const ICONS = {
  bucket: [['path', { d: 'M4 7h16v12a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V7Z' }],
           ['path', { d: 'M4 7l1.2-3.4A1 1 0 0 1 6.1 3h11.8a1 1 0 0 1 .9.6L20 7' }],
           ['path', { d: 'M10 11h4' }]],
  status: [['path', { d: 'M3 12h4l3 8 4-16 3 8h4' }]],
  search: [['circle', { cx: 11, cy: 11, r: 7 }], ['path', { d: 'm16.5 16.5 4.5 4.5' }]],
  refresh: [['path', { d: 'M20 11a8 8 0 1 0-.7 3.3' }], ['path', { d: 'M20 4v6h-6' }]],
  copy: [['rect', { x: 9, y: 9, width: 11, height: 11, rx: 2 }],
         ['path', { d: 'M5 15V5a2 2 0 0 1 2-2h10' }]],
  download: [['path', { d: 'M12 3v12' }], ['path', { d: 'm7 10 5 5 5-5' }],
             ['path', { d: 'M4 20h16' }]],
  close: [['path', { d: 'm6 6 12 12' }], ['path', { d: 'M18 6 6 18' }]],
  folder: [['path', { d: 'M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V7Z' }]],
  file: [['path', { d: 'M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8l-5-5Z' }],
         ['path', { d: 'M14 3v5h5' }]],
  inbox: [['path', { d: 'M3 12h5l1 2h6l1-2h5' }],
          ['path', { d: 'M5 4h14l2 8v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-6l2-8Z' }]],
  check: [['path', { d: 'm5 13 4 4L19 7' }]],
  alert: [['path', { d: 'M12 9v4' }], ['path', { d: 'M12 17h.01' }],
          ['path', { d: 'M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0Z' }]],
  clock: [['circle', { cx: 12, cy: 12, r: 9 }], ['path', { d: 'M12 7v5l3 2' }]],
};

/// 造一个图标节点。`title` 给了才有可读名称——装饰性图标一律 `aria-hidden`，
/// 否则读屏软件会把每个按钮念两遍（图标名 + 按钮文字）。
export function icon(name, { size = 16, title = null } = {}) {
  const paths = ICONS[name];
  if (!paths) throw new Error(`未知图标：${name}`);
  const svg = s('svg', {
    viewBox: '0 0 24 24', width: size, height: size,
    fill: 'none', stroke: 'currentColor', 'stroke-width': 1.8,
    'stroke-linecap': 'round', 'stroke-linejoin': 'round',
    role: title ? 'img' : null,
    'aria-hidden': title ? null : 'true',
    'aria-label': title,
  });
  for (const [tag, attrs] of paths) svg.append(s(tag, attrs));
  return svg;
}

// ---- Toast ---------------------------------------------------------------

/// 同一时刻最多留几条。再多会把右下角占满，而那些内容用户多半没在看——
/// 满了就丢最旧的一条。
const TOAST_MAX = 3;

let region = null;

/// 惰性建容器并 append 到 body：本文件不查 `getElementById`（见文件头）。
/// `aria-live` 挂在**容器**上而不是每条 toast 上：容器先于内容存在，
/// 读屏才会播报后来插进去的东西。
function toastRegion() {
  if (!region) {
    region = h('div', { class: 'toast-region', role: 'status', 'aria-live': 'polite' });
    document.body.append(region);
  }
  return region;
}

/// 右下角冒一条提示，`kind` 为 `'ok'` 或 `'bad'`。
/// 错误多留一会儿（6s vs 3.5s）——错误信息读者需要时间读完。
export function toast(message, kind = 'ok') {
  const box = h('div', { class: `toast ${kind}` },
    icon(kind === 'bad' ? 'alert' : 'check', { size: 15 }),
    h('span', { class: 'toast-text', text: message }));
  const host = toastRegion();
  host.append(box);
  while (host.children.length > TOAST_MAX) host.firstElementChild.remove();
  setTimeout(() => box.remove(), kind === 'bad' ? 6000 : 3500);
}

// ---- 抽屉（对象详情） ----------------------------------------------------

let drawer = null;
let backdrop = null;
let drawerTitle = null;
let drawerBody = null;
let drawerCloseBtn = null;
let lastFocus = null;

/// 惰性建抽屉与遮罩，两者常驻 DOM，靠 `data-open` 切换可见性——
/// 开合各建一次节点会让动画每帧重新触发，也让「关闭后立刻可点」变难保证。
function ensureDrawer() {
  if (drawer) return;
  // `aria-labelledby` 指向标题，读屏软件才会在打开时念出「对象详情」而不是
  // 一串孤立的字段值。id 是固定的：面板里只有一个抽屉。
  const titleId = 'drawer-title';
  drawerTitle = h('h2', { class: 'drawer-title', id: titleId });
  drawerCloseBtn = h('button', {
    class: 'btn-icon', type: 'button', 'aria-label': '关闭', onclick: closeDrawer,
  }, icon('close'));
  drawerBody = h('div', { class: 'drawer-body' });
  drawer = h('aside', {
    class: 'drawer', role: 'dialog', 'aria-modal': 'true', 'aria-labelledby': titleId,
  }, h('div', { class: 'drawer-head' }, drawerTitle, drawerCloseBtn), drawerBody);

  backdrop = h('div', { class: 'drawer-backdrop', onclick: closeDrawer });
  document.body.append(backdrop, drawer);

  document.addEventListener('keydown', (e) => {
    // `dataset.open` 存的是字符串 `'true'`——写成 `''` 的话这里读回来是空串，
    // 判断真值会**静默**失效（属性在、Esc 不管用），所以别图省事。
    if (e.key === 'Escape' && drawer.dataset.open) closeDrawer();
  });
}

/// 打开抽屉。`body` 是要展示的节点。
///
/// 焦点管理是「模态」这个词的实际含义：开时把焦点移进来，关时**还给**原处。
/// 少了后一半，键盘用户关掉抽屉后会被丢回页面开头。
export function openDrawer({ title, body }) {
  ensureDrawer();
  drawerTitle.textContent = title;
  drawerBody.replaceChildren(body);
  lastFocus = document.activeElement;
  drawer.dataset.open = 'true';
  backdrop.dataset.open = 'true';
  // 直接持有那个按钮的引用，而不是 `querySelector('button')`：抽屉头部以后
  // 若多出别的按钮，选择器会指到错的那个，而症状只是「焦点落错地方」。
  drawerCloseBtn.focus();
}

export function closeDrawer() {
  if (!drawer || !drawer.dataset.open) return;
  delete drawer.dataset.open;
  delete backdrop.dataset.open;
  if (lastFocus && document.contains(lastFocus)) lastFocus.focus();
  lastFocus = null;
}

// ---- 剪贴板 --------------------------------------------------------------

/// 复制文本到剪贴板，成败都以 toast 反馈。
///
/// `navigator.clipboard` 只在 **secure context** 下存在——面板恰好是
/// `http://127.0.0.1`，按规范算 secure context。但若哪天被人换成局域网 IP 打开，
/// 这个 API 就会是 undefined，所以这里要能优雅降级：提示手动选中，而不是静默失败。
export async function copyText(text) {
  if (!navigator.clipboard) {
    toast('此环境不支持剪贴板 API，请手动选中复制', 'bad');
    return;
  }
  try {
    await navigator.clipboard.writeText(text);
    toast('已复制到剪贴板');
  } catch (e) {
    toast(`复制失败：${e.message}`, 'bad');
  }
}

/// 「值 + 复制键」的一行。`mono` 为 true 时值用等宽字体（key / ETag 都该等宽）。
export function copyRow(text, { mono = true } = {}) {
  return h('div', { class: 'copy-row' },
    h('span', {
      class: mono ? 'copy-val mono' : 'copy-val',
      text,
      title: text,
    }),
    h('button', {
      class: 'btn-icon', type: 'button', title: '复制',
      'aria-label': `复制 ${text}`, onclick: () => copyText(text),
    }, icon('copy', { size: 14 })));
}

// ---- 空态与骨架 ----------------------------------------------------------

/// 空态：图标 + 一句说明 + 一句"下一步能做什么"。
/// 空态最容易写成「什么都没有」，而那与「加载失败」在页面上长得一模一样。
export function emptyState({ iconName = 'inbox', title, hint }) {
  return h('div', { class: 'empty' },
    icon(iconName, { size: 34 }),
    h('p', { class: 'empty-title', text: title }),
    hint ? h('p', { class: 'empty-hint', text: hint }) : null);
}

/// 加载中的骨架行。给**形状**而不是「加载中……」四个字：后者在数据到达时
/// 会让下面的内容整块跳一下。
export function skeletonRows(cols, rows = 4) {
  const out = [];
  for (let r = 0; r < rows; r++) {
    const tds = [];
    for (let c = 0; c < cols; c++) {
      const w = c === 0 ? 'sk-w-60' : c === cols - 1 ? 'sk-w-40' : 'sk-w-80';
      tds.push(h('td', {}, h('span', { class: `sk sk-line ${w}` })));
    }
    out.push(h('tr', { class: 'sk-row', 'aria-hidden': 'true' }, ...tds));
  }
  return out;
}

/// 一张键值表（定义列表）。抽屉里的键值对用定义列表而不是再嵌一张表：
/// 没有表头也没有列宽要算，而 `.kv` 用 flex 竖排即可。
/// `pairs` 是 `[标签, 值节点][]`。
export function kvList(pairs) {
  const dl = h('dl', { class: 'kv' });
  for (const [label, value] of pairs) {
    dl.append(h('dt', { text: label }), h('dd', {}, value));
  }
  return dl;
}

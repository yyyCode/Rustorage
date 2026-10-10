# website —— Rustorage 官网

一个零构建的静态单页站：展示项目的亮点与设计，不讲底层代码、不讲怎么启动。

**硬约束**（改之前先知道）：

| 约束 | 含义 |
|---|---|
| 零构建 | 没有 npm、没有 node_modules、没有产物。源码就是产物，改完刷新即见 |
| 零外部请求 | 不发任何指向第三方的请求：无 CDN、无外链字体、无外链图片、无分析脚本 |
| `file://` 可用 | 双击 `index.html` 也能看。因此**不能用 ES module**，一律经典 `<script>` |
| 无内联样式 / 脚本 | 样式全在 `assets/style.css` 的类名里，行为全在 `assets/app.js` 里 |
| 零位图 | 图形都是内联 SVG 或 CSS。没有 `.png` / `.jpg` |

## 文件

```
website/
├── index.html          唯一页面：语义结构 + data-i18n 键
├── assets/
│   ├── style.css       全部视觉：令牌、排版、组件、响应式
│   ├── i18n.js         中 / 英两份文案树 —— 唯一需要改文案的地方
│   ├── app.js          语言切换、导航折叠、滚动进场、首屏数字滚动
│   └── logo.svg        顶栏的 wordmark 标记
├── favicon.svg
├── robots.txt
├── 404.html            静态托管的兜底页
├── check.sh            本地护栏（四条，随时手跑）
└── README.md           就是本文件
```

## 本地看效果

```bash
python -m http.server 8080 --bind 127.0.0.1 --directory website
# 浏览器开 http://127.0.0.1:8080/
```

也可以直接双击 `index.html`——`file://` 下同样能用，这是设计约束之一。

## 提交前自检

```bash
bash website/check.sh
```

四条护栏，输出 `WEBSITE: OK` 或逐条列出问题：

1. **没有外部请求**——`src=` / `href=` 里不允许出现第三方的 `http(s)://`。
   指向本仓库与文档的**页面链接**走白名单（见脚本第 1 条）。
2. **没有内联样式**——HTML 里没有 `style=`，JS 里没有 `.style.` 属性写入。
   不是洁癖：内联样式在严格 CSP 下会被浏览器**静默拒掉**，只能靠护栏兜。
3. **双语键对齐**——`i18n.js` 里 `zh` 与 `en` 的键集合必须完全相同。
4. **HTML 与文案树对齐**——HTML 里用到的 `data-i18n` 键必须都在树里；
   树里有、HTML 里没用的键只提醒不拦（可能是 JS 动态写入，也可能是死文案）。

第 3 条**依赖 `i18n.js` 的书写格式**：`  zh: {` / `  en: {` 顶格两空格，
每行一个 `"扁平.键名": "值",`（缩进四空格），**不嵌套对象**。
改那个文件的排版之前先读一下 `i18n.js` 顶部的注释。

## 改文案

只改 `assets/i18n.js`。**中英两份一起改**——不要先写一份再翻译，措辞会拧。
两边的键必须一一对应，否则 `check.sh` 会拦。

页面上的每个文案节点是 `index.html` 里带 `data-i18n="键名"` 的元素；
`app.js` 应用语言时写 `textContent`（所以文案里不要写 HTML 标签）。

## 数字从哪来

`#bench` 一节的每个数字都来自仓库的 `docs/benchmarks/io-modes/`
（原始输出在 `raw/`），**不要在页面上新造数字**。那一节刻意把
唯一的负面数字（列全桶多 33% 目录遍历）也放上去了，别删。

## 部署

整个 `website/` 目录原样上传即可。三种常见做法：

**GitHub Pages** —— 仓库 Settings → Pages → Source 选分支与 `/website` 目录。
自定义域名在同一页填 Custom domain，并在域名侧加对应的 CNAME 记录。

**Cloudflare Pages / Netlify / Vercel** —— 构建命令留空，发布目录填 `website/`。

**自己的 Nginx** ——

```nginx
server {
    root /path/to/website;
    index index.html;
    error_page 404 /404.html;
}
```

`404.html` 里的资源路径是绝对路径（`/assets/...`），所以它必须挂在站点的根上；
如果把站点部署在子路径下，记得把这些路径一起改掉。

本项目**没有为官网配任何 CI / workflow**——它是纯静态目录，不该有构建步骤。

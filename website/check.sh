#!/usr/bin/env bash
# 官网的本地自检护栏。不是流水线——随时手跑。
# 用法：bash website/check.sh
#
# 四条检查都是「这个站点的硬约束」的可执行版本：
#   1. 零外部请求   2. 零内联样式   3. 双语键对齐   4. HTML 键与文案树对齐
#
# 第 3 条依赖 i18n.js 的书写格式：`zh: {` / `en: {` 顶格两空格，
# 每行一个 `"flat.dotted.key": "值",`（缩进四空格），不嵌套对象。
# 改那个文件的排版前先读这一行。
set -uo pipefail

cd "$(dirname "$0")" || exit 1
fail=0
note() { printf '  \033[31m✗\033[0m %s\n' "$1"; fail=1; }
warn() { printf '  \033[33m!\033[0m %s\n' "$1"; }
ok()   { printf '  \033[32m✓\033[0m %s\n' "$1"; }

echo "website 自检"

# 1) 没有外部请求：src= / href= 里不允许出现第三方的 http(s)://。
#    指向仓库与文档的页面链接走白名单。
outside=$(grep -nE '(src|href)="https?://' index.html 404.html assets/*.css assets/*.js 2>/dev/null \
  | grep -vE '(href|src)="https?://(github\.com/yyyCode/Rustorage|yyycode\.github\.io)' || true)
if [ -n "$outside" ]; then
  printf '%s\n' "$outside"
  note "出现了白名单之外的外部引用"
else
  ok "没有外部资源引用"
fi

# 2) 没有内联样式
if grep -nE 'style=' index.html 404.html 2>/dev/null; then
  note "HTML 里有内联 style="
elif grep -nE '\.style\.[a-zA-Z]' assets/*.js 2>/dev/null; then
  note "JS 里在写 .style. 属性"
else
  ok "没有内联样式"
fi

# 3) 双语键对齐：zh 与 en 的键集合必须完全相同
slice() { awk -v L="$1" '$0 == "  " L ": {" {f=1; next} /^  \},$/ {f=0} f' assets/i18n.js; }
zh=$(slice zh | grep -oE '^    "[^"]+"' | tr -d ' "' | sort)
en=$(slice en | grep -oE '^    "[^"]+"' | tr -d ' "' | sort)
if [ -z "$zh" ] || [ -z "$en" ]; then
  note "i18n.js 切片为空——检查 zh: { / en: { 的写法是不是被改过"
else
  d=$(diff <(printf '%s\n' "$zh") <(printf '%s\n' "$en") || true)
  if [ -n "$d" ]; then
    printf '%s\n' "$d"
    note "zh 与 en 的键不一致（左边 < 是 zh，右边 > 是 en）"
  else
    ok "zh / en 键一致（$(printf '%s\n' "$zh" | wc -l | tr -d ' ') 个）"
  fi
fi

# 4) HTML 用到的键，文案树里必须有；树里没被用到的只提醒不拦（可能是 JS 动态写入）
html_keys=$(grep -ohE 'data-i18n="[^"]+"' index.html 404.html 2>/dev/null \
  | sed 's/data-i18n="//;s/"//' | sort -u)
if [ -z "$html_keys" ]; then
  note "HTML 里没有任何 data-i18n"
else
  miss=0
  for k in $html_keys; do
    printf '%s\n' "$en" | grep -qx "$k" || { note "i18n.js 里没有这个键：$k"; miss=1; }
  done
  [ "$miss" = 0 ] && ok "HTML 用到的 $(printf '%s\n' "$html_keys" | wc -l | tr -d ' ') 个键都在"
  dead=$(comm -13 <(printf '%s\n' "$html_keys") <(printf '%s\n' "$en") 2>/dev/null || true)
  if [ -n "$dead" ]; then
    warn "树里没被 HTML 用到的键（可能是 JS 动态写入，也可能是死文案）："
    printf '      %s\n' "$(printf '%s' "$dead" | tr '\n' ' ')"
  fi
fi

if [ "$fail" = 0 ]; then echo "WEBSITE: OK"; else echo "WEBSITE: FAILED"; exit 1; fi

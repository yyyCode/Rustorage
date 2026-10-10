// Prometheus 文本格式的解析（设计 §4.4 的指标页）。
// 只解析服务端 `render()` 真正会输出的形状：`name{label="v"} value` 或 `name value`。
// 刻意不引 Prometheus 客户端库：那是一个 npm 依赖，而本面板零构建链。

const LABEL_RE = /([A-Za-z_][A-Za-z0-9_]*)="([^"]*)"/g;

function parseLabels(text) {
  const labels = {};
  // 用正则而不是 `split(',')`：标签值里出现逗号时（这里不会，但形状上允许）
  // 朴素切分会切错，而错的方式是静默的。
  for (const m of text.matchAll(LABEL_RE)) labels[m[1]] = m[2];
  return labels;
}

/// 解析成 `{ name, labels, value }[]`。注释行（`#`）与空行跳过；
/// 主体里取**最后一个空格**之后的部分当值，这样即使序列名里出现空格也不会切错。
export function parseMetrics(text) {
  const rows = [];
  for (const raw of (text || '').split('\n')) {
    const line = raw.trim();
    if (line === '' || line.startsWith('#')) continue;
    const sp = line.lastIndexOf(' ');
    if (sp < 0) continue;
    const value = Number(line.slice(sp + 1));
    if (Number.isNaN(value)) continue;
    const series = line.slice(0, sp);
    const brace = series.indexOf('{');
    rows.push(brace < 0
      ? { name: series, labels: {}, value }
      : {
          name: series.slice(0, brace),
          labels: parseLabels(series.slice(brace + 1, series.lastIndexOf('}'))),
          value,
        });
  }
  return rows;
}

/// 把标签拼成稳定的展示文本（按 key 排序，避免同样的指标两行顺序不同）。
export function labelText(labels) {
  const keys = Object.keys(labels).sort();
  return keys.map((k) => `${k}=${labels[k]}`).join(', ');
}

/// 值 → 人类可读。时长类指标（`*_seconds`）用毫秒/秒，其余原样。
/// 与其画一张没有分位数消费方的假直方图（服务端也刻意没做桶），不如给准数字。
export function formatValue(name, value) {
  if (name.endsWith('_seconds')) {
    return value >= 1 ? `${value.toFixed(3)} s` : `${(value * 1000).toFixed(1)} ms`;
  }
  return String(value);
}

// 浏览器端 SigV4 签名（设计 §5）。用 WebCrypto 现算，请求服务端自己校验。
//
// **正确性靠的是与 s3s 的规范化逐字节一致**：canonical URI / query / headers 任何一处
// 不同，服务端算出来的签名就对不上，结果是 403——而 403 不会告诉你差在哪个字符上。
// 因此下面每个编码决定都写了理由，改之前先看设计 §9 的第一条风险。

const ALGO = 'AWS4-HMAC-SHA256';
// 与 `crates/s3/src/impl_s3.rs` 测试里的 REGION 取值一致（us-east-1）。
// 服务端只要求签名与请求里声明的 scope 自洽；若将来服务端开始校验 region，
// 这里是唯一的改动点。
const REGION = 'us-east-1';
const SERVICE = 's3';
// 空请求体的 SHA256：用常量而不是现算，省一次 subtle 调用。
const EMPTY_SHA256 =
  'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';

const enc = new TextEncoder();

function toHex(buf) {
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

// S3 的规范化百分号编码。**不能用 `encodeURIComponent`**：它不编码 `!'()*`，
// 而这四个字符在 S3 的规范化里必须被编码（同一段 key 在两边算出不同签名）。
// 保留 A-Za-z0-9-_.~；`encodeSlash=false` 时额外保留 '/'（canonical URI 的路径用）。
function uriEncode(s, encodeSlash) {
  let out = '';
  for (const ch of s) {
    if (/[A-Za-z0-9\-_.~]/.test(ch)) out += ch;
    else if (ch === '/' && !encodeSlash) out += ch;
    else {
      out += [...enc.encode(ch)]
        .map((b) => '%' + b.toString(16).toUpperCase().padStart(2, '0'))
        .join('');
    }
  }
  return out;
}

async function sha256Hex(text) {
  return toHex(await crypto.subtle.digest('SHA-256', enc.encode(text)));
}

async function hmacBytes(keyBytes, msg) {
  const key = await crypto.subtle.importKey(
    'raw', keyBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'],
  );
  return new Uint8Array(await crypto.subtle.sign('HMAC', key, enc.encode(msg)));
}

async function hmacHex(keyBytes, msg) {
  return toHex(await hmacBytes(keyBytes, msg));
}

/// `Date` → SigV4 要的两个字符串：完整的 `YYYYMMDDTHHMMSSZ` 与范围里的 `YYYYMMDD`。
function amzDates(now) {
  const iso = now.toISOString().replace(/[-:]/g, '').replace(/\.\d{3}/, '');
  return { amzDate: iso, scopeDate: iso.slice(0, 8) };
}

/// 请求的 URL（含查询串）。**查询串必须与 canonicalQuery 用同一套编码**，
/// 否则服务端从实际 URL 解出的 query 与签名里声明的不一致。这里用
/// `encodeURIComponent`，它对 `list-type` / `delimiter` / `prefix` 这些键值
/// 与 `uriEncode(v, true)` 结果相同（`/` → `%2F`，`-` 不编码）。
export function buildUrl(path, query) {
  const keys = Object.keys(query || {}).sort();
  if (keys.length === 0) return path;
  const qs = keys
    .map((k) => `${encodeURIComponent(k)}=${encodeURIComponent(String(query[k]))}`)
    .join('&');
  return `${path}?${qs}`;
}

/// 签一个请求，返回要挂到 `fetch` 的请求头。
///
/// **返回值里没有 `host`**：浏览器禁止脚本设置 Host 头。但 `host` 仍在
/// `SignedHeaders` 里——服务端按声明去读它**实际收到**的 Host（= `location.host`），
/// 两边因此一致。这正是必须用 `location.host` 而不是硬编码主机名的原因。
export async function signRequest({ method, path, query, body, accessKey, secretKey }) {
  const { amzDate, scopeDate } = amzDates(new Date());
  const payloadHash = body ? await sha256Hex(body) : EMPTY_SHA256;
  const host = location.host;

  const canonicalRequest = [
    method,
    uriEncode(path, false),
    Object.keys(query || {}).sort()
      .map((k) => `${uriEncode(k, true)}=${uriEncode(String(query[k]), true)}`)
      .join('&'),
    `host:${host}\n` +
      `x-amz-content-sha256:${payloadHash}\n` +
      `x-amz-date:${amzDate}\n`,
    'host;x-amz-content-sha256;x-amz-date',
    payloadHash,
  ].join('\n');

  const scope = `${scopeDate}/${REGION}/${SERVICE}/aws4_request`;
  const stringToSign = [
    ALGO, amzDate, scope, await sha256Hex(canonicalRequest),
  ].join('\n');

  const kDate = await hmacBytes(enc.encode('AWS4' + secretKey), scopeDate);
  const kRegion = await hmacBytes(kDate, REGION);
  const kService = await hmacBytes(kRegion, SERVICE);
  const kSigning = await hmacBytes(kService, 'aws4_request');
  const signature = await hmacHex(kSigning, stringToSign);

  return {
    'x-amz-content-sha256': payloadHash,
    'x-amz-date': amzDate,
    authorization:
      `${ALGO} Credential=${accessKey}/${scope}, ` +
      `SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=${signature}`,
  };
}

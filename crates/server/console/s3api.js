// S3 数据面的薄封装（设计 §4.4/§7）。只做三件事：签名、发请求、把错误翻译成人话。

import { buildUrl, signRequest } from './sigv4.js';

/// 面板自己的错误类型。`status` 为 0 表示**请求根本没发出去**（网络层失败），
/// 与「服务端回了 4xx/5xx」是两回事，界面上的措辞不同。
export class ConsoleError extends Error {
  constructor(message, status, code) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

let creds = null;

export function setCredentials(next) { creds = next; }
export function clearCredentials() { creds = null; }
export function hasCredentials() { return creds !== null; }

/// 从 S3 的错误 XML 里取 `<Code>` 与 `<Message>`。取不到就退回状态码文本——
/// **不编造**一个更好看的错误信息（设计 §7：面板不吞错）。
async function describe(resp) {
  const text = await resp.text().catch(() => '');
  const pick = (tag) => {
    const m = text.match(new RegExp(`<${tag}>([^<]*)</${tag}>`));
    return m ? m[1] : null;
  };
  const code = pick('Code');
  const message = pick('Message');
  return new ConsoleError(message || resp.statusText, resp.status, code);
}

async function call(method, path, { query, body } = {}) {
  if (!creds) throw new ConsoleError('未登录', 401);
  const payload = body || '';
  const headers = await signRequest({
    method, path, query, body: payload,
    accessKey: creds.accessKey, secretKey: creds.secretKey,
  });
  let resp;
  try {
    resp = await fetch(buildUrl(path, query), {
      method, headers, body: payload === '' ? undefined : payload,
    });
  } catch (e) {
    // 浏览器把网络层失败一律报成同样的 TypeError，这里至少把原文带上。
    throw new ConsoleError(`请求发不出去：${e.message}`, 0);
  }
  if (!resp.ok) throw await describe(resp);
  return resp;
}

function parseXml(text) {
  return new DOMParser().parseFromString(text, 'application/xml');
}

export async function listBuckets() {
  const xml = parseXml(await (await call('GET', '/')).text());
  return [...xml.querySelectorAll('Buckets > Bucket > Name')].map((n) => n.textContent);
}

/// `ListObjectsV2` 带上 `delimiter=/` 就是目录式浏览：`prefixes` 是「文件夹」，
/// `keys` 是当前层的对象。
export async function listObjects(bucket, { prefix = '', token } = {}) {
  const query = { 'list-type': '2', delimiter: '/', prefix };
  if (token) query['continuation-token'] = token;
  const xml = parseXml(await (await call('GET', `/${bucket}`, { query })).text());
  const one = (node, tag) => {
    const el = node.querySelector(tag);
    return el ? el.textContent : '';
  };
  return {
    // 对象 key 含多段，`delimiter` 让服务端**不**返回前缀之下的 key，
    // 所以这里拿到的 key 一定属于当前层，直接拼接即可。
    keys: [...xml.querySelectorAll('Contents')].map((n) => ({
      key: one(n, 'Key'),
      size: Number(one(n, 'Size')) || 0,
      lastModified: one(n, 'LastModified'),
      etag: one(n, 'ETag'),
    })),
    prefixes: [...xml.querySelectorAll('CommonPrefixes > Prefix')].map((n) => n.textContent),
    truncated: one(xml, 'IsTruncated') === 'true',
    nextToken: one(xml, 'NextContinuationToken') || null,
  };
}

export async function headObject(bucket, key) {
  const resp = await call('HEAD', `/${bucket}/${key}`);
  return {
    size: Number(resp.headers.get('content-length')) || 0,
    etag: resp.headers.get('etag') || '',
    lastModified: resp.headers.get('last-modified') || '',
  };
}

/// 取回整份对象字节。MVP 没有 multipart，但 GET 不受那条限制（限制在 PUT 一侧）。
export async function getObjectBlob(bucket, key) {
  return (await call('GET', `/${bucket}/${key}`)).blob();
}

/// `/metrics` 与 `/ready` **不走 S3 面**：它们是 s3s 之前被截下的运维端点，
/// 既不签名也不受 ready 门控制（设计 §2.2）。
export async function fetchMetricsText() {
  const resp = await fetch('/metrics');
  return resp.ok ? resp.text() : '';
}

export async function fetchReady() {
  const resp = await fetch('/ready');
  return { ok: resp.ok, retryAfter: resp.headers.get('retry-after') };
}

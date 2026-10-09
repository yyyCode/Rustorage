# Rustorage 设计文档

> 状态：**设计定稿（v1）**，尚未实现。
> 本文是项目的架构契约。实现与本文件冲突时，以本文件为准；本文件需要变更时，
> 必须同步更新受影响的章节并说明理由，不允许让文档漂移。
>
> 定位：用 Rust 从零实现的、S3 兼容的分布式对象存储。
> 学习型完整实现——追求概念清晰、边界干净，而非功能堆叠。

---

## 目录

1. [定位与非目标](#1-定位与非目标)
2. [设计原则](#2-设计原则)
3. [与 RustFS 的对照：提什么、改什么、不抄什么](#3-与-rustfs-的对照提什么改什么不抄什么)
4. [物理层级与命名](#4-物理层级与命名)
5. [Crate 分层与依赖规则](#5-crate-分层与依赖规则)
6. [盘上布局](#6-盘上布局)
7. [format.json 与部署拓扑](#7-formatjson-与部署拓扑)
8. [元数据容器 meta.xl](#8-元数据容器-metaxl)
9. [对象定位：三级寻址](#9-对象定位三级寻址)
10. [纠删码与 Quorum](#10-纠删码与-quorum)
11. [完整性：bitrot 校验](#11-完整性bitrot-校验)
12. [提交协议与崩溃一致性](#12-提交协议与崩溃一致性)
13. [I/O 管线与变换层](#13-io-管线与变换层)
14. [请求链路](#14-请求链路)
15. [兼容层](#15-兼容层)
16. [并发控制与锁](#16-并发控制与锁)
17. [错误模型与失败分级](#17-错误模型与失败分级)
18. [运维面：readiness / metrics / 日志](#18-运维面-readiness--metrics--日志)
19. [测试策略](#19-测试策略)
20. [路线图](#20-路线图)
21. [速查表](#21-速查表)

---

## 1. 定位与非目标

### 1.1 定位

Rustorage 是一个 **S3 兼容的分布式对象存储**，对标 MinIO / RustFS 的架构路线：

- **纠删码**而非副本，最小可容忍单元是「erasure set」；
- **元数据与数据同盘**（sidecar 文件），无外部依赖，单二进制部署；
- 单节点多盘起步，物理层级从第一天就是为多节点设计的；
- 对外暴露完整的 **S3 数据面**，让 aws-cli / mc / boto3 / rclone 直接可用。

### 1.2 非目标（明确不做）

写下来是为了防止范围蔓延。这些**不是遗漏，是决策**：

| 非目标 | 理由 |
|---|---|
| MinIO 盘格式兼容（读 MinIO 写的盘） | 成本极高（字节级格式锁死 + 多种分布算法 + 双 codec），与「概念清晰」的定位冲突 |
| MinIO Admin API / Console 协议 | 需要一套独立的 RPC 契约与认证，收益不成比例 |
| 从 MinIO 双向复制 / 站点复制 | 后续如需，走标准 S3 客户端，不作为本项目的一等能力 |
| 多存储后端（网关模式） | 本项目的价值在存储引擎本身 |

---

## 2. 设计原则

按重要性排序。当多条原则冲突时，编号小的优先。

**P1 — 正确性优先于性能。** 任何优化都不得削弱 quorum 保证。绝不在低于 quorum 时报告成功。

**P2 — 失败要分级，不要一刀切。** 「确定性损坏」与「瞬时 IO 故障」必须在类型层面区分开。前者触发修复，后者只触发重试。

**P3 — 结构性命中 fail closed，值级形态 fail open。**
- 结构：magic、CRC、格式版本、必需数组长度、ID 字节数 → 不合法即拒绝（`Corrupt`）。
- 值：nil UUID、epoch 时间戳、未知字段键 → 宽容降级，向前兼容。

**P4 — 边界先于实现。** 每个 crate 单一职责，通过 trait 通信。上层只依赖契约，不依赖实现。

**P5 — 热路径零意外分配。** 缓冲复用、编解码器缓存、元数据早停（分片数 ≤ 16，用 `u16` 位图）。

**P6 — 没有失败测试的兼容代码不允许存在。** 每一条兼容性特殊处理必须能指认出「哪个客户端 + 什么行为 + 哪个回归测试」。

**P7 — 过载时拒绝，不排队。** 准入失败返回 `503 SlowDown`，绝不无界放行到 OOM。

**P8 — 文档与代码同源。** 文档描述的即是当前实现的形态。描述目标态时必须在正文标注「未实现」。

---

## 3. 与 RustFS 的对照：提什么、改什么、不抄什么

RustFS 是一份高质量的参考实现。本项目的架构骨架大量借鉴它，但**在所有会形成长期负担的地方主动偏离**。

### 3.1 采纳（骨架照搬）

| 机制 | 价值 |
|---|---|
| 纠删码 + 读写 quorum | 读 quorum = 数据块数；写 quorum 在 `data == parity` 时 +1，避免 50/50 分裂 |
| 同盘元数据 sidecar | 无外部依赖、单二进制；元数据与数据同生命周期 |
| 分片分布数组（CRC32 旋转出的排列） | 让分片在盘间错开，降低相关性故障；几乎零成本 |
| bitrot 逐块交错 `[hash][data]` | 一次向量写，读时顺手校验，能定位到具体坏块 |
| 目录 rename 作为提交原语 | 提交原子、幂等；崩溃后靠对账清理残留 |
| 浅元数据 + 懒解析 body | LIST / 元数据缓存路径只解析 header |
| 元数据 header 自带 `ec_m`/`ec_n` | quorum 决策无需解析 body |
| 小对象内联进元数据 | 小对象免去 N 次盘写 |
| 准入即拒绝（过载 → 503） | 背压语义清晰 |
| 数据搬移让路前台 | 后台任务不打爆在线流量 |
| crate 分层 + 依赖方向由 CI 强制 | 用工具而非自觉维持架构 |
| 分阶段 readiness | 集群未稳时不接流量 |
| heal 与写路径共享锁与提交模型 | 避免两套写路径互相踩踏 |

### 3.2 改进（明确偏离）

| # | RustFS 的问题 | Rustorage 的做法 |
|---|---|---|
| 1 | `Sets` = pool、`SetDisks` = set，命名反直觉 | 直接用 `Pool` / `ErasureSet` / `Disk` / `Shard` |
| 2 | `ecstore` 单体：265 文件、约 28.8 万行 | 从第一天按域切 crate，接口先行 |
| 3 | 双内部前缀、14 层 `*CompatLayer`、GF(2^16) legacy codec、DARE v2、三代分布算法 | 全部不要：单一 codec、单一格式版本、单一前缀 `x-rs-*` |
| 4 | **跨节点 generation fencing 实际不存在**（设计文档完整，实现只是 opt-in 的本地 UUID 复检） | 不假装有。单节点阶段明说无此问题；进多节点前先设计真正的 per-object 租约 |
| 5 | 元数据同盘导致 LIST 需全量扫描 | MVP 接受；接口预留命名空间索引的挂钩位 |
| 6 | 只有 OTLP 推送，无进程内 `/metrics` | 直接暴露 Prometheus `/metrics` |
| 7 | 测试以内联单测为主，缺故障注入与崩溃点测试 | 引入 `FaultyDisk` + 提交协议状态机测试（见 §19） |
| 8 | 配置类型在存储侧与核心侧重复定义的风险 | 配置类型只定义一次，其他位置通过投影函数引用 |
| 9 | 部分 Cargo 描述与实际归属不符（文档漂移） | P8：文档与代码同源 |

### 3.3 不抄（明确砍掉）

- MinIO `CRCMOD` / `SIPMOD` 多代分布算法 → 只保留一种，但**从第一天就写算法版本字段**。
- GF(2^16) 遗留编解码器 → 只用一种。
- 双前缀元数据（`x-rustfs-internal-` / `x-minio-internal-`）→ 单前缀。
- 「仅标签」存储类等历史兼容语义 → 不支持的历史概念直接报错。

---

## 4. 物理层级与命名

```
Node ──▶ Pool ──▶ ErasureSet ──▶ Disk ──▶ Shard
        扩容单位   纠删单位        一块盘     一份分片(+校验)
                  2..=16 块盘
```

| 概念 | 类型名 | 说明 |
|---|---|---|
| 节点 | `Node` | 一个运行中的进程（MVP 只有一个） |
| 池 | `Pool` | 扩容单位。一次扩容新增一个 pool，旧 pool 不重排 |
| 纠删集 | `ErasureSet` | 纠删码的最小单位。一个 pool 由若干 erasure set 组成 |
| 盘 | `Disk` | 一块物理盘或一个目录。trait `DiskAPI`，实现有 `LocalDisk` / `RemoteDisk` |
| 分片 | `Shard` | 一个对象在一个 erasure set 内的一份分片 |

**不变量**：对象的数据只落在一个 erasure set 内，永不跨 set。set 内任意 `data` 块可重建对象；任意 `data` 块 + 元数据可正常读取。

> **命名约定**：类型名用单数指代一个实例，容器用 `Vec`。不存在 RustFS 那种「单数名字持有整个集合」的陷阱。

---

## 5. Crate 分层与依赖规则

只允许**向下依赖**。由 CI 脚本强制（`scripts/check-layer-deps.sh`）。

```
rstore-server        二进制入口，唯一的装配点
   ├── rstore-s3-compat   兼容中间件（薄）
   ├── rstore-s3          s3s 的 S3 trait 实现
   ├── rstore-store       引擎核心：Pool / ErasureSet / 读写路径 / quorum / 提交
   │      ├── rstore-disk
   │      ├── rstore-erasure
   │      ├── rstore-meta
   │      └── rstore-checksum
   └── rstore-api         契约 trait，供上层消费（不依赖 store）
```

| crate | 职责 | 允许依赖 |
|---|---|---|
| `rstore-common` | 基础类型、错误分类、配置模型、ID 类型 | 无内部依赖 |
| `rstore-checksum` | bitrot 哈希（keyed BLAKE3）与 KAT 自检 | common |
| `rstore-meta` | `format.json` + `meta.xl` 容器编解码、`FileInfo` 模型 | common, checksum |
| `rstore-erasure` | 纠删编解码、workspace 复用、编解码器缓存 | common |
| `rstore-disk` | `DiskAPI` trait、`LocalDisk`、路径与 fsync 原语 | meta, checksum |
| `rstore-store` | 引擎核心（见上） | 以上全部 |
| `rstore-api` | `ObjectStore` 等契约 trait、领域错误 | common |
| `rstore-s3` | s3s 集成：`S3` trait 实现、SigV4 接入、错误映射 | api, common |
| `rstore-s3-compat` | tower 兼容中间件 | common |
| `rstore-server` | 装配、启动/关闭编排、metrics、config 加载 | 全部 |

**规则 R1**：外部只能通过 `rstore-api` 的 trait 访问引擎，不得直接依赖 `rstore-store`。
**规则 R2**：`rstore-api` 不得反向依赖任何实现 crate。
**规则 R3**：`common` 不得依赖任何内部 crate；`checksum` / `erasure` 只允许依赖 `common`。
**规则 R4**：每个上层 crate 只允许有**一个** boundary 文件把 api trait 别名化并绑定实现。

---

## 6. 盘上布局

### 6.1 固定名字

| 名字 | 位置 | 用途 |
|---|---|---|
| `format.json` | 盘根 | 部署 id、池/集拓扑、分布算法版本 |
| `.rstore.sys/` | 盘根 / 桶根 | 盘级与桶级系统数据 |
| `meta.xl` | data-dir 内 | 对象元数据容器 |
| `part.<n>` | data-dir 内 | 第 n 个 **multipart 部分**在本盘的分片（含 bitrot 校验）。单部分对象恒为 `part.1` |
| `.staging-<txid>/` | 对象目录内 | 写入暂存区，rename 的源 |
| `.healing.bin` | `.rstore.sys/` | heal 位图 |

### 6.2 树状结构

```
<disk>/
  format.json
  .rstore.sys/
      disk_id                       # 本盘 UUID
      healing.bin                   # heal 进度位图（后续）
  <bucket>/                         # 桶名不以 '.' 开头，天然不与系统目录冲突
      .rstore.sys/                  # 桶级元数据：policy / versioning / usage（后续）
      .rstore.uploads/<upload-id>/  # multipart 暂存
          upload.meta               # 目标 key、storage class 等
          part.<n>/                 # 第 n 个上传部分，结构与对象一致
              <data-dir-uuid>/{meta.xl, part.1}
              .staging-<txid>/
      <object-path>/
          <data-dir-uuid>/          # ← rename 的落点，它出现即提交
              meta.xl
              part.1                # 单部分对象：每块盘恰好一个分片文件
              # multipart 对象则为 part.1 … part.<M>（M = 部分数）
```

### 6.3 保留名规则

1. **桶名**必须以小写字母或数字开头（S3 规则），故永不与 `.` 开头的系统目录冲突。
2. **对象 key** 的第一段不得以 `.rstore` 开头。该校验在请求入口执行，违反返回 `InvalidObjectName`。

> 这条规则把「系统目录」与「用户命名空间」用可检查的语法隔开，避免 MinIO 那种保留名散落各处的问题。

---

## 7. format.json 与部署拓扑

`FormatV1`（JSON，写在每块盘根）：

```json
{
  "version": "1",
  "format": "erasure",
  "id": "<deployment-uuid>",
  "erasure": {
    "version": "1",
    "this": "<disk-uuid>",
    "sets": [
      ["<disk-uuid>", "<disk-uuid>", "..."]
    ],
    "distribution_algo": "sipmod-v1"
  },
  "disk_info": { "total": 0, "free": 0 }
}
```

要点：

- **盘 → set 的归属不是运行时算出来的**，而是格式化时一次性生成 UUID 表并持久化到每块盘。运行时只做查表（`find_disk_index_by_disk_id`）。哈希**只用于对象 → set 的路由**。
- `shared_identity()` 返回除 `this` 之外的全部字段。**同一 pool 内所有盘必须一致**，不一致时拒绝启动。
- **版本仲裁**：多盘 `format.json` 按 quorum 选权威版本。
- **全新初始化要求所有盘都报 `UnformattedDisk`** —— 网络不可达的盘绝不被当作「新拓扑」的证据。这比 quorum 更严格，是刻意为之。
- `distribution_algo` 从第一版就存在。本项目只用 `sipmod-v1`，但字段保留以便未来更换而不致旧数据不可读。

---

## 8. 元数据容器 meta.xl

### 8.1 容器格式

```
magic   "RSM1"            (4 字节)
major   u16 LE            (当前 1)
minor   u16 LE            (当前 0)
version_count u16 LE
── 版本记录 × version_count ──────────────────────────
  header  FileVersionHeader   (定长、已解析，msgpack 编码)
  body_len u32 BE
  body    bytes               (不透明，懒解析)
──────────────────────────────────────────────────────
trailer CRC32C(以上全部)
inline_data                   (可选，小对象内联载荷)
```

防损坏措施：

- magic 或 minor 过新 → `FileCorrupt`（**确定性损坏**，不会因重试而变好）；
- `version_count` 与剩余字节数不符 → 分配内存**之前**拒绝；
- 每个 `body_len` 在分配前校验剩余字节；
- CRC 不匹配 → `FileCorrupt` 并记录 `meta_crc_mismatch` 指标。

**增量读**：只读容器前缀即可完成 LIST / HEAD，不必读入 body。

### 8.2 数据模型

```rust
struct ObjectMeta {
    versions: Vec<ShallowVersion>,
    inline:   InlineData,
    meta_ver: u8,
}

struct ShallowVersion {
    header: FileVersionHeader,   // 已解析
    body:   OpaqueBody,          // 懒解析
}

struct FileVersionHeader {
    version_id: Option<Uuid>,    // 必须保留 nil 与 None 的区别
    ty:         VersionType,     // Object | DeleteMarker
    size:       u64,
    mod_time:   u64,             // unix 纳秒；None 编码为 0
    ec_m:       u8,              // data shards，quorum 决策直接可读
    ec_n:       u8,              // total shards
    flags:      Flags,           // FreeVersion | UsesDataDir | InlineData
    data_dir:   Option<Uuid>,    // 16 字节原始 UUID
}

struct ObjectBody {              // body 解析后的内容
    id:        Option<Uuid>,
    parts:     Vec<PartInfo>,    // 并行数组，长度必须一致
    ec_dist:   Vec<u8>,          // 分片分布排列
    checksum_algo: ChecksumAlgo,
    storage_class: StorageClass,
    meta_user: BTreeMap<String, String>,
    meta_sys:  BTreeMap<String, Vec<u8>>,
}

struct PartInfo {
    number: u16,
    size:   u64,
    actual_size: u64,
    etag:   String,
    index:  Option<Vec<u8>>,     // 压缩时使用，MVP 恒为 None
}
```

**浅版本的收益**：LIST 与 HEAD 只看 `header` 即可判定「最新版本是不是删除标记」「对象多大」，无需解析 body。这正是 RustFS 的设计，值得保留。

**并行数组的硬护栏**：`parts` 中任何必需字段（number / size）长度不一致 → `FileCorrupt`。软护栏（etag / index）缺失则降级跳过。

### 8.3 序列化选择

**msgpack（`rmp-serde`）**。理由：

- map 语义天然支持**未知键跳过**，给格式演进留路（P3 的 fail-open）；
- 生态成熟，与 MinIO 这类系统的做法一致；
- 代价是比自定义二进制稍慢——但元数据是热路径上的小对象，可接受。

**编码规范**：结构体一律 `#[serde(deny_unknown_fields)]` **不启用**（否则破坏向前兼容）；字段增删一律通过新的 minor 版本承载。

### 8.4 内联数据

- 载荷追加在 CRC 之后，由 `InlineData` 帧化，msgpack map：`version-key → bytes`；
- **读路径只由 body 中的 `meta_sys["inline-data"]` 标记决定**，header 的 `InlineData` flag 仅作提示；
- 门限：`DEFAULT_INLINE_BLOCK = 128 KiB`；版本化桶取 `inline_block / 8`（MVP 未启用版本化，恒用前者）；
- 小对象快路径：整对象直接编码进内联区，免去 N 次盘写。

### 8.5 元数据键命名

- 内部键统一前缀 `x-rs-`（如 `x-rs-inline-data`、`x-rs-actual-size`）；
- 用户侧键（`x-amz-*`）原样保留；
- **键名字符串是载荷承重结构**：改动一个字节会让旧 `meta.xl` 读不出来。所有常量集中在 `rstore-meta::keys`，并配逐字符变异测试。

---

## 9. 对象定位：三级寻址

### 9.1 对象 → Pool（`rstore-store` 层）

- **新对象**：构造各 pool 的可用空间快照（跳过 suspended / rebalancing），
  过滤掉使用率超过 `100 - DISK_RESERVE_FRACTION` 的 pool，再按**可用字节加权随机**选择。
  空间不足返回 `DiskFull`。
- **已有对象**：对所有 pool 并发查询，按 `mod_time` 倒序取第一个命中。
  严格读场景下，某个 pool 的读 quorum 失败会**升级**为 `ErasureReadQuorum`，
  而不是被静默当成「对象不存在」。

> MVP 只有一个 pool，此层退化为恒等映射。但接口保留，使扩容不需要改动上层。

### 9.2 对象 → ErasureSet（`Pool` 层）

```
set_index = siphash24(bucket + "/" + object, key = deployment_id) % set_count
```

以 **deployment id 作为 SipHash 的 key**：同一部署内路由稳定；更换 deployment id 会整体重排（因此 id 一经写入不可更改）。

> 相比 RustFS 的三代算法（CRCMOD / SIPMOD / SIPMOD+PARITY），本项目只有一种。
> 稳定性通过「deployment id 不可变」这一约束保证，而不是靠算法版本兜底。

### 9.3 对象 → 分片槽位（`rstore-meta` 层）

```
start = CRC32C(object_key) % N            // N = 分片总数
dist[k] = 1 + ((start + k) % N),  k = 0..N   // dist 是 1..=N 的一个排列
```

`dist[k] - 1` 即逻辑块 `k` 的物理槽位。**必须校验 `dist` 是 `1..=N` 的严格排列**，不合法则视为 `Corrupt`（防御性 fail closed），但**不得 panic**——所有下标访问都要 `checked_sub(1)` + 范围检查。

---

## 10. 纠删码与 Quorum

### 10.1 几何

- `N = data + parity`，`2 ≤ N ≤ 16`（`N = 1` 是单盘异常路径，`parity = 0`）；
- 默认 parity：

  | N | 1 | 2–3 | 4–5 | 6–7 | ≥8 |
  |---|---|---|---|---|---|
  | parity | 0 | 1 | 2 | 3 | 4 |

- 约束：`parity ≤ N/2`；`STANDARD` 的 parity 不得低于 `REDUCED_REDUNDANCY`；
- parity **按 pool 解析**（用该 pool 自己的盘数计算，不跨 pool 取全局值）；
- `BLOCK_SIZE = 1 MiB`（每个版本记录一份块大小）；
- `shard_size = div_ceil(block_size, data_shards)`（单一公式，无遗留偶对齐变体）。

### 10.2 编解码器

- 库：`reed-solomon-simd`（SIMD 加速，GF(2^16)）；
- 算法串常量：`ERASURE_ALGORITHM = "rs-gf16-v1"`。**刻意用具名版本串而非库名**——库可能被替换，
  而落盘的对象必须能长期解码；串中不含实现细节，只标识「能解出该数据的编码族」；
- **缓存**：按 `ErasureCacheKey { data, parity, block_size }` 缓存编解码器外壳（LRU，上限 32），避免冷 GET 重复构造；
- **workspace 复用**：`codec::workspace` 池化编码/解码工作区，每 stripe 零分配；
- **不 panic 原则**：从不可信元数据推导几何时，任何不合法组合返回 `ErasureConstructionError`，绝不 `unwrap`。

### 10.3 Quorum 规则

```rust
read_quorum  = N - parity              // == data
write_quorum = data;  if data == parity { +1 }
delete_quorum = N / 2 + 1              // 删除标记走多数派
```

- 读：`available < read_quorum` 时**必须** `fail closed` 返回 `ErasureReadQuorum`，绝不静默截断返回短数据；
- 写：`parity == data` 的对称情形下 +1，避免 50/50 分裂（此时任一侧都不构成多数派）；
- **读时缺分片但够 quorum**：正常返回**并**异步入队读修复（`ShardReadRepair`）。

### 10.4 元数据仲裁

- 从观察到的多盘元数据推导 `(read_quorum, write_quorum)`；
- 计票依据**内容身份哈希**：SHA-256 覆盖 `size / flags / mod_time / version_id / data_dir / parts`，
  **刻意排除易变字段**（复制状态、heal 标记），否则状态噪声会劈裂本应一致的 quorum；
- 未达 quorum → `ErasureReadQuorum`。

**元数据早停**：`MetadataQuorumAccumulator` 用 `u16` 位图记录已返回的分片（`N ≤ 16`，故热路径零分配）。
`DiskResult::Pending`（尚未返回）与 `DiskResult::Offline`（明确离线）语义**刻意区分**：前者不能计入失败。

### 10.5 并行 I/O

- **写**：`MultiWriter` 把分片 `i` 写到 writer `i`。失败/掉队的 writer 被置 `None`；
  每块结束后要求 `success_count ≥ write_quorum`，否则报降级错误；
- **读**：`ParallelReader` 按需拉取，`demand_bound_parity_admission_limit` 控制预读上限；
  跨条带会「重建数据分片并与存活校验分片比对」，不一致报 `InvalidData`；
- **错误归约**：`reduce_errs` 统计各盘错误，取占主导者（平票优先 `NotFound`）；
  达到 quorum 阈值时返回对应的 `Erasure*Quorum`，并附 `{required, achieved, failed, total, offline}` 诊断。

---

## 11. 完整性：bitrot 校验

- **哈希**：keyed **BLAKE3**，key 为编译期常量 `BITROT_KEY_V1`；
- **布局**：逐块交错 `[hash(32B)][data]`，一次向量写入；
- 文件大小：`bitrot_size = ceil(size / shard_size) * 32 + size`；
- **读时校验**：不匹配 → `FileCorrupt`（**不是**通用 IO 错误），使 heal 能识别确定性损坏；
- 单侧短读即使在校验关闭时也返回 `UnexpectedEof`，避免静默截断为合法长度；
- **启动自检**：KAT（known-answer test）验证哈希实现对已知输入产生预期输出，防止依赖升级引入静默行为变更。

> **与 RustFS 的差异**：RustFS 保留 `HighwayHash256S` 与旧 key 变体两套实现以兼容历史文件。
> 本项目只有一种算法，但 key 与算法名从第一版就是具名的常量——未来更换时用字段区分，而不是靠魔改实现。

---

## 12. 提交协议与崩溃一致性

### 12.1 提交原语：目录 rename

```
PUT:
  1. 选 data-dir uuid（新版本）
  2. 各目标盘: mkdir .staging-<txid>/ ; 写本盘分片 part.1 ; 写 meta.xl
  3. fsync 文件 → fsync 目录
  4. 各盘: rename .staging-<txid>/ → <data-dir-uuid>/
  5. 统计 rename 成功数：
        ≥ write_quorum  ⇒ 提交成功
        <  write_quorum ⇒ 尽力回滚 + 返回 ErasureWriteQuorum
  6. 覆盖写：旧 data-dir 经「多盘投票」确认为多数派取代后，才允许 GC
```

### 12.2 承诺的边界（重要）

**唯一硬承诺**：绝不在低于 quorum 时报告成功。

**不承诺**：失败时不留下任何字节。回滚是 **best-effort** 的，残留由对账清理
（`rollback_failed_rename` / `record_indeterminate_rename` / `reclaim_orphan_data_dirs`）。

> 这条边界必须写进代码注释与运维文档。把它说成「原子且不留痕」会误导后续实现者去写不可靠的清理逻辑。

### 12.3 崩溃点

需要专门测试的窗口（见 §19）：

| 崩溃点 | 期望恢复行为 |
|---|---|
| rename 前（仅有 staging） | 重启后 staging 目录被识别为孤儿并回收，对象不可见 |
| 部分盘 rename 完成 | 达到 quorum 则可见；否则残留 data-dir 由对账回收 |
| rename 后、旧版本 GC 前 | 新旧版本并存，读路径按 `mod_time` 选最新 |
| 旧版本 GC 中 | 至少保留多数派副本，不得出现「新旧都没有」 |

### 12.4 单节点阶段的简化

单节点多盘下，rename 就是本地 rename，不存在网络分区。但协议**按分布式语义实现**：
以「每盘的 ack 计数」判 quorum，而不是「本地 rename 是否返回 Ok」。
这样扩成多节点时只需把 `LocalDisk` 换成 `RemoteDisk`，协议不变。

---

## 13. I/O 管线与变换层

### 13.1 定位

MVP **不做压缩与加密**，但管线从第一天就是可组合的，避免未来在读写路径上开洞。

```rust
pub type ReadStream = dyn AsyncRead + Unpin + Send + Sync;

pub trait Reader: ReadStream {           // 能力是可选 opt-in，而非必须实现
    fn etag(&self) -> Option<&str> { None }
    fn content_hash(&self) -> Option<&[u8]> { None }
}

/// 变换层：读写两侧对称
pub trait ObjectTransformer: Send + Sync {
    fn wrap_write(&self, r: Box<Reader>) -> Box<Reader>;
    fn wrap_read(&self, r: Box<Reader>, meta: &ObjectMeta) -> Box<Reader>;
    fn name(&self) -> &'static str;
}
```

MVP 的唯一实现是 `IdentityTransformer`。

### 13.2 装配点唯一

变换层的组装只在 `rstore-store::io::pipeline` 一处发生。写路径与读路径的层序**必须互逆**：

```
写: plaintext → [Compress] → [Encrypt] → checksum 计算挂在最外层
读: ciphertext → [Decrypt] → [Decompress] → 校验
```

> RustFS 的 `ARCHITECTURE.md` 把顺序简写成「encrypt → compress → hash」，与代码不符（实际写路径是 compress → encrypt），造成长期误导。本项目把层序列为一等契约，并在 §19 用往返测试保证读写对称。

### 13.3 缓冲池

四级缓冲池，按尺寸选择，Drop 时自动归还：

| 级别 | 上限 |
|---|---|
| SMALL | 64 KiB |
| MEDIUM | 512 KiB |
| LARGE | 4 MiB |
| XLARGE | 16 MiB |

每级 = `Semaphore`（容量）+ `Mutex<Vec<BytesMut>>`（空闲表）。
`PooledBuffer` 持 `ManuallyDrop<BytesMut>` + `OwnedSemaphorePermit`；**归还路径用 `try_lock`，不阻塞**（否则 Drop 可能卡住 runtime）。池耗尽时降级为直接分配，不阻塞。

---

## 14. 请求链路

### 14.1 HTTP 栈

- 数据面：`s3s` 的 `S3Service` / `S3` trait，跑在 hyper 上；
- 中间件：`tower` / `tower-http`；
- 单端口同时服务 S3 数据面与运维端点（`/metrics`、`/health`、`/ready`）。

### 14.2 每连接服务栈（由外到内）

```
AddExtension(RemoteAddr)
  → CatchPanic            # 任何 panic 转 500，不拖垮进程
  → RateLimit             # 每连接/每 IP 限流
  → ReadinessGate         # 未就绪 → 503 + Retry-After
  → RequestId             # 生成/透传请求 id，注入日志
  → Trace / Logging
  → CompatStack           # 见 §15，薄
  → s3s S3Service         # 协议主干
```

> 层数是 **7 层显式 + compat 栈**。RustFS 有 14 层 `*CompatLayer`，本项目按 P6 只保留有失败测试支撑的层。

### 14.3 PUT 全链路

```
1. hyper 收包 → 服务栈
2. s3s 解析 Authorization，调 AuthProvider 取 secret key 验签（SigV4）
3. rstore-s3 的 impl S3::put_object
4. 校验：bucket 存在、key 合法（含保留名规则）、storage class、content-length 上限
5. 构造 Reader → 应用变换层（MVP 恒等）→ 包 checksum layer
6. store.put_object():
     选 pool → 选 set → 算分布排列 → 打开 N 个 Disk writer（bitrot writer）
     → erasure::encode → MultiWriter 并行写
     → 攒够 write_quorum → 写 meta.xl → 并行 rename
7. 返回 ETag 与版本信息
```

### 14.4 GET 全链路

```
1-3 同上（改用 GetObjectAction）
4. 解析 Range → HTTPRangeSpec
5. 选 set → 找到 data-dir（内存缓存优先）→ 读各盘 meta.xl
6. 元数据仲裁 → 得到权威 FileInfo
7. 小对象/内联：直接返回内联数据（免盘读）
   否则：建 bitrot readers → erasure::decode → 流式返回
8. 输出层：流式 body + Content-Length + Accept-Ranges + Content-Range
```

### 14.5 Multipart

- **Create**：在 `<bucket>/.rstore.uploads/<upload-id>/` 建目录，写 `upload.meta`；
- **UploadPart**：每个 part 独立纠删编码，落在 `part.<n>/<data-dir-uuid>/`，与普通 PUT 走同一编码路径；
- **Complete**：校验各 part 的存在与 etag，组装最终 `meta.xl`（parts 数组），
  rename 到目标 `<object-path>/<data-dir-uuid>/`，然后删除 upload 目录；
- **Abort**：删除 upload 目录，回收分片。

---

## 15. 兼容层

### 15.1 范围

只做**客户端生态兼容**：aws-cli / mc / boto3 / rclone / 主流 SDK 能直接连上来并正确工作。

**不做**：MinIO 盘格式、MinIO Admin API / Console 协议、双向复制。

### 15.2 结构

```
rstore-s3          协议主干：路由、XML 序列化、错误映射、SigV4 接入
rstore-s3-compat   行为差异：tower 中间件，每条必须可追溯到失败测试
```

### 15.3 准入规则（P6 的落地）

新增一条 compat 中间件，**必须**同时提交：

1. 一条注释，格式固定：
   ```rust
   // compat: <客户端> — <观察到的行为> — see tests/compat/<name>.rs
   ```
2. 一个回归测试，在移除该中间件时会失败。

没有失败测试 → 不允许新增。**不预置空层。** 这条规则是对 RustFS「14 层 CompatLayer 里有若干层无法说明来源」的直接改进。

### 15.4 已知需要覆盖的行为

| 行为 | 说明 |
|---|---|
| ListObjectsV2 分页 | `continuation-token` / `max-keys` / `delimiter` 与 `CommonPrefixes` 的边界 |
| 虚拟主机风格寻址 | `bucket.host` 形式的请求要正确解析 bucket |
| 条件请求 | `If-Match` / `If-None-Match` / `If-Modified-Since` 的 304/412 语义 |
| 错误 XML 格式 | 必须含 `Code` / `Message` / `Resource` / `RequestId` |
| 空 body 的 Content-Length | 部分客户端对 `PUT` 空对象处理不一致 |
| HEAD 的 Content-Length | 必须回真实的 `Content-Length`，但不得回 body |
| 路径中的双斜杠 | 某些客户端/list 操作会发出 `//` |

---

## 16. 并发控制与锁

### 16.1 单节点阶段

- 按 `(bucket, object_key)` 分片的 `RwLock`，避免全局锁竞争；
- **写路径与 heal 共用同一把锁**：heal 必须在对象写锁下运行，保证永不与在飞 PUT 交错；
- 锁表按 key 哈希分片，分片数 = `next_power_of_two(cpu * 4)`；
- 锁不可跨 `.await` 持有超过必要范围；不产生锁顺序反转（锁表内不存在嵌套获取）。

### 16.2 多节点阶段（设计预留，未实现）

单节点不存在 fencing 问题。进入多节点时，必须把上述锁**升级为带 quorum 的分布式租约**，并解决 RustFS 遗留的问题：

> RustFS 的 `docs/architecture/unified-object-generation.md` 描述了一套 Ballot/Generation 协议，
> 但实现只是 opt-in 的本地 UUID 复检，**不具备跨节点 generation 安全性**。
> 本项目不重蹈覆辙：在真正实现之前，文档中一律标注「未实现」，不写入任何暗示已有该保证的措辞（P8）。

### 16.3 背压

- 磁盘读：有界信号量 + 有界降级通道；
- 准入结果 `{Primary, Degraded, Unbounded, Rejected}`；**`Rejected` ⇒ 503 SlowDown**，绝不无界放行（P7）；
- 后台任务（heal / GC / 对账）走独立且有界的配额，并在前台高水位（默认 80%）时退让。

---

## 17. 错误模型与失败分级

```rust
#[non_exhaustive]
pub enum DiskError {
    /// 对象/分片不存在。参与 quorum 计数时计为「缺失」，不计为「失败」。
    NotFound,
    /// 确定性损坏：magic / CRC / 长度不符 / bitrot 不匹配 / 分布排列非法。
    /// 重试无意义，应触发 repair。
    Corrupt(CorruptKind),
    /// 瞬时故障：IO 错误、超时、连接失败。重试有意义，不计入损坏统计。
    Transient(TransientKind),
    /// 致命：只读盘、容量耗尽、配置错误。需要人工介入。
    Fatal(FatalKind),
}
```

**分级的意义**（P2）：heal 的触发条件是「观察到 `Corrupt`」，而不是「观察到任何错误」。
把两者混为一谈会导致两类错误：瞬时抖动被误判为数据损坏、或真实损坏被重试掩盖。

**HTTP 映射**：`rstore-s3` 负责把领域错误映射为 S3 错误码（`NoSuchKey` / `NoSuchBucket` /
`InvalidPart` / `SlowDown` / `InternalError` …），映射表集中在一处并有逐条单测。

---

## 18. 运维面：readiness / metrics / 日志

### 18.1 Readiness

```rust
enum SystemStage { Booting = 0, StorageReady = 1, FullReady = 2 }
```

- 单调递增，不可回退；
- `ReadinessGate` 中间件在未达 `StorageReady` 时返回 `503` + `Retry-After: 5`；
- `/health` 为存活探针（进程活着即 200），`/ready` 为就绪探针（受 stage 控制）。

> 本项目把 RustFS 的 4 段阶段简化为 3 段：MVP 没有独立的 IAM 子系统，`IamReady` 无对应物。

### 18.2 指标

- 直接暴露 **Prometheus `/metrics`**（文本格式），与 RustFS 只有 OTLP 推送不同；
- 指标名常量集中定义，且**指标开关默认关闭时 `record_*` 为 no-op**，调用方跳过 `Instant::now()`；
- 热路径用 `LazyLock` 缓存 handle；
- 关键指标：`put_duration_seconds{stage}`（stage 细分到 rename / fsync）、
  `erasure_quorum_failures_total{op}`、`bitrot_mismatch_total`、
  `buffer_pool_acquire_total{class}`、`disk_errors_total{kind}`。

### 18.3 日志

- 结构化日志（`tracing`），字段顺序固定：`event → component → subsystem → result → 上下文`；
- 敏感字段脱敏规则集中在一处（access_key / secret_key / session_token）；
- 每个请求一个 `request_id`，贯穿全链路。

---

## 19. 测试策略

这是本项目相对 RustFS 的**主要差异化投入**。RustFS 以内联单测为主，缺故障注入与崩溃点测试。

### 19.1 属性测试（proptest）

| 属性 | 说明 |
|---|---|
| 分布排列合法性 | 对任意 key 与 `N ∈ 2..=16`，`ec_dist` 恒为 `1..=N` 的严格排列 |
| 编解码往返 | 任意数据、任意 `(data, parity)` 组合，编码后再取任意 `>= data` 份分片，解码结果恒等 |
| 元数据往返 | 任意 `ObjectMeta`，encode → decode 恒等；body 懒解析与全解析结果一致 |
| 容器鲁棒性 | 对合法容器任意单字节翻转，解码必须返回 `Corrupt`，**绝不 panic** |

### 19.2 故障注入盘 `FaultyDisk`

实现 `DiskAPI` 的包装器，可配置：

- 丢弃写 / 部分写（写到一半返回 `Ok`）；
- 写入静默损坏字节（测试 bitrot 能否发现）；
- 文件截断（测试长度校验）；
- 返回 `Transient` 或 `Corrupt`；
- 在指定调用次数后开始失败（测试动态降级）。

用途：**验证 quorum 边界**。必须覆盖的用例：

| 场景 | 期望 |
|---|---|
| N-1 个盘失败，读 | 仍可读（`N-1 ≥ data`） |
| N-data 个盘失败，读 | 返回 `ErasureReadQuorum`，**不是**短数据 |
| parity 个盘失败，写 | 仍可写 |
| parity+1 个盘失败，写 | 返回 `ErasureWriteQuorum`，不报成功 |
| 少数盘 bitrot 损坏 | 读成功且触发 repair 入队 |
| 多数盘 bitrot 损坏 | `Corrupt` 向上暴露，不返回错误数据 |

### 19.3 提交协议状态机测试

把提交协议建模为显式状态机，在每个崩溃点 kill 并重建 `Pool`，断言不变量：

- **可见性**：对象要么完全可见，要么完全不可见，不存在「部分可见」；
- **可回收性**：任何残留（staging 目录、孤儿 data-dir）都能被对账流程识别并回收；
- **不丢失**：旧版本 GC 过程中断电，不得出现「新旧版本都读不到」。

### 19.4 客户端冒烟测试

脚本化真实客户端（`tests/compat/`）：

- `aws-cli`：基础 CRUD、Range、Multipart、ListObjectsV2 分页；
- `mc`：`cp` / `ls` / `cat` / `rm`；
- `rclone`：`copy` / `sync` / `check`（校验一致性）；
- `boto3`：条件请求、预签名 URL。

这些测试同时是 §15 compat 层的准入依据。

### 19.5 架构护栏

- `scripts/check-layer-deps.sh`：校验 §5 的依赖方向规则（R1–R4），CI 强制执行；
- 依赖树里出现违规边即失败，而非警告。

---

## 20. 路线图

| 阶段 | 内容 | 状态 |
|---|---|---|
| **MVP** | 单机多盘纠删码 + 核心 S3 数据面 + bitrot + 提交协议 + 测试框架 | 见 `docs/MVP.md` |
| Phase 2 | 对象版本化；压缩与 SSE 加密（走 §13 的变换层缝）；命名空间索引（解决 LIST 全扫） | 未开始 |
| Phase 3 | 多节点：`RemoteDisk`、节点间 RPC、集群发现、分布式租约与 fencing | 未开始 |
| Phase 4 | Heal 编排、scanner / 用量、弹性扩容（pool 级 rebalance） | 未开始 |

**阶段的先后依赖**：Phase 3 的前置条件是 §16.2 的分布式租约设计完成——在它落地之前，不允许把多节点标为「可用」。

---

## 21. 速查表

| 想做什么 | 去看 |
|---|---|
| 加一个 S3 操作 | `rstore-s3` 的 `impl S3` → `rstore-api` trait → `rstore-store` |
| 加一条兼容行为 | §15.3：先写失败测试，再加 compat 层 |
| 改纠删几何 / quorum | `rstore-store` 的 quorum 模块 + `rstore-erasure`，**必须**同步本文 §10 |
| 改 `meta.xl` 格式 | `rstore-meta`，**必须**同步本文 §8，并提升 minor 版本 + 加双读路径测试 |
| 改分片分布算法 | §9.3 + `format.json` 的 `distribution_algo` 字段；旧数据必须仍可读 |
| 改提交协议 | §12 + §19.3 的状态机测试 |
| 加变换层（压缩/加密） | §13，只改 `pipeline.rs` 的装配点 |
| 加指标 | §18.2，常量集中定义 |
| 加跨 crate 调用 | §5 规则 R1：先加 `rstore-api` 契约，再在唯一的 boundary 文件里绑定实现 |

---

*本文对应 Rustorage 设计 v1。任何实现偏离都必须先修改本文并说明理由——不允许文档漂移。*

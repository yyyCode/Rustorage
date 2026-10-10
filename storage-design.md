# RustFS 存储系统设计分析

> 本文是对当前工作区（`main` 分支）代码的**一次性设计分析**，用于快速建立整体认知。
> 它不追求成为权威契约——权威来源是代码本身、`ARCHITECTURE.md` 与 `docs/architecture/` 下的
> 各专题契约；一旦本文与代码冲突，以代码为准。
>
> 本文放在 `docs/analysis/`（被 `.gitignore` 忽略），不进入版本库，符合仓库
> 「一次性分析不入库」的约定。

---

## 目录

1. [总体定位与鸟瞰](#1-总体定位与鸟瞰)
2. [Crate 全景](#2-crate-全景)
3. [物理布局：Endpoint → Pool → Erasure Set → Disk](#3-物理布局endpoint--pool--erasure-set--disk)
4. [对象定位：三级寻址](#4-对象定位三级寻址)
5. [纠删码编码与 Quorum](#5-纠删码编码与-quorum)
6. [完整性：Bitrot 校验与自愈](#6-完整性bitrot-校验与自愈)
7. [元数据：xl.meta 与 filemeta](#7-元数据xlmeta-与-filemeta)
8. [I/O 管线：rio / rio-v2 / io-core](#8-io-管线rio--rio-v2--io-core)
9. [请求链路：HTTP → 磁盘](#9-请求链路http--磁盘)
10. [一致性与并发控制](#10-一致性与并发控制)
11. [后台服务与数据搬移](#11-后台服务与数据搬移)
12. [安全与身份](#12-安全与身份)
13. [可观测性](#13-可观测性)
14. [全局状态与启动生命周期](#14-全局状态与启动生命周期)
15. [设计取舍与已知问题](#15-设计取舍与已知问题)
16. [改哪里找哪里](#16-改哪里找哪里)

---

## 1. 总体定位与鸟瞰

RustFS 是一个 **S3 兼容的分布式对象存储**，在语言与架构上明确对标 MinIO：磁盘格式
（`format.json` / `xl.meta`）、放置算法、存储类语义都对 MinIO 做了兼容性设计。核心特征：

- **纠删码（Erasure Coding）** 而非副本，最小可容忍单元是「erasure set」；
- **多 pool** 部署，支持 pool 级扩容、rebalance、decommission；
- **ILM / 分层（tiering）**，可把冷对象转到远端 S3 兼容存储；
- **单二进制**，S3 API（9000）、Admin API（同端口 `/minio/`、`/rustfs/admin/`）、Console
  （9001）、节点间 RPC 全在一个进程内。

一个运行中的节点暴露：

| 面 | 入口 |
|---|---|
| S3 数据面 | 端口 9000，`s3s` crate 实现的 `S3` trait |
| Admin API | `/rustfs/admin/*`，兼容 `/minio/admin/*` |
| Console | 内嵌 SPA（axum Router），挂在 S3 路由分发之下 |
| 节点间 RPC | tonic gRPC（元数据/控制）+ HTTP 流（批量数据） |

### 数据流（PUT 为例）

```
HTTP request
  → server (hyper + s3s + tower 中间件栈：TLS/认证/限流/就绪门/压缩/CORS)
    → app/object_usecase (校验、策略、配额、Object Lock、SSE 选择)
      → storage/ecfs (S3 trait 实现 → use-case 分发；S3Access 授权)
        → storage-api traits (ObjectIO / ObjectOperations) 实现于 ECStore
          → ECStore (选 pool) → Sets (选 set) → SetDisks (选盘、纠删编码、xl.meta)
            → io_support/rio (压缩 → 加密 → 校验和)
              → disk/local (mmap / aligned pread / io_uring) 或 RemoteDisk (RPC)
```

**分层不变量**：Server → Admin/App → Storage → ecstore → rio/io-core，只允许向下依赖；
`rustfs` 二进制 crate 是唯一把所有东西拼起来的地方。

---

## 2. Crate 全景

工作区是扁平 `crates/` 布局，`Cargo.toml` 的 `[workspace].members` 是权威列表（约 50+ 个）。
按域归类：

| 域 | 主要 crate | 职责 |
|---|---|---|
| 地基 | `checksums` `common` `config` `data-usage` `heal-contracts` `scanner-metrics` `utils` | 配置模型、数据用量模型、heal 契约、扫描器遥测、工具与校验和 |
| I/O 与存储 | `ecstore` `filemeta` `rio` `rio-v2` `io-core` `io-metrics` `concurrency` `heal` `lifecycle` `lock` `replication` `scanner` `s3-client` `object-capacity` `object-data-cache` `storage-api` | 存储引擎、元数据、恢复、生命周期、复制、锁、缓存、I/O 管线 |
| 安全与身份 | `credentials` `crypto` `iam` `keystone` `kms` `policy` `security-governance` `signer` `tls-runtime` `trusted-proxies` | 凭证、认证、授权、加密、密钥管理、TLS |
| 协议与契约 | `madmin` `protos` `protocols` `s3-ops` `s3-types` `s3select-api` `s3select-query` `extension-schema` | Admin/节点间/S3/S3-Select 契约 |
| 运维集成 | `audit` `notify` `obs` `targets` `zip` | 审计、事件通知、可观测性、归档 |

### 契约层 vs 实现层

仓库有一套**显式的依赖方向护栏**（`scripts/check_architecture_migration_rules.sh`、
`scripts/check_layer_dependencies.sh`）：

- `storage-api` 是被所有上层消费的**存储契约 crate**（`ObjectIO` / `ObjectOperations` /
  `ListOperations` / `MultipartOperations` / `HealOperations` / `NamespaceLocking`），
  禁止反向依赖 `ecstore`；
- 外部 crate 只能通过 `rustfs_ecstore::api` 门面访问引擎，且每个消费方**只允许一个本地
  boundary 文件**（如 `rustfs/src/storage/storage_api.rs`、`crates/scanner/src/storage_api.rs`）；
- 叶子 crate（`config` `credentials` `crypto` `io-metrics` `madmin`）除白名单外不得依赖其他
  内部 crate。

### ECStore 门面（`crates/ecstore/src/api/mod.rs`）

门面是一个**只出不进的兼容边界**，按组导出：`storage` `layout` `disk` `error` `runtime`
`cluster` `rpc` `bucket` `config` `tier` `set_disk` `object` `erasure` `bitrot` `rebalance`
`capacity` `data_usage` `admin` `event` `global` 等。设计意图是**单调收缩**——每轮只收窄一组，
且必须先有下游编译期覆盖。`api::global` 仅限 bootstrap 写入与生命周期控制，只读运行时访问
走 `api::runtime`。

---

## 3. 物理布局：Endpoint → Pool → Erasure Set → Disk

### 3.1 层级与类型

```
Endpoint (一块盘，携带 pool_idx/set_idx/disk_idx 坐标)
  └─ Pool          = 类型 `Sets`      (crates/ecstore/src/core/sets.rs)
       └─ Erasure Set = 类型 `SetDisks` (crates/ecstore/src/set_disk/mod.rs)
            └─ Disk    = 类型 `Disk`    (crates/ecstore/src/disk/mod.rs)
```

> ⚠️ **最反直觉的命名**：`Sets` 是 **pool**，`SetDisks` 是 **erasure set**（单数形式，
> 却持有一整个 set 的盘）。`Sets.disk_set: Vec<Arc<SetDisks>>` 装的是 set 数组，不是盘数组。
> 从 MinIO 过来的读者极易读错。

**配置期静态描述**：`crates/ecstore/src/layout/disks_layout.rs` 的 `DisksLayout` 从命令行
卷参数解析出 `pools: Vec<PoolDisksLayout>`，其 `layout: Vec<Vec<String>>` 是「set × drive」
二维表。`DisksLayout::from_volumes` 处理省略号展开与 `ENV_RUSTFS_ERASURE_SET_DRIVE_COUNT`，
并通过 `possible_set_counts` / `possible_set_counts_with_symmetry` / `common_set_drive_count`
推导出 `set_count` 与 `drives_per_set`。常量：`MAX_ERASURE_SET_DRIVE_COUNT = 16`、
`SET_SIZES = [2..=16]`。

**运行时对象**：

- `ECStore`（`store/mod.rs`）：顶层，持 `pools: Vec<Arc<Sets>>`、`disk_map`、`pool_meta`、
  `rebalance_meta`、`decommission_cancelers`；
- `Sets`：一个 pool，持 `disk_set: Vec<Arc<SetDisks>>`、`format: FormatV3`、`parity_count`、
  `distribution_algo`、`ctx: Arc<InstanceContext>`；
- `SetDisks`：一个 set，持 `disks: Arc<RwLock<Vec<Option<DiskStore>>>>`、`erasure_cache`、
  `get_object_metadata_cache`、`lockers`；
- `Disk`：`enum { Local(Box<LocalDiskWrapper>), Remote(Box<RemoteDisk>) }`，统一实现
  `DiskAPI`。

### 3.2 启动构造（`ECStore::new`）

见 `crates/ecstore/src/store/init.rs`：

1. 对每个 pool 的 endpoints，`init_format::init_disks` 并发 `new_disk` → `Vec<Option<DiskStore>>`
   与 `Vec<Option<DiskError>>`；
2. `connect_load_init_formats_with_instance_ctx` 加载/创建 `format.json`；
3. `Sets::new_with_instance_ctx(disks, pool_eps, &fm, i, parity, ctx)` 把盘切片成 set：
   `set_count = fm.erasure.sets.len()`，`set_drive_count = fm.erasure.sets[0].len()`，
   对 `i in 0..set_count, j in 0..set_drive_count` 取 `idx = i * set_drive_count + j`；
   本地盘在分布式纠删模式下会被替换为实例注册的本地盘句柄；`disk_id` 为 `None` 的槽位置空；
4. `Sets` 会 spawn 一个 15s 周期的 `monitor_and_connect_endpoints_task` 做重连监控。

### 3.3 磁盘格式：`format.json` / `FormatV3`

`crates/ecstore/src/layout/format.rs`：

```rust
FormatV3 {
  version: FormatMetaVersion,        // V1 / Unknown
  format:  FormatBackend,            // "xl" (Erasure) / "xl-single" (ErasureSingle) / Unknown
  id:      Uuid,                     // deployment id（也用作 sipHash 的 key）
  erasure: FormatErasureV3 { version, this: Uuid, sets: Vec<Vec<Uuid>>, distribution_algo },
  disk_info,
}
```

- **序列化形态是 MinIO 兼容的**：`{"version":"1","format":"xl","id":...,"xl":{"version":"1|2|3",
  "this":...,"sets":[[uuid,...],...],"distributionAlgo":"CRCMOD|SIPMOD|SIPMOD+PARITY"}}`，
  并在 `test_format_v1` 中把该字面量钉死。
- `DistributionAlgoVersion`：`V1 = "CRCMOD"`、`V2 = "SIPMOD"`、`V3 = "SIPMOD+PARITY"`。
  **新建格式一律写 V3**；从 MinIO 迁移进来的保持 `CRCMOD`。
- `shared_identity()` 返回 `(version, format, id, erasure.version, erasure.sets,
  erasure.distribution_algo)`——同一个 pool 内所有盘必须一致（`this` 除外）。
- 版本仲裁：`select_format_erasure_in_quorum(&formats, 0)` 选权威格式；
  `should_init_erasure_disks(&errs)` **要求所有盘都报 `UnformattedDisk`** 才允许全新初始化——
  即「网络不可达的 peer 绝不被当作新拓扑的证据」，这比 quorum 更严格。
- MinIO 旧格式迁移是一等公民路径：`try_migrate_format` → `LegacyFormatOutcome::{Migrated,
  Incompatible, None}`，配 `PoolMetaBootstrapAuthority::{Fresh, LegacyAdoption, None}`。
- 盘上的固定名字（`disk/mod.rs`）：`.rustfs.sys`、`format.json`（`STORAGE_FORMAT_FILE`）、
  `xl.meta`、`healing.bin`。

**关键点：盘 → set 的归属不是哈希决定的**，而是格式化时一次性生成的 UUID 表
（`FormatErasureV3.sets`）持久化到每块盘，运行时按 `find_disk_index_by_disk_id` 或
扁平下标查找。哈希只用于**对象 → set 路由**。

---

## 4. 对象定位：三级寻址

### 4.1 对象 → Pool（`ECStore` 层）

`crates/ecstore/src/store/rebalance.rs` + `layout/pool_space.rs`：

- **新对象**：先 `get_pool_idx_existing_with_opts` 看对象是否已存在于某 pool
  （`skip_decommissioned` / `skip_rebalancing`）；否则 `get_available_pool_idx` ——
  构造 `ServerPoolsAvailableSpace`（跳过 suspended/rebalancing，聚合 `get_disk_infos`），
  经 `filter_max_used(100 - 100*DISK_RESERVE_FRACTION)`，再按**可用字节加权随机**选 pool。
  空间不足返回 `DiskFull`。
- **已有对象**：`internal_get_pool_info_existing_with_opts` 对**所有 pool 并发**
  `get_object_info`，按 `mod_time` 倒序取第一个成功者，容忍 `ObjectNotFound`/`VersionNotFound`。
  严格读（如删除标记解析，`require_all_pool_reads`）会把某个 pool 的读 quorum 失败**升级**为
  `ErasureWriteQuorum`，避免「某个 pool 读不了」被静默当成「对象不存在」。

> 注意：pool 选择是**容量加权随机**而非稳定哈希——同一对象在不同时刻新写入可能落到不同
> pool；只有已存在对象才通过跨 pool 扇出获得确定位置。

### 4.2 对象 → Erasure Set（`Sets` 层）

`crates/ecstore/src/core/sets.rs`：

```
get_hashed_set_index(input) =
    V1            => crc_hash(input, disk_set.len())              // CRC32-IsoHdlc mod N
    V2 / V3       => sip_hash(input, disk_set.len(), id.as_bytes()) // SipHash，key = deployment id
```

哈希原语在 `crates/utils/src/hash.rs`。这就是 MinIO 的「set pivot」逻辑；以 deployment UUID
作为 SipHash key 意味着**同一部署内路由稳定，改 deployment id 会整体重排**。

### 4.3 对象 → 分片落盘位置（`FileInfo` 层）

`crates/filemeta/src/fileinfo.rs::FileInfo::new`：

```rust
let start = CRC32IsoHdlc(object) % cardinality;
for i in 1..=cardinality { nums[i-1] = 1 + ((start + i) % cardinality); }
// distribution = nums，是 1..=N 的一个排列
```

`distribution[k]-1` 即逻辑块 `k` 的物理槽位。`is_valid_distribution` 校验它是 `1..=N` 的
严格排列（`MAX_ERASURE_SHARDS = 16`）。应用它的辅助函数在
`crates/ecstore/src/set_disk/metadata.rs`：`shuffle_disks*`、`shuffle_disks_and_parts_metadata_by_index`
等，全部用 `checked_sub(1)` + 范围检查做防御，损坏的 distribution 被跳过而不会 panic。

> 代码里**没有** `hash_order` 这个名字；等价物就是「CRC32 旋转的 distribution 数组 +
> shuffle_disks」。

---

## 5. 纠删码编码与 Quorum

规范文档：`docs/architecture/erasure-coding.md`（normative）。

### 5.1 编码后端

| 后端 | 库 | 域 | 适用 |
|---|---|---|---|
| 现代 | `rustfs-erasure-codec`（`reed-solomon-erasure` v8 的 RustFS fork） | GF(2^8)，Vandermonde | 所有新写入 + MinIO 迁移对象 |
| 遗留 | `reed-solomon-simd` v3.1 | GF(2^16) | 仅读/修复旧 RustFS `rmp_serde` 布局 |

**后端由对象的元数据决定，而不是运行时开关**：由 `uses_legacy_checksum` 选择，
算法串常量 `ERASURE_ALGORITHM = "rs-vandermonde"`。

### 5.2 几何

- `N = data_blocks + parity_blocks`，`2 ≤ N ≤ 16`（`N=1` 是单盘异常路径，parity=0）；
- 默认 parity：`default_parity_count(N)` = 1→0 / 2-3→1 / 4-5→2 / 6-7→3 / ≥8→4；
- 约束：`parity ≤ N/2`，且 STANDARD parity ≥ RRS parity；**parity 按 pool 解析**
  （`resolve_write_layout` 用该 pool 自己的盘数算）；
- `BLOCK_SIZE_V2 = 1 MiB`（每个版本记录）；
- 分片大小两条公式并存：现代 `calc_shard_size = div_ceil(block_size, data_shards)`；
  遗留 `calc_shard_size_legacy = (div_ceil + 1) & !1`（偶对齐）。注意 `filemeta` 的
  `ErasureInfo::shard_size` 一直用偶对齐形式。

### 5.3 Quorum

`crates/ecstore/src/set_disk/core/io_primitives.rs`：

```rust
default_read_quorum  = set_drive_count - default_parity_count;            // = data_blocks
default_write_quorum = data_blocks, 当 data == parity 时 +1
```

- **读 quorum = 数据块数**，解码在 `available_shards < data_blocks` 时 fail closed
  （`emit_decoded_stripe` / `StripeReadState::can_decode`），绝不静默截断；
- **写 quorum** 在 `data == parity` 的对称情形下 +1，避免 50/50 分裂；
- **删除标记（delete marker）走多数派 `N/2 + 1`**，两条路径一致；
- 读时 `available ≥ data_blocks` 但缺分片 → 正常返回**并**异步入队读修复
  （`ShardReadRepair`）。

**元数据仲裁**（`set_disk/metadata.rs`）：`object_quorum_from_meta` 从观察到的元数据推导
`(read_quorum, write_quorum)`；`find_file_info_in_quorum` 用内容身份哈希
`file_info_quorum_hash`（SHA-256，覆盖 size/flags/mod_time/transition/version_id/data_dir/parts，
**故意排除 replication-status 字段**，以免复制状态噪声劈裂 quorum）对多盘元数据分组计票，
未达 quorum 报 `ErasureReadQuorum`。

**元数据早停**被单独隔离在 `set_disk/core/metadata_quorum.rs`：`MetadataQuorumAccumulator`
用 `candidate_shard_mask: u16` 位图（分片上限 16，故**热路径零分配**），
`MetadataDiskResult::None` 表示「尚未返回」而**不算离线**（语义上刻意区分）。

### 5.4 编码/解码的并行 I/O

- **写**：`MultiWriter`（`erasure/coding/encode.rs`）把分片 `i` 写到 writer `i`，掉队/失败
  的 writer 被剔除（置 `None`）；每块后要求 `nil_count ≥ write_quorum`，否则报降级 quorum 错误。
- **读**：`ParallelReader`、`read`/`read_into_state`/`read_lockstep`、
  `demand_bound_parity_admission_limit`；跨条带会「重构数据分片并与存活校验分片比对」，
  不一致报 `InvalidData "inconsistent read source shards"`。
- 编解码器有 `codec/` 抽象：trait `ErasureDecodeEngine`，实现 `LegacyEcDecodeEngine`
  / `RustfsCodecDecodeEngine` / `CodecStreamingDecodeEngine`；`codec/workspace.rs`、
  `codec/buffer_pool.rs` 复用工作区，避免每 stripe 分配。
- `SetDisks.erasure_cache` 按 `ErasureCacheKey{data_shards,parity_shards,block_size,uses_legacy}`
  缓存编解码器外壳（上限 32），冷 GET 复用。

错误归约在 `crates/ecstore/src/disk/error_reduce.rs`：`reduce_errs` 统计并返回占主导的错误
（`None` 计为 nil，平票优先 nil），`reduce_read_quorum_errs` / `reduce_write_quorum_errs`
在主导错误数不足 quorum 时返回对应 `Erasure*Quorum`；`build_write_quorum_failure_summary`
给出 `{required, achieved, failed, total, offline_disks, ...}` 便于诊断。

---

## 6. 完整性：Bitrot 校验与自愈

### 6.1 哈希

- 生产哈希：**HighwayHash256S**（流式，32 字节），key 是 π 派生的 `MAGIC_HIGHWAY_HASH256_KEY`；
- 旧文件用固定 key 变体 `HighwayHash256SLegacy`（key `[3,4,2,1]`），读时按
  `fi.uses_legacy_checksum` 选择；
- 持久化枚举 `ChecksumAlgo { Invalid = 0, HighwayHash = 1 }`；未指定时默认 HighwayHash256S。

### 6.2 布局与计算

- `BitrotWriter::write` 把 `hash_encode(block)` **前置**到每个 block，一次向量写 →
  盘上格式是逐块交错的 `[hash][data]`；
- 文件大小：`bitrot_shard_file_size = ceil(size / shard_size) * 32 + size`
  （两个流式变体都是 32 字节摘要）；
- `BitrotReader::read` 一次性读 `[hash][data]` 并重算校验，不匹配 → `InvalidData
  "bitrot hash mismatch"`；`skip_verify` 下仍对短读返回 `UnexpectedEof`。

### 6.3 自愈

- `erasure/coding/heal.rs::Erasure::heal` 以 `write_quorum = 1` 运行，读取 heal 分片、
  交叉验证 parity，重写缺失/损坏分片；
- CRC/bitrot 不匹配被归类为 **`FileCorrupt`**（而非通用错误），这样 heal 的
  `should_heal_object_on_disk` 能识别确定性损坏；
- `ShardReadRepair` 跨条带累积 `FileCorrupt`/`FileNotFound` 观察——**少数派的损坏不会被
  健康条带「投票洗掉」**；
- 另有 `bitrot_self_test` + KAT 常量做启动自检。

### 6.4 可选的独立分片承诺

`crates/filemeta/src/shard_integrity.rs` + `crates/ecstore/src/io_support/shard_integrity.rs`
实现 SHA-256 Merkle 承诺（`shard-integrity-v1` 双前缀元数据 + `part.N.integrity.<uuid>` sidecar），
用于抵御**同长度替换攻击**。由 `RUSTFS_SHARD_INTEGRITY_WRITE` 与
`RUSTFS_SHARD_INTEGRITY_FLEET_CONFIRMED` 双开关启用，**默认关闭**。

---

## 7. 元数据：xl.meta 与 filemeta

### 7.1 容器格式（`crates/filemeta/src/filemeta/codec.rs`）

```
"XL2 "                         magic
u16 LE major = 1, minor = 3
0xc6 + bin32 len               (msgpack bin 标记)
XL_HEADER_VERSION = 3, XL_META_VERSION = 3, versions.len()
每个版本两个 msgpack bin 块：FileMetaVersionHeader（已解析） + 不透明 FileMetaVersion（懒解析）
回填 BE-u32 bin 长度
0xce + BE-u32 xxh64(meta, seed=0) as u32   ← CRC
尾部：内联 data blob（原样）
```

防损坏措施：`check_xl2_v1` 对短文件/错 magic 返回 `FileCorrupt`（**刻意区分确定性损坏与
瞬时 IO 故障**）；`versions_len > meta.len()` 在分配前就被拒绝；
每个版本的 `bin_len` 在分配前对剩余字节做校验；CRC 不匹配记
`filemeta_xl_crc_mismatch` 并返回 `FileCorrupt`。

**增量读**：`read_xl_meta_no_data(_sync)` 只读元数据前缀（受 `META_DATA_READ_DEFAULT` 约束），
同步版本与异步版本逐字节等价，便于把 open+fstat+read 折进一个 `spawn_blocking`。

### 7.2 数据模型

- `FileMeta { versions: Vec<FileMetaShallowVersion>, data: InlineData, meta_ver: u8 }`
- **浅版本** `FileMetaShallowVersion { header, meta }`：只含已解析 header + 原始 body，
  body 懒解码——列表/元数据缓存路径只看 header（如 `is_latest_delete_marker`）。
  header 必须把 nil version-id 保留为 `Some(nil)`，以便区分「空版本」。
- `VersionType { Invalid=0, Object=1, Delete=2, Legacy=3 }`、
  `Flags { FreeVersion=1<<0, UsesDataDir=1<<1, InlineData=1<<2 }`。

**V2Obj 的键**：`ID` `DDir` `EcAlgo` `EcM` `EcN` `EcBSize` `EcIndex` `EcDist` `CSumAlgo`
`PartNums` `PartETags` `PartSizes` `PartASizes` `PartIdx` `Size` `MTime` `MetaSys` `MetaUsr`。
- `ID`/`DDir` 存 **16 字节原始 UUID**，nil ⇄ None；非 16 字节则 fail closed；
- Part 数组并行且按下标对齐：`PartNums`/`PartSizes`/`PartASizes` 长度不匹配 = `FileCorrupt`
  （硬护栏），`PartETags`/`PartIdx` 是软护栏；
- `MTime` 是 unix 纳秒，恒写；`None` 编码为 0（epoch），epoch 解回 `None`；
- `FileMetaVersionHeader` v3 携带 `ec_m`/`ec_n`，使 quorum 决策**无需解析 body**。

**删除标记**（`MetaDeleteMarker`/DelObj）：恒 3 个键 `ID` `MTime` `MetaSys`；
`stable_identity` 使用域分隔 SHA-256（`rustfs-delete-marker-identity-v1\0`）。

### 7.3 双内部前缀（跨切面不变量）

`crates/utils/src/http/metadata_compat.rs`：

- `RUSTFS_INTERNAL_PREFIX = "x-rustfs-internal-"`、
  `MINIO_INTERNAL_PREFIX = "x-minio-internal-"`；
- **写时双写，读时先 RustFS 再回退 MinIO**；
- `get_str` 大小写不敏感，但 `get_bytes`（二进制 `meta_sys`）只认两个规范小写键；
- 后缀常量（`inline-data` `compression` `actual-size` `transition-status` `free-version`
  `purgestatus` `healing` …）是**载荷承重结构**，改一个就会孤立既有元数据；
- `crates/filemeta/src/metadata_keys.rs` 把用户侧键（`OBJECT_LOCK_*`、`RESTORE*`、
  `SERVER_SIDE_ENCRYPTION`、`STORAGE_CLASS`、`REPLICATION_STATUS`）逐字节钉死，并有逐字符
  变异测试——一个字节漂移就会让旧 xl.meta 读不出来（例如 restore 标记读不出就会让活数据被回收）。

### 7.4 内联数据与对象门限

- 内联载荷追加在容器 CRC 之后，由 `InlineData` 帧化（`INLINE_DATA_VER = 1`），
  msgpack map `version-key → bin`；key 由 `data_key_for_version` 给出（`"null"` 或 UUID）；
- 读路径**只由 body 标记 `meta_sys[inline-data]` 决定**；header 的 `InlineData` flag 会被写
  但不被读（容忍 MinIO 只写 body 不写 flag）；
- 门限（`crates/ecstore/src/config/storageclass.rs::should_inline`）：
  `DEFAULT_INLINE_BLOCK = 128 KiB`；版本化桶为 `inline_block/8`，否则 `inline_block`；
  压缩/加密的单次 PUT 因存储尺寸未知，改用明文 `actual_size` 推导分片大小；
- 整对象快路径：`encode_small_direct`、`encode_inline_shards_with_integrity`。

### 7.5 存储类

`config/storageclass.rs`：STANDARD（`"EC:<parity>"`）与 `REDUCED_REDUNDANCY`（默认 `"EC:1"`）。
只有这两类允许写入；AWS 的 `STANDARD_IA`/`GLACIER` 等因语义未实现而返回
`InvalidStorageClass`。历史「仅标签」对象按有效本地布局上报
（`effective_class`，`LEGACY_LABEL_BEHAVIOR = "normalized_to_effective_class"`）。

---

## 8. I/O 管线：rio / rio-v2 / io-core

### 8.1 装配点不在 rio 里

`crates/rio` 只提供**可组合的变换层**；真正的装配缝是
`crates/ecstore/src/io_support/rio.rs`，它按 Cargo feature 二选一后端并构造具体的
`WritePlan` / 读计划。

> 📌 **与 `ARCHITECTURE.md` 的措辞差异**：`ARCHITECTURE.md` 把管线简写成
> “encrypt → compress → hash”。代码里**写路径实际是 compress → encrypt**，校验和元数据
> 挂在外层；**读路径是 decrypt → decompress → hash/verify**。写路径的顺序可在
> `WritePlan::apply`（`io_support/rio.rs`）中直接读出。

### 8.2 `crates/rio` 的层

- 能力模型（`lib.rs`）：`ReadStream: AsyncRead + Unpin + Send + Sync`；
  `Reader: ReadStream + EtagResolvable + HashReaderDetector + TryGetIndex`；
  `DynReader = Box<dyn Reader>`。能力是**默认空实现 + 选择性 opt-in**，
  用宏 `delegate_reader_capabilities_generic!` 转发给 `.inner`。
- `HashReader`（`hash_reader.rs`）——核心校验和层，持有整个请求的 checksum 上下文；
  `SIZE_PRESERVE_LAYER = -1` 是「长度保持变换、尺寸待定」的哨兵。
- `EtagReader`（MD5 etag，EOF 校验，`BadDigest`）、`HardLimitReader`（严格上限 →
  `IncompleteBody`）、`LimitReader`（软上限）。
- `CompressReader`/`DecompressReader`（DEFLATE/Gzip/Zstd 分块，1 MB 块，8 字节块头 =
  类型 + 24 位长度 + CRC32；解压器有**粘性 `poisoned` 标志**）。
- `EncryptReader`/`DecryptReader`（AES-256-GCM 分帧，8 KiB 帧；帧类型
  `FRAME_TYPE_V1/V2/V2_FINAL/END`；`v2_frame_aad` 把 8 字节头 + 帧序号绑为 AEAD AAD）。
- `TeeReader`、`TrailerSource`（aws-chunked 尾部校验和）、`checksum.rs` 的
  `ChecksumType` 位标志集。

### 8.3 写路径装配（`WritePlan::apply`）

```
明文 reader ──▶ [HashReader 取 content_hash/trailer]
             ──▶ CompressReader（若启用压缩，外层再包 HashReader::SIZE_PRESERVE_LAYER）
             ──▶ EncryptReader（按 WriteEncryptionMode 选择：单部分/多部分、object-key/direct）
             ──▶ add_non_trailing_checksum(..., ignore_value = true) + set_trailer(trailer)
```

`ignore_value = true` 是刻意的：**避免对密文/压缩后再算一遍哈希**，真正的请求校验由内层
明文 reader 完成。

### 8.4 `rio-v2`：MinIO 磁盘格式兼容（默认不编译）

`crates/rio-v2` 提供 DARE V2 加密与 S2（`klauspost/compress/s2`）流压缩 + MinIO 索引格式：

- 开关在 `crates/ecstore/src/io_support/rio.rs`：
  `#[cfg(feature = "rio-v2")] pub use rustfs_rio_v2::*;` else `rustfs_rio::*`；
  `backend_name()` → `"rio-v2"` / `"legacy-rio"`；
- DARE V2：版本字 `0x20`，16 字节头 + 16 字节 tag，`DARE_PAYLOAD_SIZE = 64 KiB`，
  默认 AES-256-GCM，解密另支持 ChaCha20-Poly1305；
- S2 索引：头 `s2idx\x00`、尾 `\x00xdi2s`、Go 兼容 zigzag varint；
- 两个刻意的取舍：**小于 8 MiB 不写索引**（`MIN_INDEX_SIZE`，小对象无索引）；
  **单次 poll 最多 64 次就绪读**（`MAX_READY_READS_PER_POLL`）以免饿死 runtime。

> 即使编译了 rio-v2，**v2 帧的写入**还需要 `RUSTFS_ENCRYPTION_FRAME_V2` 显式开启
> （默认 false），因为滚动升级中不支持 v2 读的节点无法解密；**读**永远按帧类型自动分派。

### 8.5 `io-core`：缓冲池与并发原语

`crates/io-core` 的实际内容是**配置形状 + 少量原语**，不含调度算法本身：

- `pool.rs` —— **`BytesPool` 四级缓冲池**：`SMALL_MAX=64KB`、`MEDIUM_MAX=512KB`、
  `LARGE_MAX=4MB`；每级 = `Semaphore`（容量）+ `Mutex<Vec<BytesMut>>`（空闲表）。
  `PooledBuffer` 持 `ManuallyDrop<BytesMut>` + `OwnedSemaphorePermit`，Drop 时自动归还，
  归还路径 **`try_lock` 不阻塞**。这是 io-core → io-metrics 的唯一直接埋点
  （`record_bytes_pool_*`）。
- `io_profile.rs` —— `StorageMedia{Nvme,Ssd,Hdd,Unknown}`、`StorageProfile::for_media`、
  `IoPatternDetector`、`detect_storage_media`（Linux `/sys`、macOS `diskutil`）。
- `backpressure.rs` —— `BackpressureConfig`、`BackpressureMonitor::try_acquire`（CAS 循环）、
  `BackpressureState{Normal,Warning,Critical}`。
- `deadlock_detector.rs` —— 等待图 + DFS 环检测，`LockType`、`WaitGraphEdge`。
- `lock_optimizer.rs` —— 自适应自旋、`LockGuard<'a>`。
- `config.rs` —— `IoSchedulerConfig`（默认 `max_concurrent_reads = 32` 等）。
- `progress.rs` —— `OperationProgress`（`is_stale` 用来区分「慢传输」与「卡死」）。

> 📌 **文档漂移**：`Cargo.toml` 里把 `io-core` 描述为 “Buffered Bytes, mmap-then-copy,
> and aligned pread I/O helpers”，但 **mmap-then-copy 与 aligned pread 并不在 io-core**，
> 而在 `crates/ecstore/src/disk/local.rs`（`AlignedBuf`、`pread_direct_aligned`、
> `LocalReadCopyMethod::MmapCopy`、`pread_uring*`、`batch_shard_pread`），
> 由 `crates/ecstore/src/io_support/bitrot.rs` 的 `ShardReader::{InMemory,Chunked,Stream}` 消费；
> `io-core` 只保留了它们的**指标名**。而 I/O 调度算法的真正实现位于
> `rustfs/src/storage/concurrency/io_schedule.rs`（io-core 自己的 README 也这么写）。
> 这一处 Cargo 描述与实际归属不符，读代码时不要被误导。
>
> 另外 Linux 上有 **io_uring** 路径：`disk/uring_driver_budget.rs`、`uring_read_budget.rs`、
> `uring_read_chunks.rs`、`uring_probe.rs`。

### 8.6 `concurrency` crate：只有契约，没有运行时

`crates/concurrency` 明确「只承载共享的数据与契约类型，自己不跑后台任务」：

- `PipeBackpressurePolicy{buffer_size(4MB), high_watermark(80), low_watermark(50)}`
  的 `to_core_config()` → `io_core::BackpressureConfig` —— 这就是**存储侧管缓冲尺寸、
  io-core 管过载原语**的桥；
- `DeadlockMonitorPolicy::to_core_config()`、`GetObjectQueueSnapshot`、
  `Workers`（公平 FIFO `Semaphore` + `watch` 通道，避免 drain 等待者偷走 take 唤醒）；
- `WorkloadClass{ForegroundRead,ForegroundWrite,Metadata,Scanner,Repair,Replication}`、
  `AdmissionState`、`WorkloadAdmissionSnapshotProvider`、`foreground_pressure()`。

运行时对应物在 `rustfs/src/storage/concurrency/`：`ConcurrencyManager` 单例
（`disk_read_semaphore` + 有界降级通道 `degraded_read_semaphore` + `BytesPool` +
`IoLoadMetrics` + `IoPatternDetector` + `BandwidthMonitor`），准入结果是
`DiskReadAdmission{Primary,Degraded,Unbounded,Rejected}`——**Rejected ⇒ 503 SlowDown，
绝不无界放行**。

---

## 9. 请求链路：HTTP → 磁盘

### 9.1 HTTP 栈

- 数据面：**`s3s`** crate 的 `S3Service` / `S3` trait，跑在 **hyper 1.12 + hyper-util** 上；
- Console：**axum 0.8.9** 的 `Router`，通过 `RouterIntoService` **嵌进 s3s 的路由分发**；
- 中间件：`tower` / `tower-http`；
- 另有 HTTP/3 路径（`server/http3`）。

`s3s` 是 git 依赖（features `["minio"]`），`s3s-sigv4` 同仓。

### 9.2 路由：三面同栈

`rustfs/src/server/http.rs` 的 `start_http_server`：

1. `socket2` 建 listener（reuseaddr/reuseport/nodelay/keepalive/缓冲/backlog）；
2. `load_tls_material` / `build_acceptor_from_loaded`（含 `spawn_reload_loop` 热加载）；
3. 组装 S3 服务：
   ```rust
   let store = storage::ecfs::FS::with_server_ctx(server_ctx.clone());
   S3ServiceBuilder::new(store.clone())
     .set_auth(IAMAuth::with_server_context(access_key, secret_key, server_ctx.clone()))
     .set_access(store)
     .set_route(storage::metadata_route::with_metadata_route(
         admin::make_admin_route(...), metadata_route_host, website_route_domains, ...))
     .set_config(StaticConfigProvider(rustfs_s3_config()))
   ```
4. accept 循环 + 连接数信号量；每连接组装服务栈；
5. `http_server.serve_connection(stream, hybrid_service)` 受 `graceful.watch` 保护。

**S3 / Admin / Console 全部挂在 s3s 的 `S3Route` 机制下**，不是三个独立服务器：
- `MetadataRoute`（`rustfs/src/app/metadata_route.rs`）处理 website 域、MinIO 元数据扩展，
  其余委托给 admin route；
- `admin::make_admin_route`（`rustfs/src/admin/mod.rs`）用 `S3Router` 注册 30+ handler 模块；
- `admin/router.rs` 的 `S3Router` 持 `matchit::Router`，`call` 时把
  `/minio/admin` 规范化到 `/rustfs/admin`，按 `"{method}|{path}"` 匹配，并负责 console 的
  request/response 转换。

### 9.3 每连接服务栈（外网车道，由外到内）

`AddExtensionLayer<RemoteAddr>` → `TrustedProxyLayer` → `ExternalRequestContextLayer` →
`StsQueryApiCompatLayer` → `EmptyBodyContentLengthCompatLayer` → `CatchPanicLayer` →
`RateLimitLayer` → `SsecTransportLayer` → `ReadinessGateLayer` → `KeystoneAuthLayer` →
`InFlightLayer` → `TraceLayer` → `RequestLoggingLayer` → `CompressionLayer` +
`PathCategoryInjectionLayer` → 一堆兼容层（`S3ErrorMessageCompatLayer`、
`IcebergRestErrorCompatLayer`、`ObjectAttributesEtagFixLayer`、`ConditionalCorsLayer`、
`RedirectLayer`、`BodylessStatusFixLayer`、`HeadRequestBodyFixLayer`、
`PublicHealthEndpointLayer`、`VirtualHostStyleHintLayer`、
`DoubleSlashListBucketsCompatLayer`、`SigV4HeaderGuardLayer`）。

内网车道（`/rustfs/rpc`、`/node_service.NodeService`）由 `PathDispatchService` 分流，
上面挂 4 个 tonic 服务（`NodeService` / `HealControlService` / `ScannerControlService` /
`TierMutationControlService`），统一经 `InterceptedService` + `check_auth`
（`verify_tonic_rpc_signature_with_bootstrap`）。

> 「一堆 CompatLayer」是这套设计的显著特征：**兼容性（MinIO/mc/各种 SDK 的怪癖）被显式
> 建模成中间件**，而不是散落在业务代码里。

### 9.4 认证与授权

- **入站 SigV4 由 `s3s` 库完成**；RustFS 只实现 `S3Auth::get_secret_key`
  （`rustfs/src/auth.rs` 的 `IAMAuth`），查找顺序：Keystone task-local → root → `SimpleAuth`
  → IAM store；
- 出站/客户端签名由 `crates/signer`（`sign_v4` / `pre_sign_v4` / `streaming_sign_v4` …）产生，
  被 madmin、s3-client、admin 复用；
- **授权**在 `rustfs/src/storage/access.rs` 的 `impl S3Access for FS::check`：
  `authorize_request(req, Action::S3Action(...))`，顺序是 → 桶策略显式 deny 门 →
  对 `IamSys` 求值 IAM action → allow 兜底；owner/admin 可绕过；admin 路由另有
  `route_policy.rs` + `security-governance` 的静态 `AdminRouteSpec` 矩阵。

### 9.5 PUT 全链路（具体调用顺序）

1. hyper 收包 → 外网车道中间件栈；
2. s3s 解析 `Authorization` 并调 `IAMAuth::get_secret_key` 验签；
3. `MetadataRoute::is_match/call` → admin router → 非 admin/console → 落到 S3 `S3` trait；
4. `impl S3Access for FS::check` 授权（`PutObjectAction`）；
5. `impl S3 for FS::put_object`（`storage/ecfs.rs`）→
   `s3_api::object_usecase_for(self)` → `DefaultObjectUsecase::execute_put_object`
   （`app/object/put.rs`）；
6. `execute_put_object_inner` 校验：presigned 尺寸上限、storage class、SSE-KMS/POST 拒绝、
   extract 探测、`validate_object_key`、table catalog、归档编码、body 读超时、
   权威尺寸、超限拒绝；
7. `put_object_core`：配额检查、store 查找、`validate_bucket_exists`、`admit_put_object`
   并发准入、缓冲尺寸、桶 SSE 解析、复制授权、Object Lock、prelookup/条件、构造
   `HashReader`、`sse_encryption`、`WritePlan::apply`、`PutObjReader::new`；
8. `ECStore::put_object_with_old_current_size` → `Sets::put_object_with_old_current_size`
   （`get_disks_by_key` 选 set）→ `SetDisks::put_object_with_old_current_size_inner`；
9. SetDisks 内：`WriteCommitContext`、preconditions、`erasure_from_file_info`、
   `create_bitrot_writer`、`erasure.encode_with_shard_integrity(stream, &mut writers,
   write_quorum, 1, mode, protect_write)` 分发分片；组装 `xl.meta`；
   经 `rename_data_owned_with_fence` 提交（临时目录 rename 到位并标记容量）；
10. 回到 `put.rs`：缓存失效、复制调度、事件发出（`PutObjectCompletion`）。

### 9.6 GET 全链路

与 PUT 共享 1–4 步（授权用 `GetObjectAction`，range 由 `HTTPRangeSpec` 解析）：

5. `FS::get_object` → `DefaultObjectUsecase::execute_get_object`（`app/object/get.rs`）；
6. bootstrap：`init_get_object_bootstrap`、`validate_get_object_request`、
   `prepare_get_object_request_context`；
7. `prepare_get_object_read_execution` → `store.get_object_reader` →
   `ECStore` → `Sets` → `SetDisks::get_object_reader`；
8. 盘：取读锁 → `get_object_fileinfo_for_get_object_reader` 构造 `ReadPathPlan`；
   **内联/小对象走内联快路径**（`try_read_inline_data_shards_direct`），
   否则建 bitrot readers 并 `erasure.decode`；返回 `GetObjectReader`；
9. 出 HTTP：`GetObjectReaderStream<R>`（实现 `Stream` + s3s `ByteStream`，用
   `RemainingLength::new_exact` 让 s3s 出正确的 `Content-Length`）→
   `GetObjectStreamingReader<R>`（超时、断点、生命周期、`ForegroundReadGuard`）→
   `finalize_get_object_response` 注入 `Accept-Ranges` 与
   `Content-Range: bytes {start}-{end}/{total}`。

### 9.7 Multipart

- 创建：`execute_create_multipart_upload`（SSE 会话 DEK 由 `SseKmsPrincipal` /
  `PrepareEncryptionRequest` 持有）→ `SetDisks::new_multipart_upload` 建 upload-id 目录；
- 上传分片：`execute_upload_part` → `BodyReadControl`/`ObservedBody` 控制入站读取 →
  `SetDisks::put_object_part`（分片按 PUT 同样纠删编码，落在 upload 分区目录）；
  **分片的压缩/加密在 app 层就做完了**，store 层只面对已变换的流；
- 完成：`execute_complete_multipart_upload` → `SetDisks::complete_multipart_upload`
  组装并提交最终对象，删除 upload 目录；分片校验和按
  `ChecksumType::MULTIPART`/`INCLUDES_MULTIPART` 聚合；
- 中止：`abort_multipart_upload` 删 upload 目录。

### 9.8 读取路径的偏移计算

`crates/ecstore/src/object_api/readers.rs` 的 `ReadTransform` 枚举描述了要做的变换：
`{Plain, Compressed, Encrypted{compression: Option<(algo, backend)>, part_numbers,
sequence_number, decrypt_skip, plaintext_offset/length, total_plaintext_size}}`，
并配 `get_compressed_offsets`（走分片 index 求物理偏移）、`get_encrypted_offsets`
（DARE 包算术：`DARE_PAYLOAD_SIZE`、`DARE_PACKAGE_SIZE = payload + 32`）、
`get_v2_frame_offsets`（8 KiB 帧的闭式解）。range 请求因此可以**只读需要的分片范围**，
不必整对象解密解压。

---

## 10. 一致性与并发控制

### 10.1 命名空间锁

`crates/lock`：

- `DistributedLock { namespace, clients: Vec<Arc<dyn LockClient>>, quorum }`，
  `DistributedLockGuard`（Drop 释放）、`LockLostSignal`；
- `NamespaceLockGuard{Standard(DistributedLockGuard), Fast(FastLockGuard)}`——
  快路径是分片的内存锁管理器（`fast_lock/`），标准路径才是分布式锁；
- 锁 quorum 是**每个 erasure set 各自持有**；
- 重试/对冲策略有明确常量：acquire 初始退避 250 ms、单次超时 1 s、
  `LOCK_ACQUIRE_SPARE_HEDGES = 1`；错误按前缀字符串分类
  （`remote lock rpc failed:` / `timed out:` / `unrecoverable quorum failure`）来驱动重试。
- `LockLostSignal::is_lost` 包含保守 deadline；`DistributedLockGuard::run_heartbeat`
  在瞬时 RPC 失败后**保留先前的 deadline**，只有刷新 quorum 不再成立才报丢失。

### 10.2 提交模型

- 提交动作是**目录 rename**：`rename_data(_owned|_owned_with_fence)` +
  `commit_rename_data_dir`（`set_disk/core/io_primitives.rs`）；
- **回滚是 best-effort 的**：规范明确承诺的是「**绝不在低于 quorum 时报告成功**」，
  而不是「不留任何字节」；残留由 heal/scanner 对账清理
  （`inspect_incomplete_rename_rollback`、`rollback_failed_rename`、
  `record_indeterminate_rename`、`reclaim_orphan_data_dirs`）；
- 旧目录通过 `reduce_common_data_dir` 在多盘间对 `old_data_dir` 投票到 quorum，
  确认被取代后才 GC。

### 10.3 对象 generation / fencing（**设计完成、尚未实现**）

`docs/architecture/unified-object-generation.md` 是一份很长的契约文档，值得单独注意：

- 现状是 **opt-in 的协调者 UUID 相等性复检**：`assign_object_transaction_epoch` 生成随机 UUID，
  `FileInfo::set_object_transaction_epoch` 双前缀存储，`verify_object_transaction_epoch_fence`
  在 rename 扇出前重读 quorum 元数据；
- 文档明确指出这**不是持久的分布式 CAS**，并给出 4 个反例时间表；
- 目标设计是在既有 namespace-lock 参与者组上挂一个**持久、有序的 per-object 决策协议**
  （Ballot / Generation / DecisionValue、promise/accept 两阶段、Qlock 与 Wdata 分离）；
- 开关 `RUSTFS_OBJECT_TRANSACTION_FENCING_WRITE` /
  `RUSTFS_OBJECT_TRANSACTION_FENCING_FLEET_CONFIRMED` **默认关闭**，
  且文档声明「它们不构成新协议已存在的声明」。

> 读代码时务必知道这个边界：**不要以为当前的 rename 路径具备跨节点 generation 安全性**。

### 10.4 并发/背压/超时

- `ConcurrencyManager`（`rustfs/src/storage/concurrency/manager.rs`）：磁盘读信号量 +
  有界降级通道；`DiskReadAdmission::Rejected` → 503，绝不无界放行；
  前台写有 `ForegroundWriteAdmissionGate` + 有界 multipart 等待队列；
- `rustfs/src/storage/timeout_wrapper.rs`：`GetObjectTimeoutPolicy`（默认 30 s，
  按对象大小动态调整）扩展 io-core 的 `RequestTimeoutWrapper` 并配 `CancellationToken`；
- `rustfs/src/storage/deadlock_detector.rs` 在 io-core 的检测器上扩展请求资源追踪
  （锁/内存/文件句柄）；
- 三处配置类型只定义一次（`BackpressureConfig`、`DeadlockDetectorConfig` 都在 io-core），
  存储侧通过 `to_core_config()` 投影——这是仓库明确记录的一条「不要重复定义」的约束。

---

## 11. 后台服务与数据搬移

### 11.1 Scanner / Data-Usage / Quota

- `crates/scanner`：`init_scanner_with_recovery` 启动循环，`ScannerIO`/`ScannerIOCycle`
  按 set 遍历命名空间；预算控制 `ScannerCycleBudget`（runtime / 对象数 / 目录数上限）；
- 周期状态有 magic 头 `RSCYC001` 并用 CAS 持久化（`SCANNER_PERSIST_CAS_RETRIES`）；
- **单 leader**：靠锁租约 + `SCANNER_LEADER_LOCK_POLL_INTERVAL`；
- 用量发布有 `ScannerPublicationFence` / `RootPublicationProof`，**单调不回退**；
  产物对象名 `.usage.v2.json` / `.usage.observed.json` / 旧 `.usage.json`，
  桶级 `bucket-metadata/.usage.json`，备份周期 `DATA_USAGE_BACKUP_INTERVAL_CYCLES = 10`；
- **scanner 用量是硬配额准入的权威来源**（`docs/architecture/scanner-usage-authority-decision.md`
  把这条决策固定下来），发布不可用时回退到记录的「用量下限」；
- 配额消费：`crates/ecstore/src/bucket/quota/checker.rs` 的 `QuotaChecker::check_quota`，
  另有 `QuotaLedger`/`settle` 的按 operation UUID 记账的预留协议。

### 11.2 Heal

- 职责切分（`docs/architecture/crate-boundaries.md`）：**ecstore 拥有 erasure-set 修复原语**
  （quorum 元数据仲裁、EC 重建、per-disk rename 提交、悬空元数据分类、孤儿 data-dir 回收），
  因为它们与 PUT/DELETE/multipart/lifecycle/迁移**共享同一套命名空间锁与 rename 提交模型**；
  **`crates/heal` 只拥有编排**（入队、去重、准入、调度、恢复、MRF 重放、换盘跟踪、admin 状态面）；
  scanner 可以「请求」修复但不能直接执行修复原语。
- 任务模型：`HealType{Cluster,Object,Bucket,Prefix,ErasureSet,Metadata,ECDecode,DeleteMarkerPurge}`
  × `HealPriority{Low,Normal,High,Urgent}`；
- `HealManager` + `PriorityHealQueue`（去重键 `make_dedup_key_for_type`）、
  预留 10% 队列给 best-effort（`BEST_EFFORT_QUEUE_RESERVE_PERCENT = 10`）、
  已完成 token 上限 1024、`RESUME_GC_INTERVAL = 1h`；
- **MRF（Most Recently Failed）**：`mrf_queue.rs`，日志 `buckets/.heal/mrf/journal.bin`，
  `replay_journal_once` 幂等重放；
- 三层并发隔离（`docs/architecture/heal-concurrency-model.md`）：
  ① `acquire_heal_object_lock` 走对象级命名空间写锁（与 PUT/DELETE/multipart 提交共享）；
  ② heal 使用与正常提交相同的 `commit_rename_data_dir`，因此不会与在飞 PUT 交错；
  ③ 瞬时 `set_healing`/`SUFFIX_HEALING` 标记**永不持久化**（注册为 skip key），
     `HEAL_RENAME_INCOMPLETE` 标记被中断的 rename。

### 11.3 Lifecycle / ILM 分层

- 规则求值：`crates/lifecycle` 的 `Evaluator`、`expected_expiry_time`；
  `replication_status_blocks_lifecycle` / `lifecycle_action_waits_for_replication`
  ——**与在飞复制竞争的生命周期动作会被推迟**；
- 后台执行：`crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs` 的
  `ExpiryState`/`ExpiryTask`、`TransitionState`/`TransitionWorker`、`FreeVersionTask`、
  `NewerNoncurrentTask`；`init_background_expiry`、`init_background_stale_multipart_upload_cleanup`；
- 持久化契约（`docs/architecture/ilm-tiering-persistence-contracts.md`）：
  `transition_transaction.rs`（`rustfs-transition-transaction-v1`）、
  `manual_transition_job.rs`（`-v1`）、`tier_delete_journal.rs`（v1–v6）、
  `tier_free_version_recovery.rs`、`durable_namespace.rs`、`recovery_control.rs`；
- 远端 tier 通过 **`crates/s3-client`**（不是 ecstore 内部）：
  `WarmBackendS3 { client: Arc<TransitionClient>, .. }`，构造时校验非空凭证/桶、
  解析 endpoint 并做 **SSRF `validate_endpoint` 防护**、构建 SigV4 凭证；
- tier 配置持久化在 `tier-config.bin`（`TierConfigMgr`），刷新周期 15 分钟，
  变更通过 `mutation_refresh`（Notify）驱动；凭证**密封存储**（见 §12.4），
  呈现时用 `TIER_CREDENTIAL_REDACTED`。

### 11.4 复制

- **桶复制**：`crates/ecstore/src/bucket/replication/replication_pool.rs` 的
  `ReplicationPool`（`schedule_replication` / `queue_replica_task` /
  `queue_replica_delete_batch`），MRF 落盘 `MRF_V2_FILE` + `DurableMrfBacklog`；
  重同步 `init_resync_internal` / `start_bucket_resync` / `activate_bucket_resync`；
  启动时 `spawn_bucket_resync_startup_reconcile` 对账目标意图；
- **站点复制（SR）**：`rustfs/src/site_replication/` 独立子系统——
  `SiteReplicationState`、重试队列（上限 `SITE_REPLICATION_RETRY_QUEUE_LIMIT = 256`，
  溢出走升级而非无界增长）、修复状态机（iam / bucket / bucket-metadata / replication 四族）、
  `PeerTransport`；对账循环 `site_replication_reconcile.rs`
  （`RECONCILE_INTERVAL = 600s`、`RETRY_DRAIN_INTERVAL = 30s`）；
- SR 规则用**合成 ID 前缀** `site-repl-` 与普通桶复制规则区分。

### 11.5 Decommission / Rebalance

- `PoolMeta`（`pool.bin`，`POOL_META_NAME`，schema 版本 1/2/3）持有每个 pool 的状态机：
  `queue_decommission` → `decommission` → `decommission_complete` / `decommission_failed`
  / `decommission_cancel`；CAS 写入（`POOL_META_CAS_MAX_ATTEMPTS`）；
- 并发与检查点：entry 并发默认 8（硬上限 64）、bucket 并发默认 4、
  进度保存间隔 30 s 或 1000 条、阶段
  `DECOMMISSION_STAGE_{MIGRATE_OBJECT, CLEANUP_PREFLIGHT, SOURCE_CLEANUP, ENTRY_FINISHED}`；
  容量记账在 `decommission/capacity-target`，带 TTL 10 分钟的预留与栅栏；
- Rebalance：`services/rebalance/`（`rebalance.bin`，`REBAL_META_NAME`），
  `migrate_entry_version` 做逐版本拷贝，`RebalanceStopPropagationRecord` 做跨节点停止传播；
  **与 decommission 互斥**（`is_rebalance_conflicting_with_decommission`）；
- 数据搬移统一走 `crates/ecstore/src/data_movement/`，并有独立背压
  （`wait_for_data_movement_admission`，前台读/写高水位默认 80%，`RECHECK_MS` 默认 250）
  ——**数据搬移让路给前台流量**。

### 11.6 Background Controller 契约

`docs/architecture/background-controller-contract.md` 明确：**没有通用的
`BackgroundController` trait**。各服务各自暴露状态/对账面，但共用一套词汇
（desired / current / status / reconcile / side effects），例如
`MemoryObservabilityReconcilePlan`、`AllocatorReclaimControllerSnapshot`、
`MetricsRuntimeReconcilePlan`、`HealOperationsSnapshot`、`RebalanceInfo`、
`ScannerCycleScheduleStatus`。硬约束：**对账器是电平触发、幂等、不得假设收到过启动事件**。

---

## 12. 安全与身份

### 12.1 crate 职责

> 本仓库的 MVP 只落一个 `rstore-iam`（策略文档 + 身份存储 + 求值），
> **不拆** `credentials` / `policy` 两层——M1 约 300 行，拆开只会让改一处动三个 crate。

| crate | 职责 |
|---|---|
| `iam` | `IamSys<T>` / `IamCache<T>` / `IamState` / `OidcSys`；用户、组、服务账号、策略绑定、STS |
| `policy` | `Policy` / `PolicyDoc` / `BucketPolicy` / `Statement` / `Effect`；动作分类 `S3Action`/`AdminAction`/`StsAction`/`KmsAction`；条件函数（addr/date/key/number/string…） |
| `credentials` | `Credentials`、`gen_access_key`/`gen_secret_key`、进程级 `GLOBAL_RUSTFS_RPC_SECRET`、节点间 RPC token |
| `crypto` | `encrypt_data`/`decrypt_data`、流式 `encrypt_stream_io`、JWT、license token 签名 |
| `kms` | `KmsManager`（create/describe/list key、generate/decrypt/rewrap data key）、`DataKey`（drop 时 zeroize）、`ObjectEncryptionService` |
| `signer` | SigV2/V4 签名与预签名、流式签名、trailer 签名（**出站方向**） |
| `keystone` | OpenStack Keystone 联邦：`validate_token`、role→policy 映射、`enable_tenant_prefix` |
| `security-governance` | 静态治理矩阵：`AdminRouteSpec`、`RedactionRule`、`serde_policy`、`supply_chain` |
| `trusted-proxies` | `TrustedProxyLayer` / `ProxyValidator`（云厂商/配置/代理/直连四种模式） |

### 12.2 SSE 三种模式

`rustfs/src/storage/sse.rs`（约 8.9k 行）+ `crates/utils/src/http/object_encryption_keys.rs`：

- `EncryptionMaterial { sse_type, kms_key_id, algorithm, key_bytes:[u8;32],
  base_nonce:[u8;12], encrypted_data_key: Option<Vec<u8>>, customer_key_md5,
  original_size, key_kind, managed_kms_context, managed_sealed_key }`；
- **SSE-C**（优先级 1）：每请求带客户密钥，用 `customer-key-md5` 校验；
  传输层另有 `SsecTransportLayer` 桥接；
- **SSE-S3 / SSE-KMS**（优先级 2）：从 KMS 取数据密钥，信封加密后作为
  `encrypted_data_key` 存入元数据；对象数据用 DARE 密封；受管请求缺少加密数据密钥即报错；
- 元数据同时写 RustFS 内部头与 **MinIO 兼容的 sealed-key 头**
  （`MINIO_INTERNAL_ENCRYPTION_*`），以便互读；
- **KMS 信封格式**（`crates/kms/src/encryption/dek.rs`）：`DataKeyEnvelope { key_id,
  master_key_id, key_spec, encrypted_key, nonce, encryption_context, created_at,
  master_key_version, context_binding }`，JSON 序列化；`CONTEXT_BINDING_AAD_V1` 把加密上下文
  绑为 AES-GCM AAD，旧信封无 AAD 仍可读；新写入由 `ENV_KMS_ENVELOPE_AAD`（默认关）控制。

### 12.3 授权点

- 数据面：`storage/access.rs` 的 `authorize_request`（桶策略 deny → IAM action → 兜底）；
- Admin 面：`admin/route_policy.rs` + `security-governance::admin_matrix` 的静态路由矩阵，
  `validate_admin_route_specs` 在启动/测试时校验矩阵完整性；
- 健康探针路径（`HEALTH_PREFIX`）是唯一的鉴权旁路。

> **Rustorage MVP 的偏离**（见 `docs/superpowers/specs/2026-10-10-iam-design.md`）：
> 数据面的授权点落在 s3s 的 `S3Access::check`（`crates/s3/src/iam.rs`），
> 判定顺序是「匿名 → root → 身份策略」，**没有桶策略**（那是 M6）。
> 鉴权旁路不止健康探针——`/metrics` 与 `/_console` 的静态资源也在 s3s 之前
> 被截走（`crates/server/src/startup.rs` 的 accept 循环）。

### 12.4 远端凭证密封

`docs/architecture/remote-credential-sealing-adr.md` +
`crates/ecstore/src/bucket/sealed_credentials.rs`（`SealedCredential`、`SealScope`、
`install_credential_sealer`）：复制目标、远端 tier、按需迁移源的凭证**密封后才落盘**，
消费方 `bucket_target_sys.rs` / `tier_config.rs` / `on_demand_migration/config.rs`。
远端 S3 客户端构造时做 SSRF endpoint 校验。

---

## 13. 可观测性

- **`crates/obs`** 是指标注册、schema 与导出的所有者：`Recorder` 构建 `SdkMeterProvider`；
  Prometheus 风格描述（`PrometheusMetric { name, metric_type, help, labels, value }`）
  经 `report_metrics` 转成 `metrics` crate 调用，再由 OTEL bridge 推送；
- **没有进程内 `/metrics` 抓取端点**：指标通过 OTLP（`/v1/metrics`）推出去；
  运维侧经由 admin/console 的 JSON/NDJSON handler（`handlers/realtime.rs` 的
  `MetricsHandler` 流式 NDJSON、`handlers/replication.rs`、`handlers/scanner.rs`）和
  健康/就绪端点观察；
- schema 按域分文件（`bucket`、`cluster*`、`ilm`、`replication`、`scanner`、`tier`、
  `system_*`、`request`…），collector 同构；
- 运行时循环在 `metrics/scheduler.rs`：`MetricsRuntimeController` 是对账式的
  desired/state 模型，系统指标默认 15 s；`MetricsCollectorTaskId` 枚举出全部采集任务；
- **`crates/io-metrics`** 是 I/O 指标记录的「single source of truth」，**不含 HTTP 端点**；
  三个总开关（`METRICS_ENABLED` / `PUT_STAGE_METRICS_ENABLED` / `GET_STAGE_METRICS_ENABLED`）
  默认关闭时 `record_*` 变成 no-op 且调用方跳过 `Instant::now()`；
  热路径用 `LazyLock` 缓存 handle（`counter_increment_cached!`），
  仅在 `cfg(test)` 下每次重解析；
- PUT 阶段归因常量（`PUT_STAGE_SET_DISK_RENAME_QUORUM_WAIT`、
  `...FILE_FDATASYNC`、`...RENAME_SYSCALL` …）说明这套指标是**为定位写路径瓶颈而设计的**；
- `crates/audit` 多目标审计扇出（模型同 notify），`crates/notify` 桶事件通知
  （webhook/MQTT…）带持久化与重放；
- 日志约定见 `AGENTS.md`「Logging」：模块内 `EVENT_*` / `LOG_COMPONENT_*` 常量、
  字段顺序固定（event → component → subsystem → state/result → 上下文 → 短标签），
  脱敏规则集中在 `crates/obs/src/logging.rs`（access_key=Sensitive，
  authorization/secret_key/session_token/token=Secret）。

---

## 14. 全局状态与启动生命周期

### 14.1 三层状态归属

仓库正在从「进程全局」向「实例上下文」迁移（`global-state-crate-split-plan.md`）：

```
GLOBAL_* / OnceLock（进程全局）
  → owner-local 静态
    → per-instance InstanceContext
      → AppContext / ServerContextSlot 解析门面
```

- `crates/common/src/globals.rs`：`GLOBAL_CONN_MAP`（节点间 gRPC 连接缓存）、
  `GLOBAL_ROOT_CERT`、`GLOBAL_MTLS_IDENTITY` 等底层节点/网络全局；
- `crates/ecstore/src/runtime/global.rs`：`GLOBAL_OBJECT_API: OnceLock<Arc<ECStore>>`、
  `GLOBAL_LOCK_CLIENTS`、`GLOBAL_LIFECYCLE_SYS` 等历史全局；
- `crates/ecstore/src/runtime/instance.rs`：`InstanceContext` 是比较新的载体，
  字段含 `lock_manager`、`endpoints`、`tier_config_mgr`、`replication_pool`、
  `set_drives`、`background_cancel_token`、数据搬移栅栏等。Cell 是**一次性写入、
  重复写 panic**，为嵌入式多实例（backlog#1052）留的缝；
- **对象图隔离载体**：`ECStore → Vec<Arc<Sets>> → SetDisks`，每个 SetDisks 持
  `Arc<InstanceContext>`，让实例级状态（锁命名空间、搬移 epoch、用量守卫）从所属实例解析，
  而不是走环境全局；
- `runtime/sources.rs` 是给其他 crate 用的适配层（`object_store_handle()`、
  `endpoint_pools()`、`global_lock_clients()` …），避免它们直接摸全局。

### 14.2 Readiness

`crates/common/src/readiness.rs`：`SystemStage { Booting=0, StorageReady=1, IamReady=2,
FullReady=3 }`，`GlobalReadiness::mark_stage` 单调递增。

`rustfs/src/server/readiness.rs` 的 `ReadinessGateLayer` 未就绪时返回 503 +
`Retry-After: 5` + `x-rustfs-readiness-pending`。
**`FullReady` = storage_ready AND iam_ready AND lock_quorum_ready AND peer_health_ready**。

### 14.3 启动顺序（要点）

`main.rs` → `startup_entrypoint::run_process`，随后：

1. `startup_preflight` / `startup_runtime`：环境变量兼容（`MINIO_*` → `RUSTFS_*`）、license、
   observability guard；
2. `startup_server`：listen context、HTTP 服务器骨架；
3. `startup_storage`：
   `init_startup_storage_foundation`（解析 endpoints、storage class 校验、本地盘、
   lock clients）→ `init_startup_storage_runtime`（`ECStore::new_with_instance_ctx`、
   全局配置、`mark_stage(StorageReady)`、`init_background_replication`）；
4. `startup_services`：KMS → 可选运行时 → buffer profile → deadlock detector →
   bucket metadata → IAM → audit → site-replication 对账 → auth → notification →
   background → observability → heartbeat/inventory；
5. `startup_background`：bitrot 自检、AHM cancel token、`resolve_background_service_plan`
   （**heal manager 在 scanner 或 heal 任一开启时就启动**，因为 scanner 依赖 heal channel 与
   MRF consumer）、工作负载准入快照 provider 注册；
6. `startup_lifecycle`：发布 readiness、启动事件通知对账、
   `init_scanner_with_recovery`，然后等待关闭信号；
7. `ECStore::init`（`store/init.rs`）启动存储自有服务：pool meta、rebalance meta、
   decommission 恢复与看门狗、rebalance 自动恢复、后台 expiry、stale multipart 清理、
   `TransitionState::init`、tier 配置、`BucketTargetSys` 心跳。

### 14.4 关闭顺序

`startup_shutdown.rs`：`run_startup_shutdown_sequence` →
`background_shutdown_steps`（**DataScanner 先于 AHM/heal 停止**）→
`shutdown_ahm_services`（heal manager + MRF consumer）→
`finish_detached_mutations_and_clear_unclean_shutdown_markers` →
`clear_unclean_shutdown_markers`（`DETACHED_MUTATION_SHUTDOWN_TIMEOUT = 30s`）。

不变量：**不能在还有在飞变更时清除 unclean-shutdown 标记**；所有长生命周期任务都绑定到
shutdown `CancellationToken`。

---

## 15. 设计取舍与已知问题

### 15.1 值得称道的设计

1. **兼容性被显式建模**：双内部前缀、MinIO format/xl.meta 兼容测试钉死字面量、
   `rio-v2` feature gate、14 个 `*CompatLayer` 中间件——迁移与互操作是「一等公民」而不是补丁。
2. **失败分级而不是一刀切**：`FileCorrupt`（确定性损坏）与瞬时 IO 错误区分；
   `ErasureConstructionError` 保证从不可信元数据解码几何时**不 panic**；
   结构性命中 fail closed（magic/CRC/版本过新/长度不匹配的必需数组/非 16 字节 UUID），
   值级形态 fail open（nil UUID→None、epoch→None、未知键跳过）。
3. **热路径被认真优化**：元数据早停用 `u16` 位图（分片上限 16 保证零分配）；
   编解码器按 key 缓存；缓冲池归还路径 `try_lock` 不阻塞；指标 handle `LazyLock` 缓存；
   PUT 阶段细分埋点。
4. **数据搬移让路前台**：统一的 `data_movement` 背压门，decommission/rebalance
   在 80% 高水位时退让。
5. **职责切分有明确论证**：heal 的「原语留在 ecstore、编排放在 heal crate」有书面理由
   （共享锁与提交模型），而不是按名字拆。

### 15.2 已知/观察到的问题

| 问题 | 状态 |
|---|---|
| **ecstore 是单体**（265 文件、约 28.8 万行，近一半是内联测试） | 已知，拆分计划在 `ecstore-module-split-plan.md` |
| **`SetDisks` 曾是 ~19.7k 行的上帝对象** | 已按 `SetDisksCtx` / `core::io_primitives` / `ops::*` 拆分（backlog#815/#816/#820/#821），契约实现仍是 `impl <Contract> for SetDisks` |
| **对象 generation 的分布式安全尚未实现** | 设计文档完整，实现为 opt-in 本地 UUID 复检，开关默认关 |
| **`common` 里滞留 ~83% 的 scanner/heal 领域代码** | 用于打破依赖环，属已知债务 |
| **裸 `Error` 命名** 仍有 6 个 crate（`crypto` `filemeta` `heal` `iam` `policy` `replication`） | 策略已统一到 `thiserror`，只剩命名不一致 |
| **serve 侧 `s3s` 类型仍留在 ecstore** | 由 `check_s3s_footprint.sh` 做 shrink-only 基线收口 |
| **`ARCHITECTURE.md` 的管线顺序描述与代码不符** | 见 §8.1，写路径实际是 compress → encrypt |
| **`io-core` 的 Cargo 描述与实现归属不符** | 见 §8.5，mmap/aligned pread 实际在 ecstore 的 disk 层 |
| **scanner 与 data-usage 各自持有 `.usage-cache.bin` 的序列化类型** | backlog#1828 |
| **`ReplicationStats` 一名三义**（三个无关类型同名） | backlog#1847，仅命名冲突非重复实现 |

### 15.3 「文档 ≠ 现实」的几处提醒

读这个仓库时，以下位置容易踩坑：

- `Sets` = pool，`SetDisks` = set（§3.1）；
- pipeline 顺序以 `WritePlan::apply` 为准，不以 `ARCHITECTURE.md` 的简写为准（§8.1）；
- 分布式 fencing 目前**不具备**跨节点 generation 安全（§10.3）；
- `docs/architecture/` 里的若干文档描述的是**目标态**（例如 strict mode 的实现边界表），
  文档自身通常会在正文里标注实现状态——请务必读到那一段。

---

## 16. 改哪里找哪里

| 你想做的事 | 去看 |
|---|---|
| 新 S3 操作 | `rustfs/src/storage/ecfs.rs`（`impl S3 for FS`）→ `rustfs/src/app/object/<op>.rs` |
| 新 Admin 端点 | `rustfs/src/admin/handlers/` + 注册到 `admin/router.rs`；同步 `security-governance` 的路由矩阵 |
| 新指标 | `crates/obs/src/metrics/schema/`（描述符）+ `collectors/`；I/O 类走 `crates/io-metrics` |
| 改纠删几何 / quorum | `crates/ecstore/src/set_disk/core/io_primitives.rs`、`crates/filemeta/src/fileinfo.rs`，并读 `docs/architecture/erasure-coding.md` |
| 改 xl.meta 格式 | `crates/filemeta/`，**必须**读 `docs/architecture/erasure-coding.md` §12 的变更程序（格式版本、双读路径、迁移、对抗评审） |
| 改放置/修复准入 | `docs/architecture/placement-repair-invariants.md` + `set_disk/metadata.rs` 的 shuffle |
| 改 I/O 变换 | `crates/ecstore/src/io_support/rio.rs`（装配点）+ `crates/rio/` 的层 |
| 改调度/背压 | `rustfs/src/storage/concurrency/io_schedule.rs`（真实现）+ `crates/io-core`（配置形状） |
| 改生命周期/分层 | `crates/ecstore/src/bucket/lifecycle/`，并读 `docs/architecture/ilm-tiering-persistence-contracts.md` |
| 改复制 | 桶复制 `crates/ecstore/src/bucket/replication/`；站点复制 `rustfs/src/site_replication/` |
| 改 heal | 编排 `crates/heal/`；原语 `crates/ecstore/src/set_disk/ops/heal.rs`，并读 `docs/architecture/heal-concurrency-model.md` |
| 改启动/关闭顺序 | `rustfs/src/startup_*.rs`，并读 `docs/architecture/runtime-lifecycle.md` |
| 改就绪语义 | `crates/common/src/readiness.rs` + `rustfs/src/server/readiness.rs`，并读 `docs/architecture/readiness-matrix.md` |
| 加跨 crate 的引擎调用 | 先加 `rustfs-storage-api` 契约，再在**唯一的**本地 `storage_api.rs` boundary 里别名化 |

---

*本分析基于 `main` 分支（最近提交 `a9670ebb4`）。仓库演进很快，涉及具体决策时请回到
代码与 `docs/architecture/` 下的权威契约。*

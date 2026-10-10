# Multipart 上传设计

**目标**：`rstore-server` 从「multipart 一律 501」变成真正支持分片上传，
并顺带补上 `CopyObject`，从而解除 README §7 里「单次 PUT 约 8 MiB」这条限制。

**范围**：七个 multipart 操作全做（含 `UploadPartCopy`），外加 `CopyObject`。

**依据**：`docs/DESIGN.md` §14.5、`storage-design.md` §9.7、`docs/MVP.md` 的 5.6 任务。
本文与它们有**实质偏离**，逐条列在 §12。

---

## 1. 为什么这件事比看上去大

先说三条实测出来的事实，它们决定了整个设计的形状。

### 1.1 上游已经把协议层做完了

s3s 0.17 的生成式路由（`s3s-0.17.0/src/ops/generated/router.rs`）已经按查询参数
（`?uploads`、`?uploadId`、`?partNumber`）分发了全部七个操作，`S3` trait 里七个方法
也都有默认实现。我们的 `impl_s3.rs` 一个都没覆写，所以它们落到默认实现上返回 501
（`s3_trait.rs:100/269/1084/4722/5052/8178/8399`）。

**结论：本次不碰 HTTP 层。** 这与控制面板那次的形态完全不同——不存在「路由接线」这一环，
工作全在数据面。

### 1.2 当前的写路径内存放大是 3.5×对象大小

两处都在缓冲：

- `impl_s3.rs:117–133`：PUT 的 body 被 `try_collect::<Vec<Bytes>>()` 收全，再 `.concat()`
  拼成**一个连续的 `Vec<u8>`**（峰值 ≈ 2×）；
- `crates/store/src/writer.rs:20–93`：`BitrotShardWriter` 持 `buf: Vec<u8>`，整片攒够才在
  `finish()` 里一次 `write_all`。N 块盘的分片同时驻留（≈ 1.5×，4+2 时）。

合计峰值 ≈ **3.5×对象大小**。今天被「客户端超过 8 MiB 自动改走 multipart → 501」这条
外部约束掩盖着，**一旦解除这条限制，这个放大器就直接开到对象大小**。

### 1.3 但 part 的大小与对象大小无关

这是让设计变得可行的关键：客户端自己决定 part 大小（aws-cli 默认 8 MiB、rclone 默认
5 MiB、mc 默认 64 MiB），因为它必须让 part 数落在 10000 以内。

- **UploadPart 用现有的缓冲式路径完全没问题**——峰值是 O(part)，64 MiB 的 part 也就
  ~96 MiB 分片缓冲；
- **卡住的是 Complete**：它要把所有 part 重编码成**一个整体对象**，峰值变成 O(对象大小)。

**所以「流式写路径」是这套功能真正的前置件，不是可选项**，而且它是为 Complete 做的。

---

## 2. 关键决策

### 决策一：Complete 时重新编码成整体对象（不是逐 part 落盘）

两个方案的对比：

| | A：逐 part 独立编码，Complete 只写元数据 | **B：Complete 重编码成整体（采纳）** |
|---|---|---|
| 对象盘上形态 | 多段，每段各自一个 data-dir | **与今天完全一致的单个 data-dir** |
| 读路径 | 要支持「多段流式拼接」 | **零改动** |
| 对账 / GC / heal / scrub / bitrot | 全部要学会多段结构 | **零改动** |
| Complete 代价 | O(parts) 元数据，瞬时 | O(对象大小)，读一遍 + 写一遍 |
| 单对象上限 | 5 TiB（AWS 上限） | 受 Complete 时长与 IO 约束 |

**采纳 B。** 理由不是「B 更快」——恰恰相反。理由是**改动面**：本项目的正确性投入几乎
全部押在读路径与对账上（P1、bitrot、quorum、heal）。让「对象在盘上只有一种形态」，
这些机制一个都不用动，也不用为「多段对象」把每条不变量重证一遍。用 Complete 的 IO
换取读路径零风险，在这个阶段是划算的交换。

MinIO 走的是 A，因为它是生产系统，5 TiB 单对象 + 瞬时 Complete 是硬需求。本项目当前
是单节点 MVP，「概念清晰」优先。**升级路径存在**：`ObjectBody.parts` 本来就是 `Vec`，
将来若要转 A，改的是写入侧与读侧的拼接，不是元数据格式。

### 决策二：part 用现有的 PUT 路径存成「小对象」

```
<bucket>/.rstore.uploads/<upload-id>/part.<n>/<data-dir-uuid>/…
```

而不是在 upload 目录里放裸文件。理由：白拿 quorum、原子提交、bitrot 保护，且**盘上不
引入任何新形态**——这正是决策一选 B 的理由，不该在 part 这儿破例。代价是 part 要被编码
一次、Complete 时再解码并整体编码，约 2× CPU。

这个位置**是代码里预留好的**，不是新约定：

- `consts.rs:8` 的 `RESERVED_PREFIX = ".rstore"`；
- `validate.rs:18` 已禁止用户对象 key 的第一段以它开头；
- `list.rs:116` 已在桶根跳过以它开头的条目。

### 决策三：服务端搬运一律「读源 → 写目标」

`CopyObject` 与 `UploadPartCopy` 都实现成读取源对象、写向目标，**不做零拷贝 relocation**。
最诚实、最好验证，代价是 O(对象大小) 的 IO 与一次完整往返。同样依赖 §3 的流式写路径。

---

## 3. 前置件：流式写路径

这是本次最大的一块，也是唯一动到**既有正确性代码**的一块。

### 3.1 三个改动点

**a. 磁盘层补一个追加原语。** `DiskAPI`（`crates/disk/src/lib.rs:37–49`）今天只有
`write_all(rel_path, data)`——整块写，没有 append。补一个：

```rust
/// 追加写。逐块落盘的分片写入器用它。
///
/// **必须与 `write_all` 区分开**：`write_all` 创建即截断，对同一路径反复调用
/// 只会留下最后一次；`append` 则等价于「顺序写完整个文件」。
/// **不 fsync**：调用方在最后一块之后用 `sync_file_and_parent` 收尾。
async fn append(&self, rel_path: &str, data: &[u8]) -> Result<(), DiskError>;
```

保持 `write_all` 不动（内联分支与元数据写入仍用它）。两条写的语义差别在
`crates/disk/src/lib.rs` 的契约测试里被同一个测试钉住（同一路径上 `append` 三次
得到 `abcdef`，再 `write_all` 三次得到 `XYZ`）。

> **实施时的偏离（原方案是 `create_writer` 句柄 + `DiskWriter` trait）**：
> `LocalDisk` 的每个方法都是一次无状态的 `spawn_blocking`，闭包必须 `'static + Send`、
> **不能捕获 `&self`**；持有一个写入句柄就得在句柄里放 `File`，而 `spawn_blocking`
> 的闭包要按值搬走它，于是每次写都要「取出 `File` → 用完放回」，且并发下必须有个
> `expect` 的 panic 路径。**追加原语是同一件事的无状态版本**，没有句柄要持有，
> 也不需要为并发写路径设计所有权协议。
> "句柄"方案在多节点阶段仍有价值（见 §11 的风险表），但那是 `RemoteDisk` 的事。

**b. `BitrotShardWriter` 改成增量落盘。** 块间的 `bitrot_hash(data) + data` 交错格式
不变，只是从「攒在 `buf` 里」变成「每块直接 `append`」。崩溃时留下的是
`.staging-*` 里的半截文件——而 staging 目录本来就被发现逻辑跳过、由对账回收
（`DESIGN.md` §12.3），所以**不需要新的崩溃点语义**。

> **实施时补上的两点（既有测试挡下来的）**：逐块落盘把两处原先被"缓冲"掩盖的语义
> 翻了出来。(1) 一个块都没推过时盘上不会有文件，而**空分片在读侧是合法的**
> （`bitrot_size(0, bs) == 0`），读取器靠 `stat` 判存在性、缺失会被报成 `NotFound`
> ——所以 `finish` 必须显式落一个 0 字节文件（缓冲版是靠 `write_all` 的
> "创建即截断"顺手做到的）。(2) `push_block` 从此会真的碰到盘，于是它的 IO 失败
> 第一次有了提前暴露的机会；若不处理，一块掉线的盘会让**整个 PUT** 失败，而它本该
> 只是少一票。所以调用点把 `ShardLayout`（程序 bug，中止）和 `Disk`（该盘拿不到票）
> 分开处理。

**c. `put_object` 收流而不是收 `Vec<u8>`。**

```rust
/// `crates/store` 层。
pub struct PutArgs {
    pub bucket: String,
    pub key: String,
    pub body: Box<dyn AsyncRead + Unpin + Send>,
    /// `Some` = 用给定的 ETag（multipart Complete 传合成的 `-N` 形式），
    /// `None` = 边读边算整份内容的 MD5（普通 PUT，以及 CopyObject 的常规情形——
    /// 同内容同算法，重算出来的值与源一致）。
    pub etag: Option<String>,
}
```

`crates/api` 的 `ObjectStore::put_object(bucket, key, data: Vec<u8>)` 换成
`put_object(&self, req: PutRequest)`（字段与上面同形，见下），而不是并存两条路径
——**并存会立刻分叉**，而这次的目的正是把缓冲这个洞堵死。

> **实施时的两处偏离**：
> 1. **没有 `size` 字段。** 尺寸不需要调用方给：先读**一个块**，读不满就说明整个
>    对象到此为止（`known_len` 是精确值），读满了则说明对象至少一个块，而
>    `shard_step` 只看 `size.min(BLOCK_SIZE)`、与总长无关。于是 `Content-Length`
>    和 chunked 走同一条路，`None` 也不必窥探 `DEFAULT_INLINE_BLOCK + 1` 字节再回灌。
> 2. **body 约束是 `Send`，不是 `Send + Sync`。** trait 方法的**返回值**（那个 future）
>    必须 `Send`，参数随 future 一起被捕获，因此参数也只要 `Send`。`Sync` 是白加的
>    限制，会让 `StreamReader` 这类适配器白白卡住。

### 3.2 完成判据

**用计量测试钉住，不靠肉眼**：写一个 16 MiB 的对象，断言**任何一次落盘写入都不超过
一个块**（`BLOCK_SIZE + HASH_LEN`）。这条测试就是 §1.2 那个放大器的回归防线——
没有它，将来某次改动很容易把缓冲悄悄改回来。

> **实施时的偏离（原方案是"1 GiB 对象、峰值分配 < 64 MiB"，含"分配计数探针"）**：
> **分配计数探针在这个仓库里根本做不出来**——它需要一个 `GlobalAlloc` 实现，而那是
> `unsafe`，workspace lint 是 `unsafe_code = "forbid"`。
> 可实现的等价物是**记录每次写入的 payload 长度**（`testutil::RecordingDisk`，包在
> `LocalDisk` 之外、`FaultyDisk` 之内）：整份缓冲一定表现为"一次巨大的写入"，照样能
> 抓住，而且比对象大小阈值更精确——它直接断言机制本身。
> 对象也降到 16 MiB：4+2 布局下整份缓冲时每块盘一次写出 4 MiB、逐块时是 256 KiB，
> 已经差出一个数量级，跑起来却只要 1 秒。
> **这条测试必须验证过它会红**：把 `BitrotShardWriter` 临时改回攒整份分片，
> 它报「单次写入 4194816 字节（= 4 MiB + 32）」，然后还原。
> 峰值内存的绝对值另有 acceptance 脚本与 `aws-cli` 的上传实测覆盖。

---

## 4. 七个操作 + CopyObject 的落地

| 操作 | 实现要点 |
|---|---|
| `CreateMultipartUpload` | 建 `<bucket>/.rstore.uploads/<upload-id>/`，写 `upload.meta`（bucket / key / 发起时间 / 用户元数据）。upload-id 用 `Uuid::new_v4()`。返回前**必须确认桶存在**（否则拿到一个永远 Complete 不了的 id） |
| `UploadPart` | 流式收 body，走**现有 PUT 路径**写到 `part.<n>/`。同号重传**覆盖**（幂等）。返回该 part 的 ETag |
| `CompleteMultipartUpload` | ① 解析请求体里的 parts 列表（`CompletedPart { part_number, e_tag }`）；② 逐条校验存在性 / 顺序 / ETag 匹配；③ 按序流式读出各 part，喂进流式写路径，**同时算合成 ETag**；④ 提交；⑤ 删 upload 目录 |
| `AbortMultipartUpload` | 删 upload 目录。已删/不存在 → 404 |
| `ListParts` | 列 upload 目录下的 `part.<n>/`，读各自的 `meta.xl` 取大小与 ETag。分页用 `part-number-marker` + `max-parts` |
| `ListMultipartUploads` | 扫桶根的 `.rstore/uploads/`（`list.rs:116` 今天在**跳过**它，这里需要一个显式枚举入口） |
| `UploadPartCopy` | 读源对象的指定 range，走与 `UploadPart` 相同的落盘路径。**要求源对象存在且 range 合法**（`x-amz-copy-source-range`） |
| `CopyObject` | 读源 → 写目标。ETag 与源一致（同内容同算法） |

**Complete 的 parts 列表是客户端给的，必须逐条验证**，不能信：

- 顺序必须严格递增；
- 每个 `part_number` 对应的 part 必须存在；
- 客户端给的 `e_tag` 必须与实际 part 的 ETag 相符——不符就是数据错位，必须 400 而不是
  容忍。这条是 S3 里少数几个「客户端主动参与一致性校验」的地方，放过去等于静默损坏。

---

## 5. ETag 语义

multipart 对象的 ETag **不是**整对象 MD5，而是：

```
ETag = hex( md5( concat( md5(part_1), md5(part_2), … ) ) ) + "-" + <part 数>
```

rclone 与 mc 会校验它，所以不能省。在方案 B 下，对象盘上仍是**一个** `PartInfo`
（`parts` 长度为 1），合成出来的 ETag 就存在它的 `etag` 字段里。

这直接推出了 §3.1c 里 `PutArgs.etag` / `PutRequest.etag` 的存在：默认实现是「边读边算整对象 MD5」，
而 Complete 需要**外部传入**一个不同的值。

> **注意一处语义解耦**：`PartInfo` 在盘上是「纠删编码段」，而 S3 的 part 是「客户端上传
> 单元」，方案 B 下两者**不再一一对应**。`put.rs:452` 那条注释（「N 是部分号，不是盘号」）
> 因此过时，见 §12。

---

## 6. 限制与校验

今天**一条都没有**——仓库里不存在任何 part 尺寸/数量的常量（已核实）。新增：

| 规则 | 值 | 违反时 |
|---|---|---|
| 最小 part（除最后一个） | 5 MiB | `400 EntityTooSmall` |
| 最大 part | 5 GiB | `400 EntityTooLarge` |
| 最大 part 数 | 10000 | `400 TooManyParts` |
| part 号范围 | 1..=10000 | `400 InvalidArgument` |

常量集中放在 `crates/common/src/consts.rs`，与 `DEFAULT_INLINE_BLOCK` 同处——**不许在
校验点内联字面量**，这是既有代码一贯的纪律。

`upload-id` 必须是我们生成的 UUID，且**只能落在该桶的 upload 命名空间内**：要防住
`uploadId=../../..` 这类路径穿越。这与 `validate_object_key` 是同一类防线，放在同一处。

---

## 7. 垃圾回收

客户端崩溃会留下永远 Complete 不了的 upload 目录，且里面是**实打实的 part 数据**，会
真占盘。

**方案：启动时扫描 + 可配 TTL。**

- 复用既有的孤儿回收路径，扫每个桶的 `.rstore/uploads/`；
- 超过 TTL（默认 24h，`--multipart-ttl-hours` 可配）的 upload 目录整目录删除；
- 每个删除记一条日志（含 upload-id、bucket、age），不做静默清理。

**已知代价**（写进文档而不是藏起来）：长期不重启的进程不会回收——因为没有后台任务。
这是刻意的取舍：本项目现在**没有任何后台任务**，引入一个定时器要连带来周期配置、
并发安全、以及与 shutdown 的协调，收益不成比例。`AbortMultipartUpload` 仍然是即时回收
的正路；启动扫描只是兜底。

---

## 8. 错误模型

每个操作都要有**明确的**错误码，不能靠 s3s 的默认 501 兜底——那正是 README §7 里
「漏实现与刻意不支持长得一样」那条限制的成因。

| 情形 | 码 |
|---|---|
| 桶不存在 | `NoSuchBucket` |
| upload-id 不存在 / 已 abort | `NoSuchUpload` |
| Complete 时 part 缺失 | `InvalidPart` |
| Complete 时 ETag 不符 | `InvalidPart` |
| Complete 时 parts 顺序错 | `InvalidPartOrder` |
| part 小于 5 MiB 且非最后一个 | `EntityTooSmall` |
| 非法 upload-id（含路径穿越） | `InvalidArgument` |
| 源对象不存在（Copy 类） | `NoSuchKey` |

这里有个**跨两个 crate 的连带改动**：`ApiError`（`crates/api/src/error.rs:15`）目前只有 9 个
变体，表里 `NoSuchUpload` / `InvalidPart` / `InvalidPartOrder` / `EntityTooSmall` /
`EntityTooLarge` / `TooManyParts` 六个**一个都不存在**。所以要先在 `crates/api/src/error.rs`
加变体，再在 `crates/s3/src/errors.rs:18` 的 `to_s3_error` 里逐条映射。

两边都要配单测：`crates/s3/src/errors.rs:59–83` 已有 `assert_code` 辅助函数，六个新变体照
它的既有写法各加一行即可（`DESIGN.md` §17 的既定做法）。`crates/store` 一侧则负责把
`StoreError` 归到对应的新变体上。

---

## 9. 测试策略

### 9.1 单测（`crates/s3` 与 `crates/store`）

- part 尺寸/数量边界：5 MiB 下界（正常最后一个 / 非最后一个必须拒）、10000 上界；
- ETag 合成：对固定的 part 集合断言 `-N` 格式与具体值；
- Complete 的 parts 校验：缺 part / 顺序错 / ETag 不符，三条都要红；
- upload-id 路径穿越：`../../etc` 之类必须被拒；
- **流式写路径的峰值内存**（§3.2）。

### 9.2 故障注入（复用 `FaultyDisk`）

- 某块盘在 Complete 的 rename 前失败 → 未达 quorum，对象不可见，upload 目录仍在；
- rename 后崩溃 → 达 quorum 则可见，残留由对账回收。

这两个窗口与 `DESIGN.md` §12.3 的既有表格同构，**不新增崩溃点**。

### 9.3 验收脚本 `tests/multipart.sh`

与 `tests/acceptance.sh` 同风格（先构建再起进程、轮询 `/ready`、trap 收尾）：

1. 用 `aws-cli` 传一个 **20 MiB** 的文件——今天必然 501，这就是回归判据；
2. 下载回来 `cmp` 逐字节比对；
3. `mc` 与 `rclone` 各走一遍。三个客户端的 multipart 触发阈值实测记录在
   `tests/compat/*.sh` 头部：aws-cli 8 MiB、mc 64 MiB、rclone 200 MiB。
   aws-cli 直接用第 1 步的 20 MiB。**rclone 的 200 MiB 不值得真传**——它的阈值可用
   `--s3-upload-cutoff=5Mi` 压低，压到 5 MiB 后第 1 步那个文件就足以触发 multipart。
   这一步验证的是「三个客户端各自的 multipart 实现都能跟我们对上」，与第 1 步
   （验证我们自己的语义）不重复；
4. **abort 一次**：发起一个 multipart，传一个 part，abort，确认 upload 目录被删；
5. `UploadPartCopy` 与 `CopyObject` 各跑一次，比对内容与 ETag。

### 9.4 必须改掉的既有测试

`impl_s3.rs:1420` 的 `all_six_multipart_ops_are_501_not_implemented`。

**不删，收紧。** 它的价值是把「刻意不支持」钉成可执行断言，这个价值在实现之后依然存在
——只是断言的对象从「六个都 501」变成「尚未实现的那些才 501」。删掉它等于把一个已经
验证过的边界重新变成口头约定。

---

## 10. 里程碑

| 阶段 | 内容 | 完成判据 |
|---|---|---|
| **P1 流式写路径** | `DiskAPI::append` 原语、`BitrotShardWriter` 增量落盘、`PutArgs`/`PutRequest` 收流 | 既有 210 个测试全绿（**行为不变**）；任一次落盘写入 ≤ 一个块 |
| **P2 主路径** | Create / UploadPart / Complete / Abort + 限制校验 + ETag | `tests/multipart.sh` 前三步通过；`aws-cli` 20 MiB 往返逐字节相同 |
| **P3 服务端搬运** | ListParts / ListMultipartUploads / UploadPartCopy / CopyObject | 脚本第 4、5 步通过；rclone 更新已存在对象不再退出码 1 |
| **P4 文档同步** | README §7 删掉 multipart 那条、§5 的 8 MiB 警告、DESIGN §14.5 重写、§20 路线图 | 文档不漂移 |

**P1 单独成为一阶段**是有意的：它动的是既有正确性代码，必须**行为零变化**地落地并被
既有 210 个测试证明，然后 P2 才在它上面盖楼。两件事混在一个提交里，出问题时无法二分。

---

## 11. 风险

| 风险 | 说明与对策 |
|---|---|
| **P1 把既有写路径改坏** | 最大风险：动的是 quorum/commit/bitrot 所在的那条路。对策：P1 独立成阶段，判据是「既有 210 个测试全绿 + 行为零变化」，不夹带任何 multipart 逻辑 |
| Complete 的 O(对象大小) 时长 | 客户端可能超时。MVP 可接受（单机本地 IO）；**若将来要支持 TB 级对象，必须转方案 A**，本文 §2 已写明升级路径 |
| 5 MiB 最小 part 校验踩到联调 | 有些测试工具会传极小的 part。是否放宽要由实测决定——但**默认严格**，因为 AWS 就是严格的，放宽会让「本地能跑、上云失败」 |
| `append` 对 `RemoteDisk` 的适配 | 多节点阶段，逐块追加要映射成分块上传 RPC；P1 只做 `LocalDisk`。届时可能重新引入写入句柄（见 §3.1a 的偏离说明）——`DiskAPI::append` 无状态，天然对远程实现友好 |
| `ListMultipartUploads` 要动 `list.rs` | 那里现在在**跳过** `.rstore`。改动要保证既有 LIST 行为不变（`list.rs:116` 的跳过不能顺手删掉） |

---

## 12. 与既有文档的偏离（必须同步）

按项目「不允许文档漂移」的规矩，以下六处**先改文档、再实现**：

| 位置 | 现在写的 | 本次改为 |
|---|---|---|
| `DESIGN.md` §14.5 | UploadPart 编码后**直接组装**，Complete 不重编码（= 方案 A） | 方案 B，并写明理由与升级路径 |
| `DESIGN.md` §14.5 布局 | `<bucket>/.rstore.uploads/<upload-id>/part.<n>/<data-dir-uuid>/` | 保留（与本文一致），但补上「part 是小对象」的说明 |
| `crates/store/src/put.rs:452` 注释 | 「N 是部分号（multipart 的 part），不是盘号」 | 方案 B 下 `parts` 恒为 1，S3 part 号只体现在 ETag 后缀；注释按此改写 |
| `README.md` §7 / §5 / §1 | 「不支持 multipart」「六个 multipart 操作一律 501」「载荷请小于 8 MiB」 | 删除或改写；`README.md:9` 的范围声明同步 |
| `storage-design.md` §9.7 | 借来的参考设计，写的是 `SetDisks::new_multipart_upload` / `execute_upload_part` 等**本仓库并不存在**的名字（§9.2 的「三面同栈」同样如此）。它只说 Complete「组装并提交最终对象」，**并未说明是重编码还是仅拼元数据**——A/B 之别在这份文档里是空白 | 不改其参考性质，但补一句指向本文 §2 的决策，避免读者把「组装」读成方案 A |
| `docs/MVP.md:9098` | 「先改 4.5/4.7 的存储层（多 part 目录、part 索引、ETag 的 `-n` 格式），再实现六个操作」 | 顺序修正：**先补流式写路径**，再实现七个操作 |

> **计数口径**：README 与既有测试说「六个」，s3s 的 `S3` trait 与 AWS API 都是**七个**
> （多一个 `UploadPartCopy`）。本文一律用七，并在改 README 时说明差异来源。

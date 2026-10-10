# Read/Write Path and Retrieval Acceleration

**Use this when:** you need the end-to-end data path (PUT, GET, LIST, multipart) from the S3 handler down to a disk, the cache layers that sit on that path, or the inventory of what actually makes object retrieval fast in this build — and you need to know which document owns each rule.
**Source of truth:** the code, plus the normative documents this page routes to. This page owns the *path map, the cache-layer inventory, and the retrieval-acceleration inventory*. It deliberately does not restate erasure geometry, quorum arithmetic, or the on-disk byte layout.

## What this page defers

Each row is owned elsewhere; cite that document instead of restating it here.

| Concern | Owning document |
|---|---|
| Erasure geometry, shard math, bitrot frame layout, `xl.meta` bytes, quorum rules | [erasure-coding.md](erasure-coding.md) |
| Object → set placement, per-set readiness, scanner/heal admission | [placement-repair-invariants.md](placement-repair-invariants.md) |
| File-descriptor cache keys, open modes, invalidation scope | [local-descriptor-cache.md](local-descriptor-cache.md) |
| Commit fencing, read leases, superseded-directory cleanup | [unified-object-generation.md](unified-object-generation.md) |
| Persisted scanner usage artifacts and publication fences | [scanner-usage-publication.md](scanner-usage-publication.md) |
| ILM transition persistence and tier-delete recovery | [ilm-tiering-persistence-contracts.md](ilm-tiering-persistence-contracts.md) |
| The commit surface shared by PUT, multipart, delete, and heal | [heal-concurrency-model.md](heal-concurrency-model.md) |
| Pool/set/disk layout, `FormatV3`, on-disk directory boundary | [ecstore-layout-boundary.md](ecstore-layout-boundary.md) |
| MinIO legacy-interop `xl.meta` gaps | [minio-file-format-compat.md](minio-file-format-compat.md) |

## 1. Shape in one page

A single-object request never searches. The key is hashed to a pool, then to an erasure
set, then to a per-disk shard slot, so resolving *where* an object lives costs a hash plus
one metadata read per participating disk — not a directory scan. The only operation that
physically walks the namespace is LIST (and the background scanner, §4.5).

- **Write** — one body stream is transformed (`compress → encrypt`), hash-wrapped, split
  into erasure blocks, bitrot-framed, fanned out to `N` disks with a per-block write-quorum
  check, and committed by rename + directory fsync.
- **Read** — a metadata quorum picks the authoritative version; the reader converts the
  requested byte range into per-disk shard offsets, verifies bitrot before use, and only
  pays for Reed–Solomon decode when too few shards survive.
- **List** — every participating disk walks its own directory tree and emits a sorted stream; the set/pool
  layer performs a k-way merge and de-duplicates. Resumed listings carry a cache cursor
  rather than restarting the walk.
- **Retrieval accelerators** — metadata caches with generation fences, an fd cache, an
  optional object-body cache, readahead and per-part prefetch, a tiered buffer pool, read
  admission control, batched internode reads, and fast-fail on degraded disks. There is
  **no secondary index** (§6).

## 2. Write path

### 2.1 Call chain (single PUT)

1. `rustfs/src/storage/ecfs.rs` — `ECFileSystem::put_object` (S3 `s3s` trait impl) hands off
   to the use-case layer.
2. `rustfs/src/app/object/put.rs` — `execute_put_object` → `put_object_core`: body read
   timeout, small/zero-copy body fast paths, SSE selection, and the transform pipeline (§2.2).
3. `crates/ecstore/src/store/mod.rs` — `ECStore::put_object_with_old_current_size` →
   `handle_put_object` in `crates/ecstore/src/store/object.rs`, which resolves the target
   pool via `select_put_object_pool_idx`.
4. `crates/ecstore/src/core/sets.rs` — `Sets::put_object` resolves the erasure set and disk
   list with `get_disks_by_key`.
5. `crates/ecstore/src/set_disk/ops/object.rs` — `SetDisks::put_object_with_old_current_size_inner`
   owns layout resolution (`resolve_write_layout`), content shape (`classify_put_write_path`),
   encoding, bitrot writers, and the fan-out.
6. `crates/ecstore/src/set_disk/core/io_primitives.rs` — `rename_data` drives the per-disk
   commit; each disk executes `LocalDisk::rename_data_inner` in
   `crates/ecstore/src/disk/local/commit.rs`.

### 2.2 Stream shaping

The transform chain is assembled in the use-case layer, not inside the storage engine:
`HashReader::from_stream` wraps the request body with a running hash, then
`WritePlan::apply` (`crates/ecstore/src/io_support/rio.rs`) layers **compression before
encryption**. The result is wrapped in a `PutObjReader` for the engine.

Small bodies avoid that full pipeline on their own fast paths (`read_small_put_body_exact_pooled`,
`read_zero_copy_put_body_exact`, and the `POOL_BYPASS_MAX_SIZE` threshold), reusing buffers
from the tiered `BytesPool` in `crates/io-core/src/pool.rs`.

### 2.3 Shape decision, encoding, bitrot

`classify_put_write_path` (`crates/ecstore/src/set_disk/mod.rs`) picks one of
`SmallWritePath::{Inline, SingleBlockNonInline, Pipeline, PipelineBatchedLarge}`. The inline
decision is `should_inline` (`crates/ecstore/src/config/storageclass.rs`), whose default
threshold is `DEFAULT_INLINE_BLOCK` (128 KiB) with an object-level budget of
`DEFAULT_INLINE_OBJECT_BUDGET`. Inline payloads are appended to `xl.meta` itself.

Encoding uses `Erasure::try_new_with_options` and `Erasure::shard_size` /
`shard_file_size` (`crates/ecstore/src/erasure/coding/erasure.rs`); blocks go out through
`MultiWriter` in `crates/ecstore/src/erasure/coding/encode.rs`, which writes shard *i* to
writer *i*, drops failed/short writers, and enforces the write quorum **per block**. Bitrot
frames are prepended per block by `BitrotWriter` in
`crates/ecstore/src/erasure/coding/bitrot.rs`. Integrity-protected and batched variants are
`encode_with_shard_integrity` / `encode_batched_with_integrity`.

> The geometry, shard-size formulas, frame layout, and quorum arithmetic are normative in
> [erasure-coding.md](erasure-coding.md) §3–§5. Do not re-derive them here.

### 2.4 Commit sequence

Each version owns a fresh `data_dir` UUID; shards are written under it and the commit is a
rename, never an in-place overwrite. `LocalDisk::rename_data_inner` performs, per disk:

1. write the new `xl.meta` into the temporary namespace (`SyncMode::FileOnly`), concurrently
   with `fdatasync` of the shard data (`tokio::join!`, `sync_dir_files_with_limiter`);
2. rename the **data directory** into place;
3. write the `xl.meta.bkp` rollback backup;
4. rename **`xl.meta`** into place (replacing the previous version's metadata);
5. `fsync_dst_dir_group_commit` to persist the directory entries, batching fsyncs across
   commits (`FileFdatasyncGroupCommit`, `crates/ecstore/src/disk/os.rs`).

Failure between steps is a named crash point rather than an undefined state, and rollback
restores the backup without unlinking `xl.meta`. What each durability tier persists is
governed by `DurabilityMode` (`Strict`, `Relaxed`, `None`, `LegacyOff`) in
`crates/ecstore/src/disk/local.rs`, resolved once per process and cached;
object data shards are synced under `Strict`/`Relaxed`, commit metadata only under `Strict`,
and system-critical namespaces are pinned by `effective_durability`. A write that misses its
quorum is rolled back on a **best-effort** basis — see [erasure-coding.md](erasure-coding.md) §7
for the exact guarantee.

Around the commit, two further mechanisms apply: the namespace write lock
(`acquire_write_lock_diag("put_object_commit", …)` in `crates/ecstore/src/set_disk/ops/object.rs`)
and a durable quota reservation begun before the write
(`crates/ecstore/src/bucket/quota/reservation.rs`), so concurrent writers cannot oversell.

### 2.5 Multipart

Parts live in the multipart area of `.rustfs.sys` as `part.<N>` plus `part.<N>.meta`
(`crates/ecstore/src/set_disk/ops/multipart.rs`). `complete_multipart_upload` validates the
upload, recovers interrupted part transactions (`recover_part_transactions`), reads the part
list, then fans the upload directory out to the final `(bucket, object)` location with the
same renaming commit path used by a single PUT, and finally cleans the upload directory
(`cleanup_multipart_path`). Convergence classification (`classify_rename_convergence`) is
consumed only by this path; a divergent commit enqueues heal.

## 3. Read path

### 3.1 Call chain (GET)

1. `rustfs/src/storage/ecfs.rs` — `get_object`.
2. `rustfs/src/app/object/get.rs` — `execute_get_object` → `execute_get_object_inner`;
   `prepare_get_object_read_execution` evaluates conditional headers locally and prepares
   the read. IO planning and read admission happen here (§3.2).
3. `crates/ecstore/src/store/mod.rs` — `ECStore::get_object_reader`.
4. `crates/ecstore/src/core/sets.rs` — `get_disks_by_key` → `Sets::get_object_reader`.
5. `crates/ecstore/src/set_disk/ops/object.rs` — `SetDisks::get_object_reader` takes the
   namespace read lock, consults the object-body cache hook, and dispatches inline and
   direct-memory fast paths.
6. `crates/ecstore/src/set_disk/read.rs` — `get_object_with_fileinfo` is the per-part core
   read loop.

### 3.2 Admission and IO planning

Before any disk is touched, the read is admitted (`admit_disk_read` in
`rustfs/src/storage/concurrency/manager.rs`, with a primary/degraded/rejected outcome) and an
IO strategy is computed (`calculate_io_strategy_with_context`,
`IoStrategyCore::enable_readahead` in `rustfs/src/storage/concurrency/io_schedule.rs`),
guided by the storage profile in `crates/io-core/src/io_profile.rs` — sequential large reads
(above `LARGE_SEQUENTIAL_GET_THRESHOLD_BYTES` in `rustfs/src/app/object/get.rs`) prefer
readahead, random/small reads do not.

### 3.3 Metadata resolution

Metadata is read with a **partial** read: `read_xl_meta_no_data` /
`read_xl_meta_no_data_sync` (`crates/filemeta/src/filemeta/version.rs`) uses the default
`META_DATA_READ_DEFAULT` window so the inline data blob does not have to be fetched. The
authoritative version is then selected by quorum (`find_file_info_in_quorum` in
`crates/ecstore/src/set_disk/metadata.rs`), with the rules and fail-closed behavior owned by
[erasure-coding.md](erasure-coding.md) §8.

The result — `FileInfo`, part metadata, online disks, and the resolved read quorum — is
cached in-process (§5), keyed by a per-`(bucket, object)` generation so a mutation
invalidates it without a global flush.

### 3.4 Offset math and shard reads

`FileInfo::to_part_offset` (`crates/filemeta/src/fileinfo.rs`) maps an object range onto a
part, and `Erasure::shard_file_offset` (`crates/ecstore/src/erasure/coding/erasure.rs`)
converts a part offset into a shard boundary — reads always start at a shard boundary, not at
an arbitrary byte. `adjust_shard_read_params` (`crates/ecstore/src/io_support/bitrot.rs`)
then accounts for the interleaved `[hash][data]` framing, so the requested range is expanded
by the hash bytes of every touched block.

Bitrot is checked at read time, before the bytes are handed up: `create_bitrot_reader` /
`create_bitrot_reader_from_bytes` / `create_deferred_bitrot_reader` wrap the shard, and the
cost is proportional to the range actually read, not to the object size.

### 3.5 Shard I/O

Shard sources are unified behind `ShardReader` (`crates/ecstore/src/io_support/bitrot.rs`):
`InMemory`, `Chunked`, or `Stream`. Physical reads go through
`crates/ecstore/src/disk/local.rs`:

- `StdBackend::pread_bytes` — the default buffered path, mmap-then-copy on Unix
  (`LocalReadCopyMethod::{MmapCopy, DirectReadCopy}`), page-cache backed, with an opt-in
  `MAP_POPULATE` for large reads;
- `pread_direct_aligned` — O_DIRECT reads, which reuse the already-open direct descriptor
  from the fd cache instead of re-opening;
- `pread_uring` / `pread_uring_direct` — io_uring backend when enabled;
- `batch_shard_pread` — batched reads for one disk.

### 3.6 Decode only when short

Reed–Solomon decode runs only when the surviving shards fall below `data_shards`; a healthy
read touches no decoder. The decode engines are selected per object from metadata through
`ErasureDecodeEngine` (`crates/ecstore/src/erasure/codec/bridge.rs`): the GF(2⁸) backend for
current and MinIO-migrated objects, the legacy GF(2¹⁶) backend for old RustFS files.
`ParallelReader` / `Erasure::decode` (`crates/ecstore/src/erasure/coding/decode.rs`) parallelize
stripes and cross-verify reconstruction against surviving parity. A read served from fewer
than all shards also enqueues read-repair heal.

## 4. Listing path — how retrieval searches

### 4.1 There is no index; a key is located by hashing

`get_disks_by_key` (`crates/ecstore/src/core/sets.rs`) maps an object key to a set, and the
intra-set shard order is derived from the key (`FileInfo::new`). Prefix lookup, however, has
no index to consult: it is a directory-tree walk whose results are filtered by prefix.

### 4.2 Walking one disk

`LocalDisk::walk_dir` → `scan_dir` (`crates/ecstore/src/disk/local.rs`) read directory
entries and turn them into `MetaCacheEntry` values
(`crates/filemeta/src/metacache.rs`). Two encodings matter:

- directory markers are stored as `__XLDIR__` directories and decoded back to a trailing `/`
  by `decode_dir_object` (`crates/utils/src/path.rs`);
- a regular object is a directory containing `xl.meta`.

`scan_dir` applies `filter_prefix`, `forward_to`, and `limit` while walking, and each walk
carries a stall budget so a slow disk cannot pin the listing indefinitely.

### 4.3 Merging across disks, sets, and pools

Each participating disk produces its own sorted stream (`list_path_raw` in
`crates/ecstore/src/cache_value/metacache_set.rs`), and the set/pool layer performs a k-way
merge — `list_merged` / `spawn_listing_merge` / `merge_entry_channels`
(`crates/ecstore/src/store/list_objects.rs`) — over a min-heap of channels, dropping
duplicates and letting an object shadow a same-named prefix directory. The number of disks
that must answer is a listing quorum (`latest_listing_raw_min_disks`), so one slow or failed
disk degrades latency rather than availability.

### 4.4 Pagination resumes a cursor, not the walk

`ListPathOptions` (`crates/ecstore/src/store/list_objects.rs`) carries the marker, prefix,
limit, and timeouts. A truncated response encodes a continuation (`ListContinuationV2`,
`append_list_cache_id_to_marker`) that names the list-cache id plus pool/set/source index and
generation, so the next page continues from cached cursor state. Additional pruning:
`list_path_folds_common_prefixes` folds collapsed prefixes, `stop_disk_at_limit` stops a disk
once the limit is reached, and empty-directory listing results are purged rather than served.

### 4.5 The scanner is not a retrieval index

`crates/scanner` walks the same namespace on its own cycle
(`crates/scanner/src/scanner_folder.rs`, budgeted by `crates/scanner/src/scanner_budget.rs`)
to produce persisted usage data (`.usage-cache.bin`) and prefix usage estimates
(`crates/scanner/src/prefix_usage.rs`). That data serves quota admission and ILM decisions,
**not** object lookup — no GET or LIST reads it. See
[scanner-usage-publication.md](scanner-usage-publication.md) and
[scanner-usage-authority-decision.md](scanner-usage-authority-decision.md).

## 5. Cache layers

All of these are in-process and rebuilt from disk on restart; none is a durable index.

| Layer | Key / scope | Bound | Invalidation |
|---|---|---|---|
| Object metadata (`SetDisks::get_object_metadata_cache`, `crates/ecstore/src/set_disk/mod.rs`) | `{bucket, object, generation, hash}` → `FileInfo` + parts + online disks + read quorum | TTL 2 s, 4096 entries (`RUSTFS_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES`), generation fence sharded 4096 ways | generation bump per `(bucket, object)`; insert re-checks the generation to prevent stale refill |
| Erasure codec (`ErasureCache`, same file) | `{data_shards, parity_shards, block_size, uses_legacy}` → codec | `ERASURE_CACHE_MAX_ENTRIES` | construction-only; geometry is immutable per object |
| Part file descriptors (`FdCache`, `crates/ecstore/src/disk/local.rs`) | `{volume, path, direct}` → `Arc<File>` + length | 512 entries, TTL 5 s | exact / prefix (component-boundary) / volume / clear, all generation-fenced; **only** part files — never `xl.meta` |
| Bucket metadata (`BucketMetadataSys`, `crates/ecstore/src/bucket/metadata_sys.rs`) | bucket name → `Arc<BucketMetadata>` | in-memory map; negative cache TTL 30 s / 10k entries | write/reload/remove, a 15-minute refresh loop, and peer broadcast |
| Bucket existence (`crates/ecstore/src/disk/fs.rs`) | bucket name | TTL 60 s | explicit invalidate on bucket mutation |
| Object body (`crates/object-data-cache`) | identity keys: `data_dir` + mod_time + ETag + size (+ body variant) | default **disabled**; TTL 60 s, time-to-idle 30 s, 1 MiB max entry, 5 % memory cap | write anchors (a new `data_dir`/mod_time supersedes the entry); singleflight collapses concurrent fills |
| Data usage (`crates/ecstore/src/data_usage/mod.rs`) | per bucket / snapshot | moka, config-driven | scanner publication |

Request-level details ride on top: conditional headers, ETag, and `206 Partial Content` are
evaluated locally in `rustfs/src/app/object/get.rs`; the compression middleware lives in
`rustfs/src/server/compress.rs`.

## 6. Retrieval acceleration: what exists, and what does not

**Implemented**

| Mechanism | What it buys |
|---|---|
| Hash-based placement (§4.1) | single-key lookup is O(1) routing + one metadata read per disk; no scan |
| Listing cursor continuation (§4.4) | a resumed LIST does not re-walk from the top |
| Prefix folding, early stop, empty-dir purge (§4.4) | fewer directory reads per page |
| Listing quorum + k-way merge (§4.3) | tolerant of slow/failed disks, single ordered stream |
| Object metadata cache (§5) | repeated GET/HEAD skip the metadata quorum round-trip |
| Part fd cache (§5) | a hit avoids open + metadata syscalls, including for O_DIRECT |
| Object body cache (§5) | repeated GETs of small hot objects skip shards and decode entirely |
| Per-part prefetch (`PrefetchedReaderSetup`, `crates/ecstore/src/set_disk/read.rs`) | next part's bitrot reader is set up while the current part streams |
| readahead preference + IO scheduler (`crates/io-core/src/io_profile.rs`, `io_schedule.rs`) | large sequential reads are sized to the medium; random reads are not penalized |
| Tiered buffer pool + backpressure (`crates/io-core/src/pool.rs`, `backpressure.rs`) | fewer allocations, bounded in-flight memory |
| Read admission / degraded mode (§3.2) | overload returns fast rather than queueing unboundedly |
| Batched internode reads + bulk channel (`crates/ecstore/src/cluster/rpc/remote_disk.rs`) | fewer RPCs and no head-of-line blocking behind lock/health RPCs |
| Idempotent read retry + disk fast-fail | a hanging peer is retried or evicted instead of consuming quorum latency |
| ILM transition (`crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs`, `tier_sweeper.rs`) | cold payload leaves the local disks, so local reads and free space track the hot set |
| S3 Select projection and row-group pruning (`crates/s3select-query/src/dispatcher/parquet_table.rs`, `crates/s3select-api/src/object_store.rs`) | queries fetch only needed columns and byte ranges |

**Not present — do not claim these**

- **No secondary index of any kind.** No prefix index, inverted index, or embedded metadata
  store; object and prefix lookup is directory traversal. Anything that "searches" content
  does so by reading objects (S3 Select) or by walking the namespace (scanner).
- **Scanner usage data is not a retrieval index.** It serves quota and ILM only (§4.5).
- **No predicate pushdown to storage.** S3 Select declares filter pushdown `Inexact`
  (`supports_filters_pushdown`); the storage boundary pushes down column projection and byte
  ranges, never predicates.
- **No background warmup/prefetch job.** Prefetch exists only inside a live read pipeline;
  there is no crawler that pre-populates the body cache.
- **Tiering does not shorten LIST.** Transitioned objects keep metadata locally, and the
  scanner still traverses the namespace.

## 7. Tuning entry points

| Surface | Where | Notes |
|---|---|---|
| `RUSTFS_DURABILITY_MODE` (`strict`\|`relaxed`\|`none`) | `crates/ecstore/src/disk/local.rs` | default `strict`; legacy `RUSTFS_DRIVE_SYNC_ENABLE` still maps to strict/legacy-off |
| `RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE` / `..._WRITE_ENABLE` | same file (Linux only) | writes default off |
| `RUSTFS_IO_URING_READ_ENABLE` | same file | read backend |
| `RUSTFS_OBJECT_MMAP_READ_METHOD_*`, `object_mmap_read_max_length` | `crates/ecstore/src/io_support/bitrot.rs` | mmap-then-copy vs direct-read-copy, and the length cap |
| `RUSTFS_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES` | `crates/ecstore/src/set_disk/mod.rs` | metadata cache size |
| `ObjectDataCacheConfig` (mode, `max_bytes`, `max_memory_percent`, `max_entry_bytes`, TTL) | `crates/object-data-cache/src/config.rs` | disabled by default |
| `IoSchedulerConfig`, `BytesPool` tiers, `BackpressureConfig` | `crates/io-core/src/{config,pool,backpressure}.rs` | concurrency, buffer tiers, water marks |
| Workload profiles | `rustfs/src/config/workload_profiles.rs` | opt-in per-workload buffer sizing |
| `RUSTFS_METADATA_BATCH_READ` | `crates/ecstore/src/cluster/rpc/remote_disk.rs` | batched metadata RPC mode |
| S3 Select limits (`RUSTFS_S3SELECT_*`) | `crates/s3select-query/src/instance.rs` | partitions, memory, timeout, concurrency |
| Scanner cadence (`RUSTFS_SCANNER_CYCLE`, speed) | `crates/scanner/src/scanner.rs` | affects usage freshness, not retrieval |
| Inline thresholds (`EC:` storage class, inline block) | `crates/ecstore/src/config/storageclass.rs` | changes what lands in `xl.meta` |

## 8. Trade-offs and known gaps

- **Caches are deliberately short-lived.** The 2 s metadata cache and 5 s fd cache are
  staleness budgets, not correctness boundaries; correctness comes from generation fences and
  identity write anchors. Treat a cache hit as "recently true", never as authoritative.
- **A failed commit is best-effort undone.** The commit path guarantees it never *reports*
  success below quorum; residue from a partial rollback is reconciled later by heal/scanner
  ([erasure-coding.md](erasure-coding.md) §7).
- **The fd cache is not snapshot isolation** — a reader already holding a descriptor keeps
  using the replaced inode ([local-descriptor-cache.md](local-descriptor-cache.md)).
- **Several accelerators are opt-in**, so default behavior is narrower than the code's full
  capability set: shard-integrity writes, object-transaction fencing, O_DIRECT writes, the
  object body cache, and mmap reads all require explicit enablement.
- **Documentation drift to be aware of:** `ARCHITECTURE.md` attributes "mmap-then-copy and
  aligned pread I/O helpers" to `io-core`, but as of this revision the implementations live in
  `crates/ecstore/src/disk/local.rs` (`pread_bytes`, `pread_direct_aligned`, `pread_uring*`,
  `batch_shard_pread`) and are consumed by `crates/ecstore/src/io_support/bitrot.rs`; `io-core`
  retains the metrics names. Trust the code.
- **ILM tiering persistence is partially a target, not a reality.** Transition transaction v1
  is implemented; v2, recovery-control, export, and disposition are approved targets with no
  current reader or writer ([ilm-tiering-persistence-contracts.md](ilm-tiering-persistence-contracts.md)).

## 9. Reading order for a change

1. Placing, encoding, or formatting anything → [erasure-coding.md](erasure-coding.md) first.
2. Changing how an object is *found* → [placement-repair-invariants.md](placement-repair-invariants.md),
   then this page's §4.
3. Changing a cache, a threshold, or a fast path → this page's §5–§7, then the owning module.
4. Proving a performance claim → the criterion benches under each crate's `benches/`
   (`crates/ecstore/benches/`, `crates/object-data-cache/benches/`,
   `crates/filemeta/benches/xl_meta_bench.rs`); no table in this document is a measured
   throughput guarantee.

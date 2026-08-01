# StreamDB Review Hardening Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Fix the concrete durability, determinism, speed, and usability defects found in the review of the reverse-trie KV store (DeMoD-StreamDB).

**Architecture:** Rust crate `streamdb` (`Rust/`): persistent reverse `Trie` (`im::OrdMap`) mapping keys -> `Uuid`, pluggable `Backend` (`MemoryBackend` / append-only `FileBackend` v3 format with dual CRC'd header slots). Independent C reimplementation under `C/`. Uncommitted working-tree changes already implement the v3 append-then-commit-header format; this plan hardens on top of that state.

**Tech Stack:** Rust 2021, `im`, `bincode`, `crc32fast`, `parking_lot`, `lru`, `memmap2`, criterion + proptest (dev).

---

## Review findings (evidence-backed)

### Durability
1. **GOOD (keep):** v3 format is sound — dual alternating header slots, CRC32 over header/trie/index, `sync_data` before header commit, bounds-check vs `file_len`, torn-header and crash-mid-flush tests exist (`Rust/src/storage.rs:1004,1059`).
2. **BUG — `compact()` races concurrent `write()`:** `write()` reserves its offset via `next_offset.fetch_add` *outside* the file lock (`storage.rs:619-628`). `compact()` renames a new file over the path and resets `next_offset` (`storage.rs:592-597`). A writer that reserved an offset pre-compact then blocks on `self.file` writes into the *new* file at a stale offset -> silent corruption of compacted data.
3. **BUG — `compact()` resurrects deleted docs:** compact snapshots `self.documents` early (`storage.rs:520-525`); a concurrent `delete()` then removes from the live map but the snapshot still copies the doc into the new file and its new index -> deleted data returns after reopen.
4. **GAP — no parent-dir fsync after compact rename** (`storage.rs:592`). On crash the directory entry itself may be lost; the rename must be made durable with `open(dir).sync_all()`.
5. **GAP — no inter-process file locking.** Two processes (or two `StreamDb::open` calls) on the same path each keep an independent `next_offset` and interleave appends -> guaranteed corruption. Need `flock`/`try_lock` on open.
6. **GAP — concurrent `flush()` not serialized:** two flushes load the same `seq`, both write slot `(seq+1) % 2`, second clobbers first. Benign for correctness today but wastes a commit and is fragile; serialize with a flush mutex.
7. Dead code: `MemoryBackend::write` checks `docs.get(&id)` for a freshly generated UUID (`storage.rs:104-106`) — never true; remove.

### Determinism
8. **Document index serialization order is random:** `flush()` iterates a `HashMap` (`storage.rs:722-728`) -> identical logical state produces different bytes/CRCs across process runs. Fix: sort by UUID before serializing (also makes files reproducible/diffable).
9. **Doc IDs are UUID v4 (random)** — same workload never reproduces the same file. Acceptable default; needs documentation, and tests must never assert on raw file bytes. Existing fuzz test uses fixed-seed xorshift (`storage.rs:911-928`) — good precedent, keep that pattern.
10. **Trie encoding is history-dependent:** `im::OrdMap`'s B-tree shape (hence bincode bytes) depends on insertion/deletion order, so two DBs with identical key sets can differ on disk. Correctness-neutral; document it. Do NOT attempt canonical re-encode (YAGNI).
11. Suffix-search order is deterministic (byte-sorted by *reversed* key, not lexicographic). Deterministic but surprising; document or sort results lexicographically at the `StreamDb` layer.

### Speed
12. **Hot-path allocation:** every `get`/`exists`/`delete` does `key.to_vec()` just to look up the LRU cache (`lib.rs:307,352,391`). `LruCache<Vec<u8>, _>::get` accepts `&[u8]` via `Borrow` — zero-alloc fix.
13. **Stop-the-world flush:** `StreamDb::flush` holds `self.trie.read()` across serialization *and fsync* (`lib.rs:467-468`), blocking all writers for the fsync duration. Trie is persistent — clone is O(1); snapshot, drop lock, then flush.
14. **Flush cost is O(whole trie + whole index)** every commit, appended, file grows until `compact()`. Inherent to current design; mitigate with 13 + exposing `compact()`; record a benchmark so regressions are visible.
15. **CRC verify on every read** (`storage.rs:665-668,683-686`) — throughput cap on large values. Keep default ON (durability), add `Config::verify_checksums_on_read: bool = true`.
16. No file-backend or concurrency benchmarks; `benches/benchmarks.rs` only covers memory DB and raw trie.
17. `suffix_search` collects all matches unbounded (`trie.rs:176-188`) — add a limited variant for large-k workloads.

### Usability
18. **`StreamDb::compact()` does not exist** — compaction is only reachable by downcasting the backend. Add it with correct locking.
19. **Dead config/features:** `Config::flush_interval_ms` has no consumer (no auto-flush thread anywhere); `compression`, `encryption`, `async` Cargo features compile to nothing; `Error::Transaction`/`ResourceLimit` unused. Either implement or remove — plan below removes/stubs honestly (YAGNI).
20. **`Rust/src/tests.rs` is bit-rotted and NOT COMPILED** (no `mod tests;` in `lib.rs`; references nonexistent `MemoryBackend::new("test.wal")`, `bind_path_to_document`, `sled`). Delete it.
21. **`panic = "abort"` in `[profile.release]`** (`Cargo.toml:85`) makes ffi.rs's `catch_unwind` (`ffi.rs:45`) dead in release — a panic across FFI aborts the host process. Drop `panic = "abort"` (keep unwind) so FFI panic-catching works.
22. **Two C implementations** (`C/src/streamdb.c` standalone reimpl, 1160 lines, own PAL + threading; `C/src/streamdb_wrapper.c` binding) — two sources of truth that will diverge (the standalone one predates the v3 format). Consolidate on the wrapper over the Rust staticlib.
23. README/docs: no mention of the v2->v3 on-disk break, the durability contract ("committed = `flush()` returned Ok; crash loses at most the unflushed tail"), or the need to compact.

---

## Task plan

### Phase 0 — Baseline

**Task 0: Record a green baseline**

**Step 1:**
```bash
cd Rust && cargo test --all-features 2>&1 | tail -20
cargo test 2>&1 | tail -5          # default features
cargo bench -- --quick 2>&1 | tail -30   # baseline numbers, save to /tmp/bench-baseline.txt
```
Expected: all tests pass (they do as of this review). Note: `--all-features` pulls in tokio/ring/lz4 — if any fail to build, that itself confirms finding 19; record and move on.

**Step 2: Commit**
```bash
git add -A && git commit -m "chore: baseline before review hardening"
```

---

### Phase A — Durability

**Task A1: Serialize `compact()` against writers (fixes findings 2+3)**

**Objective:** Hold the document map write-lock and file lock for the whole compaction so no `write()`/`delete()` can interleave, and reject stale-offset writes.

**Files:**
- Modify: `Rust/src/storage.rs` (`FileBackend::compact`, ~lines 495-608; `write`, ~613-643; `delete`, ~691-702)

**Step 1: Write failing test** — append to `mod tests` in `Rust/src/storage.rs`:

```rust
#[cfg(feature = "persistence")]
#[test]
fn compact_is_atomic_against_concurrent_writes_and_deletes() {
    use tempfile::tempdir;
    use std::sync::Arc;
    use std::thread;

    let dir = tempdir().unwrap();
    let path = dir.path().join("race.db");
    let (backend, mut trie) = FileBackend::open(&path, &Config::default()).unwrap();
    let backend = Arc::new(backend);

    // Seed: 20 live docs, 20 soon-to-be-deleted docs.
    let mut deleted_ids = Vec::new();
    for i in 0..40u32 {
        let id = backend.write(&vec![i as u8; 512]).unwrap();
        trie = trie.insert(format!("{}.d", i).as_bytes(), id);
        if i >= 20 { deleted_ids.push((i, id)); }
    }
    backend.flush(&trie).unwrap();

    let b2 = Arc::clone(&backend);
    let t = thread::spawn(move || {
        // Writer hammering during compaction.
        for j in 0..50u32 {
            b2.write(format!("concurrent-{j}").as_bytes()).unwrap();
        }
    });

    // Deletes racing the compaction must not resurrect.
    for (_, id) in &deleted_ids { backend.delete(*id).unwrap(); }
    let trie = {
        let mut t = trie;
        for (i, _) in &deleted_ids { t = t.remove(format!("{}.d", i).as_bytes()).unwrap(); }
        t
    };
    backend.compact(&trie).unwrap();
    t.join().unwrap();

    drop(backend);
    let (backend, trie2) = FileBackend::open(&path, &Config::default()).unwrap();
    for (i, _) in &deleted_ids {
        assert_eq!(trie2.get(format!("{}.d", i).as_bytes()), None, "deleted key {i} resurrected");
    }
    for i in 0..20u32 {
        assert!(trie2.get(format!("{}.d", i).as_bytes()).is_some());
    }
}
```

**Step 2: Run, expect failure** (resurrection and/or corruption):
```bash
cd Rust && cargo test --features persistence compact_is_atomic -- --nocapture
```
Expected: FAIL or nondeterministic corruption.

**Step 3: Fix** — in `FileBackend::compact`, hold both locks for the whole operation and rebuild `next_offset` from live state:

```rust
pub fn compact(&self, trie: &Trie) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    { *self.mmap.write() = None; }

    // Lock ordering: documents before file (matches write()/read()).
    let mut docs_guard = self.documents.write();
    let mut file = self.file.lock();

    let tmp_path = { let mut p = self.path.clone().into_os_string(); p.push(".compact"); std::path::PathBuf::from(p) };
    let mut out = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&tmp_path)?;
    out.write_all(&[0u8; DATA_START as usize])?;

    let mut new_docs = HashMap::with_capacity(docs_guard.len());
    let mut total_size = 0u64;
    let mut offset = DATA_START;
    // Iterate the LIVE map (no stale snapshot) so racing deletes are honoured.
    for (id, meta) in docs_guard.iter() {
        let mut data = vec![0u8; meta.size as usize];
        file.seek(SeekFrom::Start(meta.offset))?;
        file.read_exact(&mut data)?;
        if compute_checksum(&data) != meta.checksum {
            return Err(Error::Corrupted(format!("Document {id} failed checksum during compaction; aborting")));
        }
        out.write_u32::<LittleEndian>(meta.size)?;
        out.write_u32::<LittleEndian>(meta.checksum)?;
        out.write_all(&data)?;
        new_docs.insert(*id, DocumentMeta { offset: offset + 8, size: meta.size, checksum: meta.checksum });
        total_size += meta.size as u64;
        offset += meta.size as u64 + 8;
    }

    let trie_data = bincode::serialize(trie)?;
    let trie_crc = compute_checksum(&trie_data);
    let index = serialize_index(&new_docs)?;          // added in Task B1
    let index_crc = compute_checksum(&index);
    let trie_offset = offset;
    let index_offset = trie_offset + trie_data.len() as u64;
    out.write_all(&trie_data)?;
    out.write_all(&index)?;

    let header = FileHeader { seq: 1, trie_offset, trie_len: trie_data.len() as u64, trie_crc,
        index_offset, index_len: index.len() as u64, index_crc,
        data_end: index_offset + index.len() as u64 };
    out.seek(SeekFrom::Start((header.seq % HEADER_SLOTS) * HEADER_SLOT_SIZE))?;
    out.write_all(&header.encode())?;
    out.flush()?;
    out.sync_all()?;
    drop(out);

    std::fs::rename(&tmp_path, &self.path)?;
    sync_parent_dir(&self.path)?;                     // added in Task A2

    *file = OpenOptions::new().read(true).write(true).open(&self.path)?;
    *docs_guard = new_docs;
    self.total_size.store(total_size, Ordering::SeqCst);
    self.next_offset.store(header.data_end, Ordering::SeqCst);
    self.seq.store(header.seq, Ordering::SeqCst);
    drop(file);
    drop(docs_guard);

    #[cfg(not(target_arch = "wasm32"))]
    if self.config.use_mmap { self.setup_mmap()?; }
    Ok(())
}
```

Because `write()` must take `documents.write()` *before* releasing its offset reservation, also swap its lock order to `documents` -> `file` and hold `docs` across the file write:

```rust
fn write(&self, data: &[u8]) -> Result<Uuid> {
    let id = Uuid::new_v4();
    let size = data.len() as u32;
    let checksum = Self::compute_document_checksum(data);
    let offset = self.next_offset.fetch_add(size as u64 + 8, Ordering::SeqCst);
    let mut docs = self.documents.write();          // blocks during compact()
    {
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(offset))?;
        file.write_u32::<LittleEndian>(size)?;
        file.write_u32::<LittleEndian>(checksum)?;
        file.write_all(data)?;
    }
    docs.insert(id, DocumentMeta { offset: offset + 8, size, checksum });
    self.total_size.fetch_add(size as u64, Ordering::Relaxed);
    Ok(id)
}
```
Note: an offset reserved before compact starts is still possible — but now the doc insert blocks until compact finishes, and compact's `next_offset.store` overwrites the reservation, so the subsequent blocked write proceeds at a fresh, valid offset. The stale reservation only wastes bytes in the OLD (deleted) file. State this invariant in a comment.

**Step 4: Re-run** — `cargo test --features persistence compact_is_atomic` -> PASS; then `cargo test` full suite -> PASS.

**Step 5: Commit** — `git add Rust/src/storage.rs && git commit -m "fix(storage): make compact() atomic against concurrent writes/deletes"`

---

**Task A2: fsync parent directory after compact rename (finding 4)**

**Step 1: Add helper** in `Rust/src/storage.rs`:
```rust
#[cfg(feature = "persistence")]
fn sync_parent_dir(path: &std::path::Path) -> Result<()> {
    #[cfg(unix)]
    {
        if let Some(parent) = path.parent() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }
    }
    Ok(())
}
```

**Step 2: Test** — smoke-level (durability of rename can't be crash-tested without fault injection):
```rust
#[cfg(feature = "persistence")]
#[test]
fn compact_leaves_openable_file_after_dir_sync() {
    use tempfile::tempdir;
    let dir = tempdir().unwrap();
    let path = dir.path().join("d.db");
    let (backend, mut trie) = FileBackend::open(&path, &Config::default()).unwrap();
    let id = backend.write(b"x").unwrap();
    trie = trie.insert(b"x", id);
    backend.flush(&trie).unwrap();
    backend.compact(&trie).unwrap();
    drop(backend);
    let (b2, t2) = FileBackend::open(&path, &Config::default()).unwrap();
    assert_eq!(b2.read(t2.get(b"x").unwrap()).unwrap(), b"x");
}
```
Run: `cargo test --features persistence compact_leaves_openable` -> PASS.

**Step 3: Commit** — `git commit -m "fix(storage): fsync parent dir after compaction rename"`

---

**Task A3: Inter-process file lock on open (finding 5)**

**Files:** `Rust/Cargo.toml`, `Rust/src/storage.rs` (`FileBackend::open`), `Rust/src/error.rs`

**Step 1: Add dep** — `fs2 = { version = "0.4", optional = true }` and add `"fs2"` to the `persistence` feature list in `Cargo.toml`.

**Step 2: Failing test:**
```rust
#[cfg(feature = "persistence")]
#[test]
fn second_open_of_same_path_fails() {
    use tempfile::tempdir;
    let dir = tempdir().unwrap();
    let path = dir.path().join("locked.db");
    let (_b, _t) = FileBackend::open(&path, &Config::default()).unwrap();
    let err = FileBackend::open(&path, &Config::default());
    assert!(err.is_err(), "double open must fail while first handle is alive");
}
```
Run -> currently FAILS (second open succeeds).

**Step 3: Implement** — in `FileBackend` add field `#[cfg(feature = "persistence")] _lock_guard: fs2::FileExt` usage: after `OpenOptions...open(path)?`, call `file.try_lock_exclusive()`; on failure return `Err(Error::ResourceLimit(format!("database already open: {}", path.display())))`. Store nothing extra (the lock lives as long as the `File`); keep `file` in the struct as now. On `compact()` re-`open` after rename, re-acquire `try_lock_exclusive()` on the new `File` before swapping it in.

**Step 4: Re-run test -> PASS; full suite -> PASS.**

**Step 5: Commit** — `git commit -m "feat(storage): exclusive file lock prevents double-open corruption"`

---

**Task A4: Serialize `flush()` with a mutex (finding 6)**

**Step 1:** Add `flush_lock: Mutex<()>` to `FileBackend`; first line of `flush()`: `let _guard = self.flush_lock.lock();` (parking_lot, no poisoning).

**Step 2: Test:**
```rust
#[cfg(feature = "persistence")]
#[test]
fn concurrent_flushes_commit_monotonic_seqs() {
    use tempfile::tempdir; use std::sync::Arc; use std::thread;
    let dir = tempdir().unwrap();
    let path = dir.path().join("f.db");
    let (backend, trie) = FileBackend::open(&path, &Config::default()).unwrap();
    let backend = Arc::new(backend);
    let mut hs = vec![];
    for _ in 0..4 {
        let (b, t) = (Arc::clone(&backend), trie.clone());
        hs.push(thread::spawn(move || for _ in 0..10 { b.flush(&t).unwrap(); }));
    }
    for h in hs { h.join().unwrap(); }
    drop(backend);
    let (_b, _t) = FileBackend::open(&path, &Config::default()).unwrap(); // must load cleanly
}
```
Run -> PASS (also passes pre-fix most of the time; the mutex removes the same-slot clobber window).

**Step 3: Commit** — `git commit -m "fix(storage): serialize flush commits"`

---

**Task A5: Remove dead update branch in `MemoryBackend::write` (finding 7)**

**Step 1:** Delete lines `storage.rs:103-107` (`if let Some(old) = docs.get(&id) { ... }`) — a fresh v4 UUID can never collide; the subtract is unreachable.

**Step 2:** `cargo test` -> PASS (existing `test_memory_backend_basic` covers stats).

**Step 3: Commit** — `git commit -m "chore(storage): drop unreachable update branch in MemoryBackend::write"`

---

### Phase B — Determinism

**Task B1: Sort document index by UUID before serialization (finding 8)**

**Files:** `Rust/src/storage.rs` (flush ~719-731, compact, plus new helper)

**Step 1: Failing test:**
```rust
#[cfg(feature = "persistence")]
#[test]
fn index_serialization_is_order_independent() {
    let mut a = HashMap::new();
    let mut b = HashMap::new();
    let ids: Vec<Uuid> = (0..50).map(|_| Uuid::new_v4()).collect();
    for (i, id) in ids.iter().enumerate() {
        a.insert(*id, DocumentMeta { offset: i as u64, size: 1, checksum: 0 });
    }
    for (i, id) in ids.iter().enumerate().rev() {
        b.insert(*id, DocumentMeta { offset: i as u64, size: 1, checksum: 0 });
    }
    assert_eq!(serialize_index(&a).unwrap(), serialize_index(&b).unwrap());
}
```

**Step 2: Implement helper** and use it in both `flush()` and `compact()`:
```rust
#[cfg(feature = "persistence")]
fn serialize_index(docs: &HashMap<Uuid, DocumentMeta>) -> Result<Vec<u8>> {
    let mut entries: Vec<(&Uuid, &DocumentMeta)> = docs.iter().collect();
    entries.sort_unstable_by_key(|(id, _)| *id);
    let mut buf = Vec::with_capacity(8 + entries.len() * 32);
    buf.write_u64::<LittleEndian>(entries.len() as u64)?;
    for (id, meta) in entries {
        buf.write_all(id.as_bytes())?;
        buf.write_u64::<LittleEndian>(meta.offset)?;
        buf.write_u32::<LittleEndian>(meta.size)?;
        buf.write_u32::<LittleEndian>(meta.checksum)?;
    }
    Ok(buf)
}
```
`Uuid` implements `Ord` — byte-lexicographic, stable across platforms.

**Step 3:** Run `cargo test --features persistence index_serialization` -> PASS; full suite -> PASS.

**Step 4: Commit** — `git commit -m "fix(storage): deterministic document index serialization"`

---

**Task B2: Document the determinism contract (findings 9-11)**

**Step 1:** Add a `## Determinism` section to `Rust/README.md` stating:
- Doc IDs are random (UUID v4); identical workloads never produce identical files.
- Trie on-disk bytes depend on operation history (persistent B-tree shape); correctness does not.
- After Task B1, the document index is canonical (sorted by UUID).
- `suffix_search` returns results sorted by reversed-key byte order, NOT lexicographic; sort client-side if display order matters.
- Tests use fixed-seed xorshift (`Rng` in `storage.rs` tests) — reuse that pattern; never assert on raw file bytes.

**Step 2: Commit** — `git commit -m "docs: determinism contract"`

---

### Phase C — Speed

**Task C1: Zero-alloc cache lookups (finding 12)**

**Files:** `Rust/src/lib.rs` (`get` line 307, `exists` line 352, `delete` line 391)

**Step 1:** Replace `cache.get(&key.to_vec())` with `cache.get(key)` and `cache.pop(&key.to_vec())` with `cache.pop(key)` — `lru 0.12` accepts `&Q where K: Borrow<Q>`, and `Vec<u8>: Borrow<[u8]>`. If the compiler objects about `?Sized`, use `cache.get(key as &[u8])`.

**Step 2:** `cargo test` -> PASS (no behavior change; covered by existing tests).

**Step 3:** `cargo bench db_insert 2>&1 | tail -5` vs `/tmp/bench-baseline.txt` — record improvement.

**Step 4: Commit** — `git commit -m "perf: zero-alloc LRU lookups on hot paths"`

---

**Task C2: Snapshot flush — stop holding the trie lock across fsync (finding 13)**

**Files:** `Rust/src/lib.rs` (`flush` lines 459-473)

**Step 1: Failing (latency) test** — regression-shaped correctness test:
```rust
#[test]
fn flush_does_not_block_concurrent_inserts_and_commits_them() {
    use std::sync::Arc; use std::thread;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snap.db");
    let db = Arc::new(StreamDb::open(&path, Config::default()).unwrap());
    for i in 0..500 { db.insert(format!("k{i}").as_bytes(), &[0u8; 256]).unwrap(); }
    let db2 = Arc::clone(&db);
    let writer = thread::spawn(move || {
        for i in 500..600 { db2.insert(format!("k{i}").as_bytes(), &[1u8; 256]).unwrap(); }
    });
    db.flush().unwrap();
    writer.join().unwrap();
    db.flush().unwrap();
    drop(db);
    let db = StreamDb::open(&path, Config::default()).unwrap();
    for i in 0..600 {
        assert!(db.get(format!("k{i}").as_bytes()).unwrap().is_some(), "k{i} lost");
    }
}
```

**Step 2: Implement:**
```rust
pub fn flush(&self) -> Result<()> {
    if !self.dirty.load(Ordering::Acquire) { return Ok(()); }
    info!("Flushing database");
    let snapshot = { self.trie.read().clone() };   // O(1) persistent clone, lock released immediately
    self.backend.flush(&snapshot)?;
    self.dirty.store(false, Ordering::Release);
    Ok(())
}
```

**Step 3:** `cargo test --features persistence flush_does_not_block` -> PASS; full suite PASS.

**Step 4: Commit** — `git commit -m "perf: snapshot trie before flush instead of holding read lock across fsync"`

---

**Task C3: Configurable checksum-on-read (finding 15)**

**Step 1:** Add `pub verify_checksums_on_read: bool` (default `true`) to `Config` in `Rust/src/lib.rs` with doc: "Disable only for read-mostly workloads on checksummed/ECC storage."

**Step 2:** In `FileBackend::read` (both mmap and file paths), skip `compute_checksum` when `!self.config.verify_checksums_on_read`.

**Step 3: Test:**
```rust
#[cfg(feature = "persistence")]
#[test]
fn checksum_on_read_can_be_disabled_but_defaults_on() {
    use tempfile::tempdir;
    let dir = tempdir().unwrap();
    let path = dir.path().join("c.db");
    let mut cfg = Config::default();
    assert!(cfg.verify_checksums_on_read);
    cfg.verify_checksums_on_read = false;
    let (b, _t) = FileBackend::open(&path, &cfg).unwrap();
    let id = b.write(b"data").unwrap();
    assert_eq!(b.read(id).unwrap(), b"data");
}
```

**Step 4:** `cargo test --features persistence checksum_on_read` -> PASS.

**Step 5: Commit** — `git commit -m "feat: optional checksum verification on read (default on)"`

---

**Task C4: Benchmarks for file backend + concurrent read-during-flush (findings 14, 16)**

**Step 1:** Extend `Rust/benches/benchmarks.rs` with:
```rust
fn bench_file_backend(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_backend");
    group.bench_function("insert_1KB", |b| {
        let dir = tempfile::tempdir().unwrap();
        let db = StreamDb::open(dir.path().join("b.db"), Config::default()).unwrap();
        let v = vec![0u8; 1024];
        let mut i = 0u64;
        b.iter(|| { let k = format!("k{}", i); i += 1; black_box(db.insert(k.as_bytes(), &v).unwrap()) });
    });
    group.bench_function("get_1KB", |b| {
        let dir = tempfile::tempdir().unwrap();
        let db = StreamDb::open(dir.path().join("b.db"), Config::default()).unwrap();
        let v = vec![0u8; 1024];
        let keys: Vec<_> = (0..1000).map(|i| { let k = format!("k{i}"); db.insert(k.as_bytes(), &v).unwrap(); k.into_bytes() }).collect();
        b.iter(|| for k in &keys { black_box(db.get(black_box(k)).unwrap()); });
    });
    group.bench_function("flush_1000_keys", |b| {
        let dir = tempfile::tempdir().unwrap();
        let db = StreamDb::open(dir.path().join("b.db"), Config::default()).unwrap();
        let v = vec![0u8; 64];
        for i in 0..1000 { db.insert(format!("k{i}").as_bytes(), &v).unwrap(); }
        b.iter(|| { db.insert(b"touch", b"x").unwrap(); black_box(db.flush().unwrap()); });
    });
    group.finish();
}
```
Add `bench_file_backend` to the `criterion_group!`.

**Step 2:** `cargo bench -- file_backend 2>&1 | tail -20` — save as post-C1/C2 numbers.

**Step 3: Commit** — `git commit -m "bench: file backend insert/get/flush coverage"`

---

**Task C5: `suffix_search_limit` (finding 17)**

**Step 1:** Add to `Trie` (`Rust/src/trie.rs`) a `collect_all_limited(&mut path, &mut results, limit)` and `pub fn suffix_search_limit(&self, suffix: &[u8], limit: usize) -> Vec<(Vec<u8>, Uuid)>` reusing `navigate`; expose `StreamDb::suffix_search_limit` with the same validation as `suffix_search`.

**Step 2: Test:**
```rust
#[test]
fn suffix_search_limit_caps_results() {
    let db = StreamDb::open_memory().unwrap();
    for i in 0..100 { db.insert(format!("user:{i}").as_bytes(), b"v").unwrap(); }
    let r = db.suffix_search_limit(b"user:", 10).unwrap();
    assert_eq!(r.len(), 10);
    let r = db.suffix_search_limit(b"user:", 0).unwrap();
    assert!(r.is_empty());
}
```
Wait — keys are `user:{i}`; suffix `user:` does not match (keys END with digits). Use suffix of the actual tail, e.g. keys `format!("{i}:tag")`, suffix `b":tag"`. Write the test with that correction.

**Step 3:** `cargo test suffix_search_limit` -> PASS.

**Step 4: Commit** — `git commit -m "feat: bounded suffix search"`

---

### Phase D — Usability / hygiene

**Task D1: Expose `StreamDb::compact()` (finding 18)**

**Step 1:** In `Rust/src/lib.rs` add:
```rust
/// Rewrite the database file keeping only live documents.
/// Blocks concurrent writers; see `FileBackend::compact`.
#[cfg(feature = "persistence")]
pub fn compact(&self) -> Result<()> {
    let snapshot = { self.trie.read().clone() };
    let backend = self.backend.as_any()
        .downcast_ref::<FileBackend>()
        .ok_or_else(|| Error::InvalidInput("compact requires the file backend".into()))?;
    backend.compact(&snapshot)
}
```

**Step 2: Test** — extend `compact_reclaims_space_and_preserves_live_data` style flow through `StreamDb` (insert/delete/flush -> `db.compact()` -> file shrank -> reopen -> live keys intact). Run -> PASS.

**Step 3: Commit** — `git commit -m "feat: StreamDb::compact()"`

---

**Task D2: Delete bit-rotted `Rust/src/tests.rs` (finding 20)**

**Step 1:** `git rm Rust/src/tests.rs`. It is not referenced by `lib.rs` (no `mod tests;`) and references APIs that don't exist (`MemoryBackend::new("test.wal")`, `bind_path_to_document`, transactions, `sled`). Property coverage already exists via `random_operations_match_hashmap_across_reopens`.

**Step 2:** `cargo test --all-features` -> PASS.

**Step 3: Commit** — `git commit -m "chore: delete orphaned bit-rotted tests.rs"`

---

**Task D3: Remove `panic = "abort"` so FFI `catch_unwind` works (finding 21)**

**Step 1:** In `Rust/Cargo.toml` delete `panic = "abort"` from `[profile.release]` (keep lto/codegen-units/strip).

**Step 2: Test** — add to `Rust/src/ffi.rs` tests (or integration): call an FFI fn with a null handle and assert it returns an error code instead of crashing; existing FFI null-check tests cover the pattern. Then `cargo test --features ffi` -> PASS.

**Step 3: Commit** — `git commit -m "fix(build): unwind panics in release so FFI catch_unwind is effective"`

---

**Task D4: Honest feature surface (finding 19)**

**Step 1:** `Config::flush_interval_ms`: either (a) implement a background flush thread — NOT recommended for an embedded lib (thread lifecycle, drop ordering) — or (b) mark it clearly. Plan: keep field, doc-comment "reserved; not yet implemented — call `flush()` explicitly", because removing is a breaking change and a future auto-flush is plausible. (Open question Q1 if the user wants it implemented instead.)

**Step 2:** `compression` / `encryption` / `async` features: remove from `Cargo.toml` `[features]`, remove optional deps `lz4_flex`/`ring`/`tokio`, remove the cfg'd `Config` fields and `Error::Encryption` variant. They are vaporware today; re-add when implemented. Update `full` feature to `["persistence", "ffi", "cli"]`.

**Step 3:** `cargo build --all-features && cargo test` -> PASS.

**Step 4: Commit** — `git commit -m "chore: remove unimplemented compression/encryption/async features"`

---

**Task D5: Consolidate C on the Rust-binding wrapper (finding 22)**

**Step 1:** Verify `C/src/streamdb_wrapper.c` + `C/lib/libstreamdb.a` are the Rust staticlib binding path and that `C/tests/test_streamdb.c` passes against it:
```bash
cd C && make test 2>&1 | tail -10   # or: cmake --build . && ctest
```

**Step 2:** If the wrapper path is functional: `git rm C/src/streamdb.c` and delete its build rules from `C/Makefile`/`C/CMakeLists.txt`; note in `C/README.md` that the standalone C implementation was removed to avoid format divergence (it predates on-disk v3). If the wrapper is NOT functional, invert: keep both but add a `C/README.md` warning that `streamdb.c` implements the legacy v2 format and must not be used for new data.

**Step 3: Commit** — `git commit -m "chore(C): consolidate on Rust-backed wrapper"`

---

**Task D6: README durability + migration notes (finding 23)**

**Step 1:** Add to `Rust/README.md`:
- **Durability contract:** a write is durable exactly when `flush()` has returned `Ok`. Crash safety: the store falls back to the last committed snapshot; at most the unflushed tail is lost. Per-document CRCs detect (not repair) corruption.
- **Format note:** v3 (append-then-commit-header) is not readable by v2 and vice versa; v2 files are considered corrupt-by-design and must be rebuilt.
- **Maintenance:** call `compact()` periodically on delete-heavy workloads; file is append-only between compactions.

**Step 2: Commit** — `git commit -m "docs: durability contract, v3 format note, compaction guidance"`

---

## Validation (final gate)

```bash
cd Rust
cargo test --all-features
cargo test
cargo bench 2>&1 | tee /tmp/bench-final.txt   # compare vs /tmp/bench-baseline.txt
cargo clippy --all-targets -- -D warnings
cargo doc --no-deps                            # missing_docs warnings should not grow
cd ../C && make test                            # wrapper still green
```

## Risks / tradeoffs / open questions

- **Task A1 lock-ordering** introduces `documents -> file` ordering in `write()`; `read()` only takes `documents.read()` briefly then `file` — no cycle, but re-verify with `cargo test` under `--release` and a loom/miri pass if time permits.
- **Q1:** Implement real auto-flush for `flush_interval_ms`, or keep the "reserved" doc stub? (Decision affects D4 step 1.)
- **Q2:** Deterministic ID mode (UUID v5 from key) for reproducible files — worth adding behind a `Config` flag, or is documentation (Task B2) enough? (YAGNI call.)
- **Q3:** C consolidation direction depends on whether the wrapper path currently builds — Task D5 verifies first.
- CRC32 is not cryptographic; a crafted file can pass checksums. Acceptable for an embedded store; noted here so it stays a conscious decision.

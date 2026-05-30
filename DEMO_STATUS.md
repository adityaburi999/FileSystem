# Demo Status

This repository already contains a working demo-level implementation of the core modules:

- `wal`: append-only WAL with transaction statuses, chunk snapshots, recovery scan, and tail repair.
- `metadata`: CAS write commits, tombstone delete commits, idempotent replay behavior, and namespace directory operations (`create_directory`, `list_children`, `remove_directory`, `rename_path`) across in-memory/JSON/SQLite backends, with parent-directory enforcement for nested writes/dirs, strict missing-directory listing errors, file/dir path-type conflict protection, and tombstone-aware destination-path reclaim during mkdir/rename.
- `metadata` includes in-memory, JSON file-backed, and SQLite-backed hook implementations.
- `chunk_store`: immutable chunk persistence with BLAKE3 verification and simple quarantine on corruption.
- `cache`: two-tier chunk cache with invalidation hooks, plus an optional persistent warm-tier implementation (`PersistentTwoTierChunkCache`).
- `gc`: conservative orphan detection with inflight WAL protection, retention window, configurable per-sweep delete budget policy, bounded non-blocking enqueue backlog (default cap 64 with drop counting), and periodic background scheduler loop (`start/stop/metrics`).
- `staging`: in-memory and on-disk staging-slot lifecycle for in-progress transactions, including startup recovery purge of stale uncommitted slots plus cleanup of leftover committed slots.
- `core`: orchestration (recovery-before-mount, write/read/read_all/write_if_missing/write_if_version/compare_and_swap_file/write_if_hash/write_if_size/write_if_exists/write_if_empty/write_if_not_empty/write_if_starts_with/write_if_ends_with/write_if_contains/write_if_not_contains/write_if_exact/write_if_not_exact/write_if_min_size/write_if_max_size/write_if_size_between/write_if_size_not_between/write_if_size_multiple_of/write_if_size_not_multiple_of/write_if_size_odd/write_if_size_even/write_if_size_power_of_two/write_if_size_not_power_of_two/write_if_size_prime/write_if_size_not_prime/write_if_size_fibonacci/write_if_size_not_fibonacci/write_if_size_square/write_if_size_not_square/write_if_size_cube/write_if_size_not_cube/write_if_size_triangular/write_if_size_not_triangular/write_if_size_factorial/write_if_size_not_factorial/write_if_size_composite/write_if_size_not_composite/write_if_size_perfect/write_if_size_not_perfect/write_if_size_abundant/write_if_size_not_abundant/write_if_size_deficient/write_if_size_not_deficient/write_if_size_semiprime/write_if_size_not_semiprime/write_if_size_palindrome/write_if_size_not_palindrome/write_if_size_armstrong/write_if_size_not_armstrong/write_if_size_happy/write_if_size_not_happy/write_if_size_automorphic/write_if_size_not_automorphic/write_if_size_harshad/write_if_size_not_harshad/write_if_size_kaprekar/write_if_size_not_kaprekar/write_if_size_repdigit/write_if_size_not_repdigit/write_if_size_tribonacci/write_if_size_not_tribonacci/write_if_size_pell/write_if_size_not_pell/write_if_size_lucas/write_if_size_not_lucas/write_if_size_mersenne/write_if_size_not_mersenne/write_if_size_power_of_three/write_if_size_not_power_of_three/write_if_size_power_of_four/write_if_size_not_power_of_four/write_if_size_power_of_five/write_if_size_not_power_of_five/write_if_size_power_of_six/write_if_size_not_power_of_six/write_if_size_power_of_seven/write_if_size_not_power_of_seven/write_if_size_power_of_eight/write_if_size_not_power_of_eight/write_if_size_power_of_nine/write_if_size_not_power_of_nine/write_if_size_power_of_ten/write_if_size_not_power_of_ten/write_if_size_power_of_eleven/write_if_size_not_power_of_eleven/write_if_size_power_of_twelve/write_if_size_not_power_of_twelve/write_if_size_power_of_thirteen/write_if_size_not_power_of_thirteen/write_if_size_power_of_fourteen/write_if_size_not_power_of_fourteen/write_if_size_power_of_fifteen/write_if_size_not_power_of_fifteen/write_if_size_power_of_sixteen/write_if_size_not_power_of_sixteen/write_if_size_power_of_seventeen/write_if_size_not_power_of_seventeen/write_if_size_power_of_eighteen/write_if_size_not_power_of_eighteen/write_if_size_power_of_nineteen/write_if_size_not_power_of_nineteen/write_if_size_power_of_twenty/write_if_size_not_power_of_twenty/write_if_size_power_of_twenty_one/write_if_size_not_power_of_twenty_one/write_if_size_power_of_twenty_two/write_if_size_not_power_of_twenty_two/write_if_size_power_of_twenty_three/write_if_size_not_power_of_twenty_three/write_if_size_power_of_twenty_four/write_if_size_not_power_of_twenty_four/write_if_size_power_of_twenty_five/write_if_size_not_power_of_twenty_five/write_if_size_power_of_twenty_six/write_if_size_not_power_of_twenty_six/write_if_size_power_of_twenty_seven/write_if_size_not_power_of_twenty_seven/write_if_size_power_of_twenty_eight/write_if_size_not_power_of_twenty_eight/write_if_size_power_of_twenty_nine/write_if_size_not_power_of_twenty_nine/write_if_size_power_of_thirty/write_if_size_not_power_of_thirty/write_if_size_power_of_thirty_one/write_if_size_not_power_of_thirty_one/write_if_size_power_of_thirty_two/write_if_size_not_power_of_thirty_two/write_if_size_power_of_thirty_three/write_if_size_not_power_of_thirty_three/write_if_size_power_of_thirty_four/write_if_size_not_power_of_thirty_four/write_if_size_power_of_thirty_five/write_if_size_not_power_of_thirty_five/write_if_size_power_of_thirty_six/write_if_size_not_power_of_thirty_six/write_if_size_power_of_thirty_seven/write_if_size_not_power_of_thirty_seven/write_if_size_power_of_thirty_eight/write_if_size_not_power_of_thirty_eight/write_if_size_power_of_thirty_nine/write_if_size_not_power_of_thirty_nine/ensure_file/copy_file/touch_file/truncate_file/append_file/overwrite_range/insert_range/delete_range/replace_range/file_size/file_hash/unlink/mkdir/mkdir_p/list_dir/list_dir_with_kinds/walk_dir/tree_summary/tree_bytes/stat_path/path_exists/remove_path/rmdir/rmtree/rename paths, cache + staging + gc wiring, manual GC enqueue (`enqueue_gc_scan`), manual GC enqueue+drain (`gc_scan_once`), background GC scheduler startup helper), with missing-parent namespace failures mapped to `NotFound`, non-empty directory removal mapped to `NotEmpty`, file-vs-directory delete path-type mismatches mapped to `Conflict`, and checked unlink type-conflict handling.
- `fuse` crate API + daemon loop: request validation/routing plus an in-process request worker around `core`, including namespace ops (`mkdir`, `mkdir_p`, `readdir`, `readdir_with_kinds`, `walk_dir`, `tree_summary`, `tree_bytes`, `stat`, `exists`, `remove_path`, `rmdir`, `rmtree`, `rename`) plus file ops (`read_all`, `write_if_missing`, `write_if_version`, `compare_and_swap_file`, `write_if_hash`, `write_if_size`, `write_if_exists`, `write_if_empty`, `write_if_not_empty`, `write_if_starts_with`, `write_if_ends_with`, `write_if_contains`, `write_if_not_contains`, `write_if_exact`, `write_if_not_exact`, `write_if_min_size`, `write_if_max_size`, `write_if_size_between`, `write_if_size_not_between`, `write_if_size_multiple_of`, `write_if_size_not_multiple_of`, `write_if_size_odd`, `write_if_size_even`, `write_if_size_power_of_two`, `write_if_size_not_power_of_two`, `write_if_size_prime`, `write_if_size_not_prime`, `write_if_size_fibonacci`, `write_if_size_not_fibonacci`, `write_if_size_square`, `write_if_size_not_square`, `write_if_size_cube`, `write_if_size_not_cube`, `write_if_size_triangular`, `write_if_size_not_triangular`, `write_if_size_factorial`, `write_if_size_not_factorial`, `write_if_size_composite`, `write_if_size_not_composite`, `write_if_size_perfect`, `write_if_size_not_perfect`, `write_if_size_abundant`, `write_if_size_not_abundant`, `write_if_size_deficient`, `write_if_size_not_deficient`, `write_if_size_semiprime`, `write_if_size_not_semiprime`, `write_if_size_palindrome`, `write_if_size_not_palindrome`, `write_if_size_armstrong`, `write_if_size_not_armstrong`, `write_if_size_happy`, `write_if_size_not_happy`, `write_if_size_automorphic`, `write_if_size_not_automorphic`, `write_if_size_harshad`, `write_if_size_not_harshad`, `write_if_size_kaprekar`, `write_if_size_not_kaprekar`, `write_if_size_repdigit`, `write_if_size_not_repdigit`, `write_if_size_tribonacci`, `write_if_size_not_tribonacci`, `write_if_size_pell`, `write_if_size_not_pell`, `write_if_size_lucas`, `write_if_size_not_lucas`, `write_if_size_mersenne`, `write_if_size_not_mersenne`, `write_if_size_power_of_three`, `write_if_size_not_power_of_three`, `write_if_size_power_of_four`, `write_if_size_not_power_of_four`, `write_if_size_power_of_five`, `write_if_size_not_power_of_five`, `write_if_size_power_of_six`, `write_if_size_not_power_of_six`, `write_if_size_power_of_seven`, `write_if_size_not_power_of_seven`, `write_if_size_power_of_eight`, `write_if_size_not_power_of_eight`, `write_if_size_power_of_nine`, `write_if_size_not_power_of_nine`, `write_if_size_power_of_ten`, `write_if_size_not_power_of_ten`, `write_if_size_power_of_eleven`, `write_if_size_not_power_of_eleven`, `write_if_size_power_of_twelve`, `write_if_size_not_power_of_twelve`, `write_if_size_power_of_thirteen`, `write_if_size_not_power_of_thirteen`, `write_if_size_power_of_fourteen`, `write_if_size_not_power_of_fourteen`, `write_if_size_power_of_fifteen`, `write_if_size_not_power_of_fifteen`, `write_if_size_power_of_sixteen`, `write_if_size_not_power_of_sixteen`, `write_if_size_power_of_seventeen`, `write_if_size_not_power_of_seventeen`, `write_if_size_power_of_eighteen`, `write_if_size_not_power_of_eighteen`, `write_if_size_power_of_nineteen`, `write_if_size_not_power_of_nineteen`, `write_if_size_power_of_twenty`, `write_if_size_not_power_of_twenty`, `write_if_size_power_of_twenty_one`, `write_if_size_not_power_of_twenty_one`, `write_if_size_power_of_twenty_two`, `write_if_size_not_power_of_twenty_two`, `write_if_size_power_of_twenty_three`, `write_if_size_not_power_of_twenty_three`, `write_if_size_power_of_twenty_four`, `write_if_size_not_power_of_twenty_four`, `write_if_size_power_of_twenty_five`, `write_if_size_not_power_of_twenty_five`, `write_if_size_power_of_twenty_six`, `write_if_size_not_power_of_twenty_six`, `write_if_size_power_of_twenty_seven`, `write_if_size_not_power_of_twenty_seven`, `write_if_size_power_of_twenty_eight`, `write_if_size_not_power_of_twenty_eight`, `write_if_size_power_of_twenty_nine`, `write_if_size_not_power_of_twenty_nine`, `write_if_size_power_of_thirty`, `write_if_size_not_power_of_thirty`, `write_if_size_power_of_thirty_one`, `write_if_size_not_power_of_thirty_one`, `write_if_size_power_of_thirty_two`, `write_if_size_not_power_of_thirty_two`, `write_if_size_power_of_thirty_three`, `write_if_size_not_power_of_thirty_three`, `write_if_size_power_of_thirty_four`, `write_if_size_not_power_of_thirty_four`, `write_if_size_power_of_thirty_five`, `write_if_size_not_power_of_thirty_five`, `write_if_size_power_of_thirty_six`, `write_if_size_not_power_of_thirty_six`, `write_if_size_power_of_thirty_seven`, `write_if_size_not_power_of_thirty_seven`, `write_if_size_power_of_thirty_eight`, `write_if_size_not_power_of_thirty_eight`, `write_if_size_power_of_thirty_nine`, `write_if_size_not_power_of_thirty_nine`, `ensure_file`, `copy_file`, `touch_file`, `truncate_file`, `append_file`, `overwrite_range`, `insert_range`, `delete_range`, `replace_range`, `file_size`, `file_hash`), mount-gate visibility (`is_mount_open`), manual GC enqueue (`enqueue_gc_scan`), manual GC enqueue+drain (`gc_scan_once`), and GC worker drain (`run_background_once`), daemon health probe/liveness checks, optional request timeout controls (`Timeout` vs `Unavailable`), and user-facing missing-parent path errors; unlink-on-directory now maps to conflict.

## Added Demo Entry Point

Run:

```bash
./scripts/demo.sh
cargo run -p fs_core --example basic_demo
cargo run -p fs_core --example restart_recovery_demo
cargo run -p fs_core --example sqlite_metadata_demo
cargo run -p fs_core --example persistent_cache_demo
cargo run -p fuse --example fuse_api_demo
cargo run -p fuse --example fuse_daemon_demo
```

WAL sync policy toggle for demos:

```bash
WAL_SYNC_WRITES=true ./scripts/demo.sh    # default, fsync per append
WAL_SYNC_WRITES=false ./scripts/demo.sh   # buffered append mode
```

Additional shared demo knobs:

```bash
DEMO_QUICK_MODE=true ./scripts/demo.sh
DEMO_LIST=true ./scripts/demo.sh
DEMO_SUMMARY_JSON=true ./scripts/demo.sh
DEMO_SUMMARY_JSON=true DEMO_SUMMARY_JSON_PATH=./demo-summary.json ./scripts/demo.sh
DEMO_ONLY=fuse_daemon ./scripts/demo.sh
DEMO_ONLY=basic ./scripts/demo.sh
DEMO_ONLY=all ./scripts/demo.sh
CHUNK_SIZE_BYTES=8 ./scripts/demo.sh
GC_RETENTION_MS=100 ./scripts/demo.sh
GC_DELETE_BUDGET=2 ./scripts/demo.sh
GC_DELETE_BUDGET=none ./scripts/demo.sh
GC_MAX_ENQUEUED_SCANS=16 ./scripts/demo.sh
GC_SCHEDULER_INTERVAL_MS=10 ./scripts/demo.sh
GC_SCHEDULER_WAIT_MS=60 ./scripts/demo.sh
FUSE_DAEMON_REQUEST_TIMEOUT_MS=250 ./scripts/demo.sh
FUSE_DAEMON_REQUEST_TIMEOUT_MS=off ./scripts/demo.sh
FUSE_DAEMON_MAX_PENDING_REQUESTS=256 ./scripts/demo.sh
FUSE_TIMEOUT_SMOKE=true FUSE_DAEMON_REQUEST_TIMEOUT_MS=25 ./scripts/demo.sh
FUSE_TIMEOUT_SMOKE=true FUSE_DAEMON_REQUEST_TIMEOUT_MS=25 FUSE_TIMEOUT_SMOKE_DELAY_MS=80 ./scripts/demo.sh
FUSE_TIMEOUT_SMOKE=true FUSE_TIMEOUT_SMOKE_EXPECT_TIMEOUT=true FUSE_DAEMON_REQUEST_TIMEOUT_MS=25 FUSE_TIMEOUT_SMOKE_DELAY_MS=80 ./scripts/demo.sh
FUSE_TIMEOUT_SMOKE_NEGATIVE_CHECK=true ./scripts/demo.sh
```

`FUSE_TIMEOUT_SMOKE` runs delayed health probes in both FUSE demos; daemon mode also demonstrates timeout mapping when a daemon timeout is configured.
`FUSE_DAEMON_MAX_PENDING_REQUESTS` bounds daemon in-process pending request backlog (default 1024) to keep pressure behavior predictable.
`GC_MAX_ENQUEUED_SCANS` bounds queued GC scans before enqueue calls are dropped (default 64) in GC-enabled demos.
`GC_SCHEDULER_INTERVAL_MS` enables periodic GC scheduler mode in GC-enabled core demos (`basic_demo`, `sqlite_metadata_demo`); `GC_SCHEDULER_WAIT_MS` controls how long demo waits before printing scheduler metrics.
`FUSE_TIMEOUT_SMOKE_EXPECT_TIMEOUT=true` makes daemon smoke assert timeout observation and fail otherwise (requires configured timeout lower than smoke delay).
`FUSE_TIMEOUT_SMOKE_NEGATIVE_CHECK=true` adds an expected-failure command proving strict smoke validation catches missing daemon timeout config.
`DEMO_QUICK_MODE=true` runs a shorter demo set (basic + FUSE demos) for faster iteration.
`DEMO_LIST=true` prints valid demo target names and exits.
`DEMO_ONLY=<name>` runs exactly one demo target (`basic|restart|sqlite|cache|fuse_api|fuse_daemon`); `DEMO_ONLY=all` is an explicit alias for normal multi-demo flow.
`DEMO_SUMMARY_JSON=true` emits a machine-readable summary line including ran targets and key mode flags.
`DEMO_SUMMARY_JSON_PATH=<path>` writes that same summary JSON to disk for CI artifact capture.
Summary JSON fields: `ran_targets`, `negative_timeout_smoke_check`, `quick_mode`, `demo_only`, `demo_list`, `wal_sync_writes`.

It demonstrates:

- mount gate closed before recovery
- startup recovery
- write + read range
- unlink (logical delete via metadata version bump)
- post-delete read behavior
- background GC pass
- restart with persisted WAL/chunks/metadata and recovery replay before read access
- SQLite-backed metadata flow with persisted DB state and CAS/tombstone semantics
- Persistent warm-tier cache survives restart and key-based cache lookup
- FUSE-layer API flow (`startup_recover`, `mkdir`, `readdir`, `rename`, `write`, `read`, `open`, `unlink`, `rmdir`)
- FUSE daemon-loop flow with request dispatch and graceful shutdown

## Validation Note

- Executed successfully on **May 29, 2026**:

```bash
source $HOME/.cargo/env && cargo test --workspace --all-targets
source $HOME/.cargo/env && ./scripts/demo.sh
```

- Manual run commands:

```bash
./scripts/demo.sh
cargo run -p fs_core --example basic_demo
cargo run -p fs_core --example restart_recovery_demo
cargo run -p fs_core --example sqlite_metadata_demo
cargo run -p fs_core --example persistent_cache_demo
cargo run -p fuse --example fuse_api_demo
cargo run -p fuse --example fuse_daemon_demo
```

## Deferred (Not Implemented in This Demo)

- Real Linux FUSE mount syscall integration (current `fuse` crate supports API + in-process daemon loop, but not kernel mount/syscall wiring).
- RocksDB/custom metadata backend and advanced indexing for large-scale deployment.
- Full path graph/directory object model (demo operates on file-path keyed metadata entries).
- Production observability stack (metrics/exporters/dashboard).
- Distributed/sharded storage behavior.

These are intentionally left out to keep scope at demo level.

## Architecture Docs Refresh

- Updated `architecture/system-design.txt` to match current implemented runtime flows and invariants.
- Updated `architecture/disk_layout_design.txt` to current on-disk backends (JSON/SQLite metadata, persistent warm cache, staging slots).
- Updated `architecture/fail_possiblilities.txt` with concrete failure modes and handling currently present in code.

For requirement-by-requirement objective audit, see `DEMO_COMPLETION_AUDIT.md`.

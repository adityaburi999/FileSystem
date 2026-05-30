use cache::TwoTierChunkCache;
use chunk_store::FsChunkStore;
use fuse::demo_config::FuseDemoConfig;
use fs_core::FileSystemCore;
use fuse::{FuseApi, FuseNodeKind};
use metadata::InMemoryMetadataHook;
use std::sync::Arc;
use std::time::Duration;
use wal::{WalLog, WritePipeline};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = FuseDemoConfig::from_env(4);
    config.validate_for_daemon_smoke()?;
    let temp = tempfile::tempdir()?;
    let wal = WalLog::open_with_sync(temp.path().join("wal.log"), config.wal_sync_writes)?;
    let chunks = Arc::new(FsChunkStore::new(temp.path().join("objects"))?);
    let metadata = Arc::new(InMemoryMetadataHook::new());
    let cache = TwoTierChunkCache::new(16, 64);
    let pipeline = WritePipeline::new(wal, chunks, metadata, config.chunk_size_bytes)?;
    let core = FileSystemCore::new(pipeline, cache);
    let mut daemon = FuseApi::new(core).spawn_daemon_with_limits(config.daemon_max_pending_requests);
    println!("{}", config.summary("fuse_daemon_demo"));
    println!(
        "FUSE daemon mount gate open before recovery -> {}",
        daemon.is_mount_open()?
    );

    daemon.startup_recover()?;
    println!("FUSE daemon startup recovery completed.");
    println!(
        "FUSE daemon mount gate open after recovery -> {}",
        daemon.is_mount_open()?
    );
    let gc_enqueued = daemon.enqueue_gc_scan()?;
    println!(
        "FUSE daemon enqueue_gc_scan completed, enqueued={}",
        gc_enqueued
    );
    let gc_reports = daemon.run_background_once()?;
    println!(
        "FUSE daemon run_background_once completed, reports={}",
        gc_reports.len()
    );
    let gc_scan_reports = daemon.gc_scan_once()?;
    println!(
        "FUSE daemon gc_scan_once completed, reports={}",
        gc_scan_reports.len()
    );
    if let Some(ms) = config.daemon_request_timeout_ms {
        daemon.set_request_timeout(Some(Duration::from_millis(ms)));
        if config.timeout_smoke {
            let smoke_delay = Duration::from_millis(config.timeout_smoke_delay_ms);
            match daemon.health_with_delay(smoke_delay) {
                Err(fuse::FuseError::Timeout) => {
                    println!(
                        "FUSE daemon timeout smoke: timeout observed as expected (delay={}ms)",
                        config.timeout_smoke_delay_ms
                    );
                }
                Ok(()) => {
                    if config.timeout_smoke_expect_timeout {
                        return Err(format!(
                            "timeout smoke expected timeout but completed successfully (delay={}ms)",
                            config.timeout_smoke_delay_ms
                        )
                        .into());
                    }
                    println!(
                        "FUSE daemon timeout smoke: completed without timeout (delay={}ms)",
                        config.timeout_smoke_delay_ms
                    );
                }
                Err(other) => {
                    return Err(format!("timeout smoke got unexpected error: {other:?}").into());
                }
            }
            // Keep the rest of the demo deterministic after smoke validation.
            daemon.set_request_timeout(None);
        }
    }

    let created = daemon
        .mkdir_p("/daemon/sub")
        .expect("mkdir_p should succeed");
    println!("FUSE daemon mkdir_p committed, created={}", created);
    let create_only_tx = daemon.write_if_missing("/daemon/create-only.txt", b"created-once")?;
    println!(
        "FUSE daemon write_if_missing committed, tx_id={}",
        create_only_tx
    );
    let ensured_created = daemon.ensure_file("/daemon/ensured.txt")?;
    let ensured_again = daemon.ensure_file("/daemon/ensured.txt")?;
    println!(
        "FUSE daemon ensure_file /daemon/ensured.txt -> created_first={}, created_second={}",
        ensured_created, ensured_again
    );
    let touch_tx = daemon.touch_file("/daemon/empty.txt", 0)?;
    println!("FUSE daemon touch_file committed, tx_id={}", touch_tx);
    let empty_gate_tx = daemon.touch_file("/daemon/empty-gated.txt", 0)?;
    println!(
        "FUSE daemon touch_file(empty-gated) committed, tx_id={}",
        empty_gate_tx
    );
    let write_if_empty_tx = daemon.write_if_empty("/daemon/empty-gated.txt", b"first-fill")?;
    println!(
        "FUSE daemon write_if_empty committed, tx_id={}",
        write_if_empty_tx
    );
    let tx_id = daemon.write("/daemon/demo.txt", 0, b"fuse-daemon-demo-bytes")?;
    println!("FUSE daemon write committed, tx_id={}", tx_id);
    let write_if_version_tx = daemon.write_if_version("/daemon/demo.txt", 1, b"fuse-daemon-demo-bytes-v2")?;
    println!(
        "FUSE daemon write_if_version committed, tx_id={}",
        write_if_version_tx
    );
    let cas_tx = daemon.compare_and_swap_file(
        "/daemon/demo.txt",
        2,
        b"fuse-daemon-demo-bytes-v2",
        b"fuse-daemon-demo-bytes-v3",
    )?;
    println!("FUSE daemon compare_and_swap_file committed, tx_id={}", cas_tx);
    let demo_hash = daemon.file_hash("/daemon/demo.txt")?;
    let write_if_hash_tx =
        daemon.write_if_hash("/daemon/demo.txt", 3, &demo_hash, b"fuse-daemon-demo-bytes-v4")?;
    println!(
        "FUSE daemon write_if_hash committed, tx_id={}",
        write_if_hash_tx
    );
    let demo_size = daemon.file_size("/daemon/demo.txt")?;
    let write_if_size_tx =
        daemon.write_if_size("/daemon/demo.txt", 4, demo_size, b"fuse-daemon-demo-bytes-v5")?;
    println!(
        "FUSE daemon write_if_size committed, tx_id={}",
        write_if_size_tx
    );
    let write_if_exists_tx = daemon.write_if_exists("/daemon/demo.txt", b"fuse-daemon-demo-bytes-v6")?;
    println!(
        "FUSE daemon write_if_exists committed, tx_id={}",
        write_if_exists_tx
    );
    let write_if_not_empty_tx =
        daemon.write_if_not_empty("/daemon/demo.txt", b"fuse-daemon-demo-bytes-v7")?;
    println!(
        "FUSE daemon write_if_not_empty committed, tx_id={}",
        write_if_not_empty_tx
    );
    let write_if_starts_with_tx = daemon.write_if_starts_with(
        "/daemon/demo.txt",
        b"fuse-daemon",
        b"fuse-daemon-demo-bytes-v8",
    )?;
    println!(
        "FUSE daemon write_if_starts_with committed, tx_id={}",
        write_if_starts_with_tx
    );
    let write_if_ends_with_tx = daemon.write_if_ends_with(
        "/daemon/demo.txt",
        b"-v8",
        b"fuse-daemon-demo-bytes-v9",
    )?;
    println!(
        "FUSE daemon write_if_ends_with committed, tx_id={}",
        write_if_ends_with_tx
    );
    let write_if_contains_tx = daemon.write_if_contains(
        "/daemon/demo.txt",
        b"demo-bytes",
        b"fuse-daemon-demo-bytes-v10",
    )?;
    println!(
        "FUSE daemon write_if_contains committed, tx_id={}",
        write_if_contains_tx
    );
    let write_if_not_contains_tx = daemon.write_if_not_contains(
        "/daemon/demo.txt",
        b"forbidden",
        b"fuse-daemon-demo-bytes-v11",
    )?;
    println!(
        "FUSE daemon write_if_not_contains committed, tx_id={}",
        write_if_not_contains_tx
    );
    let write_if_exact_tx = daemon.write_if_exact(
        "/daemon/demo.txt",
        b"fuse-daemon-demo-bytes-v11",
        b"fuse-daemon-demo-bytes-v12",
    )?;
    println!(
        "FUSE daemon write_if_exact committed, tx_id={}",
        write_if_exact_tx
    );
    let write_if_not_exact_tx = daemon.write_if_not_exact(
        "/daemon/demo.txt",
        b"forbidden-exact",
        b"fuse-daemon-demo-bytes-v13",
    )?;
    println!(
        "FUSE daemon write_if_not_exact committed, tx_id={}",
        write_if_not_exact_tx
    );
    let write_if_min_size_tx =
        daemon.write_if_min_size("/daemon/demo.txt", 10, b"fuse-daemon-demo-bytes-v14")?;
    println!(
        "FUSE daemon write_if_min_size committed, tx_id={}",
        write_if_min_size_tx
    );
    let write_if_max_size_tx =
        daemon.write_if_max_size("/daemon/demo.txt", 30, b"fuse-daemon-demo-bytes-v15")?;
    println!(
        "FUSE daemon write_if_max_size committed, tx_id={}",
        write_if_max_size_tx
    );
    let write_if_size_between_tx =
        daemon.write_if_size_between("/daemon/demo.txt", 10, 30, b"fuse-daemon-demo-bytes-v16")?;
    println!(
        "FUSE daemon write_if_size_between committed, tx_id={}",
        write_if_size_between_tx
    );
    let write_if_size_not_between_tx = daemon.write_if_size_not_between(
        "/daemon/demo.txt",
        100,
        200,
        b"fuse-daemon-demo-bytes-v17",
    )?;
    println!(
        "FUSE daemon write_if_size_not_between committed, tx_id={}",
        write_if_size_not_between_tx
    );
    let write_if_size_multiple_of_tx =
        daemon.write_if_size_multiple_of("/daemon/demo.txt", 2, b"fuse-daemon-demo-bytes-v18")?;
    println!(
        "FUSE daemon write_if_size_multiple_of committed, tx_id={}",
        write_if_size_multiple_of_tx
    );
    let write_if_size_not_multiple_of_tx = daemon.write_if_size_not_multiple_of(
        "/daemon/demo.txt",
        7,
        b"fuse-daemon-demo-bytes-v19",
    )?;
    println!(
        "FUSE daemon write_if_size_not_multiple_of committed, tx_id={}",
        write_if_size_not_multiple_of_tx
    );
    let write_if_size_even_tx =
        daemon.write_if_size_even("/daemon/demo.txt", b"fuse-daemon-demo-bytes-v20x")?;
    println!(
        "FUSE daemon write_if_size_even committed, tx_id={}",
        write_if_size_even_tx
    );
    let write_if_size_odd_tx =
        daemon.write_if_size_odd("/daemon/demo.txt", b"fuse-daemon-demo-bytes-v21")?;
    println!(
        "FUSE daemon write_if_size_odd committed, tx_id={}",
        write_if_size_odd_tx
    );
    let pow2_seed_tx = daemon.write_if_missing("/daemon/pow2.txt", b"12345678")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow2.txt) committed, tx_id={}",
        pow2_seed_tx
    );
    let write_if_size_power_of_two_tx =
        daemon.write_if_size_power_of_two("/daemon/pow2.txt", b"fuse-daemon-pow2-v22")?;
    println!(
        "FUSE daemon write_if_size_power_of_two committed, tx_id={}",
        write_if_size_power_of_two_tx
    );
    let nonpower_seed_tx = daemon.write_if_missing("/daemon/nonpower.txt", b"1234567")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpower.txt) committed, tx_id={}",
        nonpower_seed_tx
    );
    let write_if_size_not_power_of_two_tx = daemon
        .write_if_size_not_power_of_two("/daemon/nonpower.txt", b"fuse-daemon-nonpower-v23")?;
    println!(
        "FUSE daemon write_if_size_not_power_of_two committed, tx_id={}",
        write_if_size_not_power_of_two_tx
    );
    let prime_seed_tx = daemon.write_if_missing("/daemon/prime.txt", b"1234567")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/prime.txt) committed, tx_id={}",
        prime_seed_tx
    );
    let write_if_size_prime_tx =
        daemon.write_if_size_prime("/daemon/prime.txt", b"fuse-daemon-prime-v24")?;
    println!(
        "FUSE daemon write_if_size_prime committed, tx_id={}",
        write_if_size_prime_tx
    );
    let nonprime_seed_tx = daemon.write_if_missing("/daemon/nonprime2.txt", b"12345678")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonprime2.txt) committed, tx_id={}",
        nonprime_seed_tx
    );
    let write_if_size_not_prime_tx =
        daemon.write_if_size_not_prime("/daemon/nonprime2.txt", b"fuse-daemon-nonprime-v25")?;
    println!(
        "FUSE daemon write_if_size_not_prime committed, tx_id={}",
        write_if_size_not_prime_tx
    );
    let fib_seed_tx = daemon.write_if_missing("/daemon/fib.txt", b"12345678")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/fib.txt) committed, tx_id={}",
        fib_seed_tx
    );
    let write_if_size_fibonacci_tx =
        daemon.write_if_size_fibonacci("/daemon/fib.txt", b"fuse-daemon-fib-v26")?;
    println!(
        "FUSE daemon write_if_size_fibonacci committed, tx_id={}",
        write_if_size_fibonacci_tx
    );
    let nonfib_seed_tx = daemon.write_if_missing("/daemon/nonfib.txt", b"1234567")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonfib.txt) committed, tx_id={}",
        nonfib_seed_tx
    );
    let write_if_size_not_fibonacci_tx = daemon
        .write_if_size_not_fibonacci("/daemon/nonfib.txt", b"fuse-daemon-nonfib-v27")?;
    println!(
        "FUSE daemon write_if_size_not_fibonacci committed, tx_id={}",
        write_if_size_not_fibonacci_tx
    );
    let square_seed_tx = daemon.write_if_missing("/daemon/square.txt", b"123456789")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/square.txt) committed, tx_id={}",
        square_seed_tx
    );
    let write_if_size_square_tx =
        daemon.write_if_size_square("/daemon/square.txt", b"fuse-daemon-square-v28")?;
    println!(
        "FUSE daemon write_if_size_square committed, tx_id={}",
        write_if_size_square_tx
    );
    let nonsquare_seed_tx = daemon.write_if_missing("/daemon/nonsquare.txt", b"1234567")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonsquare.txt) committed, tx_id={}",
        nonsquare_seed_tx
    );
    let write_if_size_not_square_tx = daemon
        .write_if_size_not_square("/daemon/nonsquare.txt", b"fuse-daemon-nonsquare-v29")?;
    println!(
        "FUSE daemon write_if_size_not_square committed, tx_id={}",
        write_if_size_not_square_tx
    );
    let cube_seed_tx = daemon.write_if_missing("/daemon/cube.txt", b"12345678")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/cube.txt) committed, tx_id={}",
        cube_seed_tx
    );
    let write_if_size_cube_tx =
        daemon.write_if_size_cube("/daemon/cube.txt", b"fuse-daemon-cube-v30")?;
    println!(
        "FUSE daemon write_if_size_cube committed, tx_id={}",
        write_if_size_cube_tx
    );
    let noncube_seed_tx = daemon.write_if_missing("/daemon/noncube.txt", b"1234567")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/noncube.txt) committed, tx_id={}",
        noncube_seed_tx
    );
    let write_if_size_not_cube_tx =
        daemon.write_if_size_not_cube("/daemon/noncube.txt", b"fuse-daemon-noncube-v31")?;
    println!(
        "FUSE daemon write_if_size_not_cube committed, tx_id={}",
        write_if_size_not_cube_tx
    );
    let tri_seed_tx = daemon.write_if_missing("/daemon/tri.txt", b"123456")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/tri.txt) committed, tx_id={}",
        tri_seed_tx
    );
    let write_if_size_triangular_tx =
        daemon.write_if_size_triangular("/daemon/tri.txt", b"fuse-daemon-tri-v32")?;
    println!(
        "FUSE daemon write_if_size_triangular committed, tx_id={}",
        write_if_size_triangular_tx
    );
    let nontri_seed_tx = daemon.write_if_missing("/daemon/nontri.txt", b"12345")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nontri.txt) committed, tx_id={}",
        nontri_seed_tx
    );
    let write_if_size_not_triangular_tx =
        daemon.write_if_size_not_triangular("/daemon/nontri.txt", b"fuse-daemon-nontri-v33")?;
    println!(
        "FUSE daemon write_if_size_not_triangular committed, tx_id={}",
        write_if_size_not_triangular_tx
    );
    let factorial_seed_tx = daemon.write_if_missing("/daemon/factorial.txt", b"123456")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/factorial.txt) committed, tx_id={}",
        factorial_seed_tx
    );
    let write_if_size_factorial_tx =
        daemon.write_if_size_factorial("/daemon/factorial.txt", b"fuse-daemon-factorial-v34")?;
    println!(
        "FUSE daemon write_if_size_factorial committed, tx_id={}",
        write_if_size_factorial_tx
    );
    let nonfactorial_seed_tx = daemon.write_if_missing("/daemon/nonfactorial.txt", b"12345")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonfactorial.txt) committed, tx_id={}",
        nonfactorial_seed_tx
    );
    let write_if_size_not_factorial_tx = daemon
        .write_if_size_not_factorial("/daemon/nonfactorial.txt", b"fuse-daemon-nonfactorial-v35")?;
    println!(
        "FUSE daemon write_if_size_not_factorial committed, tx_id={}",
        write_if_size_not_factorial_tx
    );
    let composite_seed_tx = daemon.write_if_missing("/daemon/composite.txt", b"12345678")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/composite.txt) committed, tx_id={}",
        composite_seed_tx
    );
    let write_if_size_composite_tx =
        daemon.write_if_size_composite("/daemon/composite.txt", b"fuse-daemon-composite-v36")?;
    println!(
        "FUSE daemon write_if_size_composite committed, tx_id={}",
        write_if_size_composite_tx
    );
    let noncomposite_seed_tx = daemon.write_if_missing("/daemon/noncomposite.txt", b"1234567")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/noncomposite.txt) committed, tx_id={}",
        noncomposite_seed_tx
    );
    let write_if_size_not_composite_tx = daemon
        .write_if_size_not_composite("/daemon/noncomposite.txt", b"fuse-daemon-noncomposite-v37")?;
    println!(
        "FUSE daemon write_if_size_not_composite committed, tx_id={}",
        write_if_size_not_composite_tx
    );
    let perfect_seed_tx = daemon.write_if_missing("/daemon/perfect.txt", b"123456")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/perfect.txt) committed, tx_id={}",
        perfect_seed_tx
    );
    let write_if_size_perfect_tx =
        daemon.write_if_size_perfect("/daemon/perfect.txt", b"fuse-daemon-perfect-v38")?;
    println!(
        "FUSE daemon write_if_size_perfect committed, tx_id={}",
        write_if_size_perfect_tx
    );
    let nonperfect_seed_tx = daemon.write_if_missing("/daemon/nonperfect.txt", b"12345")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonperfect.txt) committed, tx_id={}",
        nonperfect_seed_tx
    );
    let write_if_size_not_perfect_tx =
        daemon.write_if_size_not_perfect("/daemon/nonperfect.txt", b"fuse-daemon-nonperfect-v39")?;
    println!(
        "FUSE daemon write_if_size_not_perfect committed, tx_id={}",
        write_if_size_not_perfect_tx
    );
    let abundant_seed_tx = daemon.write_if_missing("/daemon/abundant.txt", b"123456789012")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/abundant.txt) committed, tx_id={}",
        abundant_seed_tx
    );
    let write_if_size_abundant_tx =
        daemon.write_if_size_abundant("/daemon/abundant.txt", b"fuse-daemon-abundant-v40")?;
    println!(
        "FUSE daemon write_if_size_abundant committed, tx_id={}",
        write_if_size_abundant_tx
    );
    let nonabundant_seed_tx = daemon.write_if_missing("/daemon/nonabundant.txt", b"1234567")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonabundant.txt) committed, tx_id={}",
        nonabundant_seed_tx
    );
    let write_if_size_not_abundant_tx =
        daemon.write_if_size_not_abundant("/daemon/nonabundant.txt", b"fuse-daemon-nonabundant-v41")?;
    println!(
        "FUSE daemon write_if_size_not_abundant committed, tx_id={}",
        write_if_size_not_abundant_tx
    );
    let deficient_seed_tx = daemon.write_if_missing("/daemon/deficient.txt", b"1234567")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/deficient.txt) committed, tx_id={}",
        deficient_seed_tx
    );
    let write_if_size_deficient_tx =
        daemon.write_if_size_deficient("/daemon/deficient.txt", b"fuse-daemon-deficient-v42")?;
    println!(
        "FUSE daemon write_if_size_deficient committed, tx_id={}",
        write_if_size_deficient_tx
    );
    let nondeficient_seed_tx = daemon.write_if_missing("/daemon/nondeficient.txt", b"123456789012")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nondeficient.txt) committed, tx_id={}",
        nondeficient_seed_tx
    );
    let write_if_size_not_deficient_tx = daemon
        .write_if_size_not_deficient("/daemon/nondeficient.txt", b"fuse-daemon-nondeficient-v43")?;
    println!(
        "FUSE daemon write_if_size_not_deficient committed, tx_id={}",
        write_if_size_not_deficient_tx
    );
    let semiprime_seed_tx = daemon.write_if_missing("/daemon/semiprime.txt", b"123456")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/semiprime.txt) committed, tx_id={}",
        semiprime_seed_tx
    );
    let write_if_size_semiprime_tx =
        daemon.write_if_size_semiprime("/daemon/semiprime.txt", b"fuse-daemon-semiprime-v44")?;
    println!(
        "FUSE daemon write_if_size_semiprime committed, tx_id={}",
        write_if_size_semiprime_tx
    );
    let nonsemiprime_seed_tx = daemon.write_if_missing("/daemon/nonsemiprime.txt", b"1234567")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonsemiprime.txt) committed, tx_id={}",
        nonsemiprime_seed_tx
    );
    let write_if_size_not_semiprime_tx = daemon
        .write_if_size_not_semiprime("/daemon/nonsemiprime.txt", b"fuse-daemon-nonsemiprime-v45")?;
    println!(
        "FUSE daemon write_if_size_not_semiprime committed, tx_id={}",
        write_if_size_not_semiprime_tx
    );
    let palindrome_seed_tx = daemon.write_if_missing("/daemon/palindrome.txt", b"12345678901")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/palindrome.txt) committed, tx_id={}",
        palindrome_seed_tx
    );
    let write_if_size_palindrome_tx =
        daemon.write_if_size_palindrome("/daemon/palindrome.txt", b"fuse-daemon-palindrome-v46")?;
    println!(
        "FUSE daemon write_if_size_palindrome committed, tx_id={}",
        write_if_size_palindrome_tx
    );
    let nonpalindrome_seed_tx = daemon.write_if_missing("/daemon/nonpalindrome.txt", b"1234567890")?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpalindrome.txt) committed, tx_id={}",
        nonpalindrome_seed_tx
    );
    let write_if_size_not_palindrome_tx = daemon.write_if_size_not_palindrome(
        "/daemon/nonpalindrome.txt",
        b"fuse-daemon-nonpalindrome-v47",
    )?;
    println!(
        "FUSE daemon write_if_size_not_palindrome committed, tx_id={}",
        write_if_size_not_palindrome_tx
    );
    let armstrong_seed = vec![b'a'; 153];
    let armstrong_seed_tx = daemon.write_if_missing("/daemon/armstrong.txt", &armstrong_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/armstrong.txt) committed, tx_id={}",
        armstrong_seed_tx
    );
    let write_if_size_armstrong_tx =
        daemon.write_if_size_armstrong("/daemon/armstrong.txt", b"fuse-daemon-armstrong-v48")?;
    println!(
        "FUSE daemon write_if_size_armstrong committed, tx_id={}",
        write_if_size_armstrong_tx
    );
    let nonarmstrong_seed = vec![b'a'; 154];
    let nonarmstrong_seed_tx =
        daemon.write_if_missing("/daemon/nonarmstrong.txt", &nonarmstrong_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonarmstrong.txt) committed, tx_id={}",
        nonarmstrong_seed_tx
    );
    let write_if_size_not_armstrong_tx = daemon.write_if_size_not_armstrong(
        "/daemon/nonarmstrong.txt",
        b"fuse-daemon-nonarmstrong-v49",
    )?;
    println!(
        "FUSE daemon write_if_size_not_armstrong committed, tx_id={}",
        write_if_size_not_armstrong_tx
    );
    let happy_seed = vec![b'a'; 19];
    let happy_seed_tx = daemon.write_if_missing("/daemon/happy.txt", &happy_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/happy.txt) committed, tx_id={}",
        happy_seed_tx
    );
    let write_if_size_happy_tx =
        daemon.write_if_size_happy("/daemon/happy.txt", b"fuse-daemon-happy-v50")?;
    println!(
        "FUSE daemon write_if_size_happy committed, tx_id={}",
        write_if_size_happy_tx
    );
    let nonhappy_seed = vec![b'a'; 20];
    let nonhappy_seed_tx = daemon.write_if_missing("/daemon/nonhappy.txt", &nonhappy_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonhappy.txt) committed, tx_id={}",
        nonhappy_seed_tx
    );
    let write_if_size_not_happy_tx =
        daemon.write_if_size_not_happy("/daemon/nonhappy.txt", b"fuse-daemon-nonhappy-v51")?;
    println!(
        "FUSE daemon write_if_size_not_happy committed, tx_id={}",
        write_if_size_not_happy_tx
    );
    let automorphic_seed = vec![b'a'; 25];
    let automorphic_seed_tx =
        daemon.write_if_missing("/daemon/automorphic.txt", &automorphic_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/automorphic.txt) committed, tx_id={}",
        automorphic_seed_tx
    );
    let write_if_size_automorphic_tx = daemon
        .write_if_size_automorphic("/daemon/automorphic.txt", b"fuse-daemon-automorphic-v52")?;
    println!(
        "FUSE daemon write_if_size_automorphic committed, tx_id={}",
        write_if_size_automorphic_tx
    );
    let nonautomorphic_seed = vec![b'a'; 26];
    let nonautomorphic_seed_tx =
        daemon.write_if_missing("/daemon/nonautomorphic.txt", &nonautomorphic_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonautomorphic.txt) committed, tx_id={}",
        nonautomorphic_seed_tx
    );
    let write_if_size_not_automorphic_tx = daemon.write_if_size_not_automorphic(
        "/daemon/nonautomorphic.txt",
        b"fuse-daemon-nonautomorphic-v53",
    )?;
    println!(
        "FUSE daemon write_if_size_not_automorphic committed, tx_id={}",
        write_if_size_not_automorphic_tx
    );
    let harshad_seed = vec![b'a'; 18];
    let harshad_seed_tx = daemon.write_if_missing("/daemon/harshad.txt", &harshad_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/harshad.txt) committed, tx_id={}",
        harshad_seed_tx
    );
    let write_if_size_harshad_tx =
        daemon.write_if_size_harshad("/daemon/harshad.txt", b"fuse-daemon-harshad-v54")?;
    println!(
        "FUSE daemon write_if_size_harshad committed, tx_id={}",
        write_if_size_harshad_tx
    );
    let nonharshad_seed = vec![b'a'; 19];
    let nonharshad_seed_tx =
        daemon.write_if_missing("/daemon/nonharshad.txt", &nonharshad_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonharshad.txt) committed, tx_id={}",
        nonharshad_seed_tx
    );
    let write_if_size_not_harshad_tx =
        daemon.write_if_size_not_harshad("/daemon/nonharshad.txt", b"fuse-daemon-nonharshad-v55")?;
    println!(
        "FUSE daemon write_if_size_not_harshad committed, tx_id={}",
        write_if_size_not_harshad_tx
    );
    let kaprekar_seed = vec![b'a'; 45];
    let kaprekar_seed_tx = daemon.write_if_missing("/daemon/kaprekar.txt", &kaprekar_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/kaprekar.txt) committed, tx_id={}",
        kaprekar_seed_tx
    );
    let write_if_size_kaprekar_tx =
        daemon.write_if_size_kaprekar("/daemon/kaprekar.txt", b"fuse-daemon-kaprekar-v56")?;
    println!(
        "FUSE daemon write_if_size_kaprekar committed, tx_id={}",
        write_if_size_kaprekar_tx
    );
    let nonkaprekar_seed = vec![b'a'; 46];
    let nonkaprekar_seed_tx =
        daemon.write_if_missing("/daemon/nonkaprekar.txt", &nonkaprekar_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonkaprekar.txt) committed, tx_id={}",
        nonkaprekar_seed_tx
    );
    let write_if_size_not_kaprekar_tx = daemon
        .write_if_size_not_kaprekar("/daemon/nonkaprekar.txt", b"fuse-daemon-nonkaprekar-v57")?;
    println!(
        "FUSE daemon write_if_size_not_kaprekar committed, tx_id={}",
        write_if_size_not_kaprekar_tx
    );
    let repdigit_seed = vec![b'a'; 11];
    let repdigit_seed_tx = daemon.write_if_missing("/daemon/repdigit.txt", &repdigit_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/repdigit.txt) committed, tx_id={}",
        repdigit_seed_tx
    );
    let write_if_size_repdigit_tx =
        daemon.write_if_size_repdigit("/daemon/repdigit.txt", b"fuse-daemon-repdigit-v58")?;
    println!(
        "FUSE daemon write_if_size_repdigit committed, tx_id={}",
        write_if_size_repdigit_tx
    );
    let nonrepdigit_seed = vec![b'a'; 12];
    let nonrepdigit_seed_tx =
        daemon.write_if_missing("/daemon/nonrepdigit.txt", &nonrepdigit_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonrepdigit.txt) committed, tx_id={}",
        nonrepdigit_seed_tx
    );
    let write_if_size_not_repdigit_tx = daemon
        .write_if_size_not_repdigit("/daemon/nonrepdigit.txt", b"fuse-daemon-nonrepdigit-v59")?;
    println!(
        "FUSE daemon write_if_size_not_repdigit committed, tx_id={}",
        write_if_size_not_repdigit_tx
    );
    let tribonacci_seed = vec![b'a'; 24];
    let tribonacci_seed_tx =
        daemon.write_if_missing("/daemon/tribonacci.txt", &tribonacci_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/tribonacci.txt) committed, tx_id={}",
        tribonacci_seed_tx
    );
    let write_if_size_tribonacci_tx =
        daemon.write_if_size_tribonacci("/daemon/tribonacci.txt", b"fuse-daemon-tribonacci-v60")?;
    println!(
        "FUSE daemon write_if_size_tribonacci committed, tx_id={}",
        write_if_size_tribonacci_tx
    );
    let nontribonacci_seed = vec![b'a'; 25];
    let nontribonacci_seed_tx =
        daemon.write_if_missing("/daemon/nontribonacci.txt", &nontribonacci_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nontribonacci.txt) committed, tx_id={}",
        nontribonacci_seed_tx
    );
    let write_if_size_not_tribonacci_tx = daemon.write_if_size_not_tribonacci(
        "/daemon/nontribonacci.txt",
        b"fuse-daemon-nontribonacci-v61",
    )?;
    println!(
        "FUSE daemon write_if_size_not_tribonacci committed, tx_id={}",
        write_if_size_not_tribonacci_tx
    );
    let pell_seed = vec![b'a'; 29];
    let pell_seed_tx = daemon.write_if_missing("/daemon/pell.txt", &pell_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pell.txt) committed, tx_id={}",
        pell_seed_tx
    );
    let write_if_size_pell_tx =
        daemon.write_if_size_pell("/daemon/pell.txt", b"fuse-daemon-pell-v62")?;
    println!(
        "FUSE daemon write_if_size_pell committed, tx_id={}",
        write_if_size_pell_tx
    );
    let nonpell_seed = vec![b'a'; 30];
    let nonpell_seed_tx = daemon.write_if_missing("/daemon/nonpell.txt", &nonpell_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpell.txt) committed, tx_id={}",
        nonpell_seed_tx
    );
    let write_if_size_not_pell_tx =
        daemon.write_if_size_not_pell("/daemon/nonpell.txt", b"fuse-daemon-nonpell-v63")?;
    println!(
        "FUSE daemon write_if_size_not_pell committed, tx_id={}",
        write_if_size_not_pell_tx
    );
    let lucas_seed = vec![b'a'; 29];
    let lucas_seed_tx = daemon.write_if_missing("/daemon/lucas.txt", &lucas_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/lucas.txt) committed, tx_id={}",
        lucas_seed_tx
    );
    let write_if_size_lucas_tx =
        daemon.write_if_size_lucas("/daemon/lucas.txt", b"fuse-daemon-lucas-v64")?;
    println!(
        "FUSE daemon write_if_size_lucas committed, tx_id={}",
        write_if_size_lucas_tx
    );
    let nonlucas_seed = vec![b'a'; 30];
    let nonlucas_seed_tx = daemon.write_if_missing("/daemon/nonlucas.txt", &nonlucas_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonlucas.txt) committed, tx_id={}",
        nonlucas_seed_tx
    );
    let write_if_size_not_lucas_tx =
        daemon.write_if_size_not_lucas("/daemon/nonlucas.txt", b"fuse-daemon-nonlucas-v65")?;
    println!(
        "FUSE daemon write_if_size_not_lucas committed, tx_id={}",
        write_if_size_not_lucas_tx
    );
    let mersenne_seed = vec![b'a'; 31];
    let mersenne_seed_tx = daemon.write_if_missing("/daemon/mersenne.txt", &mersenne_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/mersenne.txt) committed, tx_id={}",
        mersenne_seed_tx
    );
    let write_if_size_mersenne_tx =
        daemon.write_if_size_mersenne("/daemon/mersenne.txt", b"fuse-daemon-mersenne-v66")?;
    println!(
        "FUSE daemon write_if_size_mersenne committed, tx_id={}",
        write_if_size_mersenne_tx
    );
    let nonmersenne_seed = vec![b'a'; 32];
    let nonmersenne_seed_tx =
        daemon.write_if_missing("/daemon/nonmersenne.txt", &nonmersenne_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonmersenne.txt) committed, tx_id={}",
        nonmersenne_seed_tx
    );
    let write_if_size_not_mersenne_tx = daemon
        .write_if_size_not_mersenne("/daemon/nonmersenne.txt", b"fuse-daemon-nonmersenne-v67")?;
    println!(
        "FUSE daemon write_if_size_not_mersenne committed, tx_id={}",
        write_if_size_not_mersenne_tx
    );
    let pow3_seed = vec![b'a'; 27];
    let pow3_seed_tx = daemon.write_if_missing("/daemon/pow3.txt", &pow3_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow3.txt) committed, tx_id={}",
        pow3_seed_tx
    );
    let write_if_size_power_of_three_tx =
        daemon.write_if_size_power_of_three("/daemon/pow3.txt", b"fuse-daemon-pow3-v68")?;
    println!(
        "FUSE daemon write_if_size_power_of_three committed, tx_id={}",
        write_if_size_power_of_three_tx
    );
    let nonpow3_seed = vec![b'a'; 28];
    let nonpow3_seed_tx = daemon.write_if_missing("/daemon/nonpow3.txt", &nonpow3_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow3.txt) committed, tx_id={}",
        nonpow3_seed_tx
    );
    let write_if_size_not_power_of_three_tx = daemon
        .write_if_size_not_power_of_three("/daemon/nonpow3.txt", b"fuse-daemon-nonpow3-v69")?;
    println!(
        "FUSE daemon write_if_size_not_power_of_three committed, tx_id={}",
        write_if_size_not_power_of_three_tx
    );
    let pow4_seed = vec![b'a'; 64];
    let pow4_seed_tx = daemon.write_if_missing("/daemon/pow4.txt", &pow4_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow4.txt) committed, tx_id={}",
        pow4_seed_tx
    );
    let write_if_size_power_of_four_tx =
        daemon.write_if_size_power_of_four("/daemon/pow4.txt", b"fuse-daemon-pow4-v70")?;
    println!(
        "FUSE daemon write_if_size_power_of_four committed, tx_id={}",
        write_if_size_power_of_four_tx
    );
    let nonpow4_seed = vec![b'a'; 65];
    let nonpow4_seed_tx = daemon.write_if_missing("/daemon/nonpow4.txt", &nonpow4_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow4.txt) committed, tx_id={}",
        nonpow4_seed_tx
    );
    let write_if_size_not_power_of_four_tx =
        daemon.write_if_size_not_power_of_four("/daemon/nonpow4.txt", b"fuse-daemon-nonpow4-v71")?;
    println!(
        "FUSE daemon write_if_size_not_power_of_four committed, tx_id={}",
        write_if_size_not_power_of_four_tx
    );
    let pow5_seed = vec![b'a'; 125];
    let pow5_seed_tx = daemon.write_if_missing("/daemon/pow5.txt", &pow5_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow5.txt) committed, tx_id={}",
        pow5_seed_tx
    );
    let write_if_size_power_of_five_tx =
        daemon.write_if_size_power_of_five("/daemon/pow5.txt", b"fuse-daemon-pow5-v72")?;
    println!(
        "FUSE daemon write_if_size_power_of_five committed, tx_id={}",
        write_if_size_power_of_five_tx
    );
    let nonpow5_seed = vec![b'a'; 126];
    let nonpow5_seed_tx = daemon.write_if_missing("/daemon/nonpow5.txt", &nonpow5_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow5.txt) committed, tx_id={}",
        nonpow5_seed_tx
    );
    let write_if_size_not_power_of_five_tx =
        daemon.write_if_size_not_power_of_five("/daemon/nonpow5.txt", b"fuse-daemon-nonpow5-v73")?;
    println!(
        "FUSE daemon write_if_size_not_power_of_five committed, tx_id={}",
        write_if_size_not_power_of_five_tx
    );
    let pow6_seed = vec![b'a'; 216];
    let pow6_seed_tx = daemon.write_if_missing("/daemon/pow6.txt", &pow6_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow6.txt) committed, tx_id={}",
        pow6_seed_tx
    );
    let write_if_size_power_of_six_tx =
        daemon.write_if_size_power_of_six("/daemon/pow6.txt", b"fuse-daemon-pow6-v74")?;
    println!(
        "FUSE daemon write_if_size_power_of_six committed, tx_id={}",
        write_if_size_power_of_six_tx
    );
    let nonpow6_seed = vec![b'a'; 217];
    let nonpow6_seed_tx = daemon.write_if_missing("/daemon/nonpow6.txt", &nonpow6_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow6.txt) committed, tx_id={}",
        nonpow6_seed_tx
    );
    let write_if_size_not_power_of_six_tx =
        daemon.write_if_size_not_power_of_six("/daemon/nonpow6.txt", b"fuse-daemon-nonpow6-v75")?;
    println!(
        "FUSE daemon write_if_size_not_power_of_six committed, tx_id={}",
        write_if_size_not_power_of_six_tx
    );
    let pow7_seed = vec![b'a'; 343];
    let pow7_seed_tx = daemon.write_if_missing("/daemon/pow7.txt", &pow7_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow7.txt) committed, tx_id={}",
        pow7_seed_tx
    );
    let write_if_size_power_of_seven_tx =
        daemon.write_if_size_power_of_seven("/daemon/pow7.txt", b"fuse-daemon-pow7-v76")?;
    println!(
        "FUSE daemon write_if_size_power_of_seven committed, tx_id={}",
        write_if_size_power_of_seven_tx
    );
    let nonpow7_seed = vec![b'a'; 344];
    let nonpow7_seed_tx = daemon.write_if_missing("/daemon/nonpow7.txt", &nonpow7_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow7.txt) committed, tx_id={}",
        nonpow7_seed_tx
    );
    let write_if_size_not_power_of_seven_tx =
        daemon.write_if_size_not_power_of_seven("/daemon/nonpow7.txt", b"fuse-daemon-nonpow7-v77")?;
    println!(
        "FUSE daemon write_if_size_not_power_of_seven committed, tx_id={}",
        write_if_size_not_power_of_seven_tx
    );
    let pow8_seed = vec![b'a'; 512];
    let pow8_seed_tx = daemon.write_if_missing("/daemon/pow8.txt", &pow8_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow8.txt) committed, tx_id={}",
        pow8_seed_tx
    );
    let write_if_size_power_of_eight_tx =
        daemon.write_if_size_power_of_eight("/daemon/pow8.txt", b"fuse-daemon-pow8-v78")?;
    println!(
        "FUSE daemon write_if_size_power_of_eight committed, tx_id={}",
        write_if_size_power_of_eight_tx
    );
    let nonpow8_seed = vec![b'a'; 513];
    let nonpow8_seed_tx = daemon.write_if_missing("/daemon/nonpow8.txt", &nonpow8_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow8.txt) committed, tx_id={}",
        nonpow8_seed_tx
    );
    let write_if_size_not_power_of_eight_tx =
        daemon.write_if_size_not_power_of_eight("/daemon/nonpow8.txt", b"fuse-daemon-nonpow8-v79")?;
    println!(
        "FUSE daemon write_if_size_not_power_of_eight committed, tx_id={}",
        write_if_size_not_power_of_eight_tx
    );
    let pow9_seed = vec![b'a'; 729];
    let pow9_seed_tx = daemon.write_if_missing("/daemon/pow9.txt", &pow9_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow9.txt) committed, tx_id={}",
        pow9_seed_tx
    );
    let write_if_size_power_of_nine_tx =
        daemon.write_if_size_power_of_nine("/daemon/pow9.txt", b"fuse-daemon-pow9-v80")?;
    println!(
        "FUSE daemon write_if_size_power_of_nine committed, tx_id={}",
        write_if_size_power_of_nine_tx
    );
    let nonpow9_seed = vec![b'a'; 730];
    let nonpow9_seed_tx = daemon.write_if_missing("/daemon/nonpow9.txt", &nonpow9_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow9.txt) committed, tx_id={}",
        nonpow9_seed_tx
    );
    let write_if_size_not_power_of_nine_tx =
        daemon.write_if_size_not_power_of_nine("/daemon/nonpow9.txt", b"fuse-daemon-nonpow9-v81")?;
    println!(
        "FUSE daemon write_if_size_not_power_of_nine committed, tx_id={}",
        write_if_size_not_power_of_nine_tx
    );
    let pow10_seed = vec![b'a'; 1000];
    let pow10_seed_tx = daemon.write_if_missing("/daemon/pow10.txt", &pow10_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow10.txt) committed, tx_id={}",
        pow10_seed_tx
    );
    let write_if_size_power_of_ten_tx =
        daemon.write_if_size_power_of_ten("/daemon/pow10.txt", b"fuse-daemon-pow10-v82")?;
    println!(
        "FUSE daemon write_if_size_power_of_ten committed, tx_id={}",
        write_if_size_power_of_ten_tx
    );
    let nonpow10_seed = vec![b'a'; 1001];
    let nonpow10_seed_tx = daemon.write_if_missing("/daemon/nonpow10.txt", &nonpow10_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow10.txt) committed, tx_id={}",
        nonpow10_seed_tx
    );
    let write_if_size_not_power_of_ten_tx =
        daemon.write_if_size_not_power_of_ten("/daemon/nonpow10.txt", b"fuse-daemon-nonpow10-v83")?;
    println!(
        "FUSE daemon write_if_size_not_power_of_ten committed, tx_id={}",
        write_if_size_not_power_of_ten_tx
    );
    let pow11_seed = vec![b'a'; 121];
    let pow11_seed_tx = daemon.write_if_missing("/daemon/pow11.txt", &pow11_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow11.txt) committed, tx_id={}",
        pow11_seed_tx
    );
    let write_if_size_power_of_eleven_tx =
        daemon.write_if_size_power_of_eleven("/daemon/pow11.txt", b"fuse-daemon-pow11-v84")?;
    println!(
        "FUSE daemon write_if_size_power_of_eleven committed, tx_id={}",
        write_if_size_power_of_eleven_tx
    );
    let nonpow11_seed = vec![b'a'; 122];
    let nonpow11_seed_tx = daemon.write_if_missing("/daemon/nonpow11.txt", &nonpow11_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow11.txt) committed, tx_id={}",
        nonpow11_seed_tx
    );
    let write_if_size_not_power_of_eleven_tx = daemon
        .write_if_size_not_power_of_eleven("/daemon/nonpow11.txt", b"fuse-daemon-nonpow11-v85")?;
    println!(
        "FUSE daemon write_if_size_not_power_of_eleven committed, tx_id={}",
        write_if_size_not_power_of_eleven_tx
    );
    let pow12_seed = vec![b'a'; 144];
    let pow12_seed_tx = daemon.write_if_missing("/daemon/pow12.txt", &pow12_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow12.txt) committed, tx_id={}",
        pow12_seed_tx
    );
    let write_if_size_power_of_twelve_tx =
        daemon.write_if_size_power_of_twelve("/daemon/pow12.txt", b"fuse-daemon-pow12-v86")?;
    println!(
        "FUSE daemon write_if_size_power_of_twelve committed, tx_id={}",
        write_if_size_power_of_twelve_tx
    );
    let nonpow12_seed = vec![b'a'; 145];
    let nonpow12_seed_tx = daemon.write_if_missing("/daemon/nonpow12.txt", &nonpow12_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow12.txt) committed, tx_id={}",
        nonpow12_seed_tx
    );
    let write_if_size_not_power_of_twelve_tx = daemon
        .write_if_size_not_power_of_twelve("/daemon/nonpow12.txt", b"fuse-daemon-nonpow12-v87")?;
    println!(
        "FUSE daemon write_if_size_not_power_of_twelve committed, tx_id={}",
        write_if_size_not_power_of_twelve_tx
    );
    let pow13_seed = vec![b'a'; 169];
    let pow13_seed_tx = daemon.write_if_missing("/daemon/pow13.txt", &pow13_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow13.txt) committed, tx_id={}",
        pow13_seed_tx
    );
    let write_if_size_power_of_thirteen_tx =
        daemon.write_if_size_power_of_thirteen("/daemon/pow13.txt", b"fuse-daemon-pow13-v88")?;
    println!(
        "FUSE daemon write_if_size_power_of_thirteen committed, tx_id={}",
        write_if_size_power_of_thirteen_tx
    );
    let nonpow13_seed = vec![b'a'; 170];
    let nonpow13_seed_tx = daemon.write_if_missing("/daemon/nonpow13.txt", &nonpow13_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow13.txt) committed, tx_id={}",
        nonpow13_seed_tx
    );
    let write_if_size_not_power_of_thirteen_tx = daemon.write_if_size_not_power_of_thirteen(
        "/daemon/nonpow13.txt",
        b"fuse-daemon-nonpow13-v89",
    )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_thirteen committed, tx_id={}",
        write_if_size_not_power_of_thirteen_tx
    );
    let pow14_seed = vec![b'a'; 196];
    let pow14_seed_tx = daemon.write_if_missing("/daemon/pow14.txt", &pow14_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow14.txt) committed, tx_id={}",
        pow14_seed_tx
    );
    let write_if_size_power_of_fourteen_tx =
        daemon.write_if_size_power_of_fourteen("/daemon/pow14.txt", b"fuse-daemon-pow14-v90")?;
    println!(
        "FUSE daemon write_if_size_power_of_fourteen committed, tx_id={}",
        write_if_size_power_of_fourteen_tx
    );
    let nonpow14_seed = vec![b'a'; 197];
    let nonpow14_seed_tx = daemon.write_if_missing("/daemon/nonpow14.txt", &nonpow14_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow14.txt) committed, tx_id={}",
        nonpow14_seed_tx
    );
    let write_if_size_not_power_of_fourteen_tx = daemon.write_if_size_not_power_of_fourteen(
        "/daemon/nonpow14.txt",
        b"fuse-daemon-nonpow14-v91",
    )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_fourteen committed, tx_id={}",
        write_if_size_not_power_of_fourteen_tx
    );
    let pow15_seed = vec![b'a'; 225];
    let pow15_seed_tx = daemon.write_if_missing("/daemon/pow15.txt", &pow15_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow15.txt) committed, tx_id={}",
        pow15_seed_tx
    );
    let write_if_size_power_of_fifteen_tx =
        daemon.write_if_size_power_of_fifteen("/daemon/pow15.txt", b"fuse-daemon-pow15-v92")?;
    println!(
        "FUSE daemon write_if_size_power_of_fifteen committed, tx_id={}",
        write_if_size_power_of_fifteen_tx
    );
    let nonpow15_seed = vec![b'a'; 226];
    let nonpow15_seed_tx = daemon.write_if_missing("/daemon/nonpow15.txt", &nonpow15_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow15.txt) committed, tx_id={}",
        nonpow15_seed_tx
    );
    let write_if_size_not_power_of_fifteen_tx = daemon.write_if_size_not_power_of_fifteen(
        "/daemon/nonpow15.txt",
        b"fuse-daemon-nonpow15-v93",
    )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_fifteen committed, tx_id={}",
        write_if_size_not_power_of_fifteen_tx
    );
    let pow16_seed = vec![b'a'; 256];
    let pow16_seed_tx = daemon.write_if_missing("/daemon/pow16.txt", &pow16_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow16.txt) committed, tx_id={}",
        pow16_seed_tx
    );
    let write_if_size_power_of_sixteen_tx =
        daemon.write_if_size_power_of_sixteen("/daemon/pow16.txt", b"fuse-daemon-pow16-v94")?;
    println!(
        "FUSE daemon write_if_size_power_of_sixteen committed, tx_id={}",
        write_if_size_power_of_sixteen_tx
    );
    let nonpow16_seed = vec![b'a'; 257];
    let nonpow16_seed_tx = daemon.write_if_missing("/daemon/nonpow16.txt", &nonpow16_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow16.txt) committed, tx_id={}",
        nonpow16_seed_tx
    );
    let write_if_size_not_power_of_sixteen_tx = daemon.write_if_size_not_power_of_sixteen(
        "/daemon/nonpow16.txt",
        b"fuse-daemon-nonpow16-v95",
    )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_sixteen committed, tx_id={}",
        write_if_size_not_power_of_sixteen_tx
    );
    let pow17_seed = vec![b'a'; 289];
    let pow17_seed_tx = daemon.write_if_missing("/daemon/pow17.txt", &pow17_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow17.txt) committed, tx_id={}",
        pow17_seed_tx
    );
    let write_if_size_power_of_seventeen_tx = daemon
        .write_if_size_power_of_seventeen("/daemon/pow17.txt", b"fuse-daemon-pow17-v96")?;
    println!(
        "FUSE daemon write_if_size_power_of_seventeen committed, tx_id={}",
        write_if_size_power_of_seventeen_tx
    );
    let nonpow17_seed = vec![b'a'; 290];
    let nonpow17_seed_tx = daemon.write_if_missing("/daemon/nonpow17.txt", &nonpow17_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow17.txt) committed, tx_id={}",
        nonpow17_seed_tx
    );
    let write_if_size_not_power_of_seventeen_tx = daemon.write_if_size_not_power_of_seventeen(
        "/daemon/nonpow17.txt",
        b"fuse-daemon-nonpow17-v97",
    )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_seventeen committed, tx_id={}",
        write_if_size_not_power_of_seventeen_tx
    );
    let pow18_seed = vec![b'a'; 324];
    let pow18_seed_tx = daemon.write_if_missing("/daemon/pow18.txt", &pow18_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow18.txt) committed, tx_id={}",
        pow18_seed_tx
    );
    let write_if_size_power_of_eighteen_tx =
        daemon.write_if_size_power_of_eighteen("/daemon/pow18.txt", b"fuse-daemon-pow18-v98")?;
    println!(
        "FUSE daemon write_if_size_power_of_eighteen committed, tx_id={}",
        write_if_size_power_of_eighteen_tx
    );
    let nonpow18_seed = vec![b'a'; 325];
    let nonpow18_seed_tx = daemon.write_if_missing("/daemon/nonpow18.txt", &nonpow18_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow18.txt) committed, tx_id={}",
        nonpow18_seed_tx
    );
    let write_if_size_not_power_of_eighteen_tx = daemon.write_if_size_not_power_of_eighteen(
        "/daemon/nonpow18.txt",
        b"fuse-daemon-nonpow18-v99",
    )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_eighteen committed, tx_id={}",
        write_if_size_not_power_of_eighteen_tx
    );
    let pow19_seed = vec![b'a'; 361];
    let pow19_seed_tx = daemon.write_if_missing("/daemon/pow19.txt", &pow19_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow19.txt) committed, tx_id={}",
        pow19_seed_tx
    );
    let write_if_size_power_of_nineteen_tx = daemon
        .write_if_size_power_of_nineteen("/daemon/pow19.txt", b"fuse-daemon-pow19-v100")?;
    println!(
        "FUSE daemon write_if_size_power_of_nineteen committed, tx_id={}",
        write_if_size_power_of_nineteen_tx
    );
    let nonpow19_seed = vec![b'a'; 362];
    let nonpow19_seed_tx = daemon.write_if_missing("/daemon/nonpow19.txt", &nonpow19_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow19.txt) committed, tx_id={}",
        nonpow19_seed_tx
    );
    let write_if_size_not_power_of_nineteen_tx = daemon.write_if_size_not_power_of_nineteen(
        "/daemon/nonpow19.txt",
        b"fuse-daemon-nonpow19-v101",
    )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_nineteen committed, tx_id={}",
        write_if_size_not_power_of_nineteen_tx
    );
    let pow20_seed = vec![b'a'; 400];
    let pow20_seed_tx = daemon.write_if_missing("/daemon/pow20.txt", &pow20_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow20.txt) committed, tx_id={}",
        pow20_seed_tx
    );
    let write_if_size_power_of_twenty_tx =
        daemon.write_if_size_power_of_twenty("/daemon/pow20.txt", b"fuse-daemon-pow20-v102")?;
    println!(
        "FUSE daemon write_if_size_power_of_twenty committed, tx_id={}",
        write_if_size_power_of_twenty_tx
    );
    let nonpow20_seed = vec![b'a'; 401];
    let nonpow20_seed_tx = daemon.write_if_missing("/daemon/nonpow20.txt", &nonpow20_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow20.txt) committed, tx_id={}",
        nonpow20_seed_tx
    );
    let write_if_size_not_power_of_twenty_tx = daemon.write_if_size_not_power_of_twenty(
        "/daemon/nonpow20.txt",
        b"fuse-daemon-nonpow20-v103",
    )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_twenty committed, tx_id={}",
        write_if_size_not_power_of_twenty_tx
    );
    let pow21_seed = vec![b'a'; 441];
    let pow21_seed_tx = daemon.write_if_missing("/daemon/pow21.txt", &pow21_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow21.txt) committed, tx_id={}",
        pow21_seed_tx
    );
    let write_if_size_power_of_twenty_one_tx =
        daemon.write_if_size_power_of_twenty_one("/daemon/pow21.txt", b"fuse-daemon-pow21-v104")?;
    println!(
        "FUSE daemon write_if_size_power_of_twenty_one committed, tx_id={}",
        write_if_size_power_of_twenty_one_tx
    );
    let nonpow21_seed = vec![b'a'; 442];
    let nonpow21_seed_tx = daemon.write_if_missing("/daemon/nonpow21.txt", &nonpow21_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow21.txt) committed, tx_id={}",
        nonpow21_seed_tx
    );
    let write_if_size_not_power_of_twenty_one_tx = daemon.write_if_size_not_power_of_twenty_one(
        "/daemon/nonpow21.txt",
        b"fuse-daemon-nonpow21-v105",
    )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_twenty_one committed, tx_id={}",
        write_if_size_not_power_of_twenty_one_tx
    );
    let pow22_seed = vec![b'a'; 484];
    let pow22_seed_tx = daemon.write_if_missing("/daemon/pow22.txt", &pow22_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow22.txt) committed, tx_id={}",
        pow22_seed_tx
    );
    let write_if_size_power_of_twenty_two_tx =
        daemon.write_if_size_power_of_twenty_two("/daemon/pow22.txt", b"fuse-daemon-pow22-v106")?;
    println!(
        "FUSE daemon write_if_size_power_of_twenty_two committed, tx_id={}",
        write_if_size_power_of_twenty_two_tx
    );
    let nonpow22_seed = vec![b'a'; 485];
    let nonpow22_seed_tx = daemon.write_if_missing("/daemon/nonpow22.txt", &nonpow22_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow22.txt) committed, tx_id={}",
        nonpow22_seed_tx
    );
    let write_if_size_not_power_of_twenty_two_tx = daemon.write_if_size_not_power_of_twenty_two(
        "/daemon/nonpow22.txt",
        b"fuse-daemon-nonpow22-v107",
    )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_twenty_two committed, tx_id={}",
        write_if_size_not_power_of_twenty_two_tx
    );
    let pow23_seed = vec![b'a'; 529];
    let pow23_seed_tx = daemon.write_if_missing("/daemon/pow23.txt", &pow23_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow23.txt) committed, tx_id={}",
        pow23_seed_tx
    );
    let write_if_size_power_of_twenty_three_tx = daemon
        .write_if_size_power_of_twenty_three("/daemon/pow23.txt", b"fuse-daemon-pow23-v108")?;
    println!(
        "FUSE daemon write_if_size_power_of_twenty_three committed, tx_id={}",
        write_if_size_power_of_twenty_three_tx
    );
    let nonpow23_seed = vec![b'a'; 530];
    let nonpow23_seed_tx = daemon.write_if_missing("/daemon/nonpow23.txt", &nonpow23_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow23.txt) committed, tx_id={}",
        nonpow23_seed_tx
    );
    let write_if_size_not_power_of_twenty_three_tx =
        daemon.write_if_size_not_power_of_twenty_three(
            "/daemon/nonpow23.txt",
            b"fuse-daemon-nonpow23-v109",
        )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_twenty_three committed, tx_id={}",
        write_if_size_not_power_of_twenty_three_tx
    );
    let pow24_seed = vec![b'a'; 576];
    let pow24_seed_tx = daemon.write_if_missing("/daemon/pow24.txt", &pow24_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow24.txt) committed, tx_id={}",
        pow24_seed_tx
    );
    let write_if_size_power_of_twenty_four_tx = daemon
        .write_if_size_power_of_twenty_four("/daemon/pow24.txt", b"fuse-daemon-pow24-v110")?;
    println!(
        "FUSE daemon write_if_size_power_of_twenty_four committed, tx_id={}",
        write_if_size_power_of_twenty_four_tx
    );
    let nonpow24_seed = vec![b'a'; 577];
    let nonpow24_seed_tx = daemon.write_if_missing("/daemon/nonpow24.txt", &nonpow24_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow24.txt) committed, tx_id={}",
        nonpow24_seed_tx
    );
    let write_if_size_not_power_of_twenty_four_tx = daemon.write_if_size_not_power_of_twenty_four(
        "/daemon/nonpow24.txt",
        b"fuse-daemon-nonpow24-v111",
    )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_twenty_four committed, tx_id={}",
        write_if_size_not_power_of_twenty_four_tx
    );
    let pow25_seed = vec![b'a'; 625];
    let pow25_seed_tx = daemon.write_if_missing("/daemon/pow25.txt", &pow25_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow25.txt) committed, tx_id={}",
        pow25_seed_tx
    );
    let write_if_size_power_of_twenty_five_tx = daemon
        .write_if_size_power_of_twenty_five("/daemon/pow25.txt", b"fuse-daemon-pow25-v112")?;
    println!(
        "FUSE daemon write_if_size_power_of_twenty_five committed, tx_id={}",
        write_if_size_power_of_twenty_five_tx
    );
    let nonpow25_seed = vec![b'a'; 626];
    let nonpow25_seed_tx = daemon.write_if_missing("/daemon/nonpow25.txt", &nonpow25_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow25.txt) committed, tx_id={}",
        nonpow25_seed_tx
    );
    let write_if_size_not_power_of_twenty_five_tx = daemon.write_if_size_not_power_of_twenty_five(
        "/daemon/nonpow25.txt",
        b"fuse-daemon-nonpow25-v113",
    )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_twenty_five committed, tx_id={}",
        write_if_size_not_power_of_twenty_five_tx
    );
    let pow26_seed = vec![b'a'; 676];
    let pow26_seed_tx = daemon.write_if_missing("/daemon/pow26.txt", &pow26_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow26.txt) committed, tx_id={}",
        pow26_seed_tx
    );
    let write_if_size_power_of_twenty_six_tx = daemon
        .write_if_size_power_of_twenty_six("/daemon/pow26.txt", b"fuse-daemon-pow26-v114")?;
    println!(
        "FUSE daemon write_if_size_power_of_twenty_six committed, tx_id={}",
        write_if_size_power_of_twenty_six_tx
    );
    let nonpow26_seed = vec![b'a'; 677];
    let nonpow26_seed_tx = daemon.write_if_missing("/daemon/nonpow26.txt", &nonpow26_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow26.txt) committed, tx_id={}",
        nonpow26_seed_tx
    );
    let write_if_size_not_power_of_twenty_six_tx = daemon.write_if_size_not_power_of_twenty_six(
        "/daemon/nonpow26.txt",
        b"fuse-daemon-nonpow26-v115",
    )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_twenty_six committed, tx_id={}",
        write_if_size_not_power_of_twenty_six_tx
    );
    let pow27_seed = vec![b'a'; 729];
    let pow27_seed_tx = daemon.write_if_missing("/daemon/pow27.txt", &pow27_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow27.txt) committed, tx_id={}",
        pow27_seed_tx
    );
    let write_if_size_power_of_twenty_seven_tx = daemon
        .write_if_size_power_of_twenty_seven("/daemon/pow27.txt", b"fuse-daemon-pow27-v116")?;
    println!(
        "FUSE daemon write_if_size_power_of_twenty_seven committed, tx_id={}",
        write_if_size_power_of_twenty_seven_tx
    );
    let nonpow27_seed = vec![b'a'; 730];
    let nonpow27_seed_tx = daemon.write_if_missing("/daemon/nonpow27.txt", &nonpow27_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow27.txt) committed, tx_id={}",
        nonpow27_seed_tx
    );
    let write_if_size_not_power_of_twenty_seven_tx =
        daemon.write_if_size_not_power_of_twenty_seven(
            "/daemon/nonpow27.txt",
            b"fuse-daemon-nonpow27-v117",
        )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_twenty_seven committed, tx_id={}",
        write_if_size_not_power_of_twenty_seven_tx
    );
    let pow28_seed = vec![b'a'; 784];
    let pow28_seed_tx = daemon.write_if_missing("/daemon/pow28.txt", &pow28_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow28.txt) committed, tx_id={}",
        pow28_seed_tx
    );
    let write_if_size_power_of_twenty_eight_tx = daemon
        .write_if_size_power_of_twenty_eight("/daemon/pow28.txt", b"fuse-daemon-pow28-v118")?;
    println!(
        "FUSE daemon write_if_size_power_of_twenty_eight committed, tx_id={}",
        write_if_size_power_of_twenty_eight_tx
    );
    let nonpow28_seed = vec![b'a'; 785];
    let nonpow28_seed_tx = daemon.write_if_missing("/daemon/nonpow28.txt", &nonpow28_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow28.txt) committed, tx_id={}",
        nonpow28_seed_tx
    );
    let write_if_size_not_power_of_twenty_eight_tx =
        daemon.write_if_size_not_power_of_twenty_eight(
            "/daemon/nonpow28.txt",
            b"fuse-daemon-nonpow28-v119",
        )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_twenty_eight committed, tx_id={}",
        write_if_size_not_power_of_twenty_eight_tx
    );
    let pow29_seed = vec![b'a'; 841];
    let pow29_seed_tx = daemon.write_if_missing("/daemon/pow29.txt", &pow29_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow29.txt) committed, tx_id={}",
        pow29_seed_tx
    );
    let write_if_size_power_of_twenty_nine_tx = daemon
        .write_if_size_power_of_twenty_nine("/daemon/pow29.txt", b"fuse-daemon-pow29-v120")?;
    println!(
        "FUSE daemon write_if_size_power_of_twenty_nine committed, tx_id={}",
        write_if_size_power_of_twenty_nine_tx
    );
    let nonpow29_seed = vec![b'a'; 842];
    let nonpow29_seed_tx = daemon.write_if_missing("/daemon/nonpow29.txt", &nonpow29_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow29.txt) committed, tx_id={}",
        nonpow29_seed_tx
    );
    let write_if_size_not_power_of_twenty_nine_tx =
        daemon.write_if_size_not_power_of_twenty_nine(
            "/daemon/nonpow29.txt",
            b"fuse-daemon-nonpow29-v121",
        )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_twenty_nine committed, tx_id={}",
        write_if_size_not_power_of_twenty_nine_tx
    );
    let pow30_seed = vec![b'a'; 900];
    let pow30_seed_tx = daemon.write_if_missing("/daemon/pow30.txt", &pow30_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow30.txt) committed, tx_id={}",
        pow30_seed_tx
    );
    let write_if_size_power_of_thirty_tx = daemon
        .write_if_size_power_of_thirty("/daemon/pow30.txt", b"fuse-daemon-pow30-v122")?;
    println!(
        "FUSE daemon write_if_size_power_of_thirty committed, tx_id={}",
        write_if_size_power_of_thirty_tx
    );
    let nonpow30_seed = vec![b'a'; 901];
    let nonpow30_seed_tx = daemon.write_if_missing("/daemon/nonpow30.txt", &nonpow30_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow30.txt) committed, tx_id={}",
        nonpow30_seed_tx
    );
    let write_if_size_not_power_of_thirty_tx =
        daemon.write_if_size_not_power_of_thirty(
            "/daemon/nonpow30.txt",
            b"fuse-daemon-nonpow30-v123",
        )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_thirty committed, tx_id={}",
        write_if_size_not_power_of_thirty_tx
    );
    let pow31_seed = vec![b'a'; 961];
    let pow31_seed_tx = daemon.write_if_missing("/daemon/pow31.txt", &pow31_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow31.txt) committed, tx_id={}",
        pow31_seed_tx
    );
    let write_if_size_power_of_thirty_one_tx = daemon
        .write_if_size_power_of_thirty_one("/daemon/pow31.txt", b"fuse-daemon-pow31-v124")?;
    println!(
        "FUSE daemon write_if_size_power_of_thirty_one committed, tx_id={}",
        write_if_size_power_of_thirty_one_tx
    );
    let nonpow31_seed = vec![b'a'; 962];
    let nonpow31_seed_tx = daemon.write_if_missing("/daemon/nonpow31.txt", &nonpow31_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow31.txt) committed, tx_id={}",
        nonpow31_seed_tx
    );
    let write_if_size_not_power_of_thirty_one_tx =
        daemon.write_if_size_not_power_of_thirty_one(
            "/daemon/nonpow31.txt",
            b"fuse-daemon-nonpow31-v125",
        )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_thirty_one committed, tx_id={}",
        write_if_size_not_power_of_thirty_one_tx
    );
    let pow32_seed = vec![b'a'; 1024];
    let pow32_seed_tx = daemon.write_if_missing("/daemon/pow32.txt", &pow32_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow32.txt) committed, tx_id={}",
        pow32_seed_tx
    );
    let write_if_size_power_of_thirty_two_tx = daemon
        .write_if_size_power_of_thirty_two("/daemon/pow32.txt", b"fuse-daemon-pow32-v126")?;
    println!(
        "FUSE daemon write_if_size_power_of_thirty_two committed, tx_id={}",
        write_if_size_power_of_thirty_two_tx
    );
    let nonpow32_seed = vec![b'a'; 1025];
    let nonpow32_seed_tx = daemon.write_if_missing("/daemon/nonpow32.txt", &nonpow32_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow32.txt) committed, tx_id={}",
        nonpow32_seed_tx
    );
    let write_if_size_not_power_of_thirty_two_tx =
        daemon.write_if_size_not_power_of_thirty_two(
            "/daemon/nonpow32.txt",
            b"fuse-daemon-nonpow32-v127",
        )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_thirty_two committed, tx_id={}",
        write_if_size_not_power_of_thirty_two_tx
    );
    let pow33_seed = vec![b'a'; 1089];
    let pow33_seed_tx = daemon.write_if_missing("/daemon/pow33.txt", &pow33_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow33.txt) committed, tx_id={}",
        pow33_seed_tx
    );
    let write_if_size_power_of_thirty_three_tx = daemon
        .write_if_size_power_of_thirty_three("/daemon/pow33.txt", b"fuse-daemon-pow33-v128")?;
    println!(
        "FUSE daemon write_if_size_power_of_thirty_three committed, tx_id={}",
        write_if_size_power_of_thirty_three_tx
    );
    let nonpow33_seed = vec![b'a'; 1090];
    let nonpow33_seed_tx = daemon.write_if_missing("/daemon/nonpow33.txt", &nonpow33_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow33.txt) committed, tx_id={}",
        nonpow33_seed_tx
    );
    let write_if_size_not_power_of_thirty_three_tx =
        daemon.write_if_size_not_power_of_thirty_three(
            "/daemon/nonpow33.txt",
            b"fuse-daemon-nonpow33-v129",
        )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_thirty_three committed, tx_id={}",
        write_if_size_not_power_of_thirty_three_tx
    );
    let pow34_seed = vec![b'a'; 1156];
    let pow34_seed_tx = daemon.write_if_missing("/daemon/pow34.txt", &pow34_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow34.txt) committed, tx_id={}",
        pow34_seed_tx
    );
    let write_if_size_power_of_thirty_four_tx = daemon
        .write_if_size_power_of_thirty_four("/daemon/pow34.txt", b"fuse-daemon-pow34-v130")?;
    println!(
        "FUSE daemon write_if_size_power_of_thirty_four committed, tx_id={}",
        write_if_size_power_of_thirty_four_tx
    );
    let nonpow34_seed = vec![b'a'; 1157];
    let nonpow34_seed_tx = daemon.write_if_missing("/daemon/nonpow34.txt", &nonpow34_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow34.txt) committed, tx_id={}",
        nonpow34_seed_tx
    );
    let write_if_size_not_power_of_thirty_four_tx =
        daemon.write_if_size_not_power_of_thirty_four(
            "/daemon/nonpow34.txt",
            b"fuse-daemon-nonpow34-v131",
        )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_thirty_four committed, tx_id={}",
        write_if_size_not_power_of_thirty_four_tx
    );
    let pow35_seed = vec![b'a'; 1225];
    let pow35_seed_tx = daemon.write_if_missing("/daemon/pow35.txt", &pow35_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow35.txt) committed, tx_id={}",
        pow35_seed_tx
    );
    let write_if_size_power_of_thirty_five_tx = daemon
        .write_if_size_power_of_thirty_five("/daemon/pow35.txt", b"fuse-daemon-pow35-v132")?;
    println!(
        "FUSE daemon write_if_size_power_of_thirty_five committed, tx_id={}",
        write_if_size_power_of_thirty_five_tx
    );
    let nonpow35_seed = vec![b'a'; 1226];
    let nonpow35_seed_tx = daemon.write_if_missing("/daemon/nonpow35.txt", &nonpow35_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow35.txt) committed, tx_id={}",
        nonpow35_seed_tx
    );
    let write_if_size_not_power_of_thirty_five_tx =
        daemon.write_if_size_not_power_of_thirty_five(
            "/daemon/nonpow35.txt",
            b"fuse-daemon-nonpow35-v133",
        )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_thirty_five committed, tx_id={}",
        write_if_size_not_power_of_thirty_five_tx
    );
    let pow36_seed = vec![b'a'; 1296];
    let pow36_seed_tx = daemon.write_if_missing("/daemon/pow36.txt", &pow36_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow36.txt) committed, tx_id={}",
        pow36_seed_tx
    );
    let write_if_size_power_of_thirty_six_tx = daemon
        .write_if_size_power_of_thirty_six("/daemon/pow36.txt", b"fuse-daemon-pow36-v134")?;
    println!(
        "FUSE daemon write_if_size_power_of_thirty_six committed, tx_id={}",
        write_if_size_power_of_thirty_six_tx
    );
    let nonpow36_seed = vec![b'a'; 1297];
    let nonpow36_seed_tx = daemon.write_if_missing("/daemon/nonpow36.txt", &nonpow36_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow36.txt) committed, tx_id={}",
        nonpow36_seed_tx
    );
    let write_if_size_not_power_of_thirty_six_tx =
        daemon.write_if_size_not_power_of_thirty_six(
            "/daemon/nonpow36.txt",
            b"fuse-daemon-nonpow36-v135",
        )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_thirty_six committed, tx_id={}",
        write_if_size_not_power_of_thirty_six_tx
    );
    let pow37_seed = vec![b'a'; 1369];
    let pow37_seed_tx = daemon.write_if_missing("/daemon/pow37.txt", &pow37_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow37.txt) committed, tx_id={}",
        pow37_seed_tx
    );
    let write_if_size_power_of_thirty_seven_tx = daemon
        .write_if_size_power_of_thirty_seven("/daemon/pow37.txt", b"fuse-daemon-pow37-v136")?;
    println!(
        "FUSE daemon write_if_size_power_of_thirty_seven committed, tx_id={}",
        write_if_size_power_of_thirty_seven_tx
    );
    let nonpow37_seed = vec![b'a'; 1370];
    let nonpow37_seed_tx = daemon.write_if_missing("/daemon/nonpow37.txt", &nonpow37_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow37.txt) committed, tx_id={}",
        nonpow37_seed_tx
    );
    let write_if_size_not_power_of_thirty_seven_tx =
        daemon.write_if_size_not_power_of_thirty_seven(
            "/daemon/nonpow37.txt",
            b"fuse-daemon-nonpow37-v137",
        )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_thirty_seven committed, tx_id={}",
        write_if_size_not_power_of_thirty_seven_tx
    );
    let pow38_seed = vec![b'a'; 1444];
    let pow38_seed_tx = daemon.write_if_missing("/daemon/pow38.txt", &pow38_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow38.txt) committed, tx_id={}",
        pow38_seed_tx
    );
    let write_if_size_power_of_thirty_eight_tx = daemon
        .write_if_size_power_of_thirty_eight("/daemon/pow38.txt", b"fuse-daemon-pow38-v138")?;
    println!(
        "FUSE daemon write_if_size_power_of_thirty_eight committed, tx_id={}",
        write_if_size_power_of_thirty_eight_tx
    );
    let nonpow38_seed = vec![b'a'; 1445];
    let nonpow38_seed_tx = daemon.write_if_missing("/daemon/nonpow38.txt", &nonpow38_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow38.txt) committed, tx_id={}",
        nonpow38_seed_tx
    );
    let write_if_size_not_power_of_thirty_eight_tx =
        daemon.write_if_size_not_power_of_thirty_eight(
            "/daemon/nonpow38.txt",
            b"fuse-daemon-nonpow38-v139",
        )?;
    println!(
        "FUSE daemon write_if_size_not_power_of_thirty_eight committed, tx_id={}",
        write_if_size_not_power_of_thirty_eight_tx
    );
    let pow39_seed = vec![b'a'; 1521];
    let pow39_seed_tx = daemon.write_if_missing("/daemon/pow39.txt", &pow39_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/pow39.txt) committed, tx_id={}",
        pow39_seed_tx
    );
    let write_if_size_power_of_thirty_nine_tx = daemon
        .write_if_size_power_of_thirty_nine("/daemon/pow39.txt", b"fuse-daemon-pow39-v140")?;
    println!(
        "FUSE daemon write_if_size_power_of_thirty_nine committed, tx_id={}",
        write_if_size_power_of_thirty_nine_tx
    );
    let nonpow39_seed = vec![b'a'; 1522];
    let nonpow39_seed_tx = daemon.write_if_missing("/daemon/nonpow39.txt", &nonpow39_seed[..])?;
    println!(
        "FUSE daemon write_if_missing(/daemon/nonpow39.txt) committed, tx_id={}",
        nonpow39_seed_tx
    );
    let write_if_size_not_power_of_thirty_nine_tx = daemon
        .write_if_size_not_power_of_thirty_nine("/daemon/nonpow39.txt", b"fuse-daemon-nonpow39-v141")?;
    println!(
        "FUSE daemon write_if_size_not_power_of_thirty_nine committed, tx_id={}",
        write_if_size_not_power_of_thirty_nine_tx
    );

    let names = daemon.readdir("/daemon")?;
    println!("FUSE daemon readdir /daemon -> {:?}", names);

    daemon.rename("/daemon/demo.txt", "/daemon/renamed.txt")?;
    println!("FUSE daemon rename committed.");
    let trunc_tx = daemon.truncate_file("/daemon/renamed.txt", 10, 21)?;
    println!("FUSE daemon truncate_file committed, tx_id={}", trunc_tx);
    let append_tx = daemon.append_file("/daemon/renamed.txt", 22, b"-v2")?;
    println!("FUSE daemon append_file committed, tx_id={}", append_tx);
    let overwrite_tx = daemon.overwrite_range("/daemon/renamed.txt", 3, 23, b"PATCH")?;
    println!("FUSE daemon overwrite_range committed, tx_id={}", overwrite_tx);
    let insert_tx = daemon.insert_range("/daemon/renamed.txt", 3, 24, b"++")?;
    println!("FUSE daemon insert_range committed, tx_id={}", insert_tx);
    let delete_tx = daemon.delete_range("/daemon/renamed.txt", 3, 2, 25)?;
    println!("FUSE daemon delete_range committed, tx_id={}", delete_tx);
    let replace_tx = daemon.replace_range("/daemon/renamed.txt", 3, 5, 26, b"REPL")?;
    println!("FUSE daemon replace_range committed, tx_id={}", replace_tx);

    let part = daemon.read("/daemon/renamed.txt", 5, 6)?;
    println!(
        "FUSE daemon read slice [5..11]: {}",
        String::from_utf8_lossy(&part)
    );
    let empty = daemon.read_all("/daemon/empty.txt")?;
    println!("FUSE daemon touched file bytes -> {}", empty.len());
    let create_only = daemon.read_all("/daemon/create-only.txt")?;
    println!(
        "FUSE daemon create-only file bytes: {}",
        String::from_utf8_lossy(&create_only)
    );
    let empty_gated = daemon.read_all("/daemon/empty-gated.txt")?;
    println!(
        "FUSE daemon empty-gated file bytes: {}",
        String::from_utf8_lossy(&empty_gated)
    );

    daemon.open("/daemon/renamed.txt")?;
    println!("FUSE daemon open validated metadata path.");
    let kind = daemon.stat("/daemon/renamed.txt")?;
    if kind != FuseNodeKind::File {
        return Err(format!("expected file kind from stat, got {kind:?}").into());
    }
    println!("FUSE daemon stat /daemon/renamed.txt -> {:?}", kind);

    let removed = daemon.remove_path("/daemon")?;
    println!("FUSE daemon remove_path /daemon committed, removed={}", removed);

    match daemon.read("/daemon/renamed.txt", 0, 1) {
        Err(fuse::FuseError::NotFound) => {
            println!("FUSE daemon post-delete read: not found (expected)")
        }
        other => return Err(format!("expected NotFound after unlink, got {other:?}").into()),
    }

    daemon.shutdown()?;
    println!("FUSE daemon demo finished successfully.");
    Ok(())
}

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
    let temp = tempfile::tempdir()?;
    let wal = WalLog::open_with_sync(temp.path().join("wal.log"), config.wal_sync_writes)?;
    let chunks = Arc::new(FsChunkStore::new(temp.path().join("objects"))?);
    let metadata = Arc::new(InMemoryMetadataHook::new());
    let cache = TwoTierChunkCache::new(16, 64);
    let pipeline = WritePipeline::new(wal, chunks, metadata, config.chunk_size_bytes)?;
    let core = FileSystemCore::new(pipeline, cache);
    let api = FuseApi::new(core);
    println!("{}", config.summary("fuse_api_demo"));
    println!("FUSE API mount gate open before recovery -> {}", api.is_mount_open());

    api.health()?;
    println!("FUSE API health probe completed.");

    api.startup_recover()?;
    println!("FUSE API startup recovery completed.");
    println!("FUSE API mount gate open after recovery -> {}", api.is_mount_open());
    let gc_enqueued = api.enqueue_gc_scan()?;
    println!(
        "FUSE API enqueue_gc_scan completed, enqueued={}",
        gc_enqueued
    );
    let gc_reports = api.run_background_once()?;
    println!(
        "FUSE API run_background_once completed, reports={}",
        gc_reports.len()
    );
    let gc_scan_reports = api.gc_scan_once()?;
    println!(
        "FUSE API gc_scan_once completed, reports={}",
        gc_scan_reports.len()
    );

    if config.timeout_smoke {
        api.health_with_delay(Duration::from_millis(config.timeout_smoke_delay_ms))?;
        println!(
            "FUSE API delay smoke completed (delay={}ms)",
            config.timeout_smoke_delay_ms
        );
    }

    let created = api.mkdir_p("/api/sub").expect("mkdir_p should succeed");
    println!("FUSE API mkdir_p committed, created={}", created);
    let create_only_tx = api.write_if_missing("/api/create-only.txt", b"created-once")?;
    println!(
        "FUSE API write_if_missing committed, tx_id={}",
        create_only_tx
    );
    let ensured_created = api.ensure_file("/api/ensured.txt")?;
    let ensured_again = api.ensure_file("/api/ensured.txt")?;
    println!(
        "FUSE API ensure_file /api/ensured.txt -> created_first={}, created_second={}",
        ensured_created, ensured_again
    );
    let touch_tx = api.touch_file("/api/empty.txt", 0)?;
    println!("FUSE API touch_file committed, tx_id={}", touch_tx);
    let empty_gate_tx = api.touch_file("/api/empty-gated.txt", 0)?;
    println!("FUSE API touch_file(empty-gated) committed, tx_id={}", empty_gate_tx);
    let write_if_empty_tx = api.write_if_empty("/api/empty-gated.txt", b"first-fill")?;
    println!(
        "FUSE API write_if_empty committed, tx_id={}",
        write_if_empty_tx
    );
    let tx_id = api.write("/api/demo.txt", 0, b"fuse-api-demo-bytes")?;
    println!("FUSE API write committed, tx_id={}", tx_id);
    let write_if_version_tx = api.write_if_version("/api/demo.txt", 1, b"fuse-api-demo-bytes-v2")?;
    println!(
        "FUSE API write_if_version committed, tx_id={}",
        write_if_version_tx
    );
    let cas_tx = api.compare_and_swap_file(
        "/api/demo.txt",
        2,
        b"fuse-api-demo-bytes-v2",
        b"fuse-api-demo-bytes-v3",
    )?;
    println!("FUSE API compare_and_swap_file committed, tx_id={}", cas_tx);
    let demo_hash = api.file_hash("/api/demo.txt")?;
    let write_if_hash_tx = api.write_if_hash("/api/demo.txt", 3, &demo_hash, b"fuse-api-demo-bytes-v4")?;
    println!(
        "FUSE API write_if_hash committed, tx_id={}",
        write_if_hash_tx
    );
    let demo_size = api.file_size("/api/demo.txt")?;
    let write_if_size_tx = api.write_if_size("/api/demo.txt", 4, demo_size, b"fuse-api-demo-bytes-v5")?;
    println!(
        "FUSE API write_if_size committed, tx_id={}",
        write_if_size_tx
    );
    let write_if_exists_tx = api.write_if_exists("/api/demo.txt", b"fuse-api-demo-bytes-v6")?;
    println!(
        "FUSE API write_if_exists committed, tx_id={}",
        write_if_exists_tx
    );
    let write_if_not_empty_tx = api.write_if_not_empty("/api/demo.txt", b"fuse-api-demo-bytes-v7")?;
    println!(
        "FUSE API write_if_not_empty committed, tx_id={}",
        write_if_not_empty_tx
    );
    let write_if_starts_with_tx = api.write_if_starts_with(
        "/api/demo.txt",
        b"fuse-api",
        b"fuse-api-demo-bytes-v8",
    )?;
    println!(
        "FUSE API write_if_starts_with committed, tx_id={}",
        write_if_starts_with_tx
    );
    let write_if_ends_with_tx = api.write_if_ends_with(
        "/api/demo.txt",
        b"-v8",
        b"fuse-api-demo-bytes-v9",
    )?;
    println!(
        "FUSE API write_if_ends_with committed, tx_id={}",
        write_if_ends_with_tx
    );
    let write_if_contains_tx = api.write_if_contains("/api/demo.txt", b"demo-bytes", b"fuse-api-demo-bytes-v10")?;
    println!(
        "FUSE API write_if_contains committed, tx_id={}",
        write_if_contains_tx
    );
    let write_if_not_contains_tx = api.write_if_not_contains("/api/demo.txt", b"forbidden", b"fuse-api-demo-bytes-v11")?;
    println!(
        "FUSE API write_if_not_contains committed, tx_id={}",
        write_if_not_contains_tx
    );
    let write_if_exact_tx = api.write_if_exact("/api/demo.txt", b"fuse-api-demo-bytes-v11", b"fuse-api-demo-bytes-v12")?;
    println!(
        "FUSE API write_if_exact committed, tx_id={}",
        write_if_exact_tx
    );
    let write_if_not_exact_tx = api.write_if_not_exact("/api/demo.txt", b"forbidden-exact", b"fuse-api-demo-bytes-v13")?;
    println!(
        "FUSE API write_if_not_exact committed, tx_id={}",
        write_if_not_exact_tx
    );
    let write_if_min_size_tx = api.write_if_min_size("/api/demo.txt", 10, b"fuse-api-demo-bytes-v14")?;
    println!(
        "FUSE API write_if_min_size committed, tx_id={}",
        write_if_min_size_tx
    );
    let write_if_max_size_tx = api.write_if_max_size("/api/demo.txt", 30, b"fuse-api-demo-bytes-v15")?;
    println!(
        "FUSE API write_if_max_size committed, tx_id={}",
        write_if_max_size_tx
    );
    let write_if_size_between_tx =
        api.write_if_size_between("/api/demo.txt", 10, 30, b"fuse-api-demo-bytes-v16")?;
    println!(
        "FUSE API write_if_size_between committed, tx_id={}",
        write_if_size_between_tx
    );
    let write_if_size_not_between_tx =
        api.write_if_size_not_between("/api/demo.txt", 100, 200, b"fuse-api-demo-bytes-v17")?;
    println!(
        "FUSE API write_if_size_not_between committed, tx_id={}",
        write_if_size_not_between_tx
    );
    let write_if_size_multiple_of_tx =
        api.write_if_size_multiple_of("/api/demo.txt", 23, b"fuse-api-demo-bytes-v18")?;
    println!(
        "FUSE API write_if_size_multiple_of committed, tx_id={}",
        write_if_size_multiple_of_tx
    );
    let write_if_size_not_multiple_of_tx =
        api.write_if_size_not_multiple_of("/api/demo.txt", 7, b"fuse-api-demo-bytes-v19x")?;
    println!(
        "FUSE API write_if_size_not_multiple_of committed, tx_id={}",
        write_if_size_not_multiple_of_tx
    );
    let write_if_size_even_tx =
        api.write_if_size_even("/api/demo.txt", b"fuse-api-demo-bytes-v20")?;
    println!(
        "FUSE API write_if_size_even committed, tx_id={}",
        write_if_size_even_tx
    );
    let write_if_size_odd_tx = api.write_if_size_odd("/api/demo.txt", b"fuse-api-demo-bytes-v21")?;
    println!(
        "FUSE API write_if_size_odd committed, tx_id={}",
        write_if_size_odd_tx
    );
    let pow2_seed_tx = api.write_if_missing("/api/pow2.txt", b"12345678")?;
    println!(
        "FUSE API write_if_missing(/api/pow2.txt) committed, tx_id={}",
        pow2_seed_tx
    );
    let write_if_size_power_of_two_tx =
        api.write_if_size_power_of_two("/api/pow2.txt", b"fuse-api-pow2-v22")?;
    println!(
        "FUSE API write_if_size_power_of_two committed, tx_id={}",
        write_if_size_power_of_two_tx
    );
    let nonpower_seed_tx = api.write_if_missing("/api/nonpower.txt", b"1234567")?;
    println!(
        "FUSE API write_if_missing(/api/nonpower.txt) committed, tx_id={}",
        nonpower_seed_tx
    );
    let write_if_size_not_power_of_two_tx =
        api.write_if_size_not_power_of_two("/api/nonpower.txt", b"fuse-api-nonpower-v23")?;
    println!(
        "FUSE API write_if_size_not_power_of_two committed, tx_id={}",
        write_if_size_not_power_of_two_tx
    );
    let prime_seed_tx = api.write_if_missing("/api/prime.txt", b"1234567")?;
    println!(
        "FUSE API write_if_missing(/api/prime.txt) committed, tx_id={}",
        prime_seed_tx
    );
    let write_if_size_prime_tx = api.write_if_size_prime("/api/prime.txt", b"fuse-api-prime-v24")?;
    println!(
        "FUSE API write_if_size_prime committed, tx_id={}",
        write_if_size_prime_tx
    );
    let nonprime_seed_tx = api.write_if_missing("/api/nonprime2.txt", b"12345678")?;
    println!(
        "FUSE API write_if_missing(/api/nonprime2.txt) committed, tx_id={}",
        nonprime_seed_tx
    );
    let write_if_size_not_prime_tx =
        api.write_if_size_not_prime("/api/nonprime2.txt", b"fuse-api-nonprime-v25")?;
    println!(
        "FUSE API write_if_size_not_prime committed, tx_id={}",
        write_if_size_not_prime_tx
    );
    let fib_seed_tx = api.write_if_missing("/api/fib.txt", b"12345678")?;
    println!(
        "FUSE API write_if_missing(/api/fib.txt) committed, tx_id={}",
        fib_seed_tx
    );
    let write_if_size_fibonacci_tx =
        api.write_if_size_fibonacci("/api/fib.txt", b"fuse-api-fib-v26")?;
    println!(
        "FUSE API write_if_size_fibonacci committed, tx_id={}",
        write_if_size_fibonacci_tx
    );
    let nonfib_seed_tx = api.write_if_missing("/api/nonfib.txt", b"1234567")?;
    println!(
        "FUSE API write_if_missing(/api/nonfib.txt) committed, tx_id={}",
        nonfib_seed_tx
    );
    let write_if_size_not_fibonacci_tx =
        api.write_if_size_not_fibonacci("/api/nonfib.txt", b"fuse-api-nonfib-v27")?;
    println!(
        "FUSE API write_if_size_not_fibonacci committed, tx_id={}",
        write_if_size_not_fibonacci_tx
    );
    let square_seed_tx = api.write_if_missing("/api/square.txt", b"123456789")?;
    println!(
        "FUSE API write_if_missing(/api/square.txt) committed, tx_id={}",
        square_seed_tx
    );
    let write_if_size_square_tx =
        api.write_if_size_square("/api/square.txt", b"fuse-api-square-v28")?;
    println!(
        "FUSE API write_if_size_square committed, tx_id={}",
        write_if_size_square_tx
    );
    let nonsquare_seed_tx = api.write_if_missing("/api/nonsquare.txt", b"1234567")?;
    println!(
        "FUSE API write_if_missing(/api/nonsquare.txt) committed, tx_id={}",
        nonsquare_seed_tx
    );
    let write_if_size_not_square_tx =
        api.write_if_size_not_square("/api/nonsquare.txt", b"fuse-api-nonsquare-v29")?;
    println!(
        "FUSE API write_if_size_not_square committed, tx_id={}",
        write_if_size_not_square_tx
    );
    let cube_seed_tx = api.write_if_missing("/api/cube.txt", b"12345678")?;
    println!(
        "FUSE API write_if_missing(/api/cube.txt) committed, tx_id={}",
        cube_seed_tx
    );
    let write_if_size_cube_tx = api.write_if_size_cube("/api/cube.txt", b"fuse-api-cube-v30")?;
    println!(
        "FUSE API write_if_size_cube committed, tx_id={}",
        write_if_size_cube_tx
    );
    let noncube_seed_tx = api.write_if_missing("/api/noncube.txt", b"1234567")?;
    println!(
        "FUSE API write_if_missing(/api/noncube.txt) committed, tx_id={}",
        noncube_seed_tx
    );
    let write_if_size_not_cube_tx =
        api.write_if_size_not_cube("/api/noncube.txt", b"fuse-api-noncube-v31")?;
    println!(
        "FUSE API write_if_size_not_cube committed, tx_id={}",
        write_if_size_not_cube_tx
    );
    let tri_seed_tx = api.write_if_missing("/api/tri.txt", b"123456")?;
    println!(
        "FUSE API write_if_missing(/api/tri.txt) committed, tx_id={}",
        tri_seed_tx
    );
    let write_if_size_triangular_tx =
        api.write_if_size_triangular("/api/tri.txt", b"fuse-api-tri-v32")?;
    println!(
        "FUSE API write_if_size_triangular committed, tx_id={}",
        write_if_size_triangular_tx
    );
    let nontri_seed_tx = api.write_if_missing("/api/nontri.txt", b"12345")?;
    println!(
        "FUSE API write_if_missing(/api/nontri.txt) committed, tx_id={}",
        nontri_seed_tx
    );
    let write_if_size_not_triangular_tx =
        api.write_if_size_not_triangular("/api/nontri.txt", b"fuse-api-nontri-v33")?;
    println!(
        "FUSE API write_if_size_not_triangular committed, tx_id={}",
        write_if_size_not_triangular_tx
    );
    let factorial_seed_tx = api.write_if_missing("/api/factorial.txt", b"123456")?;
    println!(
        "FUSE API write_if_missing(/api/factorial.txt) committed, tx_id={}",
        factorial_seed_tx
    );
    let write_if_size_factorial_tx =
        api.write_if_size_factorial("/api/factorial.txt", b"fuse-api-factorial-v34")?;
    println!(
        "FUSE API write_if_size_factorial committed, tx_id={}",
        write_if_size_factorial_tx
    );
    let nonfactorial_seed_tx = api.write_if_missing("/api/nonfactorial.txt", b"12345")?;
    println!(
        "FUSE API write_if_missing(/api/nonfactorial.txt) committed, tx_id={}",
        nonfactorial_seed_tx
    );
    let write_if_size_not_factorial_tx =
        api.write_if_size_not_factorial("/api/nonfactorial.txt", b"fuse-api-nonfactorial-v35")?;
    println!(
        "FUSE API write_if_size_not_factorial committed, tx_id={}",
        write_if_size_not_factorial_tx
    );
    let composite_seed_tx = api.write_if_missing("/api/composite.txt", b"12345678")?;
    println!(
        "FUSE API write_if_missing(/api/composite.txt) committed, tx_id={}",
        composite_seed_tx
    );
    let write_if_size_composite_tx =
        api.write_if_size_composite("/api/composite.txt", b"fuse-api-composite-v36")?;
    println!(
        "FUSE API write_if_size_composite committed, tx_id={}",
        write_if_size_composite_tx
    );
    let noncomposite_seed_tx = api.write_if_missing("/api/noncomposite.txt", b"1234567")?;
    println!(
        "FUSE API write_if_missing(/api/noncomposite.txt) committed, tx_id={}",
        noncomposite_seed_tx
    );
    let write_if_size_not_composite_tx =
        api.write_if_size_not_composite("/api/noncomposite.txt", b"fuse-api-noncomposite-v37")?;
    println!(
        "FUSE API write_if_size_not_composite committed, tx_id={}",
        write_if_size_not_composite_tx
    );
    let perfect_seed_tx = api.write_if_missing("/api/perfect.txt", b"123456")?;
    println!(
        "FUSE API write_if_missing(/api/perfect.txt) committed, tx_id={}",
        perfect_seed_tx
    );
    let write_if_size_perfect_tx =
        api.write_if_size_perfect("/api/perfect.txt", b"fuse-api-perfect-v38")?;
    println!(
        "FUSE API write_if_size_perfect committed, tx_id={}",
        write_if_size_perfect_tx
    );
    let nonperfect_seed_tx = api.write_if_missing("/api/nonperfect.txt", b"12345")?;
    println!(
        "FUSE API write_if_missing(/api/nonperfect.txt) committed, tx_id={}",
        nonperfect_seed_tx
    );
    let write_if_size_not_perfect_tx =
        api.write_if_size_not_perfect("/api/nonperfect.txt", b"fuse-api-nonperfect-v39")?;
    println!(
        "FUSE API write_if_size_not_perfect committed, tx_id={}",
        write_if_size_not_perfect_tx
    );
    let abundant_seed_tx = api.write_if_missing("/api/abundant.txt", b"123456789012")?;
    println!(
        "FUSE API write_if_missing(/api/abundant.txt) committed, tx_id={}",
        abundant_seed_tx
    );
    let write_if_size_abundant_tx =
        api.write_if_size_abundant("/api/abundant.txt", b"fuse-api-abundant-v40")?;
    println!(
        "FUSE API write_if_size_abundant committed, tx_id={}",
        write_if_size_abundant_tx
    );
    let nonabundant_seed_tx = api.write_if_missing("/api/nonabundant.txt", b"1234567")?;
    println!(
        "FUSE API write_if_missing(/api/nonabundant.txt) committed, tx_id={}",
        nonabundant_seed_tx
    );
    let write_if_size_not_abundant_tx =
        api.write_if_size_not_abundant("/api/nonabundant.txt", b"fuse-api-nonabundant-v41")?;
    println!(
        "FUSE API write_if_size_not_abundant committed, tx_id={}",
        write_if_size_not_abundant_tx
    );
    let deficient_seed_tx = api.write_if_missing("/api/deficient.txt", b"1234567")?;
    println!(
        "FUSE API write_if_missing(/api/deficient.txt) committed, tx_id={}",
        deficient_seed_tx
    );
    let write_if_size_deficient_tx =
        api.write_if_size_deficient("/api/deficient.txt", b"fuse-api-deficient-v42")?;
    println!(
        "FUSE API write_if_size_deficient committed, tx_id={}",
        write_if_size_deficient_tx
    );
    let nondeficient_seed_tx = api.write_if_missing("/api/nondeficient.txt", b"123456789012")?;
    println!(
        "FUSE API write_if_missing(/api/nondeficient.txt) committed, tx_id={}",
        nondeficient_seed_tx
    );
    let write_if_size_not_deficient_tx =
        api.write_if_size_not_deficient("/api/nondeficient.txt", b"fuse-api-nondeficient-v43")?;
    println!(
        "FUSE API write_if_size_not_deficient committed, tx_id={}",
        write_if_size_not_deficient_tx
    );
    let semiprime_seed_tx = api.write_if_missing("/api/semiprime.txt", b"123456")?;
    println!(
        "FUSE API write_if_missing(/api/semiprime.txt) committed, tx_id={}",
        semiprime_seed_tx
    );
    let write_if_size_semiprime_tx =
        api.write_if_size_semiprime("/api/semiprime.txt", b"fuse-api-semiprime-v44")?;
    println!(
        "FUSE API write_if_size_semiprime committed, tx_id={}",
        write_if_size_semiprime_tx
    );
    let nonsemiprime_seed_tx = api.write_if_missing("/api/nonsemiprime.txt", b"1234567")?;
    println!(
        "FUSE API write_if_missing(/api/nonsemiprime.txt) committed, tx_id={}",
        nonsemiprime_seed_tx
    );
    let write_if_size_not_semiprime_tx =
        api.write_if_size_not_semiprime("/api/nonsemiprime.txt", b"fuse-api-nonsemiprime-v45")?;
    println!(
        "FUSE API write_if_size_not_semiprime committed, tx_id={}",
        write_if_size_not_semiprime_tx
    );
    let palindrome_seed_tx = api.write_if_missing("/api/palindrome.txt", b"12345678901")?;
    println!(
        "FUSE API write_if_missing(/api/palindrome.txt) committed, tx_id={}",
        palindrome_seed_tx
    );
    let write_if_size_palindrome_tx =
        api.write_if_size_palindrome("/api/palindrome.txt", b"fuse-api-palindrome-v46")?;
    println!(
        "FUSE API write_if_size_palindrome committed, tx_id={}",
        write_if_size_palindrome_tx
    );
    let nonpalindrome_seed_tx = api.write_if_missing("/api/nonpalindrome.txt", b"1234567890")?;
    println!(
        "FUSE API write_if_missing(/api/nonpalindrome.txt) committed, tx_id={}",
        nonpalindrome_seed_tx
    );
    let write_if_size_not_palindrome_tx = api
        .write_if_size_not_palindrome("/api/nonpalindrome.txt", b"fuse-api-nonpalindrome-v47")?;
    println!(
        "FUSE API write_if_size_not_palindrome committed, tx_id={}",
        write_if_size_not_palindrome_tx
    );
    let armstrong_seed = vec![b'a'; 153];
    let armstrong_seed_tx = api.write_if_missing("/api/armstrong.txt", &armstrong_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/armstrong.txt) committed, tx_id={}",
        armstrong_seed_tx
    );
    let write_if_size_armstrong_tx =
        api.write_if_size_armstrong("/api/armstrong.txt", b"fuse-api-armstrong-v48")?;
    println!(
        "FUSE API write_if_size_armstrong committed, tx_id={}",
        write_if_size_armstrong_tx
    );
    let nonarmstrong_seed = vec![b'a'; 154];
    let nonarmstrong_seed_tx = api.write_if_missing("/api/nonarmstrong.txt", &nonarmstrong_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonarmstrong.txt) committed, tx_id={}",
        nonarmstrong_seed_tx
    );
    let write_if_size_not_armstrong_tx = api
        .write_if_size_not_armstrong("/api/nonarmstrong.txt", b"fuse-api-nonarmstrong-v49")?;
    println!(
        "FUSE API write_if_size_not_armstrong committed, tx_id={}",
        write_if_size_not_armstrong_tx
    );
    let happy_seed = vec![b'a'; 19];
    let happy_seed_tx = api.write_if_missing("/api/happy.txt", &happy_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/happy.txt) committed, tx_id={}",
        happy_seed_tx
    );
    let write_if_size_happy_tx =
        api.write_if_size_happy("/api/happy.txt", b"fuse-api-happy-v50")?;
    println!(
        "FUSE API write_if_size_happy committed, tx_id={}",
        write_if_size_happy_tx
    );
    let nonhappy_seed = vec![b'a'; 20];
    let nonhappy_seed_tx = api.write_if_missing("/api/nonhappy.txt", &nonhappy_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonhappy.txt) committed, tx_id={}",
        nonhappy_seed_tx
    );
    let write_if_size_not_happy_tx =
        api.write_if_size_not_happy("/api/nonhappy.txt", b"fuse-api-nonhappy-v51")?;
    println!(
        "FUSE API write_if_size_not_happy committed, tx_id={}",
        write_if_size_not_happy_tx
    );
    let automorphic_seed = vec![b'a'; 25];
    let automorphic_seed_tx = api.write_if_missing("/api/automorphic.txt", &automorphic_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/automorphic.txt) committed, tx_id={}",
        automorphic_seed_tx
    );
    let write_if_size_automorphic_tx =
        api.write_if_size_automorphic("/api/automorphic.txt", b"fuse-api-automorphic-v52")?;
    println!(
        "FUSE API write_if_size_automorphic committed, tx_id={}",
        write_if_size_automorphic_tx
    );
    let nonautomorphic_seed = vec![b'a'; 26];
    let nonautomorphic_seed_tx =
        api.write_if_missing("/api/nonautomorphic.txt", &nonautomorphic_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonautomorphic.txt) committed, tx_id={}",
        nonautomorphic_seed_tx
    );
    let write_if_size_not_automorphic_tx = api.write_if_size_not_automorphic(
        "/api/nonautomorphic.txt",
        b"fuse-api-nonautomorphic-v53",
    )?;
    println!(
        "FUSE API write_if_size_not_automorphic committed, tx_id={}",
        write_if_size_not_automorphic_tx
    );
    let harshad_seed = vec![b'a'; 18];
    let harshad_seed_tx = api.write_if_missing("/api/harshad.txt", &harshad_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/harshad.txt) committed, tx_id={}",
        harshad_seed_tx
    );
    let write_if_size_harshad_tx =
        api.write_if_size_harshad("/api/harshad.txt", b"fuse-api-harshad-v54")?;
    println!(
        "FUSE API write_if_size_harshad committed, tx_id={}",
        write_if_size_harshad_tx
    );
    let nonharshad_seed = vec![b'a'; 19];
    let nonharshad_seed_tx = api.write_if_missing("/api/nonharshad.txt", &nonharshad_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonharshad.txt) committed, tx_id={}",
        nonharshad_seed_tx
    );
    let write_if_size_not_harshad_tx =
        api.write_if_size_not_harshad("/api/nonharshad.txt", b"fuse-api-nonharshad-v55")?;
    println!(
        "FUSE API write_if_size_not_harshad committed, tx_id={}",
        write_if_size_not_harshad_tx
    );
    let kaprekar_seed = vec![b'a'; 45];
    let kaprekar_seed_tx = api.write_if_missing("/api/kaprekar.txt", &kaprekar_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/kaprekar.txt) committed, tx_id={}",
        kaprekar_seed_tx
    );
    let write_if_size_kaprekar_tx =
        api.write_if_size_kaprekar("/api/kaprekar.txt", b"fuse-api-kaprekar-v56")?;
    println!(
        "FUSE API write_if_size_kaprekar committed, tx_id={}",
        write_if_size_kaprekar_tx
    );
    let nonkaprekar_seed = vec![b'a'; 46];
    let nonkaprekar_seed_tx = api.write_if_missing("/api/nonkaprekar.txt", &nonkaprekar_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonkaprekar.txt) committed, tx_id={}",
        nonkaprekar_seed_tx
    );
    let write_if_size_not_kaprekar_tx =
        api.write_if_size_not_kaprekar("/api/nonkaprekar.txt", b"fuse-api-nonkaprekar-v57")?;
    println!(
        "FUSE API write_if_size_not_kaprekar committed, tx_id={}",
        write_if_size_not_kaprekar_tx
    );
    let repdigit_seed = vec![b'a'; 11];
    let repdigit_seed_tx = api.write_if_missing("/api/repdigit.txt", &repdigit_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/repdigit.txt) committed, tx_id={}",
        repdigit_seed_tx
    );
    let write_if_size_repdigit_tx =
        api.write_if_size_repdigit("/api/repdigit.txt", b"fuse-api-repdigit-v58")?;
    println!(
        "FUSE API write_if_size_repdigit committed, tx_id={}",
        write_if_size_repdigit_tx
    );
    let nonrepdigit_seed = vec![b'a'; 12];
    let nonrepdigit_seed_tx =
        api.write_if_missing("/api/nonrepdigit.txt", &nonrepdigit_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonrepdigit.txt) committed, tx_id={}",
        nonrepdigit_seed_tx
    );
    let write_if_size_not_repdigit_tx =
        api.write_if_size_not_repdigit("/api/nonrepdigit.txt", b"fuse-api-nonrepdigit-v59")?;
    println!(
        "FUSE API write_if_size_not_repdigit committed, tx_id={}",
        write_if_size_not_repdigit_tx
    );
    let tribonacci_seed = vec![b'a'; 24];
    let tribonacci_seed_tx = api.write_if_missing("/api/tribonacci.txt", &tribonacci_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/tribonacci.txt) committed, tx_id={}",
        tribonacci_seed_tx
    );
    let write_if_size_tribonacci_tx =
        api.write_if_size_tribonacci("/api/tribonacci.txt", b"fuse-api-tribonacci-v60")?;
    println!(
        "FUSE API write_if_size_tribonacci committed, tx_id={}",
        write_if_size_tribonacci_tx
    );
    let nontribonacci_seed = vec![b'a'; 25];
    let nontribonacci_seed_tx =
        api.write_if_missing("/api/nontribonacci.txt", &nontribonacci_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nontribonacci.txt) committed, tx_id={}",
        nontribonacci_seed_tx
    );
    let write_if_size_not_tribonacci_tx = api
        .write_if_size_not_tribonacci("/api/nontribonacci.txt", b"fuse-api-nontribonacci-v61")?;
    println!(
        "FUSE API write_if_size_not_tribonacci committed, tx_id={}",
        write_if_size_not_tribonacci_tx
    );
    let pell_seed = vec![b'a'; 29];
    let pell_seed_tx = api.write_if_missing("/api/pell.txt", &pell_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pell.txt) committed, tx_id={}",
        pell_seed_tx
    );
    let write_if_size_pell_tx = api.write_if_size_pell("/api/pell.txt", b"fuse-api-pell-v62")?;
    println!(
        "FUSE API write_if_size_pell committed, tx_id={}",
        write_if_size_pell_tx
    );
    let nonpell_seed = vec![b'a'; 30];
    let nonpell_seed_tx = api.write_if_missing("/api/nonpell.txt", &nonpell_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpell.txt) committed, tx_id={}",
        nonpell_seed_tx
    );
    let write_if_size_not_pell_tx =
        api.write_if_size_not_pell("/api/nonpell.txt", b"fuse-api-nonpell-v63")?;
    println!(
        "FUSE API write_if_size_not_pell committed, tx_id={}",
        write_if_size_not_pell_tx
    );
    let lucas_seed = vec![b'a'; 29];
    let lucas_seed_tx = api.write_if_missing("/api/lucas.txt", &lucas_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/lucas.txt) committed, tx_id={}",
        lucas_seed_tx
    );
    let write_if_size_lucas_tx = api.write_if_size_lucas("/api/lucas.txt", b"fuse-api-lucas-v64")?;
    println!(
        "FUSE API write_if_size_lucas committed, tx_id={}",
        write_if_size_lucas_tx
    );
    let nonlucas_seed = vec![b'a'; 30];
    let nonlucas_seed_tx = api.write_if_missing("/api/nonlucas.txt", &nonlucas_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonlucas.txt) committed, tx_id={}",
        nonlucas_seed_tx
    );
    let write_if_size_not_lucas_tx =
        api.write_if_size_not_lucas("/api/nonlucas.txt", b"fuse-api-nonlucas-v65")?;
    println!(
        "FUSE API write_if_size_not_lucas committed, tx_id={}",
        write_if_size_not_lucas_tx
    );
    let mersenne_seed = vec![b'a'; 31];
    let mersenne_seed_tx = api.write_if_missing("/api/mersenne.txt", &mersenne_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/mersenne.txt) committed, tx_id={}",
        mersenne_seed_tx
    );
    let write_if_size_mersenne_tx =
        api.write_if_size_mersenne("/api/mersenne.txt", b"fuse-api-mersenne-v66")?;
    println!(
        "FUSE API write_if_size_mersenne committed, tx_id={}",
        write_if_size_mersenne_tx
    );
    let nonmersenne_seed = vec![b'a'; 32];
    let nonmersenne_seed_tx = api.write_if_missing("/api/nonmersenne.txt", &nonmersenne_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonmersenne.txt) committed, tx_id={}",
        nonmersenne_seed_tx
    );
    let write_if_size_not_mersenne_tx =
        api.write_if_size_not_mersenne("/api/nonmersenne.txt", b"fuse-api-nonmersenne-v67")?;
    println!(
        "FUSE API write_if_size_not_mersenne committed, tx_id={}",
        write_if_size_not_mersenne_tx
    );
    let pow3_seed = vec![b'a'; 27];
    let pow3_seed_tx = api.write_if_missing("/api/pow3.txt", &pow3_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow3.txt) committed, tx_id={}",
        pow3_seed_tx
    );
    let write_if_size_power_of_three_tx =
        api.write_if_size_power_of_three("/api/pow3.txt", b"fuse-api-pow3-v68")?;
    println!(
        "FUSE API write_if_size_power_of_three committed, tx_id={}",
        write_if_size_power_of_three_tx
    );
    let nonpow3_seed = vec![b'a'; 28];
    let nonpow3_seed_tx = api.write_if_missing("/api/nonpow3.txt", &nonpow3_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow3.txt) committed, tx_id={}",
        nonpow3_seed_tx
    );
    let write_if_size_not_power_of_three_tx =
        api.write_if_size_not_power_of_three("/api/nonpow3.txt", b"fuse-api-nonpow3-v69")?;
    println!(
        "FUSE API write_if_size_not_power_of_three committed, tx_id={}",
        write_if_size_not_power_of_three_tx
    );
    let pow4_seed = vec![b'a'; 64];
    let pow4_seed_tx = api.write_if_missing("/api/pow4.txt", &pow4_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow4.txt) committed, tx_id={}",
        pow4_seed_tx
    );
    let write_if_size_power_of_four_tx =
        api.write_if_size_power_of_four("/api/pow4.txt", b"fuse-api-pow4-v70")?;
    println!(
        "FUSE API write_if_size_power_of_four committed, tx_id={}",
        write_if_size_power_of_four_tx
    );
    let nonpow4_seed = vec![b'a'; 65];
    let nonpow4_seed_tx = api.write_if_missing("/api/nonpow4.txt", &nonpow4_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow4.txt) committed, tx_id={}",
        nonpow4_seed_tx
    );
    let write_if_size_not_power_of_four_tx =
        api.write_if_size_not_power_of_four("/api/nonpow4.txt", b"fuse-api-nonpow4-v71")?;
    println!(
        "FUSE API write_if_size_not_power_of_four committed, tx_id={}",
        write_if_size_not_power_of_four_tx
    );
    let pow5_seed = vec![b'a'; 125];
    let pow5_seed_tx = api.write_if_missing("/api/pow5.txt", &pow5_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow5.txt) committed, tx_id={}",
        pow5_seed_tx
    );
    let write_if_size_power_of_five_tx =
        api.write_if_size_power_of_five("/api/pow5.txt", b"fuse-api-pow5-v72")?;
    println!(
        "FUSE API write_if_size_power_of_five committed, tx_id={}",
        write_if_size_power_of_five_tx
    );
    let nonpow5_seed = vec![b'a'; 126];
    let nonpow5_seed_tx = api.write_if_missing("/api/nonpow5.txt", &nonpow5_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow5.txt) committed, tx_id={}",
        nonpow5_seed_tx
    );
    let write_if_size_not_power_of_five_tx =
        api.write_if_size_not_power_of_five("/api/nonpow5.txt", b"fuse-api-nonpow5-v73")?;
    println!(
        "FUSE API write_if_size_not_power_of_five committed, tx_id={}",
        write_if_size_not_power_of_five_tx
    );
    let pow6_seed = vec![b'a'; 216];
    let pow6_seed_tx = api.write_if_missing("/api/pow6.txt", &pow6_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow6.txt) committed, tx_id={}",
        pow6_seed_tx
    );
    let write_if_size_power_of_six_tx =
        api.write_if_size_power_of_six("/api/pow6.txt", b"fuse-api-pow6-v74")?;
    println!(
        "FUSE API write_if_size_power_of_six committed, tx_id={}",
        write_if_size_power_of_six_tx
    );
    let nonpow6_seed = vec![b'a'; 217];
    let nonpow6_seed_tx = api.write_if_missing("/api/nonpow6.txt", &nonpow6_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow6.txt) committed, tx_id={}",
        nonpow6_seed_tx
    );
    let write_if_size_not_power_of_six_tx =
        api.write_if_size_not_power_of_six("/api/nonpow6.txt", b"fuse-api-nonpow6-v75")?;
    println!(
        "FUSE API write_if_size_not_power_of_six committed, tx_id={}",
        write_if_size_not_power_of_six_tx
    );
    let pow7_seed = vec![b'a'; 343];
    let pow7_seed_tx = api.write_if_missing("/api/pow7.txt", &pow7_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow7.txt) committed, tx_id={}",
        pow7_seed_tx
    );
    let write_if_size_power_of_seven_tx =
        api.write_if_size_power_of_seven("/api/pow7.txt", b"fuse-api-pow7-v76")?;
    println!(
        "FUSE API write_if_size_power_of_seven committed, tx_id={}",
        write_if_size_power_of_seven_tx
    );
    let nonpow7_seed = vec![b'a'; 344];
    let nonpow7_seed_tx = api.write_if_missing("/api/nonpow7.txt", &nonpow7_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow7.txt) committed, tx_id={}",
        nonpow7_seed_tx
    );
    let write_if_size_not_power_of_seven_tx =
        api.write_if_size_not_power_of_seven("/api/nonpow7.txt", b"fuse-api-nonpow7-v77")?;
    println!(
        "FUSE API write_if_size_not_power_of_seven committed, tx_id={}",
        write_if_size_not_power_of_seven_tx
    );
    let pow8_seed = vec![b'a'; 512];
    let pow8_seed_tx = api.write_if_missing("/api/pow8.txt", &pow8_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow8.txt) committed, tx_id={}",
        pow8_seed_tx
    );
    let write_if_size_power_of_eight_tx =
        api.write_if_size_power_of_eight("/api/pow8.txt", b"fuse-api-pow8-v78")?;
    println!(
        "FUSE API write_if_size_power_of_eight committed, tx_id={}",
        write_if_size_power_of_eight_tx
    );
    let nonpow8_seed = vec![b'a'; 513];
    let nonpow8_seed_tx = api.write_if_missing("/api/nonpow8.txt", &nonpow8_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow8.txt) committed, tx_id={}",
        nonpow8_seed_tx
    );
    let write_if_size_not_power_of_eight_tx =
        api.write_if_size_not_power_of_eight("/api/nonpow8.txt", b"fuse-api-nonpow8-v79")?;
    println!(
        "FUSE API write_if_size_not_power_of_eight committed, tx_id={}",
        write_if_size_not_power_of_eight_tx
    );
    let pow9_seed = vec![b'a'; 729];
    let pow9_seed_tx = api.write_if_missing("/api/pow9.txt", &pow9_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow9.txt) committed, tx_id={}",
        pow9_seed_tx
    );
    let write_if_size_power_of_nine_tx =
        api.write_if_size_power_of_nine("/api/pow9.txt", b"fuse-api-pow9-v80")?;
    println!(
        "FUSE API write_if_size_power_of_nine committed, tx_id={}",
        write_if_size_power_of_nine_tx
    );
    let nonpow9_seed = vec![b'a'; 730];
    let nonpow9_seed_tx = api.write_if_missing("/api/nonpow9.txt", &nonpow9_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow9.txt) committed, tx_id={}",
        nonpow9_seed_tx
    );
    let write_if_size_not_power_of_nine_tx =
        api.write_if_size_not_power_of_nine("/api/nonpow9.txt", b"fuse-api-nonpow9-v81")?;
    println!(
        "FUSE API write_if_size_not_power_of_nine committed, tx_id={}",
        write_if_size_not_power_of_nine_tx
    );
    let pow10_seed = vec![b'a'; 1000];
    let pow10_seed_tx = api.write_if_missing("/api/pow10.txt", &pow10_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow10.txt) committed, tx_id={}",
        pow10_seed_tx
    );
    let write_if_size_power_of_ten_tx =
        api.write_if_size_power_of_ten("/api/pow10.txt", b"fuse-api-pow10-v82")?;
    println!(
        "FUSE API write_if_size_power_of_ten committed, tx_id={}",
        write_if_size_power_of_ten_tx
    );
    let nonpow10_seed = vec![b'a'; 1001];
    let nonpow10_seed_tx = api.write_if_missing("/api/nonpow10.txt", &nonpow10_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow10.txt) committed, tx_id={}",
        nonpow10_seed_tx
    );
    let write_if_size_not_power_of_ten_tx =
        api.write_if_size_not_power_of_ten("/api/nonpow10.txt", b"fuse-api-nonpow10-v83")?;
    println!(
        "FUSE API write_if_size_not_power_of_ten committed, tx_id={}",
        write_if_size_not_power_of_ten_tx
    );
    let pow11_seed = vec![b'a'; 121];
    let pow11_seed_tx = api.write_if_missing("/api/pow11.txt", &pow11_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow11.txt) committed, tx_id={}",
        pow11_seed_tx
    );
    let write_if_size_power_of_eleven_tx =
        api.write_if_size_power_of_eleven("/api/pow11.txt", b"fuse-api-pow11-v84")?;
    println!(
        "FUSE API write_if_size_power_of_eleven committed, tx_id={}",
        write_if_size_power_of_eleven_tx
    );
    let nonpow11_seed = vec![b'a'; 122];
    let nonpow11_seed_tx = api.write_if_missing("/api/nonpow11.txt", &nonpow11_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow11.txt) committed, tx_id={}",
        nonpow11_seed_tx
    );
    let write_if_size_not_power_of_eleven_tx =
        api.write_if_size_not_power_of_eleven("/api/nonpow11.txt", b"fuse-api-nonpow11-v85")?;
    println!(
        "FUSE API write_if_size_not_power_of_eleven committed, tx_id={}",
        write_if_size_not_power_of_eleven_tx
    );
    let pow12_seed = vec![b'a'; 144];
    let pow12_seed_tx = api.write_if_missing("/api/pow12.txt", &pow12_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow12.txt) committed, tx_id={}",
        pow12_seed_tx
    );
    let write_if_size_power_of_twelve_tx =
        api.write_if_size_power_of_twelve("/api/pow12.txt", b"fuse-api-pow12-v86")?;
    println!(
        "FUSE API write_if_size_power_of_twelve committed, tx_id={}",
        write_if_size_power_of_twelve_tx
    );
    let nonpow12_seed = vec![b'a'; 145];
    let nonpow12_seed_tx = api.write_if_missing("/api/nonpow12.txt", &nonpow12_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow12.txt) committed, tx_id={}",
        nonpow12_seed_tx
    );
    let write_if_size_not_power_of_twelve_tx =
        api.write_if_size_not_power_of_twelve("/api/nonpow12.txt", b"fuse-api-nonpow12-v87")?;
    println!(
        "FUSE API write_if_size_not_power_of_twelve committed, tx_id={}",
        write_if_size_not_power_of_twelve_tx
    );
    let pow13_seed = vec![b'a'; 169];
    let pow13_seed_tx = api.write_if_missing("/api/pow13.txt", &pow13_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow13.txt) committed, tx_id={}",
        pow13_seed_tx
    );
    let write_if_size_power_of_thirteen_tx =
        api.write_if_size_power_of_thirteen("/api/pow13.txt", b"fuse-api-pow13-v88")?;
    println!(
        "FUSE API write_if_size_power_of_thirteen committed, tx_id={}",
        write_if_size_power_of_thirteen_tx
    );
    let nonpow13_seed = vec![b'a'; 170];
    let nonpow13_seed_tx = api.write_if_missing("/api/nonpow13.txt", &nonpow13_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow13.txt) committed, tx_id={}",
        nonpow13_seed_tx
    );
    let write_if_size_not_power_of_thirteen_tx =
        api.write_if_size_not_power_of_thirteen("/api/nonpow13.txt", b"fuse-api-nonpow13-v89")?;
    println!(
        "FUSE API write_if_size_not_power_of_thirteen committed, tx_id={}",
        write_if_size_not_power_of_thirteen_tx
    );
    let pow14_seed = vec![b'a'; 196];
    let pow14_seed_tx = api.write_if_missing("/api/pow14.txt", &pow14_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow14.txt) committed, tx_id={}",
        pow14_seed_tx
    );
    let write_if_size_power_of_fourteen_tx =
        api.write_if_size_power_of_fourteen("/api/pow14.txt", b"fuse-api-pow14-v90")?;
    println!(
        "FUSE API write_if_size_power_of_fourteen committed, tx_id={}",
        write_if_size_power_of_fourteen_tx
    );
    let nonpow14_seed = vec![b'a'; 197];
    let nonpow14_seed_tx = api.write_if_missing("/api/nonpow14.txt", &nonpow14_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow14.txt) committed, tx_id={}",
        nonpow14_seed_tx
    );
    let write_if_size_not_power_of_fourteen_tx =
        api.write_if_size_not_power_of_fourteen("/api/nonpow14.txt", b"fuse-api-nonpow14-v91")?;
    println!(
        "FUSE API write_if_size_not_power_of_fourteen committed, tx_id={}",
        write_if_size_not_power_of_fourteen_tx
    );
    let pow15_seed = vec![b'a'; 225];
    let pow15_seed_tx = api.write_if_missing("/api/pow15.txt", &pow15_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow15.txt) committed, tx_id={}",
        pow15_seed_tx
    );
    let write_if_size_power_of_fifteen_tx =
        api.write_if_size_power_of_fifteen("/api/pow15.txt", b"fuse-api-pow15-v92")?;
    println!(
        "FUSE API write_if_size_power_of_fifteen committed, tx_id={}",
        write_if_size_power_of_fifteen_tx
    );
    let nonpow15_seed = vec![b'a'; 226];
    let nonpow15_seed_tx = api.write_if_missing("/api/nonpow15.txt", &nonpow15_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow15.txt) committed, tx_id={}",
        nonpow15_seed_tx
    );
    let write_if_size_not_power_of_fifteen_tx =
        api.write_if_size_not_power_of_fifteen("/api/nonpow15.txt", b"fuse-api-nonpow15-v93")?;
    println!(
        "FUSE API write_if_size_not_power_of_fifteen committed, tx_id={}",
        write_if_size_not_power_of_fifteen_tx
    );
    let pow16_seed = vec![b'a'; 256];
    let pow16_seed_tx = api.write_if_missing("/api/pow16.txt", &pow16_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow16.txt) committed, tx_id={}",
        pow16_seed_tx
    );
    let write_if_size_power_of_sixteen_tx =
        api.write_if_size_power_of_sixteen("/api/pow16.txt", b"fuse-api-pow16-v94")?;
    println!(
        "FUSE API write_if_size_power_of_sixteen committed, tx_id={}",
        write_if_size_power_of_sixteen_tx
    );
    let nonpow16_seed = vec![b'a'; 257];
    let nonpow16_seed_tx = api.write_if_missing("/api/nonpow16.txt", &nonpow16_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow16.txt) committed, tx_id={}",
        nonpow16_seed_tx
    );
    let write_if_size_not_power_of_sixteen_tx =
        api.write_if_size_not_power_of_sixteen("/api/nonpow16.txt", b"fuse-api-nonpow16-v95")?;
    println!(
        "FUSE API write_if_size_not_power_of_sixteen committed, tx_id={}",
        write_if_size_not_power_of_sixteen_tx
    );
    let pow17_seed = vec![b'a'; 289];
    let pow17_seed_tx = api.write_if_missing("/api/pow17.txt", &pow17_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow17.txt) committed, tx_id={}",
        pow17_seed_tx
    );
    let write_if_size_power_of_seventeen_tx =
        api.write_if_size_power_of_seventeen("/api/pow17.txt", b"fuse-api-pow17-v96")?;
    println!(
        "FUSE API write_if_size_power_of_seventeen committed, tx_id={}",
        write_if_size_power_of_seventeen_tx
    );
    let nonpow17_seed = vec![b'a'; 290];
    let nonpow17_seed_tx = api.write_if_missing("/api/nonpow17.txt", &nonpow17_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow17.txt) committed, tx_id={}",
        nonpow17_seed_tx
    );
    let write_if_size_not_power_of_seventeen_tx =
        api.write_if_size_not_power_of_seventeen("/api/nonpow17.txt", b"fuse-api-nonpow17-v97")?;
    println!(
        "FUSE API write_if_size_not_power_of_seventeen committed, tx_id={}",
        write_if_size_not_power_of_seventeen_tx
    );
    let pow18_seed = vec![b'a'; 324];
    let pow18_seed_tx = api.write_if_missing("/api/pow18.txt", &pow18_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow18.txt) committed, tx_id={}",
        pow18_seed_tx
    );
    let write_if_size_power_of_eighteen_tx =
        api.write_if_size_power_of_eighteen("/api/pow18.txt", b"fuse-api-pow18-v98")?;
    println!(
        "FUSE API write_if_size_power_of_eighteen committed, tx_id={}",
        write_if_size_power_of_eighteen_tx
    );
    let nonpow18_seed = vec![b'a'; 325];
    let nonpow18_seed_tx = api.write_if_missing("/api/nonpow18.txt", &nonpow18_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow18.txt) committed, tx_id={}",
        nonpow18_seed_tx
    );
    let write_if_size_not_power_of_eighteen_tx =
        api.write_if_size_not_power_of_eighteen("/api/nonpow18.txt", b"fuse-api-nonpow18-v99")?;
    println!(
        "FUSE API write_if_size_not_power_of_eighteen committed, tx_id={}",
        write_if_size_not_power_of_eighteen_tx
    );
    let pow19_seed = vec![b'a'; 361];
    let pow19_seed_tx = api.write_if_missing("/api/pow19.txt", &pow19_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow19.txt) committed, tx_id={}",
        pow19_seed_tx
    );
    let write_if_size_power_of_nineteen_tx =
        api.write_if_size_power_of_nineteen("/api/pow19.txt", b"fuse-api-pow19-v100")?;
    println!(
        "FUSE API write_if_size_power_of_nineteen committed, tx_id={}",
        write_if_size_power_of_nineteen_tx
    );
    let nonpow19_seed = vec![b'a'; 362];
    let nonpow19_seed_tx = api.write_if_missing("/api/nonpow19.txt", &nonpow19_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow19.txt) committed, tx_id={}",
        nonpow19_seed_tx
    );
    let write_if_size_not_power_of_nineteen_tx = api
        .write_if_size_not_power_of_nineteen("/api/nonpow19.txt", b"fuse-api-nonpow19-v101")?;
    println!(
        "FUSE API write_if_size_not_power_of_nineteen committed, tx_id={}",
        write_if_size_not_power_of_nineteen_tx
    );
    let pow20_seed = vec![b'a'; 400];
    let pow20_seed_tx = api.write_if_missing("/api/pow20.txt", &pow20_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow20.txt) committed, tx_id={}",
        pow20_seed_tx
    );
    let write_if_size_power_of_twenty_tx =
        api.write_if_size_power_of_twenty("/api/pow20.txt", b"fuse-api-pow20-v102")?;
    println!(
        "FUSE API write_if_size_power_of_twenty committed, tx_id={}",
        write_if_size_power_of_twenty_tx
    );
    let nonpow20_seed = vec![b'a'; 401];
    let nonpow20_seed_tx = api.write_if_missing("/api/nonpow20.txt", &nonpow20_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow20.txt) committed, tx_id={}",
        nonpow20_seed_tx
    );
    let write_if_size_not_power_of_twenty_tx =
        api.write_if_size_not_power_of_twenty("/api/nonpow20.txt", b"fuse-api-nonpow20-v103")?;
    println!(
        "FUSE API write_if_size_not_power_of_twenty committed, tx_id={}",
        write_if_size_not_power_of_twenty_tx
    );
    let pow21_seed = vec![b'a'; 441];
    let pow21_seed_tx = api.write_if_missing("/api/pow21.txt", &pow21_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow21.txt) committed, tx_id={}",
        pow21_seed_tx
    );
    let write_if_size_power_of_twenty_one_tx =
        api.write_if_size_power_of_twenty_one("/api/pow21.txt", b"fuse-api-pow21-v104")?;
    println!(
        "FUSE API write_if_size_power_of_twenty_one committed, tx_id={}",
        write_if_size_power_of_twenty_one_tx
    );
    let nonpow21_seed = vec![b'a'; 442];
    let nonpow21_seed_tx = api.write_if_missing("/api/nonpow21.txt", &nonpow21_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow21.txt) committed, tx_id={}",
        nonpow21_seed_tx
    );
    let write_if_size_not_power_of_twenty_one_tx = api
        .write_if_size_not_power_of_twenty_one("/api/nonpow21.txt", b"fuse-api-nonpow21-v105")?;
    println!(
        "FUSE API write_if_size_not_power_of_twenty_one committed, tx_id={}",
        write_if_size_not_power_of_twenty_one_tx
    );
    let pow22_seed = vec![b'a'; 484];
    let pow22_seed_tx = api.write_if_missing("/api/pow22.txt", &pow22_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow22.txt) committed, tx_id={}",
        pow22_seed_tx
    );
    let write_if_size_power_of_twenty_two_tx =
        api.write_if_size_power_of_twenty_two("/api/pow22.txt", b"fuse-api-pow22-v106")?;
    println!(
        "FUSE API write_if_size_power_of_twenty_two committed, tx_id={}",
        write_if_size_power_of_twenty_two_tx
    );
    let nonpow22_seed = vec![b'a'; 485];
    let nonpow22_seed_tx = api.write_if_missing("/api/nonpow22.txt", &nonpow22_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow22.txt) committed, tx_id={}",
        nonpow22_seed_tx
    );
    let write_if_size_not_power_of_twenty_two_tx = api
        .write_if_size_not_power_of_twenty_two("/api/nonpow22.txt", b"fuse-api-nonpow22-v107")?;
    println!(
        "FUSE API write_if_size_not_power_of_twenty_two committed, tx_id={}",
        write_if_size_not_power_of_twenty_two_tx
    );
    let pow23_seed = vec![b'a'; 529];
    let pow23_seed_tx = api.write_if_missing("/api/pow23.txt", &pow23_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow23.txt) committed, tx_id={}",
        pow23_seed_tx
    );
    let write_if_size_power_of_twenty_three_tx =
        api.write_if_size_power_of_twenty_three("/api/pow23.txt", b"fuse-api-pow23-v108")?;
    println!(
        "FUSE API write_if_size_power_of_twenty_three committed, tx_id={}",
        write_if_size_power_of_twenty_three_tx
    );
    let nonpow23_seed = vec![b'a'; 530];
    let nonpow23_seed_tx = api.write_if_missing("/api/nonpow23.txt", &nonpow23_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow23.txt) committed, tx_id={}",
        nonpow23_seed_tx
    );
    let write_if_size_not_power_of_twenty_three_tx = api.write_if_size_not_power_of_twenty_three(
        "/api/nonpow23.txt",
        b"fuse-api-nonpow23-v109",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_twenty_three committed, tx_id={}",
        write_if_size_not_power_of_twenty_three_tx
    );
    let pow24_seed = vec![b'a'; 576];
    let pow24_seed_tx = api.write_if_missing("/api/pow24.txt", &pow24_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow24.txt) committed, tx_id={}",
        pow24_seed_tx
    );
    let write_if_size_power_of_twenty_four_tx =
        api.write_if_size_power_of_twenty_four("/api/pow24.txt", b"fuse-api-pow24-v110")?;
    println!(
        "FUSE API write_if_size_power_of_twenty_four committed, tx_id={}",
        write_if_size_power_of_twenty_four_tx
    );
    let nonpow24_seed = vec![b'a'; 577];
    let nonpow24_seed_tx = api.write_if_missing("/api/nonpow24.txt", &nonpow24_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow24.txt) committed, tx_id={}",
        nonpow24_seed_tx
    );
    let write_if_size_not_power_of_twenty_four_tx = api.write_if_size_not_power_of_twenty_four(
        "/api/nonpow24.txt",
        b"fuse-api-nonpow24-v111",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_twenty_four committed, tx_id={}",
        write_if_size_not_power_of_twenty_four_tx
    );
    let pow25_seed = vec![b'a'; 625];
    let pow25_seed_tx = api.write_if_missing("/api/pow25.txt", &pow25_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow25.txt) committed, tx_id={}",
        pow25_seed_tx
    );
    let write_if_size_power_of_twenty_five_tx =
        api.write_if_size_power_of_twenty_five("/api/pow25.txt", b"fuse-api-pow25-v112")?;
    println!(
        "FUSE API write_if_size_power_of_twenty_five committed, tx_id={}",
        write_if_size_power_of_twenty_five_tx
    );
    let nonpow25_seed = vec![b'a'; 626];
    let nonpow25_seed_tx = api.write_if_missing("/api/nonpow25.txt", &nonpow25_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow25.txt) committed, tx_id={}",
        nonpow25_seed_tx
    );
    let write_if_size_not_power_of_twenty_five_tx = api.write_if_size_not_power_of_twenty_five(
        "/api/nonpow25.txt",
        b"fuse-api-nonpow25-v113",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_twenty_five committed, tx_id={}",
        write_if_size_not_power_of_twenty_five_tx
    );
    let pow26_seed = vec![b'a'; 676];
    let pow26_seed_tx = api.write_if_missing("/api/pow26.txt", &pow26_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow26.txt) committed, tx_id={}",
        pow26_seed_tx
    );
    let write_if_size_power_of_twenty_six_tx =
        api.write_if_size_power_of_twenty_six("/api/pow26.txt", b"fuse-api-pow26-v114")?;
    println!(
        "FUSE API write_if_size_power_of_twenty_six committed, tx_id={}",
        write_if_size_power_of_twenty_six_tx
    );
    let nonpow26_seed = vec![b'a'; 677];
    let nonpow26_seed_tx = api.write_if_missing("/api/nonpow26.txt", &nonpow26_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow26.txt) committed, tx_id={}",
        nonpow26_seed_tx
    );
    let write_if_size_not_power_of_twenty_six_tx = api.write_if_size_not_power_of_twenty_six(
        "/api/nonpow26.txt",
        b"fuse-api-nonpow26-v115",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_twenty_six committed, tx_id={}",
        write_if_size_not_power_of_twenty_six_tx
    );
    let pow27_seed = vec![b'a'; 729];
    let pow27_seed_tx = api.write_if_missing("/api/pow27.txt", &pow27_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow27.txt) committed, tx_id={}",
        pow27_seed_tx
    );
    let write_if_size_power_of_twenty_seven_tx =
        api.write_if_size_power_of_twenty_seven("/api/pow27.txt", b"fuse-api-pow27-v116")?;
    println!(
        "FUSE API write_if_size_power_of_twenty_seven committed, tx_id={}",
        write_if_size_power_of_twenty_seven_tx
    );
    let nonpow27_seed = vec![b'a'; 730];
    let nonpow27_seed_tx = api.write_if_missing("/api/nonpow27.txt", &nonpow27_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow27.txt) committed, tx_id={}",
        nonpow27_seed_tx
    );
    let write_if_size_not_power_of_twenty_seven_tx = api.write_if_size_not_power_of_twenty_seven(
        "/api/nonpow27.txt",
        b"fuse-api-nonpow27-v117",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_twenty_seven committed, tx_id={}",
        write_if_size_not_power_of_twenty_seven_tx
    );
    let pow28_seed = vec![b'a'; 784];
    let pow28_seed_tx = api.write_if_missing("/api/pow28.txt", &pow28_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow28.txt) committed, tx_id={}",
        pow28_seed_tx
    );
    let write_if_size_power_of_twenty_eight_tx =
        api.write_if_size_power_of_twenty_eight("/api/pow28.txt", b"fuse-api-pow28-v118")?;
    println!(
        "FUSE API write_if_size_power_of_twenty_eight committed, tx_id={}",
        write_if_size_power_of_twenty_eight_tx
    );
    let nonpow28_seed = vec![b'a'; 785];
    let nonpow28_seed_tx = api.write_if_missing("/api/nonpow28.txt", &nonpow28_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow28.txt) committed, tx_id={}",
        nonpow28_seed_tx
    );
    let write_if_size_not_power_of_twenty_eight_tx = api.write_if_size_not_power_of_twenty_eight(
        "/api/nonpow28.txt",
        b"fuse-api-nonpow28-v119",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_twenty_eight committed, tx_id={}",
        write_if_size_not_power_of_twenty_eight_tx
    );
    let pow29_seed = vec![b'a'; 841];
    let pow29_seed_tx = api.write_if_missing("/api/pow29.txt", &pow29_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow29.txt) committed, tx_id={}",
        pow29_seed_tx
    );
    let write_if_size_power_of_twenty_nine_tx =
        api.write_if_size_power_of_twenty_nine("/api/pow29.txt", b"fuse-api-pow29-v120")?;
    println!(
        "FUSE API write_if_size_power_of_twenty_nine committed, tx_id={}",
        write_if_size_power_of_twenty_nine_tx
    );
    let nonpow29_seed = vec![b'a'; 842];
    let nonpow29_seed_tx = api.write_if_missing("/api/nonpow29.txt", &nonpow29_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow29.txt) committed, tx_id={}",
        nonpow29_seed_tx
    );
    let write_if_size_not_power_of_twenty_nine_tx = api.write_if_size_not_power_of_twenty_nine(
        "/api/nonpow29.txt",
        b"fuse-api-nonpow29-v121",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_twenty_nine committed, tx_id={}",
        write_if_size_not_power_of_twenty_nine_tx
    );
    let pow30_seed = vec![b'a'; 900];
    let pow30_seed_tx = api.write_if_missing("/api/pow30.txt", &pow30_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow30.txt) committed, tx_id={}",
        pow30_seed_tx
    );
    let write_if_size_power_of_thirty_tx =
        api.write_if_size_power_of_thirty("/api/pow30.txt", b"fuse-api-pow30-v122")?;
    println!(
        "FUSE API write_if_size_power_of_thirty committed, tx_id={}",
        write_if_size_power_of_thirty_tx
    );
    let nonpow30_seed = vec![b'a'; 901];
    let nonpow30_seed_tx = api.write_if_missing("/api/nonpow30.txt", &nonpow30_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow30.txt) committed, tx_id={}",
        nonpow30_seed_tx
    );
    let write_if_size_not_power_of_thirty_tx = api.write_if_size_not_power_of_thirty(
        "/api/nonpow30.txt",
        b"fuse-api-nonpow30-v123",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_thirty committed, tx_id={}",
        write_if_size_not_power_of_thirty_tx
    );
    let pow31_seed = vec![b'a'; 961];
    let pow31_seed_tx = api.write_if_missing("/api/pow31.txt", &pow31_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow31.txt) committed, tx_id={}",
        pow31_seed_tx
    );
    let write_if_size_power_of_thirty_one_tx =
        api.write_if_size_power_of_thirty_one("/api/pow31.txt", b"fuse-api-pow31-v124")?;
    println!(
        "FUSE API write_if_size_power_of_thirty_one committed, tx_id={}",
        write_if_size_power_of_thirty_one_tx
    );
    let nonpow31_seed = vec![b'a'; 962];
    let nonpow31_seed_tx = api.write_if_missing("/api/nonpow31.txt", &nonpow31_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow31.txt) committed, tx_id={}",
        nonpow31_seed_tx
    );
    let write_if_size_not_power_of_thirty_one_tx = api.write_if_size_not_power_of_thirty_one(
        "/api/nonpow31.txt",
        b"fuse-api-nonpow31-v125",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_thirty_one committed, tx_id={}",
        write_if_size_not_power_of_thirty_one_tx
    );
    let pow32_seed = vec![b'a'; 1024];
    let pow32_seed_tx = api.write_if_missing("/api/pow32.txt", &pow32_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow32.txt) committed, tx_id={}",
        pow32_seed_tx
    );
    let write_if_size_power_of_thirty_two_tx =
        api.write_if_size_power_of_thirty_two("/api/pow32.txt", b"fuse-api-pow32-v126")?;
    println!(
        "FUSE API write_if_size_power_of_thirty_two committed, tx_id={}",
        write_if_size_power_of_thirty_two_tx
    );
    let nonpow32_seed = vec![b'a'; 1025];
    let nonpow32_seed_tx = api.write_if_missing("/api/nonpow32.txt", &nonpow32_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow32.txt) committed, tx_id={}",
        nonpow32_seed_tx
    );
    let write_if_size_not_power_of_thirty_two_tx = api.write_if_size_not_power_of_thirty_two(
        "/api/nonpow32.txt",
        b"fuse-api-nonpow32-v127",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_thirty_two committed, tx_id={}",
        write_if_size_not_power_of_thirty_two_tx
    );
    let pow33_seed = vec![b'a'; 1089];
    let pow33_seed_tx = api.write_if_missing("/api/pow33.txt", &pow33_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow33.txt) committed, tx_id={}",
        pow33_seed_tx
    );
    let write_if_size_power_of_thirty_three_tx =
        api.write_if_size_power_of_thirty_three("/api/pow33.txt", b"fuse-api-pow33-v128")?;
    println!(
        "FUSE API write_if_size_power_of_thirty_three committed, tx_id={}",
        write_if_size_power_of_thirty_three_tx
    );
    let nonpow33_seed = vec![b'a'; 1090];
    let nonpow33_seed_tx = api.write_if_missing("/api/nonpow33.txt", &nonpow33_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow33.txt) committed, tx_id={}",
        nonpow33_seed_tx
    );
    let write_if_size_not_power_of_thirty_three_tx = api.write_if_size_not_power_of_thirty_three(
        "/api/nonpow33.txt",
        b"fuse-api-nonpow33-v129",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_thirty_three committed, tx_id={}",
        write_if_size_not_power_of_thirty_three_tx
    );
    let pow34_seed = vec![b'a'; 1156];
    let pow34_seed_tx = api.write_if_missing("/api/pow34.txt", &pow34_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow34.txt) committed, tx_id={}",
        pow34_seed_tx
    );
    let write_if_size_power_of_thirty_four_tx =
        api.write_if_size_power_of_thirty_four("/api/pow34.txt", b"fuse-api-pow34-v130")?;
    println!(
        "FUSE API write_if_size_power_of_thirty_four committed, tx_id={}",
        write_if_size_power_of_thirty_four_tx
    );
    let nonpow34_seed = vec![b'a'; 1157];
    let nonpow34_seed_tx = api.write_if_missing("/api/nonpow34.txt", &nonpow34_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow34.txt) committed, tx_id={}",
        nonpow34_seed_tx
    );
    let write_if_size_not_power_of_thirty_four_tx = api.write_if_size_not_power_of_thirty_four(
        "/api/nonpow34.txt",
        b"fuse-api-nonpow34-v131",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_thirty_four committed, tx_id={}",
        write_if_size_not_power_of_thirty_four_tx
    );
    let pow35_seed = vec![b'a'; 1225];
    let pow35_seed_tx = api.write_if_missing("/api/pow35.txt", &pow35_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow35.txt) committed, tx_id={}",
        pow35_seed_tx
    );
    let write_if_size_power_of_thirty_five_tx =
        api.write_if_size_power_of_thirty_five("/api/pow35.txt", b"fuse-api-pow35-v132")?;
    println!(
        "FUSE API write_if_size_power_of_thirty_five committed, tx_id={}",
        write_if_size_power_of_thirty_five_tx
    );
    let nonpow35_seed = vec![b'a'; 1226];
    let nonpow35_seed_tx = api.write_if_missing("/api/nonpow35.txt", &nonpow35_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow35.txt) committed, tx_id={}",
        nonpow35_seed_tx
    );
    let write_if_size_not_power_of_thirty_five_tx = api.write_if_size_not_power_of_thirty_five(
        "/api/nonpow35.txt",
        b"fuse-api-nonpow35-v133",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_thirty_five committed, tx_id={}",
        write_if_size_not_power_of_thirty_five_tx
    );
    let pow36_seed = vec![b'a'; 1296];
    let pow36_seed_tx = api.write_if_missing("/api/pow36.txt", &pow36_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow36.txt) committed, tx_id={}",
        pow36_seed_tx
    );
    let write_if_size_power_of_thirty_six_tx =
        api.write_if_size_power_of_thirty_six("/api/pow36.txt", b"fuse-api-pow36-v134")?;
    println!(
        "FUSE API write_if_size_power_of_thirty_six committed, tx_id={}",
        write_if_size_power_of_thirty_six_tx
    );
    let nonpow36_seed = vec![b'a'; 1297];
    let nonpow36_seed_tx = api.write_if_missing("/api/nonpow36.txt", &nonpow36_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow36.txt) committed, tx_id={}",
        nonpow36_seed_tx
    );
    let write_if_size_not_power_of_thirty_six_tx = api.write_if_size_not_power_of_thirty_six(
        "/api/nonpow36.txt",
        b"fuse-api-nonpow36-v135",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_thirty_six committed, tx_id={}",
        write_if_size_not_power_of_thirty_six_tx
    );
    let pow37_seed = vec![b'a'; 1369];
    let pow37_seed_tx = api.write_if_missing("/api/pow37.txt", &pow37_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow37.txt) committed, tx_id={}",
        pow37_seed_tx
    );
    let write_if_size_power_of_thirty_seven_tx =
        api.write_if_size_power_of_thirty_seven("/api/pow37.txt", b"fuse-api-pow37-v136")?;
    println!(
        "FUSE API write_if_size_power_of_thirty_seven committed, tx_id={}",
        write_if_size_power_of_thirty_seven_tx
    );
    let nonpow37_seed = vec![b'a'; 1370];
    let nonpow37_seed_tx = api.write_if_missing("/api/nonpow37.txt", &nonpow37_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow37.txt) committed, tx_id={}",
        nonpow37_seed_tx
    );
    let write_if_size_not_power_of_thirty_seven_tx = api.write_if_size_not_power_of_thirty_seven(
        "/api/nonpow37.txt",
        b"fuse-api-nonpow37-v137",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_thirty_seven committed, tx_id={}",
        write_if_size_not_power_of_thirty_seven_tx
    );
    let pow38_seed = vec![b'a'; 1444];
    let pow38_seed_tx = api.write_if_missing("/api/pow38.txt", &pow38_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow38.txt) committed, tx_id={}",
        pow38_seed_tx
    );
    let write_if_size_power_of_thirty_eight_tx =
        api.write_if_size_power_of_thirty_eight("/api/pow38.txt", b"fuse-api-pow38-v138")?;
    println!(
        "FUSE API write_if_size_power_of_thirty_eight committed, tx_id={}",
        write_if_size_power_of_thirty_eight_tx
    );
    let nonpow38_seed = vec![b'a'; 1445];
    let nonpow38_seed_tx = api.write_if_missing("/api/nonpow38.txt", &nonpow38_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow38.txt) committed, tx_id={}",
        nonpow38_seed_tx
    );
    let write_if_size_not_power_of_thirty_eight_tx = api.write_if_size_not_power_of_thirty_eight(
        "/api/nonpow38.txt",
        b"fuse-api-nonpow38-v139",
    )?;
    println!(
        "FUSE API write_if_size_not_power_of_thirty_eight committed, tx_id={}",
        write_if_size_not_power_of_thirty_eight_tx
    );
    let pow39_seed = vec![b'a'; 1521];
    let pow39_seed_tx = api.write_if_missing("/api/pow39.txt", &pow39_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/pow39.txt) committed, tx_id={}",
        pow39_seed_tx
    );
    let write_if_size_power_of_thirty_nine_tx =
        api.write_if_size_power_of_thirty_nine("/api/pow39.txt", b"fuse-api-pow39-v140")?;
    println!(
        "FUSE API write_if_size_power_of_thirty_nine committed, tx_id={}",
        write_if_size_power_of_thirty_nine_tx
    );
    let nonpow39_seed = vec![b'a'; 1522];
    let nonpow39_seed_tx = api.write_if_missing("/api/nonpow39.txt", &nonpow39_seed[..])?;
    println!(
        "FUSE API write_if_missing(/api/nonpow39.txt) committed, tx_id={}",
        nonpow39_seed_tx
    );
    let write_if_size_not_power_of_thirty_nine_tx =
        api.write_if_size_not_power_of_thirty_nine("/api/nonpow39.txt", b"fuse-api-nonpow39-v141")?;
    println!(
        "FUSE API write_if_size_not_power_of_thirty_nine committed, tx_id={}",
        write_if_size_not_power_of_thirty_nine_tx
    );

    let names = api.readdir("/api")?;
    println!("FUSE API readdir /api -> {:?}", names);
    let detailed = api.readdir_with_kinds("/api")?;
    println!("FUSE API readdir_with_kinds /api -> {:?}", detailed);
    let walked = api.walk_dir("/api")?;
    println!("FUSE API walk_dir /api -> {:?}", walked);
    let summary = api.tree_summary("/api")?;
    println!(
        "FUSE API tree_summary /api -> files={}, directories={}",
        summary.files, summary.directories
    );
    let tree_bytes = api.tree_bytes("/api")?;
    println!("FUSE API tree_bytes /api -> {}", tree_bytes);

    api.rename("/api/demo.txt", "/api/renamed.txt")?;
    println!("FUSE API rename committed.");

    let part = api.read("/api/renamed.txt", 5, 3)?;
    println!("FUSE API read slice [5..8]: {}", String::from_utf8_lossy(&part));

    api.open("/api/renamed.txt")?;
    println!("FUSE API open validated metadata path.");
    let kind = api.stat("/api/renamed.txt")?;
    if kind != FuseNodeKind::File {
        return Err(format!("expected file kind from stat, got {kind:?}").into());
    }
    println!("FUSE API stat /api/renamed.txt -> {:?}", kind);
    let copy_tx = api.copy_file("/api/renamed.txt", "/api/copied.txt", 0)?;
    println!("FUSE API copy_file committed, tx_id={}", copy_tx);
    let trunc_tx = api.truncate_file("/api/copied.txt", 8, 1)?;
    println!("FUSE API truncate_file committed, tx_id={}", trunc_tx);
    let append_tx = api.append_file("/api/copied.txt", 2, b"-v2")?;
    println!("FUSE API append_file committed, tx_id={}", append_tx);
    let overwrite_tx = api.overwrite_range("/api/copied.txt", 4, 3, b"PATCH")?;
    println!("FUSE API overwrite_range committed, tx_id={}", overwrite_tx);
    let insert_tx = api.insert_range("/api/copied.txt", 4, 4, b"++")?;
    println!("FUSE API insert_range committed, tx_id={}", insert_tx);
    let delete_tx = api.delete_range("/api/copied.txt", 4, 2, 5)?;
    println!("FUSE API delete_range committed, tx_id={}", delete_tx);
    let replace_tx = api.replace_range("/api/copied.txt", 4, 5, 6, b"REPL")?;
    println!("FUSE API replace_range committed, tx_id={}", replace_tx);
    let copied = api.read_all("/api/copied.txt")?;
    println!("FUSE API copied file bytes: {}", String::from_utf8_lossy(&copied));
    let create_only = api.read_all("/api/create-only.txt")?;
    println!(
        "FUSE API create-only file bytes: {}",
        String::from_utf8_lossy(&create_only)
    );
    let empty_gated = api.read_all("/api/empty-gated.txt")?;
    println!(
        "FUSE API empty-gated file bytes: {}",
        String::from_utf8_lossy(&empty_gated)
    );
    let empty = api.read_all("/api/empty.txt")?;
    println!("FUSE API touched file bytes -> {}", empty.len());
    let copied_size = api.file_size("/api/copied.txt")?;
    println!("FUSE API file_size /api/copied.txt -> {}", copied_size);
    let copied_hash = api.file_hash("/api/copied.txt")?;
    println!("FUSE API file_hash /api/copied.txt -> {}", copied_hash);
    let exists_before_delete = api.exists("/api/renamed.txt")?;
    println!(
        "FUSE API exists /api/renamed.txt before delete -> {}",
        exists_before_delete
    );

    let removed = api.remove_path("/api")?;
    println!("FUSE API remove_path /api committed, removed={}", removed);

    match api.read("/api/renamed.txt", 0, 1) {
        Err(fuse::FuseError::NotFound) => {
            println!("FUSE API post-delete read: not found (expected)")
        }
        other => return Err(format!("expected NotFound after unlink, got {other:?}").into()),
    }

    println!("FUSE API demo finished successfully.");
    Ok(())
}

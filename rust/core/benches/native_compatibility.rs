use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use synor_core::state::stable_path::StableKey;
use synor_core::state_store::{Storage, StorageSettings};
use tempfile::TempDir;

fn benchmark_inactive_native_hooks(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("create benchmark runtime");
    let directory = TempDir::new().expect("create benchmark directory");
    let storage = runtime
        .block_on(Storage::new(&StorageSettings {
            db_path: directory.path().to_path_buf(),
            lmdb_max_dbs: 16,
            lmdb_map_size: 64 * 1024 * 1024,
        }))
        .expect("open benchmark storage");
    let store = runtime
        .block_on(storage.create_app_store("native-compatibility"))
        .expect("create benchmark app");
    let sequence = AtomicU64::new(1);
    let key = StableKey::Symbol("benchmark/compatibility".into());

    let mut group = criterion.benchmark_group("native_compatibility_commit");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("operational_write", |bencher| {
        bencher.to_async(&runtime).iter(|| {
            let storage = storage.clone();
            let store = store.clone();
            let key = key.clone();
            let next = sequence.fetch_add(1, Ordering::Relaxed);
            async move {
                storage
                    .run_txn(move |txn| {
                        let store = store.clone();
                        let key = key.clone();
                        Box::pin(async move { store.write_id_sequence(txn, &key, next).await })
                    })
                    .await
                    .expect("commit operational write");
            }
        });
    });

    group.bench_function("operational_write_with_inactive_native_hooks", |bencher| {
        bencher.to_async(&runtime).iter(|| {
            let storage = storage.clone();
            let store = store.clone();
            let key = key.clone();
            let next = sequence.fetch_add(1, Ordering::Relaxed);
            async move {
                storage
                    .run_txn(move |txn| {
                        let store = store.clone();
                        let key = key.clone();
                        Box::pin(async move {
                            store
                                .apply_precommit_native_effects_in_txn(txn, &[], &[])
                                .await?;
                            store.write_id_sequence(txn, &key, next).await?;
                            store.finalize_native_effects_in_txn(txn, &[]).await
                        })
                    })
                    .await
                    .expect("commit operational write with inactive native hooks");
            }
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_inactive_native_hooks);
criterion_main!(benches);

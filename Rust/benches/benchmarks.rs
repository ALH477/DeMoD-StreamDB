//! Benchmarks for StreamDB

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use streamdb::{StreamDb, Trie};
use uuid::Uuid;

fn bench_trie_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("trie_insert");
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let trie = Trie::new();
            let keys: Vec<Vec<u8>> = (0..size)
                .map(|i| format!("key:{:08}", i).into_bytes())
                .collect();
            let ids: Vec<Uuid> = (0..size).map(|_| Uuid::new_v4()).collect();
            
            b.iter(|| {
                let mut t = trie.clone();
                for (key, id) in keys.iter().zip(ids.iter()) {
                    t = t.insert(black_box(key), *id);
                }
                t
            });
        });
    }
    
    group.finish();
}

fn bench_trie_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("trie_get");
    
    for size in [100, 1000, 10000].iter() {
        // Pre-populate trie
        let mut trie = Trie::new();
        let keys: Vec<Vec<u8>> = (0..*size)
            .map(|i| format!("key:{:08}", i).into_bytes())
            .collect();
        
        for key in &keys {
            trie = trie.insert(key, Uuid::new_v4());
        }
        
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                for key in &keys {
                    black_box(trie.get(black_box(key)));
                }
            });
        });
    }
    
    group.finish();
}

fn bench_trie_suffix_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("trie_suffix_search");
    
    // Create trie with varied suffixes
    let mut trie = Trie::new();
    let suffixes = ["alice", "bob", "charlie", "david", "eve"];
    
    for i in 0..1000 {
        let suffix = suffixes[i % suffixes.len()];
        let key = format!("user:{}:{}", i, suffix).into_bytes();
        trie = trie.insert(&key, Uuid::new_v4());
    }
    
    group.bench_function("search_suffix", |b| {
        b.iter(|| {
            black_box(trie.suffix_search(black_box(b"alice")))
        });
    });
    
    group.finish();
}

fn bench_db_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("db_insert");
    
    let db = StreamDb::open_memory().unwrap();
    
    group.bench_function("100B_value", |b| {
        let value = vec![0u8; 100];
        let mut i = 0u64;
        
        b.iter(|| {
            let key = format!("key:{}", i).into_bytes();
            i += 1;
            black_box(db.insert(black_box(&key), black_box(&value)).unwrap())
        });
    });
    
    group.bench_function("1KB_value", |b| {
        let value = vec![0u8; 1024];
        let mut i = 0u64;
        
        b.iter(|| {
            let key = format!("key:{}", i).into_bytes();
            i += 1;
            black_box(db.insert(black_box(&key), black_box(&value)).unwrap())
        });
    });
    
    group.finish();
}

fn bench_db_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("db_get");
    
    let db = StreamDb::open_memory().unwrap();
    
    // Pre-populate
    let keys: Vec<Vec<u8>> = (0..1000)
        .map(|i| format!("key:{:08}", i).into_bytes())
        .collect();
    let value = vec![0u8; 100];
    
    for key in &keys {
        db.insert(key, &value).unwrap();
    }
    
    group.bench_function("cached", |b| {
        // First access caches
        for key in &keys {
            let _ = db.get(key);
        }
        
        b.iter(|| {
            for key in &keys {
                black_box(db.get(black_box(key)).unwrap());
            }
        });
    });
    
    group.bench_function("cold", |b| {
        b.iter(|| {
            db.clear_cache();
            for key in &keys {
                black_box(db.get(black_box(key)).unwrap());
            }
        });
    });
    
    group.finish();
}

fn bench_db_suffix_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("db_suffix_search");
    
    let db = StreamDb::open_memory().unwrap();
    
    // Pre-populate with pattern data
    let suffixes = ["alice", "bob", "charlie", "david", "eve"];
    for i in 0..1000 {
        let suffix = suffixes[i % suffixes.len()];
        let key = format!("user:{}:{}", i, suffix).into_bytes();
        db.insert(&key, b"value").unwrap();
    }
    
    group.bench_function("common_suffix", |b| {
        b.iter(|| {
            black_box(db.suffix_search(black_box(b"alice")).unwrap())
        });
    });
    
    group.bench_function("rare_suffix", |b| {
        b.iter(|| {
            black_box(db.suffix_search(black_box(b"nonexistent")).unwrap())
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_trie_insert,
    bench_trie_get,
    bench_trie_suffix_search,
    bench_db_insert,
    bench_db_get,
    bench_db_suffix_search,
);

criterion_main!(benches);

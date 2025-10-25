//! Tests for StreamDb.
//!
//! This module contains unit tests, integration tests, and benchmarks for StreamDb. It uses `proptest` for property-based testing of Trie operations and Criterion for performance benchmarks comparing to `sled`.

use super::*;
use test::Bencher;
use proptest::prelude::*;
use sled;

proptest! {
    #[test]
    fn prop_insert_delete_search(path in "\\PC*", id in any::<Uuid>()) {
        let mut backend = MemoryBackend::new("test.wal").unwrap();
        backend.bind_path_to_document(&path, id).unwrap();
        prop_assert_eq!(backend.get_document_id_by_path(&path).unwrap(), id);
        let prefix = &path[0..path.len()/2];
        let results = backend.search_paths(prefix).unwrap();
        prop_assert!(results.contains(&path));
        backend.delete_paths_for_document(id).unwrap();
        prop_assert!(backend.get_document_id_by_path(&path).is_err());
    }
}

#[test]
fn test_backend_integration() {
    let mut backend = MemoryBackend::new("test.wal").unwrap();
    let id = backend.write_document(&mut Cursor::new(b"data")).unwrap();
    backend.bind_path_to_document("path/to/doc", id).unwrap();
    assert_eq!(backend.get_document_id_by_path("path/to/doc").unwrap(), id);
    backend.delete_paths_for_document(id).unwrap();
    assert!(backend.get_document_id_by_path("path/to/doc").is_err());
}

#[bench]
fn bench_streamdb_insert(b: &mut Bencher) {
    let mut db = StreamDb::open_with_config("test.db", Config::default()).unwrap();
    b.iter(|| {
        let data = vec![0u8; 1024];
        let mut cursor = Cursor::new(&data);
        db.write_document("path/to/doc", &mut cursor).unwrap();
    });
}

#[bench]
fn bench_sled_insert(b: &mut Bencher) {
    let sled_db = sled::open("test_sled.db").unwrap();
    b.iter(|| {
        sled_db.insert(b"path/to/doc", vec![0u8; 1024]).unwrap();
    });
}

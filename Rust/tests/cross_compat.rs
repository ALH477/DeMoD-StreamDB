//! Cross-compat: read a database file written by the C implementation.
//!
//! Skipped unless STREAMDB_C_FIXTURE points at a C-written v3 file (the
//! `crosscheck` make target in C/ sets it up). The reverse direction —
//! C reading a Rust-written file — is covered by the C test
//! `cross_read_rust_fixture` against `format_fixture.rs`'s output.

#![cfg(feature = "persistence")]

use streamdb::{Config, StreamDb};

#[test]
fn rust_reads_c_written_database() {
    let path = match std::env::var("STREAMDB_C_FIXTURE") {
        Ok(p) if !p.is_empty() => p,
        _ => {
            eprintln!("skipped: STREAMDB_C_FIXTURE not set");
            return;
        }
    };

    let db = StreamDb::open(&path, Config::default()).unwrap();

    assert_eq!(
        db.get(b"cross:a").unwrap(),
        Some(b"alpha-from-c".to_vec()),
        "C-written string doc unreadable"
    );
    assert_eq!(
        db.get(b"cross:b").unwrap(),
        Some(b"beta-from-c".to_vec())
    );

    let png1: Vec<u8> = (0..256u32).map(|i| (i % 256) as u8).collect();
    assert_eq!(db.get(b"pic:one.png").unwrap(), Some(png1));

    let png2: Vec<u8> = (0..512u32).map(|i| ((i * 7) % 256) as u8).collect();
    assert_eq!(db.get(b"pic:two.png").unwrap(), Some(png2));

    let results = db.suffix_search(b".png").unwrap();
    assert_eq!(results.len(), 2, "suffix index lost across implementations");

    // The file must stay writable through the Rust side, too.
    db.insert(b"rust:added", b"post-cross").unwrap();
    db.flush().unwrap();
    drop(db);

    let db = StreamDb::open(&path, Config::default()).unwrap();
    assert_eq!(db.get(b"rust:added").unwrap(), Some(b"post-cross".to_vec()));
    assert_eq!(db.get(b"cross:a").unwrap(), Some(b"alpha-from-c".to_vec()));
}

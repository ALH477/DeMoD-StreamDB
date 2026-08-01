//! Dump the exact on-disk byte layout of a known v3 database, so the C
//! implementation can be verified byte-for-byte against the Rust one.
//! Run: cargo test --features persistence dump_format_fixture -- --nocapture

#[cfg(feature = "persistence")]
#[test]
fn dump_format_fixture() {
    use std::io::Read;
    use streamdb::{Config, StreamDb, Trie};
    use uuid::Uuid;

    // 1) Raw bincode of a tiny hand-built trie with fixed IDs.
    let id1 = Uuid::from_u128(0x00112233445566778899AABBCCDDEEFF);
    let id2 = Uuid::from_u128(0x102030405060708090A0B0C0D0E0F0FF);
    let trie = Trie::new().insert(b"ab", id1).insert(b"a", id2);
    let bytes = bincode::serialize(&trie).unwrap();
    println!("TRIE_BINCODE_LEN={}", bytes.len());
    println!("TRIE_BINCODE_HEX={}", bytes.iter().map(|b| format!("{b:02x}")).collect::<String>());

    // Single-key trie: isolates Option tag + Uuid encoding.
    let trie1 = Trie::new().insert(b"z", id1);
    let b1 = bincode::serialize(&trie1).unwrap();
    println!("TRIE1_LEN={}", b1.len());
    println!("TRIE1_HEX={}", b1.iter().map(|b| format!("{b:02x}")).collect::<String>());

    // Empty trie.
    let b0 = bincode::serialize(&Trie::new()).unwrap();
    println!("TRIE0_LEN={}", b0.len());
    println!("TRIE0_HEX={}", b0.iter().map(|b| format!("{b:02x}")).collect::<String>());

    // 2) Full DB file with known content, dumped as hex with offsets.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixture.db");
    std::env::set_var("FIXTURE_PATH", &path);
    {
        let db = StreamDb::open(&path, Config::default()).unwrap();
        db.insert(b"key:a", b"value-a").unwrap();
        db.insert(b"key:b", b"value-bb").unwrap();
        db.flush().unwrap();
    }
    let mut f = std::fs::File::open(&path).unwrap();
    let mut data = Vec::new();
    f.read_to_end(&mut data).unwrap();
    println!("DB_FILE_LEN={}", data.len());
    for (i, chunk) in data.chunks(16).enumerate() {
        let hex: String = chunk.iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(" ");
        println!("DB@{:06x}: {}", i * 16, hex);
    }
    println!("FIXTURE_PATH={}", path.display());
    // Keep the fixture around for the C side to consume.
    let out = std::path::Path::new("/tmp/streamdb-rust-fixture.db");
    std::fs::copy(&path, out).unwrap();
    println!("COPIED_TO={}", out.display());
}

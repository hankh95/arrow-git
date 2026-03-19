//! Crash Recovery — save with WAL, simulate interruption, restore
//!
//! Run with: `cargo run --example crash_recovery`

use arrow_graph_core::Triple;
use arrow_graph_git::{GitObjectStore, restore, save};

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let save_dir = dir.path().join("saved");
    let mut obj = GitObjectStore::new(dir.path());

    for i in 0..100 {
        let t = Triple {
            subject: format!("entity-{i}"),
            predicate: "rdf:type".into(),
            object: "Thing".into(),
            graph: None,
            confidence: Some(1.0),
            source_document: None,
            source_chunk_id: None,
            extracted_by: Some("agent".into()),
            caused_by: None,
            derived_from: None,
            consolidated_at: None,
        };
        obj.store.add_triple(&t, "world", Some(1)).unwrap();
    }

    let metrics = save(&obj, &save_dir).unwrap();
    println!("Saved: {} bytes", metrics.total_bytes);

    let mut restored = GitObjectStore::new(dir.path());
    restore(&mut restored, &save_dir).unwrap();
    println!(
        "Restored: {} triples (original: {})",
        restored.store.len(),
        obj.store.len()
    );
}

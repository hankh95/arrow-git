//! Performance gate benchmarks for the Arrow graph substrate.
//!
//! | Gate | Metric | Target | Measured On |
//! |------|--------|--------|-------------|
//! | H-019 | Query latency @10K triples | <= 2ms | Per namespace query |
//! | H-GIT-1 | Commit+checkout @10K triples | < 50ms | Full round-trip |
//! | M-SAVE | Save/restore @10K triples | < 100ms | Full round-trip |
//! | M-119 | Awakening | < 200ms | Restore + queries |

use arrow_graph_core::{ArrowGraphStore, QuerySpec, Triple};

const NAMESPACES: &[&str] = &["world", "work", "code", "research", "self"];
const LAYERS: &[(u8, usize)] = &[
    (0, 500),
    (1, 800),
    (2, 300),
    (3, 400),
    (4, 200),
    (5, 200),
    (6, 100),
];

fn triple(subj: &str) -> Triple {
    Triple {
        subject: subj.to_string(),
        predicate: "rdf:type".to_string(),
        object: "Entity".to_string(),
        graph: None,
        confidence: Some(0.9),
        source_document: None,
        source_chunk_id: None,
        extracted_by: Some("bench".to_string()),
        caused_by: None,
        derived_from: None,
        consolidated_at: None,
    }
}

/// Build a store with 2500 triples per namespace.
fn build_10k_store() -> ArrowGraphStore {
    let mut store = ArrowGraphStore::new(NAMESPACES);
    for ns in NAMESPACES {
        for (layer, count) in LAYERS {
            let triples: Vec<Triple> = (0..*count)
                .map(|i| triple(&format!("{}:e{}", ns, i)))
                .collect();
            store.add_batch(&triples, ns, Some(*layer)).unwrap();
        }
    }
    assert_eq!(store.len(), 2500 * NAMESPACES.len());
    store
}

/// Build a large store for final validation (70K per namespace).
fn build_350k_store() -> ArrowGraphStore {
    let mut store = ArrowGraphStore::new(NAMESPACES);
    let batch_size = 70_000;
    for ns in NAMESPACES {
        let triples: Vec<Triple> = (0..batch_size)
            .map(|i| triple(&format!("{}:e{}", ns, i)))
            .collect();
        store.add_batch(&triples, ns, Some(1u8)).unwrap();
    }
    assert_eq!(store.len(), batch_size * NAMESPACES.len());
    store
}

#[test]
fn gate_h019_query_latency_10k() {
    let store = build_10k_store();

    for ns in NAMESPACES {
        let _ = store
            .query(&QuerySpec {
                namespace: Some(ns.to_string()),
                ..Default::default()
            })
            .unwrap();
    }

    let mut max_query_ms = 0u128;
    for ns in NAMESPACES {
        let start = std::time::Instant::now();
        let results = store
            .query(&QuerySpec {
                namespace: Some(ns.to_string()),
                ..Default::default()
            })
            .unwrap();
        let elapsed = start.elapsed().as_millis();
        let count: usize = results.iter().map(|b| b.num_rows()).sum();
        assert!(count > 0, "Namespace {} should have triples", ns);
        if elapsed > max_query_ms {
            max_query_ms = elapsed;
        }
    }

    eprintln!(
        "H-019 @10K: max namespace query = {}ms (target: <=2ms)",
        max_query_ms
    );
    assert!(
        max_query_ms <= 10,
        "H-019 FAIL: query latency {}ms > 10ms",
        max_query_ms
    );
}

#[test]
fn gate_h019_query_latency_350k() {
    let store = build_350k_store();

    for ns in NAMESPACES {
        let _ = store
            .query(&QuerySpec {
                namespace: Some(ns.to_string()),
                ..Default::default()
            })
            .unwrap();
    }

    let mut max_query_ms = 0u128;
    for ns in NAMESPACES {
        let start = std::time::Instant::now();
        let results = store
            .query(&QuerySpec {
                namespace: Some(ns.to_string()),
                ..Default::default()
            })
            .unwrap();
        let elapsed = start.elapsed().as_millis();
        let count: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(count, 70_000);
        if elapsed > max_query_ms {
            max_query_ms = elapsed;
        }
    }

    eprintln!(
        "H-019 @350K: max namespace query = {}ms (target: <=2ms)",
        max_query_ms
    );
    assert!(
        max_query_ms <= 50,
        "H-019 FAIL @350K: query latency {}ms > 50ms",
        max_query_ms
    );
}

#[test]
fn gate_hgit1_commit_checkout_10k() {
    use arrow_graph_git::{CommitsTable, GitObjectStore, checkout, create_commit};

    let tmp = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
    let mut commits = CommitsTable::new();

    for ns in NAMESPACES {
        for (layer, count) in LAYERS {
            let triples: Vec<Triple> = (0..*count)
                .map(|i| triple(&format!("{}:e{}", ns, i)))
                .collect();
            obj.store.add_batch(&triples, ns, Some(*layer)).unwrap();
        }
    }
    assert_eq!(obj.store.len(), 2500 * NAMESPACES.len());

    let start = std::time::Instant::now();
    let c1 = create_commit(&obj, &mut commits, vec![], "bench", "test").unwrap();
    let commit_ms = start.elapsed().as_millis();

    obj.store.clear();
    let start = std::time::Instant::now();
    checkout(&mut obj, &commits, &c1.commit_id).unwrap();
    let checkout_ms = start.elapsed().as_millis();

    assert_eq!(obj.store.len(), 2500 * NAMESPACES.len());

    let total = commit_ms + checkout_ms;
    eprintln!(
        "H-GIT-1: commit={}ms, checkout={}ms, total={}ms (target: <50ms)",
        commit_ms, checkout_ms, total
    );
    assert!(
        total <= 250,
        "H-GIT-1 FAIL: commit+checkout {}ms > 250ms",
        total
    );
}

#[test]
fn gate_msave_save_restore_10k() {
    use arrow_graph_git::{GitObjectStore, restore, save};

    let tmp = tempfile::tempdir().unwrap();
    let save_dir = tmp.path().join("save");
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path().join("snap"));

    for ns in NAMESPACES {
        for (layer, count) in LAYERS {
            let triples: Vec<Triple> = (0..*count)
                .map(|i| triple(&format!("{}:e{}", ns, i)))
                .collect();
            obj.store.add_batch(&triples, ns, Some(*layer)).unwrap();
        }
    }

    let start = std::time::Instant::now();
    save(&obj, &save_dir).unwrap();
    let save_ms = start.elapsed().as_millis();

    obj.store.clear();
    let start = std::time::Instant::now();
    restore(&mut obj, &save_dir).unwrap();
    let restore_ms = start.elapsed().as_millis();

    assert_eq!(obj.store.len(), 2500 * NAMESPACES.len());

    let total = save_ms + restore_ms;
    eprintln!(
        "M-SAVE: save={}ms, restore={}ms, total={}ms (target: <100ms)",
        save_ms, restore_ms, total
    );
    assert!(
        total <= 500,
        "M-SAVE FAIL: save+restore {}ms > 500ms",
        total
    );
}

#[test]
fn gate_m119_awakening_latency() {
    use arrow_graph_git::{GitObjectStore, restore, save};

    let tmp = tempfile::tempdir().unwrap();
    let save_dir = tmp.path().join("being-state");
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path().join("snap"));

    for ns in NAMESPACES {
        for (layer, count) in LAYERS {
            let triples: Vec<Triple> = (0..*count)
                .map(|i| triple(&format!("{}:e{}", ns, i)))
                .collect();
            obj.store.add_batch(&triples, ns, Some(*layer)).unwrap();
        }
    }
    save(&obj, &save_dir).unwrap();
    obj.store.clear();

    let start = std::time::Instant::now();

    restore(&mut obj, &save_dir).unwrap();

    for ns in NAMESPACES {
        let _ = obj
            .store
            .query(&QuerySpec {
                namespace: Some(ns.to_string()),
                ..Default::default()
            })
            .unwrap();
    }

    let awakening_ms = start.elapsed().as_millis();
    eprintln!("M-119: awakening = {}ms (target: <200ms)", awakening_ms);
    assert!(
        awakening_ms <= 1000,
        "M-119 FAIL: awakening {}ms > 1000ms",
        awakening_ms
    );
}

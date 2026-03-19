//! End-to-end integration tests for arrow-git.
//!
//! Exercises the full stack:
//! - Create a graph with multiple namespaces
//! - Populate each with triples across layers
//! - Commit, branch, modify, merge
//! - Query across namespaces
//! - Parquet durability (save/restore)
//! - Provenance preservation through commit/checkout/merge cycles
//! - Performance benchmark: 10K triples full commit cycle

use arrow::array::Array;
use arrow_graph_core::{QuerySpec, Triple, col};
use arrow_graph_git::{
    CommitsTable, GitObjectStore, MergeResult, RefsTable, checkout, create_commit, merge,
};

const NAMESPACES: &[&str] = &["world", "work", "code", "research", "self"];
const LAYERS: &[(u8, &str); 7] = &[
    (0, "prose"),
    (1, "semantic"),
    (2, "reasoning"),
    (3, "experience"),
    (4, "journal"),
    (5, "procedural"),
    (6, "metacognitive"),
];

fn triple(subj: &str, pred: &str, obj: &str) -> Triple {
    Triple {
        subject: subj.to_string(),
        predicate: pred.to_string(),
        object: obj.to_string(),
        graph: None,
        confidence: Some(0.9),
        source_document: None,
        source_chunk_id: None,
        extracted_by: Some("integration-test".to_string()),
        caused_by: None,
        derived_from: None,
        consolidated_at: None,
    }
}

fn triple_with_provenance(
    subj: &str,
    pred: &str,
    obj: &str,
    caused_by: Option<&str>,
    derived_from: Option<&str>,
) -> Triple {
    Triple {
        caused_by: caused_by.map(String::from),
        derived_from: derived_from.map(String::from),
        consolidated_at: Some(chrono::Utc::now().timestamp_millis()),
        ..triple(subj, pred, obj)
    }
}

/// Populate store with triples across all namespaces x layers.
fn populate_full(store: &mut arrow_graph_core::ArrowGraphStore, per_partition: usize) {
    for ns in NAMESPACES {
        for (layer_id, layer_name) in LAYERS {
            let triples: Vec<Triple> = (0..per_partition)
                .map(|i| {
                    triple(
                        &format!("{}:{}-{}", ns, layer_name, i),
                        "rdf:type",
                        &format!("{}-Entity", layer_name),
                    )
                })
                .collect();
            store.add_batch(&triples, ns, Some(*layer_id)).unwrap();
        }
    }
}

#[test]
fn test_full_commit_branch_merge_cycle() {
    let tmp = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
    let mut commits = CommitsTable::new();

    // 1. Populate all namespaces x 7 layers
    populate_full(&mut obj.store, 10);
    assert_eq!(obj.store.len(), NAMESPACES.len() * 7 * 10);

    // 2. Initial commit
    let base_count = NAMESPACES.len() * 7 * 10;
    let c0 = create_commit(&obj, &mut commits, vec![], "initial triples", "test").unwrap();

    // 3. Branch A: add research triples
    obj.store
        .add_triple(
            &triple("research:finding-a", "validates", "H-019"),
            "research",
            Some(2u8),
        )
        .unwrap();
    let ca = create_commit(
        &obj,
        &mut commits,
        vec![c0.commit_id.clone()],
        "branch-a: research finding",
        "test",
    )
    .unwrap();

    // 4. Checkout base, Branch B: add work triples
    checkout(&mut obj, &commits, &c0.commit_id).unwrap();
    assert_eq!(obj.store.len(), base_count);

    obj.store
        .add_triple(
            &triple("work:task-b", "kb:status", "done"),
            "work",
            Some(5u8),
        )
        .unwrap();
    let cb = create_commit(
        &obj,
        &mut commits,
        vec![c0.commit_id.clone()],
        "branch-b: work task",
        "test",
    )
    .unwrap();

    // 5. Merge (non-conflicting — different namespaces)
    let result = merge(&mut obj, &mut commits, &ca.commit_id, &cb.commit_id, "test").unwrap();

    match result {
        MergeResult::Clean(mc) => {
            assert_eq!(mc.parent_ids.len(), 2);
            assert!(
                obj.store.len() >= base_count + 2,
                "Expected >= {} triples after merge, got {}",
                base_count + 2,
                obj.store.len()
            );
        }
        MergeResult::Conflict(c) => panic!("Expected clean merge, got {} conflicts", c.len()),
        MergeResult::NoCommonAncestor => panic!("Expected common ancestor"),
    }
}

#[test]
fn test_conflict_detection_same_namespace() {
    let tmp = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
    let mut commits = CommitsTable::new();

    obj.store
        .add_triple(&triple("entity", "rdf:type", "Base"), "world", Some(1u8))
        .unwrap();
    let c0 = create_commit(&obj, &mut commits, vec![], "base", "test").unwrap();

    obj.store
        .add_triple(&triple("entity", "status", "active"), "world", Some(1u8))
        .unwrap();
    let ca = create_commit(
        &obj,
        &mut commits,
        vec![c0.commit_id.clone()],
        "a: active",
        "test",
    )
    .unwrap();

    checkout(&mut obj, &commits, &c0.commit_id).unwrap();
    obj.store
        .add_triple(
            &triple("entity", "status", "deprecated"),
            "world",
            Some(1u8),
        )
        .unwrap();
    let cb = create_commit(
        &obj,
        &mut commits,
        vec![c0.commit_id.clone()],
        "b: deprecated",
        "test",
    )
    .unwrap();

    let result = merge(&mut obj, &mut commits, &ca.commit_id, &cb.commit_id, "test").unwrap();

    match result {
        MergeResult::Conflict(conflicts) => {
            assert_eq!(conflicts.len(), 1);
            assert_eq!(conflicts[0].subject, "entity");
            assert_eq!(conflicts[0].predicate, "status");
            assert_eq!(conflicts[0].object_a, "active");
            assert_eq!(conflicts[0].object_b, "deprecated");
        }
        _ => panic!("Expected conflict"),
    }
}

#[test]
fn test_provenance_survives_commit_checkout() {
    use arrow::array::{StringArray, TimestampMillisecondArray};

    let tmp = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
    let mut commits = CommitsTable::new();
    let now_ms = chrono::Utc::now().timestamp_millis();

    let t = Triple {
        subject: "s1".to_string(),
        predicate: "p1".to_string(),
        object: "o1".to_string(),
        graph: Some("prov-test".to_string()),
        confidence: Some(0.99),
        source_document: Some("source.md".to_string()),
        source_chunk_id: None,
        extracted_by: Some("test".to_string()),
        caused_by: Some("t-cause".to_string()),
        derived_from: Some("t-origin".to_string()),
        consolidated_at: Some(now_ms),
    };
    obj.store.add_triple(&t, "world", Some(2u8)).unwrap();

    let c1 = create_commit(&obj, &mut commits, vec![], "with provenance", "test").unwrap();

    obj.store.clear();
    assert_eq!(obj.store.len(), 0);
    checkout(&mut obj, &commits, &c1.commit_id).unwrap();
    assert_eq!(obj.store.len(), 1);

    let batches = obj
        .store
        .query(&QuerySpec {
            subject: Some("s1".to_string()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    let caused = batch
        .column(col::CAUSED_BY)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(caused.value(0), "t-cause");

    let derived = batch
        .column(col::DERIVED_FROM)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(derived.value(0), "t-origin");

    let consolidated = batch
        .column(col::CONSOLIDATED_AT)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert!(!consolidated.is_null(0));
    assert_eq!(consolidated.value(0), now_ms);

    let source = batch
        .column(col::SOURCE_DOCUMENT)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(source.value(0), "source.md");
}

#[test]
fn test_provenance_survives_merge() {
    let tmp = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
    let mut commits = CommitsTable::new();

    obj.store
        .add_triple(&triple("base", "rdf:type", "Base"), "world", Some(1u8))
        .unwrap();
    let c0 = create_commit(&obj, &mut commits, vec![], "base", "test").unwrap();

    let t_a = triple_with_provenance(
        "research:finding",
        "validates",
        "H-019",
        Some("t-root"),
        None,
    );
    obj.store.add_triple(&t_a, "research", Some(2u8)).unwrap();
    let ca = create_commit(
        &obj,
        &mut commits,
        vec![c0.commit_id.clone()],
        "a: finding",
        "test",
    )
    .unwrap();

    checkout(&mut obj, &commits, &c0.commit_id).unwrap();
    let t_b = triple_with_provenance("work:task", "status", "done", None, Some("t-derived"));
    obj.store.add_triple(&t_b, "work", Some(5u8)).unwrap();
    let cb = create_commit(
        &obj,
        &mut commits,
        vec![c0.commit_id.clone()],
        "b: task",
        "test",
    )
    .unwrap();

    let result = merge(&mut obj, &mut commits, &ca.commit_id, &cb.commit_id, "test").unwrap();
    assert!(
        matches!(result, MergeResult::Clean(_)),
        "Expected clean merge"
    );

    let findings = obj
        .store
        .query(&QuerySpec {
            subject: Some("research:finding".to_string()),
            ..Default::default()
        })
        .unwrap();
    assert!(
        !findings.is_empty(),
        "Branch A's triple should exist after merge"
    );
    let batch = &findings[0];
    use arrow::array::StringArray;
    let caused = batch
        .column(col::CAUSED_BY)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(caused.value(0), "t-root", "caused_by should survive merge");
}

#[test]
fn test_causal_chain_survives_commit_checkout() {
    let tmp = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
    let mut commits = CommitsTable::new();

    let id0 = obj
        .store
        .add_triple(&triple("root", "rdf:type", "Origin"), "world", Some(0u8))
        .unwrap();

    let t1 = Triple {
        caused_by: Some(id0.clone()),
        ..triple("derived", "rdf:type", "Conclusion")
    };
    let id1 = obj.store.add_triple(&t1, "research", Some(2u8)).unwrap();

    let c1 = create_commit(&obj, &mut commits, vec![], "causal chain", "test").unwrap();

    obj.store.clear();
    checkout(&mut obj, &commits, &c1.commit_id).unwrap();

    let chain = obj.store.causal_chain(&id1);
    assert_eq!(
        chain.len(),
        2,
        "Causal chain should survive commit/checkout"
    );
    assert_eq!(chain[0].triple_id, id1);
    assert_eq!(chain[0].caused_by, Some(id0.clone()));
    assert_eq!(chain[1].triple_id, id0);
}

#[test]
fn test_save_restore_with_provenance() {
    use arrow_graph_git::{restore, save};

    let tmp = tempfile::tempdir().unwrap();
    let save_dir = tmp.path().join("savepoint");
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path().join("snapshots"));
    let now_ms = chrono::Utc::now().timestamp_millis();

    let t = Triple {
        subject: "s1".to_string(),
        predicate: "p1".to_string(),
        object: "o1".to_string(),
        graph: None,
        confidence: Some(0.95),
        source_document: Some("doc.md".to_string()),
        source_chunk_id: None,
        extracted_by: Some("test".to_string()),
        caused_by: Some("cause-id".to_string()),
        derived_from: Some("derive-id".to_string()),
        consolidated_at: Some(now_ms),
    };
    obj.store.add_triple(&t, "world", Some(1u8)).unwrap();

    save(&obj, &save_dir).unwrap();

    obj.store.clear();
    restore(&mut obj, &save_dir).unwrap();

    assert_eq!(obj.store.len(), 1);

    let batches = obj
        .store
        .query(&QuerySpec {
            subject: Some("s1".to_string()),
            ..Default::default()
        })
        .unwrap();
    let batch = &batches[0];
    use arrow::array::StringArray;
    let caused = batch
        .column(col::CAUSED_BY)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(caused.value(0), "cause-id");
}

#[test]
fn test_refs_branch_management() {
    let tmp = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
    let mut commits = CommitsTable::new();
    let mut refs = RefsTable::new();

    obj.store
        .add_triple(&triple("s1", "rdf:type", "Thing"), "world", Some(1u8))
        .unwrap();
    let c0 = create_commit(&obj, &mut commits, vec![], "init", "test").unwrap();
    refs.init_main(&c0.commit_id);

    refs.create_branch("feature", &c0.commit_id).unwrap();
    refs.switch_head("feature").unwrap();
    assert_eq!(refs.head().unwrap().ref_name, "feature");
    assert_eq!(refs.resolve("feature"), Some(c0.commit_id.as_str()));

    obj.store
        .add_triple(&triple("s2", "rdf:type", "Feature"), "world", Some(1u8))
        .unwrap();
    let c1 = create_commit(
        &obj,
        &mut commits,
        vec![c0.commit_id.clone()],
        "feature work",
        "test",
    )
    .unwrap();
    refs.update_ref("feature", &c1.commit_id).unwrap();

    refs.switch_head("main").unwrap();
    assert_eq!(refs.head().unwrap().ref_name, "main");
    assert_eq!(refs.resolve("main"), Some(c0.commit_id.as_str()));

    assert_eq!(refs.branches().len(), 2);
}

#[test]
fn test_full_save_restore_with_commits_and_refs() {
    use arrow_graph_git::{restore_full, save_full};

    let tmp = tempfile::tempdir().unwrap();
    let save_dir = tmp.path().join("full-save");
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path().join("snapshots"));
    let mut commits = CommitsTable::new();
    let mut refs = RefsTable::new();

    populate_full(&mut obj.store, 5);
    let c0 = create_commit(&obj, &mut commits, vec![], "full state", "test").unwrap();
    refs.init_main(&c0.commit_id);
    refs.create_branch("dev", &c0.commit_id).unwrap();

    save_full(&obj, Some(&commits), Some(&refs), &save_dir).unwrap();

    let mut obj2 = GitObjectStore::with_snapshot_dir(tmp.path().join("snapshots2"));
    let (rc, rr) = restore_full(&mut obj2, &save_dir).unwrap();

    assert_eq!(obj2.store.len(), NAMESPACES.len() * 7 * 5);
    assert_eq!(rc.unwrap().len(), 1);
    assert_eq!(rr.unwrap().branches().len(), 2);
}

#[test]
fn test_10k_triples_full_commit_cycle_benchmark() {
    let tmp = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
    let mut commits = CommitsTable::new();

    let layers: &[(u8, usize)] = &[
        (0, 500),
        (1, 800),
        (2, 300),
        (3, 400),
        (4, 200),
        (5, 200),
        (6, 100),
    ];

    for ns in NAMESPACES {
        for (layer, count) in layers {
            let triples: Vec<Triple> = (0..*count)
                .map(|i| triple(&format!("{}:e{}", ns, i), "rdf:type", "Entity"))
                .collect();
            obj.store.add_batch(&triples, ns, Some(*layer)).unwrap();
        }
    }
    assert_eq!(obj.store.len(), 2500 * NAMESPACES.len());

    let start = std::time::Instant::now();
    let c1 = create_commit(&obj, &mut commits, vec![], "10K commit", "test").unwrap();
    let commit_ms = start.elapsed().as_millis();

    obj.store.clear();
    let start = std::time::Instant::now();
    checkout(&mut obj, &commits, &c1.commit_id).unwrap();
    let checkout_ms = start.elapsed().as_millis();

    assert_eq!(obj.store.len(), 2500 * NAMESPACES.len());

    let start = std::time::Instant::now();
    for ns in NAMESPACES {
        let _ = obj
            .store
            .query(&QuerySpec {
                namespace: Some(ns.to_string()),
                ..Default::default()
            })
            .unwrap();
    }
    let query_ms = start.elapsed().as_millis();

    let total = commit_ms + checkout_ms;
    eprintln!(
        "10K benchmark — commit: {}ms, checkout: {}ms, total: {}ms, queries: {}ms",
        commit_ms, checkout_ms, total, query_ms
    );

    assert!(
        total < 500,
        "10K commit+checkout took {total}ms — target <50ms (CI margin 500ms)"
    );

    assert!(
        query_ms < 50,
        "namespace queries at 10K took {query_ms}ms — target <10ms"
    );
}

//! Stress tests for arrow-git.
//!
//! Push beyond normal usage to find edge cases and verify
//! graceful degradation at scale.

use arrow_graph_core::{QuerySpec, Triple};
use arrow_graph_git::{
    CommitsTable, GitObjectStore, checkout, create_commit, diff, merge, restore, save,
};

const NAMESPACES: &[&str] = &["world", "work", "code", "research", "self"];

fn triple(subj: &str, pred: &str, obj: &str) -> Triple {
    Triple {
        subject: subj.to_string(),
        predicate: pred.to_string(),
        object: obj.to_string(),
        graph: None,
        confidence: Some(0.9),
        source_document: None,
        source_chunk_id: None,
        extracted_by: Some("stress".to_string()),
        caused_by: None,
        derived_from: None,
        consolidated_at: None,
    }
}

fn simple_triple(subj: &str) -> Triple {
    triple(subj, "rdf:type", "Entity")
}

/// Populate a store with `n` triples per namespace.
fn populate(obj: &mut GitObjectStore, n: usize) {
    for ns in NAMESPACES {
        let triples: Vec<Triple> = (0..n)
            .map(|i| simple_triple(&format!("{}:e{}", ns, i)))
            .collect();
        obj.store.add_batch(&triples, ns, Some(1u8)).unwrap();
    }
}

#[test]
fn stress_100k_commit_checkout() {
    let tmp = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
    let mut commits = CommitsTable::new();

    let per_ns = 20_000;
    populate(&mut obj, per_ns);
    let expected = per_ns * NAMESPACES.len();
    assert_eq!(obj.store.len(), expected);

    let start = std::time::Instant::now();
    let c1 = create_commit(&obj, &mut commits, vec![], "large commit", "test").unwrap();
    let commit_ms = start.elapsed().as_millis();

    obj.store.clear();
    assert_eq!(obj.store.len(), 0);

    let start = std::time::Instant::now();
    checkout(&mut obj, &commits, &c1.commit_id).unwrap();
    let checkout_ms = start.elapsed().as_millis();

    assert_eq!(obj.store.len(), expected);
    eprintln!(
        "{}K: commit={}ms, checkout={}ms",
        expected / 1000,
        commit_ms,
        checkout_ms
    );
}

#[test]
#[ignore]
fn stress_500k_save_restore() {
    let tmp = tempfile::tempdir().unwrap();
    let save_dir = tmp.path().join("save");
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path().join("snap"));

    let per_ns = 100_000;
    populate(&mut obj, per_ns);
    let expected = per_ns * NAMESPACES.len();
    assert_eq!(obj.store.len(), expected);

    let start = std::time::Instant::now();
    save(&obj, &save_dir).unwrap();
    let save_ms = start.elapsed().as_millis();

    obj.store.clear();

    let start = std::time::Instant::now();
    restore(&mut obj, &save_dir).unwrap();
    let restore_ms = start.elapsed().as_millis();

    assert_eq!(obj.store.len(), expected);
    eprintln!(
        "{}K: save={}ms, restore={}ms",
        expected / 1000,
        save_ms,
        restore_ms
    );
}

#[test]
fn stress_100_sequential_commits() {
    let tmp = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
    let mut commits = CommitsTable::new();

    let base_triples: Vec<Triple> = (0..100)
        .map(|i| simple_triple(&format!("base:e{}", i)))
        .collect();
    obj.store
        .add_batch(&base_triples, "world", Some(1u8))
        .unwrap();

    let mut parent_ids: Vec<String> = vec![];

    for i in 0..100 {
        obj.store
            .add_triple(
                &simple_triple(&format!("commit{}:entity", i)),
                "work",
                Some(3u8),
            )
            .unwrap();
        let c = create_commit(
            &obj,
            &mut commits,
            parent_ids,
            &format!("commit {}", i),
            "test",
        )
        .unwrap();
        parent_ids = vec![c.commit_id.clone()];
    }

    let log = arrow_graph_git::log(&commits, &parent_ids[0], 200);
    assert_eq!(log.len(), 100, "Should have 100 commits in history");

    obj.store.clear();
    checkout(&mut obj, &commits, &parent_ids[0]).unwrap();
    assert_eq!(obj.store.len(), 200);

    let first = &log[log.len() - 1];
    obj.store.clear();
    checkout(&mut obj, &commits, &first.commit_id).unwrap();
    assert_eq!(obj.store.len(), 101);
}

#[test]
fn stress_branch_merge_cascade() {
    let tmp = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
    let mut commits = CommitsTable::new();
    let mut refs = arrow_graph_git::RefsTable::new();

    let base_triples: Vec<Triple> = (0..100)
        .map(|i| simple_triple(&format!("base:e{}", i)))
        .collect();
    obj.store
        .add_batch(&base_triples, "world", Some(1u8))
        .unwrap();
    let base = create_commit(&obj, &mut commits, vec![], "base", "test").unwrap();
    refs.init_main(&base.commit_id);

    let mut branch_heads = Vec::new();
    for b in 0..10 {
        obj.store.clear();
        checkout(&mut obj, &commits, &base.commit_id).unwrap();

        for i in 0..50 {
            obj.store
                .add_triple(
                    &triple(
                        &format!("branch{}:e{}", b, i),
                        "rdf:type",
                        &format!("Branch{}Entity", b),
                    ),
                    "work",
                    Some(3u8),
                )
                .unwrap();
        }

        let bc = create_commit(
            &obj,
            &mut commits,
            vec![base.commit_id.clone()],
            &format!("branch-{}", b),
            "test",
        )
        .unwrap();
        refs.create_branch(&format!("branch-{}", b), &bc.commit_id)
            .unwrap();
        branch_heads.push(bc.commit_id);
    }

    let mut current_main = base.commit_id.clone();
    for (b, head) in branch_heads.iter().enumerate() {
        let result = merge(&mut obj, &mut commits, &current_main, head, "test").unwrap();
        match result {
            arrow_graph_git::MergeResult::Clean(c) => {
                current_main = c.commit_id;
            }
            arrow_graph_git::MergeResult::Conflict(conflicts) => {
                panic!(
                    "Unexpected conflict merging branch {}: {:?}",
                    b,
                    conflicts.iter().map(|c| &c.subject).collect::<Vec<_>>()
                );
            }
            arrow_graph_git::MergeResult::NoCommonAncestor => {
                panic!("No common ancestor for branch {}", b);
            }
        }
    }

    obj.store.clear();
    checkout(&mut obj, &commits, &current_main).unwrap();
    assert_eq!(obj.store.len(), 600);
}

#[test]
fn stress_rapid_save_restore_cycles() {
    let tmp = tempfile::tempdir().unwrap();
    let save_dir = tmp.path().join("save");
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path().join("snap"));

    populate(&mut obj, 250);
    let initial_count = obj.store.len();
    assert_eq!(initial_count, 250 * NAMESPACES.len());

    for i in 0..100 {
        save(&obj, &save_dir).unwrap();
        obj.store.clear();
        restore(&mut obj, &save_dir).unwrap();
        assert_eq!(
            obj.store.len(),
            initial_count,
            "Data loss at cycle {}: expected {}, got {}",
            i,
            initial_count,
            obj.store.len()
        );
    }
}

#[test]
fn stress_many_conflicts() {
    let tmp = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
    let mut commits = CommitsTable::new();

    let base_triples: Vec<Triple> = (0..100)
        .map(|i| simple_triple(&format!("entity:{}", i)))
        .collect();
    obj.store
        .add_batch(&base_triples, "world", Some(1u8))
        .unwrap();
    let base = create_commit(&obj, &mut commits, vec![], "base", "test").unwrap();

    obj.store.clear();
    checkout(&mut obj, &commits, &base.commit_id).unwrap();
    for i in 0..500 {
        obj.store
            .add_triple(
                &triple(&format!("conflict:e{}", i), "value", "branch_a_value"),
                "work",
                Some(3u8),
            )
            .unwrap();
    }
    let ca = create_commit(
        &obj,
        &mut commits,
        vec![base.commit_id.clone()],
        "branch-a",
        "test",
    )
    .unwrap();

    obj.store.clear();
    checkout(&mut obj, &commits, &base.commit_id).unwrap();
    for i in 0..500 {
        obj.store
            .add_triple(
                &triple(&format!("conflict:e{}", i), "value", "branch_b_value"),
                "work",
                Some(3u8),
            )
            .unwrap();
    }
    let cb = create_commit(
        &obj,
        &mut commits,
        vec![base.commit_id.clone()],
        "branch-b",
        "test",
    )
    .unwrap();

    let result = merge(&mut obj, &mut commits, &ca.commit_id, &cb.commit_id, "test").unwrap();
    match result {
        arrow_graph_git::MergeResult::Conflict(conflicts) => {
            assert_eq!(
                conflicts.len(),
                500,
                "Expected 500 conflicts, got {}",
                conflicts.len()
            );
            for c in &conflicts {
                assert_eq!(c.object_a, "branch_a_value");
                assert_eq!(c.object_b, "branch_b_value");
            }
        }
        other => panic!("Expected Conflict, got {:?}", other),
    }
}

#[test]
fn stress_namespace_integrity_under_churn() {
    let tmp = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
    let mut commits = CommitsTable::new();

    let mut parent_ids: Vec<String> = vec![];

    for round in 0..20 {
        for ns in NAMESPACES {
            let triples: Vec<Triple> = (0..50)
                .map(|i| simple_triple(&format!("r{}:{}:e{}", round, ns, i)))
                .collect();
            obj.store.add_batch(&triples, ns, Some(1u8)).unwrap();
        }

        let c = create_commit(
            &obj,
            &mut commits,
            parent_ids,
            &format!("round {}", round),
            "test",
        )
        .unwrap();
        parent_ids = vec![c.commit_id.clone()];
    }

    let expected = 20 * 50 * NAMESPACES.len();
    obj.store.clear();
    checkout(&mut obj, &commits, &parent_ids[0]).unwrap();
    assert_eq!(obj.store.len(), expected);

    for ns in NAMESPACES {
        let results = obj
            .store
            .query(&QuerySpec {
                namespace: Some(ns.to_string()),
                ..Default::default()
            })
            .unwrap();
        let count: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            count,
            20 * 50,
            "Namespace {} should have {} triples, got {}",
            ns,
            20 * 50,
            count
        );
    }
}

#[test]
fn stress_large_diff() {
    let tmp = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
    let mut commits = CommitsTable::new();

    let triples1: Vec<Triple> = (0..1000)
        .map(|i| simple_triple(&format!("entity:{}", i)))
        .collect();
    obj.store.add_batch(&triples1, "world", Some(1u8)).unwrap();
    let c1 = create_commit(&obj, &mut commits, vec![], "v1", "test").unwrap();

    let triples2: Vec<Triple> = (1000..1500)
        .map(|i| simple_triple(&format!("entity:{}", i)))
        .collect();
    obj.store.add_batch(&triples2, "world", Some(1u8)).unwrap();
    let c2 = create_commit(&obj, &mut commits, vec![c1.commit_id.clone()], "v2", "test").unwrap();

    let d = diff(&mut obj, &commits, &c1.commit_id, &c2.commit_id).unwrap();
    assert_eq!(d.added.len(), 500, "Should show 500 additions");
    assert_eq!(d.removed.len(), 0, "Should show 0 removals");
}

#[test]
fn stress_full_state_persistence_after_churn() {
    let tmp = tempfile::tempdir().unwrap();
    let save_dir = tmp.path().join("full-save");
    let mut obj = GitObjectStore::with_snapshot_dir(tmp.path().join("snap"));
    let mut commits = CommitsTable::new();
    let mut refs = arrow_graph_git::RefsTable::new();

    populate(&mut obj, 500);
    let c1 = create_commit(&obj, &mut commits, vec![], "initial", "test").unwrap();
    refs.init_main(&c1.commit_id);

    let more: Vec<Triple> = (0..500)
        .map(|i| simple_triple(&format!("extra:e{}", i)))
        .collect();
    obj.store.add_batch(&more, "work", Some(3u8)).unwrap();
    let c2 = create_commit(
        &obj,
        &mut commits,
        vec![c1.commit_id.clone()],
        "more data",
        "test",
    )
    .unwrap();
    refs.update_ref("main", &c2.commit_id).unwrap();
    refs.create_branch("feature", &c2.commit_id).unwrap();

    let original_len = obj.store.len();
    let original_commit_count = commits.all().len();

    arrow_graph_git::save_full(&obj, Some(&commits), Some(&refs), &save_dir).unwrap();

    obj.store.clear();

    let (restored_commits, restored_refs) =
        arrow_graph_git::restore_full(&mut obj, &save_dir).unwrap();

    assert_eq!(obj.store.len(), original_len);

    let rc = restored_commits.expect("commits should be restored");
    assert_eq!(rc.all().len(), original_commit_count);

    let rr = restored_refs.expect("refs should be restored");
    assert_eq!(
        rr.get("main").map(|r| r.commit_id.as_str()),
        Some(c2.commit_id.as_str())
    );
    assert_eq!(
        rr.get("feature").map(|r| r.commit_id.as_str()),
        Some(c2.commit_id.as_str())
    );
}

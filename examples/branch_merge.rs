//! Branch & Merge — create branches, diverge, merge with conflict resolution
//!
//! Run with: `cargo run --example branch_merge`

use arrow_graph_core::Triple;
use arrow_graph_git::{
    CommitsTable, GitObjectStore, MergeResult, MergeStrategy, checkout, create_commit,
    merge_with_strategy,
};

fn t(s: &str, p: &str, o: &str) -> Triple {
    Triple {
        subject: s.into(),
        predicate: p.into(),
        object: o.into(),
        graph: None,
        confidence: Some(1.0),
        source_document: None,
        source_chunk_id: None,
        extracted_by: None,
        caused_by: None,
        derived_from: None,
        consolidated_at: None,
    }
}

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(dir.path());
    let mut commits = CommitsTable::new();

    obj.store
        .add_triple(&t("Alice", "knows", "Bob"), "world", Some(1))
        .unwrap();
    let base = create_commit(&obj, &mut commits, vec![], "base", "agent").unwrap();

    obj.store
        .add_triple(&t("Alice", "likes", "Carol"), "world", Some(1))
        .unwrap();
    let a = create_commit(
        &obj,
        &mut commits,
        vec![base.commit_id.clone()],
        "branch-a",
        "agent",
    )
    .unwrap();

    checkout(&mut obj, &commits, &base.commit_id).unwrap();
    obj.store
        .add_triple(&t("Bob", "likes", "Dave"), "world", Some(1))
        .unwrap();
    let b = create_commit(
        &obj,
        &mut commits,
        vec![base.commit_id.clone()],
        "branch-b",
        "agent",
    )
    .unwrap();

    let result = merge_with_strategy(
        &mut obj,
        &mut commits,
        &a.commit_id,
        &b.commit_id,
        "agent",
        &MergeStrategy::Ours,
    )
    .unwrap();

    match result {
        MergeResult::Clean(commit) => println!("Clean merge: {}", commit.commit_id),
        MergeResult::Conflict(conflicts) => {
            println!("Conflicts: {}", conflicts.len())
        }
        MergeResult::NoCommonAncestor => println!("No common ancestor"),
    }
    println!("Store now has {} triples", obj.store.len());
}

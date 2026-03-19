//! Branch & Merge — create branches, diverge, merge with conflict resolution
//!
//! Run with: `cargo run --example branch_merge`

use arrow_graph_core::Triple;
use arrow_graph_git::{
    CommitsTable, GitObjectStore, MergeStrategy, checkout, create_commit, merge_with_strategy,
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
    let mut obj = GitObjectStore::new(dir.path());
    let mut commits = CommitsTable::new();

    obj.store
        .add_triple(&t("Alice", "knows", "Bob"), "world", Some(1))
        .unwrap();
    let base = create_commit(&obj, &mut commits, "base", None, "agent").unwrap();

    obj.store
        .add_triple(&t("Alice", "likes", "Carol"), "world", Some(1))
        .unwrap();
    let a = create_commit(
        &obj,
        &mut commits,
        "branch-a",
        Some(&base.commit_id),
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
        "branch-b",
        Some(&base.commit_id),
        "agent",
    )
    .unwrap();

    let result = merge_with_strategy(
        &mut obj,
        &commits,
        &a.commit_id,
        &b.commit_id,
        MergeStrategy::Ours,
    )
    .unwrap();
    println!(
        "Merge: +{} conflicts={}",
        result.additions,
        result.conflicts.len()
    );
}

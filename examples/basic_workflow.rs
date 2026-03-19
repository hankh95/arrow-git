//! Basic Workflow — add triples, commit, checkout, diff
//!
//! Run with: `cargo run --example basic_workflow`

use arrow_graph_core::Triple;
use arrow_graph_git::{CommitsTable, GitObjectStore, checkout, create_commit, diff};

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut obj = GitObjectStore::with_snapshot_dir(dir.path());
    let mut commits = CommitsTable::new();

    let t1 = Triple {
        subject: "Alice".into(),
        predicate: "knows".into(),
        object: "Bob".into(),
        graph: None,
        confidence: Some(0.9),
        source_document: None,
        source_chunk_id: None,
        extracted_by: Some("agent".into()),
        caused_by: None,
        derived_from: None,
        consolidated_at: None,
    };
    obj.store.add_triple(&t1, "world", Some(1)).unwrap();

    let c1 = create_commit(&obj, &mut commits, vec![], "Initial", "agent").unwrap();
    println!("Commit 1: {}", c1.commit_id);

    let t2 = Triple {
        subject: "Bob".into(),
        predicate: "knows".into(),
        object: "Carol".into(),
        graph: None,
        confidence: Some(0.8),
        source_document: None,
        source_chunk_id: None,
        extracted_by: Some("agent".into()),
        caused_by: None,
        derived_from: None,
        consolidated_at: None,
    };
    obj.store.add_triple(&t2, "world", Some(1)).unwrap();

    let c2 = create_commit(
        &obj,
        &mut commits,
        vec![c1.commit_id.clone()],
        "Add Bob→Carol",
        "agent",
    )
    .unwrap();

    let d = diff(&mut obj, &commits, &c1.commit_id, &c2.commit_id).unwrap();
    println!("Diff: +{} -{}", d.added.len(), d.removed.len());

    checkout(&mut obj, &commits, &c1.commit_id).unwrap();
    println!("Checked out c1: {} triples", obj.store.len());

    let history = arrow_graph_git::log(&commits, &c2.commit_id, 0);
    println!("History: {} commits", history.len());
}

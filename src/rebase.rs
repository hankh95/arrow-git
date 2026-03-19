//! Rebase — replay a sequence of commits onto a new base.
//!
//! Walks from `start` to `end` following parent pointers, collects commits
//! in order, then cherry-picks each onto the new base sequentially.

use crate::cherry_pick::{CherryPickError, cherry_pick};
use crate::commit::CommitsTable;
use crate::history::log;
use crate::object_store::GitObjectStore;

/// Errors from rebase operations.
#[derive(Debug, thiserror::Error)]
pub enum RebaseError {
    #[error("Cherry-pick failed: {0}")]
    CherryPick(#[from] CherryPickError),

    #[error("Commit not found: {0}")]
    CommitNotFound(String),

    #[error("Nothing to rebase (start equals onto)")]
    NothingToRebase,
}

/// Result of a rebase operation.
pub struct RebaseResult {
    /// The new HEAD commit ID after rebase.
    pub new_head: String,
    /// Number of commits replayed.
    pub replayed: usize,
}

/// Rebase commits from `start_commit_id` (exclusive) through `end_commit_id`
/// (inclusive) onto `onto_commit_id`.
///
/// Walks the commit chain from end back to start, collects commits in
/// chronological order, then cherry-picks each onto the new base.
///
/// Returns the new HEAD after all commits are replayed.
pub fn rebase(
    obj_store: &mut GitObjectStore,
    commits_table: &mut CommitsTable,
    start_commit_id: &str, // The old base (exclusive — commits after this)
    end_commit_id: &str,   // The tip to rebase (inclusive)
    onto_commit_id: &str,  // The new base to replay onto
    author: &str,
) -> Result<RebaseResult, RebaseError> {
    if start_commit_id == onto_commit_id {
        return Err(RebaseError::NothingToRebase);
    }

    let all = log(commits_table, end_commit_id, 0);
    let mut to_replay: Vec<String> = Vec::new();
    for commit in &all {
        if commit.commit_id == start_commit_id {
            break;
        }
        to_replay.push(commit.commit_id.clone());
    }

    // log() returns newest-first; we need oldest-first for replay
    to_replay.reverse();

    if to_replay.is_empty() {
        return Err(RebaseError::NothingToRebase);
    }

    // Cherry-pick each commit onto the new base
    let mut current_head = onto_commit_id.to_string();
    let mut replayed = 0;

    for commit_id in &to_replay {
        let new_id = cherry_pick(obj_store, commits_table, commit_id, &current_head, author)?;
        current_head = new_id;
        replayed += 1;
    }

    Ok(RebaseResult {
        new_head: current_head,
        replayed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommitsTable, GitObjectStore, checkout, create_commit};
    use arrow_graph_core::Triple;

    fn make_triple(s: &str, p: &str, o: &str) -> Triple {
        Triple {
            subject: s.to_string(),
            predicate: p.to_string(),
            object: o.to_string(),
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

    #[test]
    fn test_rebase_linear_chain() {
        let tmp = tempfile::tempdir().unwrap();
        let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
        let mut commits = CommitsTable::new();

        obj.store
            .add_triple(&make_triple("a", "r", "1"), "world", Some(1u8))
            .unwrap();
        let base = create_commit(&obj, &mut commits, vec![], "base", "test").unwrap();

        obj.store
            .add_triple(&make_triple("b", "r", "2"), "world", Some(1u8))
            .unwrap();
        let c1 = create_commit(
            &obj,
            &mut commits,
            vec![base.commit_id.clone()],
            "c1",
            "test",
        )
        .unwrap();

        obj.store
            .add_triple(&make_triple("c", "r", "3"), "world", Some(1u8))
            .unwrap();
        let c2 =
            create_commit(&obj, &mut commits, vec![c1.commit_id.clone()], "c2", "test").unwrap();

        checkout(&mut obj, &commits, &base.commit_id).unwrap();
        obj.store
            .add_triple(&make_triple("d", "r", "4"), "world", Some(1u8))
            .unwrap();
        let new_base = create_commit(
            &obj,
            &mut commits,
            vec![base.commit_id.clone()],
            "new_base",
            "test",
        )
        .unwrap();

        let result = rebase(
            &mut obj,
            &mut commits,
            &base.commit_id,
            &c2.commit_id,
            &new_base.commit_id,
            "test",
        )
        .unwrap();

        assert_eq!(result.replayed, 2);
        assert_ne!(result.new_head, c2.commit_id);
    }

    #[test]
    fn test_rebase_nothing_to_rebase() {
        let tmp = tempfile::tempdir().unwrap();
        let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
        let mut commits = CommitsTable::new();

        obj.store
            .add_triple(&make_triple("a", "r", "1"), "world", Some(1u8))
            .unwrap();
        let base = create_commit(&obj, &mut commits, vec![], "base", "test").unwrap();

        let result = rebase(
            &mut obj,
            &mut commits,
            &base.commit_id,
            &base.commit_id,
            &base.commit_id,
            "test",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_rebase_single_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
        let mut commits = CommitsTable::new();

        obj.store
            .add_triple(&make_triple("a", "r", "1"), "world", Some(1u8))
            .unwrap();
        let base = create_commit(&obj, &mut commits, vec![], "base", "test").unwrap();

        obj.store
            .add_triple(&make_triple("b", "r", "2"), "world", Some(1u8))
            .unwrap();
        let c1 = create_commit(
            &obj,
            &mut commits,
            vec![base.commit_id.clone()],
            "c1",
            "test",
        )
        .unwrap();

        checkout(&mut obj, &commits, &base.commit_id).unwrap();
        obj.store
            .add_triple(&make_triple("x", "r", "9"), "world", Some(1u8))
            .unwrap();
        let new_base = create_commit(
            &obj,
            &mut commits,
            vec![base.commit_id.clone()],
            "new_base",
            "test",
        )
        .unwrap();

        let result = rebase(
            &mut obj,
            &mut commits,
            &base.commit_id,
            &c1.commit_id,
            &new_base.commit_id,
            "test",
        )
        .unwrap();

        assert_eq!(result.replayed, 1);
    }
}

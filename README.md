# arrow-graph-git

Graph-native git primitives on Arrow RecordBatches.

Version-control RDF-like knowledge graphs with 8 git operations:
- **commit** — snapshot graph state to Parquet
- **checkout** — restore graph state from a commit
- **diff** — object-level comparison between commits
- **merge** — 3-way merge with conflict detection and resolution strategies
- **rebase** — replay commits onto a new base
- **blame** — trace which commit introduced a triple
- **cherry-pick** — apply specific commits to current state
- **revert** — undo a commit's changes

Built on [`arrow-graph-core`](https://crates.io/crates/arrow-graph-core) for the underlying graph store.

## Quick Start

```rust
use arrow_graph_core::Triple;
use arrow_graph_git::{CommitsTable, GitObjectStore, create_commit, checkout, diff};

let dir = tempfile::tempdir().unwrap();
let mut obj = GitObjectStore::with_snapshot_dir(dir.path());
let mut commits = CommitsTable::new();

// Add data and commit
let triple = Triple {
    subject: "Alice".into(), predicate: "knows".into(), object: "Bob".into(),
    graph: None, confidence: Some(0.9), source_document: None,
    source_chunk_id: None, extracted_by: None,
    caused_by: None, derived_from: None, consolidated_at: None,
};
obj.store.add_triple(&triple, "world", Some(1)).unwrap();
let c1 = create_commit(&obj, &mut commits, vec![], "Initial", "agent").unwrap();

// Checkout restores state from any commit
checkout(&mut obj, &commits, &c1.commit_id).unwrap();
```

## Merge Strategies

```rust
use arrow_graph_git::{MergeStrategy, merge_with_strategy};

// Automatic conflict resolution
let result = merge_with_strategy(
    &mut obj, &mut commits,
    &branch_a_id, &branch_b_id,
    "agent",
    &MergeStrategy::LastWriterWins,
).unwrap();
```

Available strategies: `Ours`, `Theirs`, `LastWriterWins`, or custom closures.

## Crash-Safe Persistence

```rust
use arrow_graph_git::{save, restore};

// WAL + atomic rename — safe against crashes
save(&obj, &save_dir).unwrap();
restore(&mut obj, &save_dir).unwrap();
```

## Performance

Benchmarked on Apple M4 (10K triples):

| Operation | Time | Gate |
|-----------|------|------|
| Commit | ~3ms | < 25ms |
| Checkout | ~1.3ms | < 25ms |
| Save+Restore | ~4.3ms | < 100ms |
| Batch add 10K | ~4.3ms | < 10ms |

## License

MIT

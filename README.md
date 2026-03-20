# arrow-graph-git

[![Crates.io](https://img.shields.io/crates/v/arrow-graph-git.svg)](https://crates.io/crates/arrow-graph-git)
[![Documentation](https://docs.rs/arrow-graph-git/badge.svg)](https://docs.rs/arrow-graph-git)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

Graph-native git primitives on Arrow RecordBatches.

Add to your `Cargo.toml`:

```toml
[dependencies]
arrow-graph-git = "0.14"
```

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

## Diff, Blame, Rebase, Cherry-pick, Revert

```rust
use arrow_graph_git::*;

// Diff — object-level comparison between commits
let diff = diff(&mut obj, &commits, &c1, &c2)?;
println!("Added: {}, Removed: {}", diff.added.len(), diff.removed.len());

// Blame — per-triple provenance
let entries = blame(&mut obj, &commits, &head_id, 100)?;
for e in &entries {
    println!("{} {} {} — commit {} by {}", e.subject, e.predicate, e.object, e.commit_id, e.author);
}

// Rebase — replay commits onto a new base
let result = rebase(&mut obj, &mut commits, &start, &end, &onto, "agent")?;

// Cherry-pick — apply a specific commit
let new_id = cherry_pick(&mut obj, &mut commits, &source_commit, &head, "agent")?;

// Revert — undo a commit (rejects merge commits)
let revert_id = revert(&mut obj, &mut commits, &bad_commit, &head, "agent")?;
```

## Performance

Benchmarked on Apple M4 (10K triples):

| Operation | Time | Gate |
|-----------|------|------|
| Commit | ~3ms | < 25ms |
| Checkout | ~1.3ms | < 25ms |
| Save+Restore | ~4.3ms | < 100ms |
| Batch add 10K | ~4.3ms | < 10ms |

## Requirements

- Rust 1.85+ (edition 2024)
- [`arrow-graph-core`](https://crates.io/crates/arrow-graph-core) for the underlying graph store

## License

MIT — Copyright (c) Hank Head / Congruent Systems PBC

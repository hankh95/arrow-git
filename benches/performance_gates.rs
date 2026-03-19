//! Criterion benchmarks for arrow-graph-git performance gates.
//!
//! Run with: `cargo bench --bench performance_gates`
//!
//! Performance gates:
//! | Gate    | Metric                          | Target  |
//! |---------|---------------------------------|---------|
//! | H-GIT-1 | Commit+checkout @10K triples   | < 50ms  |
//! | M-SAVE  | Save/restore @10K triples      | < 100ms |
//! | M-119   | Awakening (restore + 4 queries) | < 200ms |

use arrow_graph_core::{QuerySpec, Triple};
use arrow_graph_git::{CommitsTable, GitObjectStore, checkout, create_commit, restore, save};
use criterion::{Criterion, criterion_group, criterion_main};

const NAMESPACES: &[&str] = &["world", "work", "code", "research", "self"];

const LAYER_DISTRIBUTION: &[(u8, usize)] = &[
    (0, 500), // prose
    (1, 800), // semantic
    (2, 300), // reasoning
    (3, 400), // experience
    (4, 200), // journal
    (5, 200), // procedural
    (6, 100), // metacognitive
];

fn make_triple(subj: &str) -> Triple {
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

/// Populate a GitObjectStore with 10K triples across namespaces × layers.
fn populate_10k(obj: &mut GitObjectStore) {
    for ns in NAMESPACES {
        for (layer, count) in LAYER_DISTRIBUTION {
            let triples: Vec<Triple> = (0..*count)
                .map(|i| make_triple(&format!("{ns}:e{i}")))
                .collect();
            obj.store.add_batch(&triples, ns, Some(*layer)).unwrap();
        }
    }
}

fn bench_commit_10k(c: &mut Criterion) {
    c.bench_function("commit @10K triples", |b| {
        b.iter_with_setup(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
                let commits = CommitsTable::new();
                populate_10k(&mut obj);
                (tmp, obj, commits)
            },
            |(_tmp, obj, mut commits)| {
                create_commit(&obj, &mut commits, vec![], "bench", "agent").unwrap();
            },
        );
    });
}

fn bench_checkout_10k(c: &mut Criterion) {
    c.bench_function("checkout @10K triples", |b| {
        b.iter_with_setup(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
                let mut commits = CommitsTable::new();
                populate_10k(&mut obj);
                let commit = create_commit(&obj, &mut commits, vec![], "bench", "agent").unwrap();
                obj.store.clear();
                (tmp, obj, commits, commit.commit_id)
            },
            |(_tmp, mut obj, commits, commit_id)| {
                checkout(&mut obj, &commits, &commit_id).unwrap();
            },
        );
    });
}

fn bench_commit_checkout_roundtrip_10k(c: &mut Criterion) {
    c.bench_function("H-GIT-1: commit+checkout roundtrip @10K", |b| {
        b.iter_with_setup(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let mut obj = GitObjectStore::with_snapshot_dir(tmp.path());
                let commits = CommitsTable::new();
                populate_10k(&mut obj);
                (tmp, obj, commits)
            },
            |(_tmp, mut obj, mut commits)| {
                let c1 = create_commit(&obj, &mut commits, vec![], "bench", "agent").unwrap();
                obj.store.clear();
                checkout(&mut obj, &commits, &c1.commit_id).unwrap();
            },
        );
    });
}

fn bench_save_10k(c: &mut Criterion) {
    c.bench_function("save @10K triples", |b| {
        b.iter_with_setup(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let save_dir = tmp.path().join("save");
                let mut obj = GitObjectStore::with_snapshot_dir(tmp.path().join("snap"));
                populate_10k(&mut obj);
                (tmp, obj, save_dir)
            },
            |(_tmp, obj, save_dir)| {
                save(&obj, &save_dir).unwrap();
            },
        );
    });
}

fn bench_restore_10k(c: &mut Criterion) {
    c.bench_function("restore @10K triples", |b| {
        b.iter_with_setup(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let save_dir = tmp.path().join("save");
                let mut obj = GitObjectStore::with_snapshot_dir(tmp.path().join("snap"));
                populate_10k(&mut obj);
                save(&obj, &save_dir).unwrap();
                obj.store.clear();
                (tmp, obj, save_dir)
            },
            |(_tmp, mut obj, save_dir)| {
                restore(&mut obj, &save_dir).unwrap();
            },
        );
    });
}

fn bench_save_restore_roundtrip_10k(c: &mut Criterion) {
    c.bench_function("M-SAVE: save+restore roundtrip @10K", |b| {
        b.iter_with_setup(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let save_dir = tmp.path().join("save");
                let mut obj = GitObjectStore::with_snapshot_dir(tmp.path().join("snap"));
                populate_10k(&mut obj);
                (tmp, obj, save_dir)
            },
            |(_tmp, mut obj, save_dir)| {
                save(&obj, &save_dir).unwrap();
                obj.store.clear();
                restore(&mut obj, &save_dir).unwrap();
            },
        );
    });
}

fn bench_awakening_10k(c: &mut Criterion) {
    c.bench_function("M-119: awakening (restore + 4 queries) @10K", |b| {
        b.iter_with_setup(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let save_dir = tmp.path().join("state");
                let mut obj = GitObjectStore::with_snapshot_dir(tmp.path().join("snap"));
                populate_10k(&mut obj);
                save(&obj, &save_dir).unwrap();
                obj.store.clear();
                (tmp, obj, save_dir)
            },
            |(_tmp, mut obj, save_dir)| {
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
            },
        );
    });
}

fn bench_batch_add_10k(c: &mut Criterion) {
    c.bench_function("batch add 10K triples", |b| {
        b.iter_with_setup(
            || {
                let obj = GitObjectStore::new();
                let triples: Vec<Triple> = (0..10_000)
                    .map(|i| make_triple(&format!("entity:{i}")))
                    .collect();
                (obj, triples)
            },
            |(mut obj, triples)| {
                obj.store.add_batch(&triples, "world", Some(1)).unwrap();
            },
        );
    });
}

criterion_group!(
    git_operations,
    bench_commit_10k,
    bench_checkout_10k,
    bench_commit_checkout_roundtrip_10k,
);

criterion_group!(
    persistence,
    bench_save_10k,
    bench_restore_10k,
    bench_save_restore_roundtrip_10k,
    bench_awakening_10k,
);

criterion_group!(store_operations, bench_batch_add_10k,);

criterion_main!(git_operations, persistence, store_operations);

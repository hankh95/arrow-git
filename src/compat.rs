//! Compatibility types for Namespace and Layer.
//!
//! `arrow-graph-core` uses string-based namespace keys and `Option<u8>` layers.
//! This module provides typed wrappers that make the arrow-git API ergonomic
//! while remaining compatible with any namespace scheme.

/// Default namespace partitions (matching the common knowledge graph pattern).
pub const DEFAULT_NAMESPACES: &[&str] = &["world", "work", "code", "research", "self"];

/// Convenience re-export of the core store types using string-based namespaces.
pub use arrow_graph_core::{ArrowGraphStore, CausalNode, QuerySpec, StoreError, Triple};

/// Create an ArrowGraphStore with the default namespace set.
pub fn default_store() -> ArrowGraphStore {
    ArrowGraphStore::new(DEFAULT_NAMESPACES)
}

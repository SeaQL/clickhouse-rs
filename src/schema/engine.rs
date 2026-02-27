use std::fmt::{self, Display};

/// ClickHouse table engine.
///
/// See <https://clickhouse.com/docs/engines/table-engines>
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Engine {
    MergeTree,
    ReplacingMergeTree,
    SummingMergeTree,
    AggregatingMergeTree,
    /// CollapsingMergeTree with the sign column name.
    CollapsingMergeTree(String),
    /// VersionedCollapsingMergeTree with sign and version column names.
    VersionedCollapsingMergeTree(String, String),
    Memory,
    Log,
    TinyLog,
}

impl Engine {
    pub(crate) fn is_merge_tree_family(&self) -> bool {
        matches!(
            self,
            Engine::MergeTree
                | Engine::ReplacingMergeTree
                | Engine::SummingMergeTree
                | Engine::AggregatingMergeTree
                | Engine::CollapsingMergeTree(_)
                | Engine::VersionedCollapsingMergeTree(_, _)
        )
    }
}

impl Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Engine::MergeTree => write!(f, "MergeTree()"),
            Engine::ReplacingMergeTree => write!(f, "ReplacingMergeTree()"),
            Engine::SummingMergeTree => write!(f, "SummingMergeTree()"),
            Engine::AggregatingMergeTree => write!(f, "AggregatingMergeTree()"),
            Engine::CollapsingMergeTree(sign) => write!(f, "CollapsingMergeTree({sign})"),
            Engine::VersionedCollapsingMergeTree(sign, ver) => {
                write!(f, "VersionedCollapsingMergeTree({sign}, {ver})")
            }
            Engine::Memory => write!(f, "Memory()"),
            Engine::Log => write!(f, "Log()"),
            Engine::TinyLog => write!(f, "TinyLog()"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_merge_tree_family() {
        assert!(Engine::MergeTree.is_merge_tree_family());
        assert!(Engine::ReplacingMergeTree.is_merge_tree_family());
        assert!(Engine::SummingMergeTree.is_merge_tree_family());
        assert!(Engine::AggregatingMergeTree.is_merge_tree_family());
        assert!(Engine::CollapsingMergeTree("s".into()).is_merge_tree_family());
        assert!(
            Engine::VersionedCollapsingMergeTree("s".into(), "v".into()).is_merge_tree_family()
        );
        assert!(!Engine::Memory.is_merge_tree_family());
        assert!(!Engine::Log.is_merge_tree_family());
        assert!(!Engine::TinyLog.is_merge_tree_family());
    }
}

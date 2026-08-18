use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// Query languages understood by a database driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryLanguage {
    Sql,
    MongoQuery,
    RedisCommand,
}

/// Data mutation support exposed by a driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrudCapabilities {
    pub create: bool,
    pub update: bool,
    pub delete: bool,
    pub transactional: bool,
}

/// Execution-plan modes exposed by a driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplainCapabilities {
    pub estimated: bool,
    pub analyzed: bool,
}

/// Slow-query facilities exposed by a driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlowQueryCapabilities {
    pub available: bool,
    pub configurable: bool,
}

/// An optional database feature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "settings", rename_all = "snake_case")]
pub enum Capability {
    Crud(CrudCapabilities),
    Explain(ExplainCapabilities),
    SlowQueries(SlowQueryCapabilities),
    TableData,
    /// The driver stops a running statement on the server, not just client-side.
    Cancellation,
}

/// Serializable feature declaration used by both native and process drivers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilitySet {
    query_languages: BTreeSet<QueryLanguage>,
    capabilities: Vec<Capability>,
}

impl CapabilitySet {
    #[must_use]
    pub fn builder() -> CapabilitySetBuilder {
        CapabilitySetBuilder::default()
    }

    #[must_use]
    pub fn query_languages(&self) -> &BTreeSet<QueryLanguage> {
        &self.query_languages
    }

    #[must_use]
    pub fn capabilities(&self) -> &[Capability] {
        &self.capabilities
    }

    #[must_use]
    pub fn supports_crud(&self) -> bool {
        self.capabilities
            .iter()
            .any(|capability| matches!(capability, Capability::Crud(_)))
    }

    #[must_use]
    pub fn supports_explain(&self) -> bool {
        self.capabilities
            .iter()
            .any(|capability| matches!(capability, Capability::Explain(_)))
    }

    #[must_use]
    pub fn supports_slow_queries(&self) -> bool {
        self.capabilities
            .iter()
            .any(|capability| matches!(capability, Capability::SlowQueries(_)))
    }

    #[must_use]
    pub fn supports_table_data(&self) -> bool {
        self.capabilities
            .iter()
            .any(|capability| matches!(capability, Capability::TableData))
    }

    #[must_use]
    pub fn supports(&self, capability: &Capability) -> bool {
        self.capabilities.contains(capability)
    }
}

#[derive(Debug, Default)]
pub struct CapabilitySetBuilder {
    query_languages: BTreeSet<QueryLanguage>,
    capabilities: Vec<Capability>,
}

impl CapabilitySetBuilder {
    #[must_use]
    pub fn query_language(mut self, language: QueryLanguage) -> Self {
        self.query_languages.insert(language);
        self
    }

    #[must_use]
    pub fn enable(mut self, capability: Capability) -> Self {
        if !self.capabilities.contains(&capability) {
            self.capabilities.push(capability);
        }
        self
    }

    #[must_use]
    pub fn build(self) -> CapabilitySet {
        CapabilitySet {
            query_languages: self.query_languages,
            capabilities: self.capabilities,
        }
    }
}

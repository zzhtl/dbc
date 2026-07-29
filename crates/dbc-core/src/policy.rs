use serde::{Deserialize, Serialize};

/// Maximum operation class available to an actor.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    #[default]
    ReadOnly,
    SafeWrite,
    HighRiskWrite,
}

/// Entry point requesting database access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Actor {
    Desktop,
    Ai,
    Mcp,
}

/// Risk class of the requested operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Read,
    SafeWrite,
    HighRiskWrite,
}

impl Operation {
    #[must_use]
    pub fn required_mode(self) -> AccessMode {
        match self {
            Self::Read => AccessMode::ReadOnly,
            Self::SafeWrite => AccessMode::SafeWrite,
            Self::HighRiskWrite => AccessMode::HighRiskWrite,
        }
    }
}

/// Per-actor access settings for one connection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecurityPolicy {
    desktop: AccessMode,
    ai: AccessMode,
    mcp: AccessMode,
}

impl SecurityPolicy {
    #[must_use]
    pub fn with_mode(mut self, actor: Actor, mode: AccessMode) -> Self {
        match actor {
            Actor::Desktop => self.desktop = mode,
            Actor::Ai => self.ai = mode,
            Actor::Mcp => self.mcp = mode,
        }
        self
    }

    #[must_use]
    pub fn mode_for(self, actor: Actor) -> AccessMode {
        match actor {
            Actor::Desktop => self.desktop,
            Actor::Ai => self.ai,
            Actor::Mcp => self.mcp,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyDecision {
    Allowed,
    RequiresConfirmation,
    Denied {
        required: AccessMode,
        configured: AccessMode,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct PolicyEngine {
    policy: SecurityPolicy,
}

impl PolicyEngine {
    #[must_use]
    pub fn new(policy: SecurityPolicy) -> Self {
        Self { policy }
    }

    #[must_use]
    pub fn authorize(self, actor: Actor, operation: Operation) -> PolicyDecision {
        let configured = self.policy.mode_for(actor);
        let required = operation.required_mode();

        if configured < required {
            return PolicyDecision::Denied {
                required,
                configured,
            };
        }

        match operation {
            Operation::Read => PolicyDecision::Allowed,
            Operation::SafeWrite | Operation::HighRiskWrite => {
                PolicyDecision::RequiresConfirmation
            }
        }
    }
}

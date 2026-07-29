//! Condition helpers and the [`ConditionContext`] builder.
//!
//! Kubernetes status conditions follow a well-defined convention: each condition
//! has a `type`, `status` (True/False/Unknown), `reason`, `message`,
//! `lastTransitionTime`, and `observedGeneration`. This module provides ergonomic
//! helpers for constructing conditions consistently throughout your controller,
//! and a [`set_condition`] function that performs the upsert-with-transition-time
//! semantics that the Kubernetes API conventions require.
//!
//! The [`ConditionContext`] is created once per reconcile pass (with the current
//! generation) and passed to all gates, effects, and polls that need to produce
//! conditions. This ensures `observedGeneration` is always consistent across the
//! entire status.

use std::fmt::Display;

use k8s_openapi::{
    apimachinery::pkg::apis::meta::v1::{Condition, Time},
    jiff::Timestamp,
};

/// Returns the current wall-clock time as a Kubernetes `Time` value.
pub fn now() -> Time {
    Time(Timestamp::now())
}

/// The three valid values for a Kubernetes condition's `status` field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConditionStatus {
    /// The condition is satisfied.
    True,
    /// The condition is not satisfied.
    False,
    /// The condition's state could not be determined.
    Unknown,
}

impl Display for ConditionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::True => f.write_str("True"),
            Self::False => f.write_str("False"),
            Self::Unknown => f.write_str("Unknown"),
        }
    }
}

/// A factory for building conditions with consistent `observedGeneration` and
/// ready-condition naming.
///
/// Create one at the start of your reconcile function and pass it (or a reference)
/// to your gates, effects, and polls. This keeps generation tracking and condition
/// naming centralised rather than scattered across step implementations.
///
/// ```rust
/// # use kubo::utils::ConditionContext;
/// # use kube::api::ObjectMeta;
/// # struct MyObj {
/// #     metadata: ObjectMeta,
/// # }
/// # let obj = MyObj { metadata: ObjectMeta::default() };
/// let cx = ConditionContext::new(obj.metadata.generation.unwrap_or(0))
///     .with_ready_reason("Provisioned", "All resources created");
///
/// // In a gate:
/// cx.not_ready_condition("DependencyPending", "upstream database not ready yet")
/// # ;
/// ```
pub struct ConditionContext {
    generation: i64,
    ready_condition_kind: Option<String>,
    ready_reason: String,
    ready_message: String,
}

impl ConditionContext {
    /// The default condition type used for the terminal "all done" condition.
    pub const DEFAULT_READY_CONDITION_KIND: &str = "Ready";

    /// Create a new context for the given resource generation.
    ///
    /// All conditions produced by this context will have `observedGeneration`
    /// set to this value.
    pub fn new(generation: i64) -> Self {
        Self {
            generation,
            ready_condition_kind: None,
            ready_reason: "Reconciled".to_string(),
            ready_message: "All steps completed successfully".to_string(),
        }
    }

    /// Override the condition type used for the terminal ready condition.
    ///
    /// Defaults to `"Ready"`. Some CRDs use domain-specific types like
    /// `"Provisioned"` or `"Available"`.
    pub fn with_ready_condition_kind(mut self, kind: impl Display) -> Self {
        self.ready_condition_kind = Some(kind.to_string());
        self
    }

    /// Set the reason and message for the terminal ready condition emitted
    /// when all pipeline steps pass.
    pub fn with_ready_reason(mut self, reason: impl Display, message: impl Display) -> Self {
        self.ready_reason = reason.to_string();
        self.ready_message = message.to_string();
        self
    }

    /// Returns the condition type for the terminal ready condition.
    pub fn ready_condition_kind(&self) -> &str {
        self.ready_condition_kind
            .as_deref()
            .unwrap_or(Self::DEFAULT_READY_CONDITION_KIND)
    }

    /// Build a condition with the given type, status, reason, and message.
    ///
    /// `observedGeneration` and `lastTransitionTime` are set automatically.
    pub fn condition(
        &self,
        kind: impl Display,
        status: ConditionStatus,
        reason: impl Display,
        message: &str,
    ) -> Condition {
        Condition {
            type_: kind.to_string(),
            status: status.to_string(),
            reason: reason.to_string(),
            message: message.to_string(),
            last_transition_time: now(),
            observed_generation: Some(self.generation),
        }
    }

    /// Build a `Ready=True` condition with a custom reason and message.
    pub fn ready_condition(&self, reason: impl Display, message: &str) -> Condition {
        self.condition(
            self.ready_condition_kind(),
            ConditionStatus::True,
            reason,
            message,
        )
    }

    /// Build the terminal ready condition emitted when all steps pass.
    ///
    /// Uses the reason and message configured via [`with_ready_reason`](Self::with_ready_reason),
    /// or the defaults (`"Reconciled"` / `"All steps completed successfully"`).
    pub fn reconciled_condition(&self) -> Condition {
        self.condition(
            self.ready_condition_kind(),
            ConditionStatus::True,
            &self.ready_reason,
            &self.ready_message,
        )
    }

    /// Build a `Ready=False` condition (the pipeline is blocked or waiting).
    pub fn not_ready_condition(&self, reason: impl Display, message: &str) -> Condition {
        self.condition(
            self.ready_condition_kind(),
            ConditionStatus::False,
            reason,
            message,
        )
    }
}

/// Returns `true` if a condition with the given `type_` has `status: "True"`.
pub fn is_condition_true(conditions: &[Condition], condition_type: impl Display) -> bool {
    let ct = condition_type.to_string();
    conditions
        .iter()
        .any(|c| c.type_ == ct && c.status == "True")
}

/// Upsert a condition into a conditions vec, following Kubernetes conventions.
///
/// If a condition with the same `type_` already exists:
/// - If `status`, `reason`, or `message` differ → replace it entirely (new
///   `lastTransitionTime` reflects the state change).
/// - If only `observedGeneration` differs → update that field in place (no
///   transition occurred, just a new generation observed).
///
/// If no condition with that `type_` exists, the new condition is appended.
pub fn set_condition(conditions: &mut Vec<Condition>, new: Condition) {
    if let Some(existing) = conditions.iter_mut().find(|c| c.type_ == new.type_) {
        if existing.status != new.status
            || existing.reason != new.reason
            || existing.message != new.message
        {
            *existing = new;
        } else if existing.observed_generation != new.observed_generation {
            existing.observed_generation = new.observed_generation;
        }
    } else {
        conditions.push(new);
    }
}

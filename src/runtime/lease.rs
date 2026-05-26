use std::time::Duration;

use k8s_openapi::{
    api::coordination::v1::{Lease, LeaseSpec},
    apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta},
    jiff::{self, Timestamp},
};
use kube::{Api, Client, api::PostParams};
use tracing::{debug, warn};

#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[allow(unused)]
    #[error("{0}")]
    Other(String),
}

#[cfg(test)]
impl Clone for LeaseError {
    fn clone(&self) -> Self {
        match self {
            Self::Kube(_) => Self::Other("cloned kube error".into()),
            Self::Other(s) => Self::Other(s.clone()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseOutcome {
    Acquired,
    NotAcquired,
}

/// The leader election mechanism behind the runtime.
///
/// [`LeaseLock`] implements this against the Coordination v1 Lease API.
/// Implement this trait yourself if you need a different backend (etcd
/// directly, Redis, etc.) or to supply a fake for integration testing.
pub trait LeaderLease: Send + Sync {
    fn try_acquire_or_renew(&self)
    -> impl Future<Output = Result<LeaseOutcome, LeaseError>> + Send;
    fn step_down(&self) -> impl Future<Output = ()> + Send;
}

pub struct LeaseLock {
    api: Api<Lease>,
    holder_id: String,
    lease_name: String,
    lease_ttl: Duration,
}

impl LeaseLock {
    pub fn new(
        client: Client,
        namespace: &str,
        holder_id: &str,
        lease_name: &str,
        lease_ttl: Duration,
    ) -> Self {
        Self {
            api: Api::namespaced(client, namespace),
            holder_id: holder_id.to_string(),
            lease_name: lease_name.to_string(),
            lease_ttl,
        }
    }

    async fn do_acquire_or_renew(&self) -> Result<LeaseOutcome, LeaseError> {
        let now = MicroTime(Timestamp::now());
        let ttl_secs = self.lease_ttl.as_secs() as i32;

        match self.api.get_opt(&self.lease_name).await? {
            None => self.try_create(now, ttl_secs).await,
            Some(existing) => self.try_update(existing, now, ttl_secs).await,
        }
    }

    async fn do_step_down(&self) {
        let result: Result<(), LeaseError> = async {
            let Some(existing) = self.api.get_opt(&self.lease_name).await? else {
                return Ok(());
            };

            let rv = existing
                .metadata
                .resource_version
                .as_deref()
                .unwrap_or_default();

            if rv.is_empty() {
                return Ok(());
            }

            let holder = existing
                .spec
                .as_ref()
                .and_then(|s| s.holder_identity.as_deref())
                .unwrap_or_default();

            if holder != self.holder_id {
                return Ok(());
            }

            let lease = Lease {
                metadata: ObjectMeta {
                    name: Some(self.lease_name.clone()),
                    resource_version: Some(rv.to_string()),
                    ..Default::default()
                },
                spec: Some(LeaseSpec {
                    holder_identity: None,
                    lease_duration_seconds: Some(1),
                    renew_time: Some(MicroTime(Timestamp::now())),
                    acquire_time: existing.spec.as_ref().and_then(|s| s.acquire_time.clone()),
                    lease_transitions: existing.spec.as_ref().and_then(|s| s.lease_transitions),
                    ..Default::default()
                }),
            };
            self.api
                .replace(&self.lease_name, &PostParams::default(), &lease)
                .await?;
            Ok(())
        }
        .await;

        if let Err(e) = result {
            warn!(error = %e, "Failed to step down from lease, TTL will expire naturally");
        }
    }

    async fn try_create(&self, now: MicroTime, ttl_secs: i32) -> Result<LeaseOutcome, LeaseError> {
        let lease = Lease {
            metadata: ObjectMeta {
                name: Some(self.lease_name.clone()),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(self.holder_id.clone()),
                lease_duration_seconds: Some(ttl_secs),
                acquire_time: Some(now.clone()),
                renew_time: Some(now),
                lease_transitions: Some(0),
                ..Default::default()
            }),
        };

        match self.api.create(&PostParams::default(), &lease).await {
            Ok(_) => Ok(LeaseOutcome::Acquired),
            Err(kube::Error::Api(ref resp)) if resp.code == 409 => {
                debug!("Lease already exists (409 Conflict), will retry");
                Ok(LeaseOutcome::NotAcquired)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn try_update(
        &self,
        existing: Lease,
        now: MicroTime,
        ttl_secs: i32,
    ) -> Result<LeaseOutcome, LeaseError> {
        let spec = existing.spec.as_ref();
        let holder = spec
            .and_then(|s| s.holder_identity.as_deref())
            .unwrap_or_default();
        let rv = existing
            .metadata
            .resource_version
            .as_deref()
            .unwrap_or_default()
            .to_string();

        let we_are_holder = holder == self.holder_id;

        if !we_are_holder && !is_expired(spec) {
            debug!(holder = %holder, "Lease held by another instance, not expired");
            return Ok(LeaseOutcome::NotAcquired);
        }

        let transitions = spec.and_then(|s| s.lease_transitions).unwrap_or(0);
        let new_transitions = if we_are_holder {
            transitions
        } else {
            transitions + 1
        };
        let acquire_time = if we_are_holder {
            spec.and_then(|s| s.acquire_time.clone())
        } else {
            Some(now.clone())
        };

        let lease = Lease {
            metadata: ObjectMeta {
                name: Some(self.lease_name.clone()),
                resource_version: Some(rv),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(self.holder_id.clone()),
                lease_duration_seconds: Some(ttl_secs),
                acquire_time,
                renew_time: Some(now),
                lease_transitions: Some(new_transitions),
                ..Default::default()
            }),
        };

        match self
            .api
            .replace(&self.lease_name, &PostParams::default(), &lease)
            .await
        {
            Ok(_) => Ok(LeaseOutcome::Acquired),
            Err(kube::Error::Api(ref resp)) if resp.code == 409 => {
                debug!("Stale resource_version (409 Conflict), will retry");
                Ok(LeaseOutcome::NotAcquired)
            }
            Err(e) => Err(e.into()),
        }
    }
}

impl LeaderLease for LeaseLock {
    async fn try_acquire_or_renew(&self) -> Result<LeaseOutcome, LeaseError> {
        self.do_acquire_or_renew().await
    }

    async fn step_down(&self) {
        self.do_step_down().await
    }
}

/// Check whether a lease has expired based on its renew_time and duration.
fn is_expired(spec: Option<&LeaseSpec>) -> bool {
    let Some(spec) = spec else { return true };
    let Some(ref renew_time) = spec.renew_time else {
        return true;
    };
    let duration_secs = spec.lease_duration_seconds.unwrap_or(0);
    if duration_secs <= 0 {
        return true;
    }

    let expiry = renew_time
        .0
        .checked_add(jiff::Span::new().seconds(duration_secs));
    match expiry {
        Ok(expiry) => Timestamp::now() > expiry,
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_when_no_spec() {
        assert!(is_expired(None));
    }

    #[test]
    fn expired_when_no_renew_time() {
        let spec = LeaseSpec::default();
        assert!(is_expired(Some(&spec)));
    }

    #[test]
    fn expired_when_zero_duration() {
        let spec = LeaseSpec {
            renew_time: Some(MicroTime(Timestamp::now())),
            lease_duration_seconds: Some(0),
            ..Default::default()
        };
        assert!(is_expired(Some(&spec)));
    }

    #[test]
    fn not_expired_when_recently_renewed() {
        let spec = LeaseSpec {
            renew_time: Some(MicroTime(Timestamp::now())),
            lease_duration_seconds: Some(15),
            ..Default::default()
        };
        assert!(!is_expired(Some(&spec)));
    }

    #[test]
    fn expired_when_past_ttl() {
        let past = Timestamp::now()
            .checked_sub(jiff::Span::new().seconds(20))
            .unwrap();
        let spec = LeaseSpec {
            renew_time: Some(MicroTime(past)),
            lease_duration_seconds: Some(15),
            ..Default::default()
        };
        assert!(is_expired(Some(&spec)));
    }
}

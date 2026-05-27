use std::{fmt::Debug, pin::Pin, sync::Arc, time::Duration};

use anyhow::Result;
use futures::StreamExt;
use kube::{
    Api,
    runtime::{Controller, controller::Action},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    HasConditions,
    environment::{EnvResource, Environment},
    extensions::Extensions,
    listener::{ControllerContext, Listener},
    reconcile::{Reconcile, ReconcileError},
    runtime::lease::LeaseLock,
};

mod leader;
mod lease;

/// Type-erased controller factory. Produces a future that runs one controller
/// for the duration of a leadership term.
type ControllerFn<E> = Box<
    dyn Fn(
            &RuntimeContext<E>,
            CancellationToken,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>
        + Send
        + Sync,
>;

/// Tuning knobs for the leader election lifecycle.
///
/// These parameters directly affect failover behaviour: how quickly a standby
/// replica takes over when the leader dies, how tolerant the system is to
/// transient API server unavailability, and how long in-flight reconciliations
/// get to complete before being killed on a leadership transition.
///
/// Use [`RuntimeConfig::from_env`] to resolve pod identity from the Kubernetes
/// downward API.
pub struct RuntimeConfig {
    /// Used as the Coordination v1 Lease object name and in log output.
    pub name: String,
    /// Identity written into `spec.holderIdentity` on the Lease. Must be unique
    /// across replicas — typically the pod name via the downward API.
    pub holder_id: String,
    /// Namespace where the Lease object is created.
    pub namespace: String,
    /// Upper bound on failover time. After the leader stops renewing, a standby
    /// can claim the lease once this duration elapses. Lower values mean faster
    /// failover but less tolerance for API server latency.
    pub lease_ttl: Duration,
    /// Interval between renewal attempts while leading, and between acquisition
    /// attempts while standing by. Must be well below `lease_ttl` to allow
    /// retries before expiry.
    pub retry_period: Duration,
    /// Grace period for controllers to finish in-flight reconciliations after
    /// cancellation. Once this elapses, controller tasks are forcibly aborted
    /// to guarantee no two leadership terms overlap.
    pub drain_timeout: Duration,
}

impl RuntimeConfig {
    /// Build a config by reading `POD_NAME` and `POD_NAMESPACE` from the
    /// environment (set via the downward API in your Pod spec). Falls back to
    /// the OS hostname and `kaphera-system` respectively.
    ///
    /// Timing defaults: 15 s TTL, 5 s retry, 10 s drain.
    pub fn from_env(name: impl Into<String>) -> Self {
        let holder_id = std::env::var("POD_NAME").unwrap_or_else(|_| {
            warn!("POD_NAME not set, using hostname");
            gethostname::gethostname().to_string_lossy().into_owned()
        });
        let namespace = std::env::var("POD_NAMESPACE").unwrap_or_else(|_| {
            warn!("POD_NAMESPACE not set, defaulting to kaphera-system");
            "kaphera-system".into()
        });

        Self {
            name: name.into(),
            holder_id,
            namespace,
            lease_ttl: Duration::from_secs(15),
            retry_period: Duration::from_secs(5),
            drain_timeout: Duration::from_secs(10),
        }
    }
}

/// Collects listeners and runs them under a unified lifecycle.
///
/// Manages leader election via a Coordination v1 Lease, signal handling
/// (SIGTERM/SIGINT), and controller spawning/cancellation.
///
/// ```text
/// Runtime (leader election, signals, drain)
///   └─ Listener (declarative controller)
///       └─ Reconcile (step pipeline)
/// ```
///
/// # Example
///
/// ```ignore
/// use kubo::runtime::Runtime;
///
/// Runtime::new("my-operator", client, env)
///     .with(reqwest::Client::new())
///     .listener(VaultInstanceListener)
///     .listener(PostgresDatabaseListener)
///     .run()
///     .await
/// ```
pub struct Runtime<E: Environment + Sync + 'static> {
    name: &'static str,
    client: kube::Client,
    env: E,
    extensions: Extensions,
    controllers: Vec<(&'static str, ControllerFn<E>)>,
}

impl<E: Environment + Sync + 'static> Runtime<E> {
    /// Create a new runtime.
    ///
    /// The `client` is used for leader election (Lease API) and is
    /// automatically inserted into the extensions map so listeners
    /// can access it via `ctx.require::<kube::Client>()`.
    pub fn new(name: &'static str, client: kube::Client, env: E) -> Self {
        let mut extensions = Extensions::new();
        extensions.insert(client.clone());

        Self {
            name,
            client,
            env,
            extensions,
            controllers: Vec::new(),
        }
    }

    /// Insert an arbitrary value into the operator's extensions map.
    ///
    /// Anything inserted here is available to all listeners via
    /// `ctx.get::<T>()` in both `controller()` and `reconcile()`.
    pub fn with<T: Send + Sync + 'static>(mut self, val: T) -> Self {
        self.extensions.insert(val);
        self
    }

    /// Register a listener. The resource type `R` is erased here — the
    /// Runtime only retains a name and a closure that knows how to spawn
    /// the typed controller loop.
    pub fn listener<R, L>(mut self, listener: L) -> Self
    where
        R: EnvResource + HasConditions + Debug + 'static,
        L: Listener<E, R> + 'static,
    {
        let listener = Arc::new(listener);
        let client = self.client.clone();

        let factory: ControllerFn<E> = Box::new(move |rt_ctx, cancel| {
            let listener = Arc::clone(&listener);
            let env = rt_ctx.env_arc();
            let extensions = rt_ctx.extensions_arc();
            let client = client.clone();

            Box::pin(async move {
                let ctrl_ctx = Arc::new(ControllerContext::new(env, extensions));
                let controller = {
                    use kube::runtime::watcher::Config as WatcherConfig;

                    let api = Api::<R>::all(client);
                    let controller = Controller::new(api, WatcherConfig::default());

                    listener.controller(&ctrl_ctx, controller)
                };

                info!(controller = L::NAME, "Starting controller");

                controller
                    .graceful_shutdown_on(cancel.cancelled_owned())
                    .run(
                        {
                            let listener = Arc::clone(&listener);
                            move |obj: Arc<R>, ctx: Arc<ControllerContext<E>>| {
                                let listener = Arc::clone(&listener);
                                async move {
                                    let reconciler = Reconcile::new(ctx.env(), obj.clone());
                                    listener
                                        .reconcile(&ctx, obj, reconciler)
                                        .execute(L::FINALIZER)
                                        .await
                                }
                            }
                        },
                        |_obj: Arc<R>, _err: &ReconcileError, _ctx: Arc<ControllerContext<E>>| {
                            Action::requeue(std::time::Duration::from_secs(5))
                        },
                        ctrl_ctx,
                    )
                    .for_each(|res| async move {
                        if let Err(e) = res {
                            error!(
                                controller = L::NAME,
                                error = ?e,
                                "Reconciliation failed"
                            );
                        }
                    })
                    .await;
            })
        });

        self.controllers.push((L::NAME, factory));
        self
    }

    /// Run all registered controllers under leader election.
    ///
    /// Blocks for the lifetime of the process. While this instance holds
    /// the Coordination v1 Lease, all registered controllers run concurrently.
    /// When leadership is lost or SIGTERM/SIGINT is received, controllers are
    /// cancelled and given the drain timeout to finish before being aborted.
    pub async fn run(self) -> Result<()> {
        let config = RuntimeConfig::from_env(self.name);
        let shutdown = CancellationToken::new();

        let lease_lock = LeaseLock::new(
            self.client,
            &config.namespace,
            &config.holder_id,
            &config.name,
            config.lease_ttl,
        );

        install_signal_handler(shutdown.clone());

        let controller_names: Vec<&str> = self.controllers.iter().map(|(n, _)| *n).collect();
        info!(
            operator = %self.name,
            controllers = ?controller_names,
            "Starting operator with {} controllers",
            self.controllers.len(),
        );

        let rt_ctx = Arc::new(RuntimeContext {
            name: self.name,
            env: Arc::new(self.env),
            extensions: Arc::new(self.extensions),
        });

        let controllers: Arc<Vec<ControllerFn<E>>> =
            Arc::new(self.controllers.into_iter().map(|(_, f)| f).collect());

        leader::run(
            lease_lock,
            Arc::clone(&rt_ctx),
            shutdown,
            &config,
            move |rt_ctx, cancel: CancellationToken| {
                let controllers = Arc::clone(&controllers);
                async move {
                    let futs: Vec<_> = controllers
                        .iter()
                        .map(|f| f(&rt_ctx, cancel.clone()))
                        .collect();
                    futures::future::join_all(futs).await;
                    Ok(())
                }
            },
        )
        .await
    }
}

/// Process-scoped context that the Runtime owns and shares with all controllers.
///
/// Holds the operator name (used for leader election and SSA field manager)
/// and an extensions map for operator-specific state. No hardcoded fields
/// beyond the name — everything else goes through `.get::<T>()`.
pub struct RuntimeContext<E: Environment + Sync> {
    name: &'static str,
    env: Arc<E>,
    extensions: Arc<Extensions>,
}

impl<E: Environment + Sync> RuntimeContext<E> {
    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn env(&self) -> &E {
        &self.env
    }

    pub fn env_arc(&self) -> Arc<E> {
        Arc::clone(&self.env)
    }

    /// Access the extensions type-map.
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.extensions.get::<T>()
    }

    /// Access a required extension, panicking if absent.
    pub fn require<T: Send + Sync + 'static>(&self) -> &T {
        self.extensions.require::<T>()
    }

    fn extensions_arc(&self) -> Arc<Extensions> {
        Arc::clone(&self.extensions)
    }
}

fn install_signal_handler(shutdown: CancellationToken) {
    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");

        tokio::select! {
            _ = ctrl_c => info!("Received SIGINT"),
            _ = sigterm.recv() => info!("Received SIGTERM"),
        }

        shutdown.cancel();
    });
}

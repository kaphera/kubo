use std::time::Duration;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use super::{RuntimeConfig, lease::LeaderLease, lease::LeaseOutcome};

enum LeadershipEnd {
    Shutdown,
    Lost,
    ControllersExited(Option<anyhow::Error>),
}

/// Run the leader election loop. Returns when shutdown is signalled.
///
/// Each time the lease is acquired, `run_controllers` is invoked with a fresh
/// `CancellationToken` scoped to that leadership term. When the term ends
/// (loss, shutdown, or controller exit), the token is cancelled and the loop
/// either re-enters acquisition or returns.
pub async fn run<L, C, F, Fut>(
    lease: L,
    ctx: C,
    shutdown: CancellationToken,
    config: &RuntimeConfig,
    run_controllers: F,
) -> Result<()>
where
    L: LeaderLease,
    C: Clone + Send + Sync + 'static,
    F: Fn(C, CancellationToken) -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    loop {
        if !acquire(&lease, &shutdown, config).await {
            return Ok(());
        }
        info!("Leadership acquired, starting controllers");

        match hold(&lease, &ctx, &shutdown, config, &run_controllers).await {
            LeadershipEnd::Shutdown => {
                info!("Shutdown requested, stepping down");
                lease.step_down().await;
                return Ok(());
            }
            LeadershipEnd::Lost => {
                warn!("Leadership lost, re-entering acquisition loop");
            }
            LeadershipEnd::ControllersExited(err) => {
                if let Some(ref e) = err {
                    error!(error = %e, "Controllers exited with error, re-entering leader loop");
                } else {
                    info!("Controllers exited, re-entering leader loop");
                }
            }
        }
    }
}

/// Block until leadership is acquired or shutdown is requested.
/// Returns `true` if acquired, `false` if shutdown.
async fn acquire(
    lease: &impl LeaderLease,
    shutdown: &CancellationToken,
    config: &RuntimeConfig,
) -> bool {
    loop {
        match lease.try_acquire_or_renew().await {
            Ok(LeaseOutcome::Acquired) => return true,
            Ok(LeaseOutcome::NotAcquired) => {}
            Err(e) => {
                warn!(error = %e, "Lease acquisition attempt failed");
            }
        }
        tokio::select! {
            _ = shutdown.cancelled() => return false,
            _ = tokio::time::sleep(config.retry_period) => {},
        }
    }
}

/// Run controllers while maintaining leadership.
async fn hold<L, C, F, Fut>(
    lease: &L,
    ctx: &C,
    shutdown: &CancellationToken,
    config: &RuntimeConfig,
    run_controllers: &F,
) -> LeadershipEnd
where
    L: LeaderLease,
    C: Clone + Send + Sync + 'static,
    F: Fn(C, CancellationToken) -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let controllers_cancel = CancellationToken::new();
    let controllers_ctx = ctx.clone();

    let controllers_handle =
        tokio::spawn(run_controllers(controllers_ctx, controllers_cancel.clone()));
    let abort_handle = controllers_handle.abort_handle();

    tokio::select! {
        result = controllers_handle => {
            match result {
                Ok(Ok(())) => LeadershipEnd::ControllersExited(None),
                Ok(Err(e)) => LeadershipEnd::ControllersExited(Some(e)),
                Err(e) => LeadershipEnd::ControllersExited(Some(e.into())),
            }
        }
        _ = renew_loop(lease, config) => {
            controllers_cancel.cancel();
            drain_controllers(abort_handle, config.drain_timeout).await;
            LeadershipEnd::Lost
        }
        _ = shutdown.cancelled() => {
            controllers_cancel.cancel();
            drain_controllers(abort_handle, config.drain_timeout).await;
            LeadershipEnd::Shutdown
        }
    }
}

/// Periodically renew the lease. Returns when leadership is considered lost.
///
/// Tolerates transient API server errors (network partitions, 5xx responses)
/// as long as the time since the last successful renewal stays below
/// `lease_ttl`. Only gives up immediately on an explicit `NotAcquired`
/// response, which means another replica has claimed the lease.
async fn renew_loop(lease: &impl LeaderLease, config: &RuntimeConfig) {
    let mut last_success = tokio::time::Instant::now();

    loop {
        tokio::time::sleep(config.retry_period).await;

        match lease.try_acquire_or_renew().await {
            Ok(LeaseOutcome::Acquired) => {
                last_success = tokio::time::Instant::now();
            }
            Ok(LeaseOutcome::NotAcquired) => {
                warn!("Lease renewal returned not-acquired, leadership lost");
                return;
            }
            Err(e) => {
                warn!(error = %e, "Lease renewal failed");
                if last_success.elapsed() >= config.lease_ttl {
                    warn!("Lease has likely expired, assuming leadership lost");
                    return;
                }
            }
        }
    }
}

/// Give controllers up to `timeout` to finish in-flight reconciliations.
/// Forcibly aborts them if they exceed it, guaranteeing that no two leadership
/// terms have controllers running concurrently.
async fn drain_controllers(abort_handle: tokio::task::AbortHandle, timeout: Duration) {
    tokio::time::sleep(timeout).await;
    if !abort_handle.is_finished() {
        warn!(
            "Controllers did not drain within {}s, aborting",
            timeout.as_secs()
        );
        abort_handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use tokio::sync::Mutex;

    use super::super::lease::{LeaderLease, LeaseError, LeaseOutcome};
    use super::*;

    fn test_config() -> RuntimeConfig {
        RuntimeConfig {
            name: "test-operator".into(),
            holder_id: "test-pod".into(),
            namespace: "test-ns".into(),
            lease_ttl: Duration::from_secs(15),
            retry_period: Duration::from_secs(5),
            drain_timeout: Duration::from_secs(10),
        }
    }

    /// A scriptable fake that yields pre-programmed responses, then repeats
    /// the last one forever (so tests don't panic on unexpected calls).
    struct FakeLease {
        responses: Mutex<VecDeque<Result<LeaseOutcome, LeaseError>>>,
        fallback: Result<LeaseOutcome, LeaseError>,
        step_down_count: AtomicUsize,
    }

    impl FakeLease {
        fn new(responses: Vec<Result<LeaseOutcome, LeaseError>>) -> Self {
            let fallback = responses
                .last()
                .cloned()
                .unwrap_or(Ok(LeaseOutcome::NotAcquired));
            Self {
                responses: Mutex::new(VecDeque::from(responses)),
                fallback,
                step_down_count: AtomicUsize::new(0),
            }
        }

        fn always_leader() -> Self {
            Self::new(vec![Ok(LeaseOutcome::Acquired)])
        }

        fn step_downs(&self) -> usize {
            self.step_down_count.load(Ordering::SeqCst)
        }
    }

    impl LeaderLease for FakeLease {
        async fn try_acquire_or_renew(&self) -> Result<LeaseOutcome, LeaseError> {
            let mut queue = self.responses.lock().await;
            match queue.pop_front() {
                Some(r) => r,
                None => self.fallback.clone(),
            }
        }

        async fn step_down(&self) {
            self.step_down_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Clone)]
    struct FakeCtx;

    struct LeaseFacade(Arc<FakeLease>);

    impl LeaderLease for LeaseFacade {
        async fn try_acquire_or_renew(&self) -> Result<LeaseOutcome, LeaseError> {
            self.0.try_acquire_or_renew().await
        }

        async fn step_down(&self) {
            self.0.step_down().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_acquisition_exits_cleanly() {
        let lease = FakeLease::new(vec![
            Ok(LeaseOutcome::NotAcquired),
            Ok(LeaseOutcome::NotAcquired),
        ]);
        let ctx = FakeCtx;
        let shutdown = CancellationToken::new();
        let config = test_config();

        let shutdown_trigger = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(12)).await;
            shutdown_trigger.cancel();
        });

        let result = run(
            lease,
            ctx,
            shutdown,
            &config,
            |_ctx: FakeCtx, _cancel| async { Ok(()) },
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_while_holding_steps_down_and_cancels_controllers() {
        let lease = Arc::new(FakeLease::always_leader());
        let ctx = FakeCtx;
        let shutdown = CancellationToken::new();
        let config = test_config();

        let shutdown_trigger = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            shutdown_trigger.cancel();
        });

        let controller_cancelled = Arc::new(AtomicUsize::new(0));
        let cc = Arc::clone(&controller_cancelled);

        let lease_ref = Arc::clone(&lease);
        let result = run(
            LeaseFacade(lease_ref),
            ctx,
            shutdown,
            &config,
            move |_ctx: FakeCtx, cancel| {
                let cc = Arc::clone(&cc);
                async move {
                    cancel.cancelled().await;
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(controller_cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(lease.step_downs(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn leadership_lost_cancels_controllers_and_reacquires() {
        let lease = FakeLease::new(vec![
            Ok(LeaseOutcome::Acquired),
            Ok(LeaseOutcome::NotAcquired),
            Ok(LeaseOutcome::NotAcquired),
            Ok(LeaseOutcome::NotAcquired),
        ]);
        let ctx = FakeCtx;
        let shutdown = CancellationToken::new();
        let config = test_config();

        let shutdown_trigger = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            shutdown_trigger.cancel();
        });

        let starts = Arc::new(AtomicUsize::new(0));
        let starts_c = Arc::clone(&starts);

        let result = run(
            lease,
            ctx,
            shutdown,
            &config,
            move |_ctx: FakeCtx, cancel| {
                let starts_c = Arc::clone(&starts_c);
                async move {
                    starts_c.fetch_add(1, Ordering::SeqCst);
                    cancel.cancelled().await;
                    Ok(())
                }
            },
        )
        .await;

        assert!(result.is_ok());
        assert!(starts.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test(start_paused = true)]
    async fn controller_error_reenters_leader_loop() {
        let lease = FakeLease::new(vec![Ok(LeaseOutcome::Acquired), Ok(LeaseOutcome::Acquired)]);
        let ctx = FakeCtx;
        let shutdown = CancellationToken::new();
        let config = test_config();

        let shutdown_trigger = shutdown.clone();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_c = Arc::clone(&attempts);

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(20)).await;
            shutdown_trigger.cancel();
        });

        let result = run(
            lease,
            ctx,
            shutdown,
            &config,
            move |_ctx: FakeCtx, cancel| {
                let attempts_c = Arc::clone(&attempts_c);
                async move {
                    let n = attempts_c.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        anyhow::bail!("controller crashed");
                    }
                    cancel.cancelled().await;
                    Ok(())
                }
            },
        )
        .await;

        assert!(result.is_ok());
        assert!(attempts.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test(start_paused = true)]
    async fn acquisition_retries_on_error() {
        let lease = FakeLease::new(vec![
            Err(LeaseError::Other("network blip".into())),
            Err(LeaseError::Other("another blip".into())),
            Ok(LeaseOutcome::Acquired),
        ]);
        let ctx = FakeCtx;
        let shutdown = CancellationToken::new();
        let config = test_config();

        let shutdown_trigger = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(20)).await;
            shutdown_trigger.cancel();
        });

        let started = Arc::new(AtomicUsize::new(0));
        let started_c = Arc::clone(&started);

        let result = run(
            lease,
            ctx,
            shutdown,
            &config,
            move |_ctx: FakeCtx, cancel| {
                let started_c = Arc::clone(&started_c);
                async move {
                    started_c.fetch_add(1, Ordering::SeqCst);
                    cancel.cancelled().await;
                    Ok(())
                }
            },
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(started.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn renewal_tolerates_transient_errors_within_ttl() {
        let lease = FakeLease::new(vec![
            Ok(LeaseOutcome::Acquired),
            Err(LeaseError::Other("blip 1".into())),
            Err(LeaseError::Other("blip 2".into())),
            Ok(LeaseOutcome::Acquired),
            Ok(LeaseOutcome::Acquired),
        ]);
        let ctx = FakeCtx;
        let shutdown = CancellationToken::new();
        let config = test_config();

        let shutdown_trigger = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(25)).await;
            shutdown_trigger.cancel();
        });

        let started = Arc::new(AtomicUsize::new(0));
        let started_c = Arc::clone(&started);

        let result = run(
            lease,
            ctx,
            shutdown,
            &config,
            move |_ctx: FakeCtx, cancel| {
                let started_c = Arc::clone(&started_c);
                async move {
                    started_c.fetch_add(1, Ordering::SeqCst);
                    cancel.cancelled().await;
                    Ok(())
                }
            },
        )
        .await;

        assert!(result.is_ok());

        assert_eq!(started.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn renewal_gives_up_after_ttl_expires() {
        let lease = FakeLease::new(vec![
            Ok(LeaseOutcome::Acquired),
            Err(LeaseError::Other("blip 1".into())),
            Err(LeaseError::Other("blip 2".into())),
            Err(LeaseError::Other("blip 3".into())),
        ]);
        let ctx = FakeCtx;
        let shutdown = CancellationToken::new();
        let config = test_config();

        let shutdown_trigger = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            shutdown_trigger.cancel();
        });

        let starts = Arc::new(AtomicUsize::new(0));
        let starts_c = Arc::clone(&starts);

        let result = run(
            lease,
            ctx,
            shutdown,
            &config,
            move |_ctx: FakeCtx, cancel| {
                let starts_c = Arc::clone(&starts_c);
                async move {
                    starts_c.fetch_add(1, Ordering::SeqCst);
                    cancel.cancelled().await;
                    Ok(())
                }
            },
        )
        .await;

        assert!(result.is_ok());

        assert!(starts.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_controllers_are_aborted_on_drain_timeout() {
        let lease = Arc::new(FakeLease::new(vec![
            Ok(LeaseOutcome::Acquired),
            Ok(LeaseOutcome::NotAcquired),
        ]));
        let ctx = FakeCtx;
        let shutdown = CancellationToken::new();
        let config = test_config();

        let shutdown_trigger = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            shutdown_trigger.cancel();
        });

        let aborted = Arc::new(AtomicUsize::new(0));
        let aborted_c = Arc::clone(&aborted);

        let result = run(
            LeaseFacade(Arc::clone(&lease)),
            ctx,
            shutdown,
            &config,
            move |_ctx: FakeCtx, _cancel| {
                let aborted_c = Arc::clone(&aborted_c);
                async move {
                    tokio::time::sleep(Duration::from_secs(999)).await;
                    aborted_c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await;

        assert!(result.is_ok());

        assert_eq!(aborted.load(Ordering::SeqCst), 0);
    }
}

#[cfg(test)]
mod proptests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use proptest::prelude::*;
    use tokio::sync::Mutex;
    use tokio_util::sync::CancellationToken;

    use super::super::lease::{LeaderLease, LeaseError, LeaseOutcome};
    use super::*;

    #[derive(Debug, Clone)]
    enum LeaseAction {
        Acquired,
        NotAcquired,
        Error,
    }

    fn lease_action_strategy() -> impl Strategy<Value = LeaseAction> {
        prop_oneof![
            3 => Just(LeaseAction::Acquired),
            3 => Just(LeaseAction::NotAcquired),
            1 => Just(LeaseAction::Error),
        ]
    }

    fn test_config() -> RuntimeConfig {
        RuntimeConfig {
            name: "prop-operator".into(),
            holder_id: "prop-pod".into(),
            namespace: "prop-ns".into(),
            lease_ttl: Duration::from_secs(15),
            retry_period: Duration::from_secs(5),
            drain_timeout: Duration::from_secs(10),
        }
    }

    struct InstrumentedLease {
        responses: Mutex<VecDeque<LeaseAction>>,
        call_count: AtomicUsize,
        shutdown: CancellationToken,
        shutdown_after: usize,
        step_down_count: AtomicUsize,
    }

    impl InstrumentedLease {
        fn new(
            responses: Vec<LeaseAction>,
            shutdown: CancellationToken,
            shutdown_after: usize,
        ) -> Self {
            Self {
                responses: Mutex::new(VecDeque::from(responses)),
                call_count: AtomicUsize::new(0),
                shutdown,
                shutdown_after,
                step_down_count: AtomicUsize::new(0),
            }
        }

        fn step_downs(&self) -> usize {
            self.step_down_count.load(Ordering::SeqCst)
        }
    }

    impl LeaderLease for InstrumentedLease {
        async fn try_acquire_or_renew(&self) -> Result<LeaseOutcome, LeaseError> {
            let call = self.call_count.fetch_add(1, Ordering::SeqCst);

            if call == self.shutdown_after {
                self.shutdown.cancel();
            }

            let mut queue = self.responses.lock().await;
            let action = queue.pop_front().unwrap_or(LeaseAction::NotAcquired);
            drop(queue);

            match action {
                LeaseAction::Acquired => Ok(LeaseOutcome::Acquired),
                LeaseAction::NotAcquired => Ok(LeaseOutcome::NotAcquired),
                LeaseAction::Error => Err(LeaseError::Other("simulated failure".into())),
            }
        }

        async fn step_down(&self) {
            self.step_down_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Clone)]
    struct FakeCtx;

    proptest! {
        /// **Termination**: for any sequence of lease outcomes and any shutdown
        /// point, the leader loop always terminates with `Ok(())`.
        #[test]
        fn always_terminates(
            outcomes in prop::collection::vec(lease_action_strategy(), 1..50),
            shutdown_idx in 0usize..60,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap();

            rt.block_on(async {
                let shutdown = CancellationToken::new();
                let config = test_config();
                let lease = InstrumentedLease::new(
                    outcomes,
                    shutdown.clone(),
                    shutdown_idx,
                );
                let ctx = FakeCtx;

                let result = run(lease, ctx, shutdown, &config, |_ctx: FakeCtx, cancel| async move {
                    cancel.cancelled().await;
                    Ok(())
                })
                .await;

                prop_assert!(result.is_ok());
                Ok(())
            })?;
        }

        /// **Step-down discipline**: `step_down` is called at most once, and
        /// only when shutdown occurs while holding leadership.
        #[test]
        fn step_down_at_most_once(
            outcomes in prop::collection::vec(lease_action_strategy(), 1..50),
            shutdown_idx in 0usize..60,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap();

            rt.block_on(async {
                let shutdown = CancellationToken::new();
                let config = test_config();
                let lease = Arc::new(InstrumentedLease::new(
                    outcomes,
                    shutdown.clone(),
                    shutdown_idx,
                ));
                let ctx = FakeCtx;

                struct Wrapper(Arc<InstrumentedLease>);
                impl LeaderLease for Wrapper {
                    async fn try_acquire_or_renew(&self) -> Result<LeaseOutcome, LeaseError> {
                        self.0.try_acquire_or_renew().await
                    }
                    async fn step_down(&self) {
                        self.0.step_down().await
                    }
                }

                let wrapper = Wrapper(Arc::clone(&lease));

                let _ = run(wrapper, ctx, shutdown, &config, |_ctx: FakeCtx, cancel| async move {
                    cancel.cancelled().await;
                    Ok(())
                })
                .await;

                let downs = lease.step_downs();
                prop_assert!(downs <= 1, "step_down called {} times", downs);
                Ok(())
            })?;
        }

        /// **Controller lifecycle**: controllers are never started more times
        /// than leadership was acquired.
        #[test]
        fn controllers_bounded_by_acquisitions(
            outcomes in prop::collection::vec(lease_action_strategy(), 1..50),
            shutdown_idx in 0usize..60,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap();

            rt.block_on(async {
                let shutdown = CancellationToken::new();
                let config = test_config();

                let max_terms = outcomes.iter()
                    .take(shutdown_idx.min(outcomes.len()))
                    .filter(|o| matches!(o, LeaseAction::Acquired))
                    .count();

                let lease = InstrumentedLease::new(
                    outcomes,
                    shutdown.clone(),
                    shutdown_idx,
                );
                let ctx = FakeCtx;

                let starts = Arc::new(AtomicUsize::new(0));
                let starts_c = Arc::clone(&starts);

                let _ = run(lease, ctx, shutdown, &config, move |_ctx: FakeCtx, cancel| {
                    let starts_c = Arc::clone(&starts_c);
                    async move {
                        starts_c.fetch_add(1, Ordering::SeqCst);
                        cancel.cancelled().await;
                        Ok(())
                    }
                })
                .await;

                let actual_starts = starts.load(Ordering::SeqCst);
                prop_assert!(
                    actual_starts <= max_terms + 1,
                    "controllers started {} times but at most {} leadership terms",
                    actual_starts,
                    max_terms,
                );
                Ok(())
            })?;
        }

        /// **No concurrent controller terms**: at most one batch of controllers
        /// is active at any point. The previous batch must have exited before a
        /// new one starts.
        #[test]
        fn no_concurrent_controller_terms(
            outcomes in prop::collection::vec(lease_action_strategy(), 1..50),
            shutdown_idx in 0usize..60,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap();

            rt.block_on(async {
                let shutdown = CancellationToken::new();
                let config = test_config();
                let lease = InstrumentedLease::new(
                    outcomes,
                    shutdown.clone(),
                    shutdown_idx,
                );
                let ctx = FakeCtx;

                let active = Arc::new(AtomicUsize::new(0));
                let max_active = Arc::new(AtomicUsize::new(0));
                let active_c = Arc::clone(&active);
                let max_active_c = Arc::clone(&max_active);

                let _ = run(lease, ctx, shutdown, &config, move |_ctx: FakeCtx, cancel| {
                    let active_c = Arc::clone(&active_c);
                    let max_active_c = Arc::clone(&max_active_c);
                    async move {
                        let current = active_c.fetch_add(1, Ordering::SeqCst) + 1;
                        max_active_c.fetch_max(current, Ordering::SeqCst);
                        cancel.cancelled().await;
                        active_c.fetch_sub(1, Ordering::SeqCst);
                        Ok(())
                    }
                })
                .await;

                let peak = max_active.load(Ordering::SeqCst);
                prop_assert!(peak <= 1, "peak concurrent controllers: {}", peak);
                Ok(())
            })?;
        }

        /// **Step-down only when leadership was held**: step_down is never
        /// called if we never acquired leadership.
        #[test]
        fn step_down_requires_prior_acquisition(
            outcomes in prop::collection::vec(lease_action_strategy(), 1..50),
            shutdown_idx in 0usize..60,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap();

            rt.block_on(async {
                let shutdown = CancellationToken::new();
                let config = test_config();
                let lease = Arc::new(InstrumentedLease::new(
                    outcomes.clone(),
                    shutdown.clone(),
                    shutdown_idx,
                ));
                let ctx = FakeCtx;

                let ever_started = Arc::new(AtomicBool::new(false));
                let ever_started_c = Arc::clone(&ever_started);

                struct Wrapper(Arc<InstrumentedLease>);
                impl LeaderLease for Wrapper {
                    async fn try_acquire_or_renew(&self) -> Result<LeaseOutcome, LeaseError> {
                        self.0.try_acquire_or_renew().await
                    }
                    async fn step_down(&self) {
                        self.0.step_down().await
                    }
                }

                let wrapper = Wrapper(Arc::clone(&lease));

                let _ = run(wrapper, ctx, shutdown, &config, move |_ctx: FakeCtx, cancel| {
                    let ever_started_c = Arc::clone(&ever_started_c);
                    async move {
                        ever_started_c.store(true, Ordering::SeqCst);
                        cancel.cancelled().await;
                        Ok(())
                    }
                })
                .await;

                let downs = lease.step_downs();
                let did_lead = ever_started.load(Ordering::SeqCst);

                if !did_lead {
                    prop_assert_eq!(downs, 0, "step_down called but leadership was never acquired");
                }
                Ok(())
            })?;
        }

        /// **Panicking controllers don't crash the runtime**: even if a
        /// controller panics (JoinError), the runtime still terminates
        /// cleanly with `Ok(())`.
        #[test]
        fn panic_resilience(
            outcomes in prop::collection::vec(lease_action_strategy(), 1..50),
            shutdown_idx in 0usize..60,
            panic_on_term in 0usize..5,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap();

            rt.block_on(async {
                let shutdown = CancellationToken::new();
                let config = test_config();
                let lease = InstrumentedLease::new(
                    outcomes,
                    shutdown.clone(),
                    shutdown_idx,
                );
                let ctx = FakeCtx;

                let term = Arc::new(AtomicUsize::new(0));
                let term_c = Arc::clone(&term);

                let result = run(lease, ctx, shutdown, &config, move |_ctx: FakeCtx, cancel| {
                    let term_c = Arc::clone(&term_c);
                    async move {
                        let t = term_c.fetch_add(1, Ordering::SeqCst);
                        if t == panic_on_term {
                            panic!("controller panic on term {}", t);
                        }
                        cancel.cancelled().await;
                        Ok(())
                    }
                })
                .await;

                prop_assert!(result.is_ok());
                Ok(())
            })?;
        }
    }
}

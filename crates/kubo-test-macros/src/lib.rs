//! Attribute macros for kubo controller tests.
//!
//! This crate exports [`test`] and [`property_test`], re-exported by the
//! `kubo` crate (behind its `test-support` feature) as `kubo::test` and
//! `kubo::property_test`. [`test`] wraps the annotated async function in
//! `#[tokio::test(start_paused = true)]` and constructs a
//! `kubo::testing::World` for each `&mut World` parameter, printing the
//! replay seed to stderr on failure. [`property_test`] additionally draws
//! parameters from proptest strategies. See the item documentation for
//! attribute options and seed handling.

use proc_macro::TokenStream;
use proc_macro2::{Literal, Span};
use quote::quote;
use syn::{FnArg, ItemFn, LitInt, Token, Type, parse::Parse, parse_macro_input};

/// Test attribute macro for kubo controller tests.
///
/// Wraps the annotated async function in `#[tokio::test(start_paused = true)]`
/// and constructs a `kubo::testing::World` for each `&mut World` parameter.
/// On test failure the seed is printed to stderr so it can be replayed:
///
/// ```text
/// seed = 7193847216284
/// You can rerun from this seed by setting the SEED environment variable to 7193847216284
/// ```
///
/// # Attribute options
///
/// | Attribute          | Semantics                                                                         |
/// |--------------------|-----------------------------------------------------------------------------------|
/// | *(none)*           | Run once; seed from `SEED` env var (default `0`).                                |
/// | `iterations = N`   | Run N times with seeds `0..N`; `SEED` pins to one run, `ITERATIONS` overrides N. |
/// | `seed = N`         | Run once with exactly seed N (env vars ignored).                                  |
/// | `seeds = [a,b,c]`  | Run with each listed seed (env vars ignored).                                     |
/// | `retries = N`      | Retry up to N additional times on failure before reporting.                       |
///
/// # Execution strategy
///
/// **Single-run tests** (default, `seed = N`, `iterations = 1`) run the body
/// directly in the test task — no extra `spawn`.  A RAII guard prints the seed
/// to stderr when the test panics, without disrupting tokio's cooperative
/// scheduling.  This preserves the exact scheduling behaviour that multi-world
/// tests depend on.
///
/// **Multi-run tests** (`iterations = N` with N > 1, `seeds = [a,b,c]`,
/// `retries = N`) use `tokio::task::spawn` to isolate each attempt so that
/// panics can be caught, annotated with the seed, and re-raised cleanly.
///
/// # Single-world form
///
/// ```rust,ignore
/// #[kubo::test]
/// async fn converges(world: &mut World) {
///     world.seed_listener(MyListener);
///     world.create(make_obj("ns", "name")).await;
///     world.run_until_idle().await;
///     assert!(world.condition::<MyResource>("ns", "name", "Ready"));
/// }
/// ```
///
/// # Multi-world form
///
/// ```rust,ignore
/// #[kubo::test]
/// async fn two_clusters(world_a: &mut World, world_b: &mut World) {
///     world_a.create(make_obj("ns", "a")).await;
///     world_b.create(make_obj("ns", "b")).await;
///     world_a.run_until_idle().await;
///     world_b.run_until_idle().await;
/// }
/// ```
///
/// # Iteration form
///
/// ```rust,ignore
/// #[kubo::test(iterations = 64)]
/// async fn is_reproducible(world: &mut World) {
///     world.seed_listener(MyListener);
///     world.create(make_obj("ns", "obj")).await;
///     world.run_until_idle().await;
///     assert!(world.condition::<MyResource>("ns", "obj", "Ready"));
/// }
/// ```
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    // ── Parse attribute options ────────────────────────────────────────────────
    let mut default_iterations_opt: Option<u64> = None;
    let mut explicit_seeds_opt: Option<Vec<u64>> = None;
    let mut retries: u64 = 0;

    let attr_parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("iterations") {
            let val: LitInt = meta.value()?.parse()?;
            default_iterations_opt = Some(val.base10_parse()?);
            Ok(())
        } else if meta.path.is_ident("seed") {
            let val: LitInt = meta.value()?.parse()?;
            explicit_seeds_opt = Some(vec![val.base10_parse()?]);
            Ok(())
        } else if meta.path.is_ident("seeds") {
            let value = meta.value()?;
            let content;
            syn::bracketed!(content in value);
            let list = content.parse_terminated(LitInt::parse, Token![,])?;
            explicit_seeds_opt = Some(
                list.iter()
                    .map(|l| l.base10_parse::<u64>())
                    .collect::<syn::Result<Vec<_>>>()?,
            );
            Ok(())
        } else if meta.path.is_ident("retries") {
            let val: LitInt = meta.value()?.parse()?;
            retries = val.base10_parse()?;
            Ok(())
        } else {
            Err(meta.error("unknown attribute for kubo::test"))
        }
    });
    parse_macro_input!(attr with attr_parser);

    // ── Extract function components ────────────────────────────────────────────
    let fn_name = &input.sig.ident;
    let fn_attrs = &input.attrs;
    let stmts = &input.block.stmts;

    // Collect all `&mut World` parameters.  Each gets a uniquely-named owned
    // variable; the original parameter name is rebound as a mutable reference.
    let mut world_vars: Vec<proc_macro2::Ident> = Vec::new();
    let mut binding_stmts: Vec<proc_macro2::TokenStream> = Vec::new();

    for (i, arg) in input.sig.inputs.iter().enumerate() {
        if let FnArg::Typed(pat_type) = arg
            && is_world_ref_mut(&pat_type.ty)
        {
            let var = proc_macro2::Ident::new(&format!("__kubo_world_{i}"), Span::call_site());
            let pat = &pat_type.pat;
            world_vars.push(var.clone());
            binding_stmts.push(quote! { let #pat = &mut #var; });
        }
    }

    // ── Choose execution strategy ─────────────────────────────────────────────
    //
    // RAII guard (no spawn): used for single-run tests.
    //   - Preserves the exact cooperative-scheduling behaviour that multi-world
    //     tests rely on (no extra task in the runtime's run queue).
    //   - A locally-defined guard type prints the seed when the test panics.
    //
    // Spawn loop: used when the test body must run more than once (iterations,
    //   multi-seed lists, or retries).
    //   - Each iteration runs in an isolated `tokio::task::spawn` so panics can
    //     be caught, annotated, and re-raised without aborting the loop.
    //
    let is_multi_run = retries > 0
        || explicit_seeds_opt
            .as_ref()
            .map(|s| s.len() > 1)
            .unwrap_or(false)
        || default_iterations_opt.is_some_and(|n| n > 1);

    // Inline RAII guard token stream (reused in both paths).
    let guard_struct = quote! {
        struct __KuboSeedGuard(u64);
        impl ::std::ops::Drop for __KuboSeedGuard {
            fn drop(&mut self) {
                if ::std::thread::panicking() {
                    ::std::eprintln!("seed = {}", self.0);
                    ::std::eprintln!(
                        "You can rerun from this seed by setting the SEED environment variable to {}",
                        self.0
                    );
                }
            }
        }
    };

    let test_body = if !is_multi_run {
        // ── RAII guard path (single run, no retries) ──────────────────────────
        //
        // Seed source:
        //   - Explicit `seed = N` attr  → fixed value N.
        //   - Default (no attrs)        → `SEED` env var, default `0`.
        let seed_expr = if let Some(seeds) = &explicit_seeds_opt {
            // seeds = [N] or seed = N: single fixed seed.
            let lit = Literal::u64_suffixed(seeds[0]);
            quote! { #lit }
        } else {
            // Read from `SEED` env var at runtime.
            quote! {
                ::std::env::var("SEED")
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0u64)
            }
        };

        let world_constructions = world_vars.iter().map(|var| {
            quote! {
                let mut #var = ::kubo::testing::World::builder()
                    .with_seed(__kubo_seed)
                    .build()
                    .await;
            }
        });

        quote! {
            let __kubo_seed: u64 = #seed_expr;
            #guard_struct
            let __kubo_guard = __KuboSeedGuard(__kubo_seed);
            #(#world_constructions)*
            #(#binding_stmts)*
            #(#stmts)*
            ::std::mem::drop(__kubo_guard);
        }
    } else {
        // ── Spawn loop path (multiple runs or retries) ────────────────────────
        let retries_lit = Literal::u64_suffixed(retries);

        let seeds_expr = if let Some(seeds) = &explicit_seeds_opt {
            // Explicit seed list: no env-var influence.
            let seed_lits: Vec<Literal> = seeds.iter().map(|&s| Literal::u64_suffixed(s)).collect();
            quote! { ::std::vec![#(#seed_lits),*] }
        } else {
            // Derive seeds from env vars and the attribute's default.
            let default_n_lit = Literal::u64_suffixed(default_iterations_opt.unwrap_or(1));
            quote! {
                {
                    let __base: u64 = ::std::env::var("SEED")
                        .ok()
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(0u64);
                    let __pinned: bool = ::std::env::var("SEED").is_ok();
                    let __n: u64 = if __pinned {
                        1u64
                    } else {
                        ::std::env::var("ITERATIONS")
                            .ok()
                            .and_then(|s| s.parse::<u64>().ok())
                            .unwrap_or(#default_n_lit)
                    };
                    (0u64..__n)
                        .map(|i| __base.wrapping_add(i))
                        .collect::<::std::vec::Vec<u64>>()
                }
            }
        };

        let world_constructions = world_vars.iter().map(|var| {
            quote! {
                let mut #var = ::kubo::testing::World::builder()
                    .with_seed(__kubo_seed_copy)
                    .build()
                    .await;
            }
        });

        quote! {
            let __kubo_seeds: ::std::vec::Vec<u64> = #seeds_expr;
            for __kubo_seed in __kubo_seeds {
                let mut __kubo_last_err: ::std::option::Option<::tokio::task::JoinError> = None;
                for _ in 0u64..=#retries_lit {
                    // u64 is Copy: shadow to move an independent copy into the async block.
                    let __kubo_seed_copy: u64 = __kubo_seed;
                    let __kubo_result = ::tokio::task::spawn(async move {
                        #(#world_constructions)*
                        #(#binding_stmts)*
                        #(#stmts)*
                    })
                    .await;
                    match __kubo_result {
                        ::std::result::Result::Ok(_) => {
                            __kubo_last_err = None;
                            break;
                        }
                        ::std::result::Result::Err(e) => {
                            __kubo_last_err = Some(e);
                        }
                    }
                }
                if let Some(__kubo_err) = __kubo_last_err {
                    ::std::eprintln!("seed = {__kubo_seed}");
                    ::std::eprintln!(
                        "You can rerun from this seed by setting the SEED environment variable to {__kubo_seed}"
                    );
                    if __kubo_err.is_panic() {
                        ::std::panic::resume_unwind(__kubo_err.into_panic());
                    }
                    panic!("kubo::test spawned task was cancelled unexpectedly");
                }
            }
        }
    };

    let output = quote! {
        #(#fn_attrs)*
        #[::tokio::test(start_paused = true)]
        async fn #fn_name() {
            #test_body
        }
    };

    output.into()
}

/// Returns `true` when `ty` is `&mut SomePath::World` (matches on the final
/// path segment being `World`).
fn is_world_ref_mut(ty: &Type) -> bool {
    if let Type::Reference(ref_ty) = ty
        && ref_ty.mutability.is_some()
        && let Type::Path(path) = &*ref_ty.elem
    {
        return path
            .path
            .segments
            .last()
            .map(|s| s.ident == "World")
            .unwrap_or(false);
    }
    false
}

/// Property test attribute macro for kubo controller tests.
///
/// Wraps the annotated async function in a `proptest` runner.  Each proptest
/// case runs in a fresh `tokio` paused-time runtime so time-control methods
/// work correctly.
///
/// # Parameters
///
/// - `&mut World` parameters are constructed from `kubo::testing::World::builder`
///   and injected at the start of each case.
/// - Parameters annotated with `#[strategy(expr)]` are drawn from `expr` via
///   proptest's strategy API.  The expression is evaluated once and used for
///   every case.
///
/// # Seed bridging
///
/// When the `SEED` environment variable is set, both the `World`'s RNG and
/// proptest's case-generation RNG are seeded from that value, making a
/// failing case byte-for-byte reproducible:
///
/// ```sh
/// SEED=42 cargo test dummy_listener_well_behaved
/// ```
///
/// # Example
///
/// ```rust,ignore
/// #[kubo::property_test]
/// async fn dummy_listener_well_behaved(
///     world: &mut World,
///     #[strategy(op_sequence(16))] ops: Vec<Op<DummyResource>>,
/// ) {
///     world.seed_listener(DummyListener);
///     for op in ops {
///         world.apply_op(op).await;
///         world.run_until_idle().await;
///     }
///     world.advance(Duration::from_secs(60)).await;
///     world.run_until_idle().await;
///     for obj_ref in world.list_refs::<DummyResource>() {
///         let ticks = world.status_ticks_for::<DummyStatus>(&obj_ref);
///         if !world.is_deleted::<DummyResource>(&obj_ref) {
///             assert_converges(&ticks, |s| is_ready(&s.conditions), &obj_ref.to_string());
///         }
///         assert_monotonic(&ticks, |s| s.touched.is_some(), "touched");
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn property_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    let fn_name = &input.sig.ident;
    let fn_attrs = &input.attrs;
    let stmts = &input.block.stmts;

    // ── Classify parameters ───────────────────────────────────────────────────

    struct StrategyParam {
        pat: Box<syn::Pat>,
        strategy_expr: syn::Expr,
    }

    let mut world_vars: Vec<proc_macro2::Ident> = Vec::new();
    let mut world_bindings: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut strategy_params: Vec<StrategyParam> = Vec::new();

    for (i, arg) in input.sig.inputs.iter().enumerate() {
        if let FnArg::Typed(pat_type) = arg {
            if is_world_ref_mut(&pat_type.ty) {
                let var = proc_macro2::Ident::new(&format!("__kubo_world_{i}"), Span::call_site());
                let pat = &pat_type.pat;
                world_vars.push(var.clone());
                world_bindings.push(quote! { let #pat = &mut #var; });
            } else {
                // Look for #[strategy(expr)] attribute.
                let maybe_expr = pat_type
                    .attrs
                    .iter()
                    .find(|a| a.path().is_ident("strategy"))
                    .and_then(|a| a.parse_args::<syn::Expr>().ok());

                if let Some(expr) = maybe_expr {
                    strategy_params.push(StrategyParam {
                        pat: pat_type.pat.clone(),
                        strategy_expr: expr,
                    });
                }
            }
        }
    }

    // ── Build strategy and closure param ────────────────────────────────────

    let (strategy_ts, closure_param_ts) = match strategy_params.len() {
        0 => (quote! { ::proptest::prelude::Just(()) }, quote! { _ }),
        1 => {
            let expr = &strategy_params[0].strategy_expr;
            let pat = &strategy_params[0].pat;
            (quote! { #expr }, quote! { #pat })
        }
        _ => {
            let exprs: Vec<_> = strategy_params.iter().map(|p| &p.strategy_expr).collect();
            let pats: Vec<_> = strategy_params.iter().map(|p| &p.pat).collect();
            (quote! { (#(#exprs),*) }, quote! { (#(#pats),*) })
        }
    };

    // ── World constructions ──────────────────────────────────────────────────

    let world_constructions: Vec<_> = world_vars
        .iter()
        .map(|var| {
            quote! {
                let mut #var = ::kubo::testing::World::builder()
                    .with_seed(__kubo_seed)
                    .build()
                    .await;
            }
        })
        .collect();

    // ── Generated function ───────────────────────────────────────────────────
    //
    // Strategy: generate all proptest cases synchronously upfront, then run
    // each one as a `tokio::task::spawn` inside a single paused-time runtime
    // (identical to the `#[kubo::test(iterations = N)]` pattern).  This keeps
    // all cases in the same scheduler so paused-time advances are shared and
    // there is no per-case runtime-initialisation overhead.

    let output = quote! {
        #(#fn_attrs)*
        #[::tokio::test(start_paused = true)]
        async fn #fn_name() {
            // Seed from SEED env var — controls both World RNG and proptest RNG.
            let __kubo_seed: u64 = ::std::env::var("SEED")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0u64);

            // Number of cases: PROPTEST_CASES env var (default 32 — matches
            // the default for `#[kubo::test(iterations = N)]`).
            let __cases: u32 = ::std::env::var("PROPTEST_CASES")
                .ok()
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(8u32);

            // Seed proptest's case-generation RNG from SEED so a SEED=N rerun
            // generates the exact same sequence of test cases.
            let mut __seed_bytes = [0u8; 32];
            __seed_bytes[..8].copy_from_slice(&__kubo_seed.to_le_bytes());
            let __rng = ::proptest::test_runner::TestRng::from_seed(
                ::proptest::test_runner::RngAlgorithm::ChaCha,
                &__seed_bytes,
            );
            let __config = ::proptest::test_runner::Config {
                failure_persistence: None,
                ..::proptest::test_runner::Config::default()
            };
            let mut __runner =
                ::proptest::test_runner::TestRunner::new_with_rng(__config, __rng);

            let __strategy = #strategy_ts;

            // Generate all case values synchronously before entering async.
            let mut __cases_vec: ::std::vec::Vec<_> = ::std::vec::Vec::new();
            for _ in 0u32..__cases {
                if let ::std::result::Result::Ok(__tree) =
                    ::proptest::strategy::Strategy::new_tree(&__strategy, &mut __runner)
                {
                    __cases_vec.push(
                        ::proptest::strategy::ValueTree::current(&__tree)
                    );
                }
            }

            // Run each case in an isolated spawn (so panics don't abort the
            // runtime and the seed/index can be reported before re-raising).
            let mut __last_err: ::std::option::Option<(u32, ::tokio::task::JoinError)> = None;
            for (__case_idx, #closure_param_ts) in __cases_vec.into_iter().enumerate() {
                let __case_idx = __case_idx as u32;
                let __result = ::tokio::task::spawn(async move {
                    #(#world_constructions)*
                    #(#world_bindings)*
                    #(#stmts)*
                })
                .await;
                if let ::std::result::Result::Err(__e) = __result {
                    __last_err = Some((__case_idx, __e));
                    break;
                }
            }

            if let Some((__idx, __e)) = __last_err {
                ::std::eprintln!("seed = {__kubo_seed}  case = {__idx}");
                ::std::eprintln!(
                    "You can rerun from this seed by setting the SEED environment variable to {__kubo_seed}"
                );
                if __e.is_panic() {
                    ::std::panic::resume_unwind(__e.into_panic());
                }
                panic!("kubo::property_test spawned task was cancelled unexpectedly");
            }
        }
    };

    output.into()
}

//! Property-based test for multi-candidate iteration correctness.
//!
//! Feature: gateway-refactor, Property 5: Multi-candidate iteration correctness
//!
//! **Validates: Requirements 7.2, 7.3, 7.6**
//!
//! For any ordered list of Route candidates from the Resolver and any set of
//! Gateway responses:
//! - Routes SHALL be attempted in the order provided by the Resolver
//! - When a Gateway returns a Resolution error, the Daemon SHALL continue to
//!   the next candidate
//! - When a Gateway returns an Execution error, the Daemon SHALL return
//!   immediately without trying further candidates

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use proptest::prelude::*;
use tokio::sync::mpsc;

use std::collections::HashMap;

use rhop::types::CopySpec;
use rhop::protocol::ServerListRow;
use rhop::daemon::gateway::{
    ErrorKind, ExecRequest, Gateway, GatewayError, GatewayKind, InteractiveHandle,
    InteractiveRequest, Route,
};

// ---------------------------------------------------------------------------
// Mock Gateway that returns configurable results and records call order.
// ---------------------------------------------------------------------------

/// The result a mock gateway should return for a given exec call.
#[derive(Clone, Debug)]
enum MockResult {
    /// Return Ok with given exit code.
    Ok(i32),
    /// Return a Resolution error.
    ResolutionError,
    /// Return an Execution error.
    ExecutionError,
}

/// A mock Gateway that records which targets it was called with and returns
/// configurable results per target.
struct MockGateway {
    gateway_name: String,
    /// Map from end_target → result to return.
    results: HashMap<String, MockResult>,
    /// Records the order of targets this gateway was called with.
    call_log: Arc<Mutex<Vec<String>>>,
}

impl MockGateway {
    fn new(
        name: &str,
        results: HashMap<String, MockResult>,
        call_log: Arc<Mutex<Vec<String>>>,
    ) -> Self {
        Self {
            gateway_name: name.to_string(),
            results,
            call_log,
        }
    }
}

#[async_trait]
impl Gateway for MockGateway {
    async fn exec(&self, target: &str, _request: &ExecRequest) -> Result<i32, GatewayError> {
        // Record that this gateway was called with this target
        self.call_log
            .lock()
            .unwrap()
            .push(format!("{}:{}", self.gateway_name, target));

        let result = self
            .results
            .get(target)
            .cloned()
            .unwrap_or(MockResult::ResolutionError);

        match result {
            MockResult::Ok(code) => Ok(code),
            MockResult::ResolutionError => {
                Err(GatewayError::resolution(anyhow::anyhow!("not found: {}", target)))
            }
            MockResult::ExecutionError => {
                Err(GatewayError::execution(anyhow::anyhow!("exec failed: {}", target)))
            }
        }
    }

    async fn copy(&self, _target: &str, _spec: &CopySpec) -> Result<(), GatewayError> {
        unimplemented!("not needed for this test")
    }

    async fn exec_interactive(
        &self,
        _target: &str,
        _request: &InteractiveRequest,
    ) -> Result<InteractiveHandle, GatewayError> {
        unimplemented!("not needed for this test")
    }

    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError> {
        unimplemented!("not needed for this test")
    }

    fn kind(&self) -> GatewayKind {
        GatewayKind::Direct
    }

    fn name(&self) -> &str {
        &self.gateway_name
    }

    async fn prune_idle(&self) {}
}

// ---------------------------------------------------------------------------
// Dispatch loop simulation — mirrors the design doc's process_execute logic.
// ---------------------------------------------------------------------------

/// Simulates the daemon's multi-candidate dispatch loop as specified in the
/// design document.
async fn dispatch_exec(
    gateways: &[(String, Arc<dyn Gateway>)],
    routes: &[Route],
    request: &ExecRequest,
) -> Result<i32, GatewayError> {
    let mut last_error = None;
    for route in routes {
        let gateway = gateways
            .iter()
            .find(|(n, _)| n == &route.gateway_name)
            .map(|(_, gw)| gw)
            .expect("gateway not found in test setup");

        match gateway.exec(&route.end_target, request).await {
            Ok(code) => return Ok(code),
            Err(e) if e.kind == ErrorKind::Resolution => {
                last_error = Some(e);
                continue; // try next candidate
            }
            Err(e) => return Err(e), // Execution/Transport → stop
        }
    }
    Err(last_error.unwrap_or_else(|| GatewayError::resolution(anyhow::anyhow!("no routes"))))
}

// ---------------------------------------------------------------------------
// Proptest strategies
// ---------------------------------------------------------------------------

/// Strategy for generating a MockResult.
fn arb_mock_result() -> impl Strategy<Value = MockResult> {
    prop_oneof![
        3 => (0i32..=255).prop_map(MockResult::Ok),
        4 => Just(MockResult::ResolutionError),
        3 => Just(MockResult::ExecutionError),
    ]
}

/// Strategy for generating a single route candidate.
fn arb_route() -> impl Strategy<Value = (Route, MockResult)> {
    (
        "[a-z]{3,8}",  // gateway_name
        "[a-z0-9]{3,12}", // end_target
        arb_mock_result(),
    )
        .prop_map(|(gw_name, target, result)| {
            (
                Route {
                    gateway_name: gw_name,
                    end_target: target,
                },
                result,
            )
        })
}

/// Strategy for generating a list of route candidates (1–5).
fn arb_route_list() -> impl Strategy<Value = Vec<(Route, MockResult)>> {
    proptest::collection::vec(arb_route(), 1..=5)
}

// ---------------------------------------------------------------------------
// Helper: build gateways map and ExecRequest from test data.
// ---------------------------------------------------------------------------

fn build_test_setup(
    route_results: &[(Route, MockResult)],
    call_log: Arc<Mutex<Vec<String>>>,
) -> (Vec<(String, Arc<dyn Gateway>)>, Vec<Route>) {
    let mut gateways: Vec<(String, Arc<dyn Gateway>)> = Vec::new();
    let mut routes = Vec::new();

    // Group results by gateway_name
    let mut gateway_results: HashMap<String, HashMap<String, MockResult>> = HashMap::new();
    for (route, result) in route_results {
        gateway_results
            .entry(route.gateway_name.clone())
            .or_default()
            .insert(route.end_target.clone(), result.clone());
        routes.push(route.clone());
    }

    // Build mock gateways
    for (name, results) in gateway_results {
        let gw = MockGateway::new(&name, results, call_log.clone());
        gateways.push((name, Arc::new(gw)));
    }

    (gateways, routes)
}

fn make_exec_request() -> ExecRequest {
    let (sender, _rx) = mpsc::unbounded_channel();
    ExecRequest {
        argv: vec!["echo".to_string(), "hello".to_string()],
        sender,
        pty: false,
        cols: 80,
        rows: 24,
        shell: String::new(),
    }
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

    /// **Validates: Requirements 7.2, 7.3, 7.6**
    ///
    /// For any ordered list of Route candidates and any set of Gateway
    /// responses:
    /// - Routes are attempted in the order provided by the Resolver
    /// - Resolution errors cause continuation to the next candidate
    /// - Execution errors stop iteration immediately
    #[test]
    fn prop_multi_candidate_dispatch_correctness(
        route_results in arb_route_list(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let call_log = Arc::new(Mutex::new(Vec::new()));
            let (gateways, routes) = build_test_setup(&route_results, call_log.clone());
            let request = make_exec_request();

            let result = dispatch_exec(&gateways, &routes, &request).await;

            let log = call_log.lock().unwrap().clone();

            // Determine expected behavior by walking the route_results in order
            let mut expected_calls = Vec::new();
            let mut expected_result: Option<Result<i32, ErrorKind>> = None;

            for (route, mock_result) in &route_results {
                expected_calls.push(format!("{}:{}", route.gateway_name, route.end_target));
                match mock_result {
                    MockResult::Ok(code) => {
                        expected_result = Some(Ok(*code));
                        break;
                    }
                    MockResult::ResolutionError => {
                        // Continue to next candidate
                        expected_result = Some(Err(ErrorKind::Resolution));
                        continue;
                    }
                    MockResult::ExecutionError => {
                        expected_result = Some(Err(ErrorKind::Execution));
                        break;
                    }
                }
            }

            // PROPERTY 1: Order preserved — calls match expected order exactly
            prop_assert_eq!(
                &log, &expected_calls,
                "Call order mismatch. Expected: {:?}, Got: {:?}", expected_calls, log
            );

            // PROPERTY 2: Result correctness
            match expected_result {
                Some(Ok(expected_code)) => {
                    prop_assert!(
                        result.is_ok(),
                        "Expected Ok({}), got Err({:?})", expected_code, result.err()
                    );
                    prop_assert_eq!(result.unwrap(), expected_code);
                }
                Some(Err(expected_kind)) => {
                    prop_assert!(
                        result.is_err(),
                        "Expected Err({:?}), got Ok({:?})", expected_kind, result.ok()
                    );
                    prop_assert_eq!(result.as_ref().unwrap_err().kind, expected_kind);
                }
                None => {
                    // Empty routes — should get resolution error (no routes)
                    prop_assert!(result.is_err());
                }
            }

            Ok(())
        })?;
    }

    /// **Validates: Requirements 7.2, 7.6**
    ///
    /// When all candidates return Resolution errors, the dispatch loop
    /// SHALL exhaust all candidates and return the last Resolution error.
    #[test]
    fn prop_all_resolution_errors_exhausts_candidates(
        num_candidates in 1usize..=5,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let call_log = Arc::new(Mutex::new(Vec::new()));

            let route_results: Vec<(Route, MockResult)> = (0..num_candidates)
                .map(|i| {
                    (
                        Route {
                            gateway_name: format!("gw{}", i),
                            end_target: format!("target{}", i),
                        },
                        MockResult::ResolutionError,
                    )
                })
                .collect();

            let (gateways, routes) = build_test_setup(&route_results, call_log.clone());
            let request = make_exec_request();

            let result = dispatch_exec(&gateways, &routes, &request).await;

            let log = call_log.lock().unwrap().clone();

            // ALL candidates must be tried
            prop_assert_eq!(
                log.len(), num_candidates,
                "All {} candidates should be tried, but only {} were",
                num_candidates, log.len()
            );

            // Final result must be a Resolution error
            prop_assert!(result.is_err());
            prop_assert_eq!(result.unwrap_err().kind, ErrorKind::Resolution);

            Ok(())
        })?;
    }

    /// **Validates: Requirements 7.3**
    ///
    /// When the first Ok result is encountered, the dispatch loop SHALL
    /// stop immediately without trying further candidates.
    #[test]
    fn prop_first_ok_stops_iteration(
        prefix_resolution_errors in 0usize..=4,
        ok_exit_code in 0i32..=255,
        suffix_len in 0usize..=3,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let call_log = Arc::new(Mutex::new(Vec::new()));

            let mut route_results: Vec<(Route, MockResult)> = Vec::new();

            // Prefix: all Resolution errors
            for i in 0..prefix_resolution_errors {
                route_results.push((
                    Route {
                        gateway_name: format!("gw{}", i),
                        end_target: format!("target{}", i),
                    },
                    MockResult::ResolutionError,
                ));
            }

            // The Ok candidate
            route_results.push((
                Route {
                    gateway_name: "ok_gw".to_string(),
                    end_target: "ok_target".to_string(),
                },
                MockResult::Ok(ok_exit_code),
            ));

            // Suffix: should never be reached
            for i in 0..suffix_len {
                route_results.push((
                    Route {
                        gateway_name: format!("suffix_gw{}", i),
                        end_target: format!("suffix_target{}", i),
                    },
                    MockResult::Ok(99),
                ));
            }

            let (gateways, routes) = build_test_setup(&route_results, call_log.clone());
            let request = make_exec_request();

            let result = dispatch_exec(&gateways, &routes, &request).await;

            let log = call_log.lock().unwrap().clone();

            // Should stop at the Ok candidate (index = prefix_resolution_errors)
            let expected_call_count = prefix_resolution_errors + 1;
            prop_assert_eq!(
                log.len(), expected_call_count,
                "Should stop after Ok at position {}. Got {} calls: {:?}",
                prefix_resolution_errors, log.len(), log
            );

            // Result must be the expected exit code
            prop_assert!(result.is_ok());
            prop_assert_eq!(result.unwrap(), ok_exit_code);

            Ok(())
        })?;
    }

    /// **Validates: Requirements 7.3**
    ///
    /// When an Execution error is encountered, the dispatch loop SHALL
    /// return immediately without trying further candidates.
    #[test]
    fn prop_execution_error_stops_immediately(
        prefix_resolution_errors in 0usize..=4,
        suffix_len in 0usize..=3,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let call_log = Arc::new(Mutex::new(Vec::new()));

            let mut route_results: Vec<(Route, MockResult)> = Vec::new();

            // Prefix: all Resolution errors (these are skipped via continue)
            for i in 0..prefix_resolution_errors {
                route_results.push((
                    Route {
                        gateway_name: format!("gw{}", i),
                        end_target: format!("target{}", i),
                    },
                    MockResult::ResolutionError,
                ));
            }

            // The Execution error candidate
            route_results.push((
                Route {
                    gateway_name: "exec_err_gw".to_string(),
                    end_target: "exec_err_target".to_string(),
                },
                MockResult::ExecutionError,
            ));

            // Suffix: should never be reached
            for i in 0..suffix_len {
                route_results.push((
                    Route {
                        gateway_name: format!("suffix_gw{}", i),
                        end_target: format!("suffix_target{}", i),
                    },
                    MockResult::Ok(0),
                ));
            }

            let (gateways, routes) = build_test_setup(&route_results, call_log.clone());
            let request = make_exec_request();

            let result = dispatch_exec(&gateways, &routes, &request).await;

            let log = call_log.lock().unwrap().clone();

            // Should stop at the Execution error candidate
            let expected_call_count = prefix_resolution_errors + 1;
            prop_assert_eq!(
                log.len(), expected_call_count,
                "Should stop after Execution error at position {}. Got {} calls: {:?}",
                prefix_resolution_errors, log.len(), log
            );

            // Result must be an Execution error
            prop_assert!(result.is_err());
            prop_assert_eq!(result.unwrap_err().kind, ErrorKind::Execution);

            Ok(())
        })?;
    }
}

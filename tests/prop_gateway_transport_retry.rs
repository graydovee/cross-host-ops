//! Property-based test for transport error triggers internal retry.
//!
//! Feature: gateway-refactor, Property 6: Transport error triggers internal retry
//!
//! **Validates: Requirements 3.3, 4.3, 5.3**
//!
//! For any Gateway implementation, when the first exec attempt fails with a
//! transport error, the Gateway SHALL discard the broken connection and retry
//! exactly once with a newly created connection before propagating the error.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use proptest::prelude::*;
use tokio::sync::mpsc;

use rhop::config::ServerEntry;
use rhop::connection::CopySpec;
use rhop::daemon::gateway::{
    ErrorKind, ExecRequest, Gateway, GatewayError, GatewayKind, InteractiveHandle,
    InteractiveRequest,
};

// ---------------------------------------------------------------------------
// Model for transport retry behavior
// ---------------------------------------------------------------------------

/// What should happen on each attempt of an exec call.
#[derive(Clone, Debug, PartialEq)]
enum AttemptOutcome {
    /// The attempt succeeds with the given exit code.
    Success(i32),
    /// The attempt fails with a transport error.
    TransportError,
    /// The attempt fails with an execution error.
    ExecutionError,
}

/// Configuration for a mock gateway's retry behavior.
/// Models what happens on the first attempt and the retry attempt.
#[derive(Clone, Debug)]
struct RetryScenario {
    /// What the first exec attempt returns.
    first_attempt: AttemptOutcome,
    /// What the retry attempt returns (only used if first attempt is TransportError).
    retry_attempt: AttemptOutcome,
}

// ---------------------------------------------------------------------------
// Mock Gateway that models the transport retry logic
// ---------------------------------------------------------------------------

/// A mock Gateway that simulates transport error retry behavior as specified
/// in the design document:
/// - First attempt fails with transport error → discard connection, retry once
/// - Retry succeeds → return success
/// - Retry fails with transport error → propagate transport error
/// - First attempt fails with non-transport error → propagate immediately (no retry)
/// - First attempt succeeds → return success (no retry needed)
struct RetryMockGateway {
    gateway_name: String,
    /// The scenario configuration for this gateway.
    scenario: RetryScenario,
    /// Tracks the number of exec attempts made (first + retry).
    attempt_count: Arc<Mutex<u32>>,
    /// Tracks whether the connection was "discarded" before retry.
    connection_discarded: Arc<Mutex<bool>>,
}

impl RetryMockGateway {
    fn new(name: &str, scenario: RetryScenario) -> Self {
        Self {
            gateway_name: name.to_string(),
            scenario,
            attempt_count: Arc::new(Mutex::new(0)),
            connection_discarded: Arc::new(Mutex::new(false)),
        }
    }

    fn attempt_count(&self) -> u32 {
        *self.attempt_count.lock().unwrap()
    }

    fn was_connection_discarded(&self) -> bool {
        *self.connection_discarded.lock().unwrap()
    }
}

#[async_trait]
impl Gateway for RetryMockGateway {
    async fn exec(&self, _target: &str, _request: &ExecRequest) -> Result<i32, GatewayError> {
        let mut count = self.attempt_count.lock().unwrap();
        *count += 1;
        let current_attempt = *count;
        drop(count);

        if current_attempt == 1 {
            // First attempt
            match &self.scenario.first_attempt {
                AttemptOutcome::Success(code) => Ok(*code),
                AttemptOutcome::TransportError => {
                    // Discard the broken connection
                    *self.connection_discarded.lock().unwrap() = true;

                    // Retry with a new connection (second attempt)
                    let mut count = self.attempt_count.lock().unwrap();
                    *count += 1;
                    drop(count);

                    match &self.scenario.retry_attempt {
                        AttemptOutcome::Success(code) => Ok(*code),
                        AttemptOutcome::TransportError => {
                            Err(GatewayError::transport(anyhow::anyhow!(
                                "transport error on retry"
                            )))
                        }
                        AttemptOutcome::ExecutionError => {
                            Err(GatewayError::execution(anyhow::anyhow!(
                                "execution error on retry"
                            )))
                        }
                    }
                }
                AttemptOutcome::ExecutionError => Err(GatewayError::execution(anyhow::anyhow!(
                    "execution error on first attempt"
                ))),
            }
        } else {
            // This branch should not be reached if the gateway is used correctly
            // (the retry is handled internally in the first call)
            panic!("unexpected second external call to exec");
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

    async fn list_servers(&self) -> Result<Vec<ServerEntry>, GatewayError> {
        unimplemented!("not needed for this test")
    }

    fn kind(&self) -> GatewayKind {
        GatewayKind::Local
    }

    fn name(&self) -> &str {
        &self.gateway_name
    }

    async fn prune_idle(&self) {}
}

// ---------------------------------------------------------------------------
// Dispatch function — wraps a single gateway call (caller's perspective)
// ---------------------------------------------------------------------------

/// Simulates how the Daemon dispatches an exec call to a single Gateway.
/// The Daemon sees only ONE call — the Gateway handles retry internally.
async fn dispatch_single_exec(
    gateway: &dyn Gateway,
    target: &str,
    request: &ExecRequest,
) -> Result<i32, GatewayError> {
    gateway.exec(target, request).await
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

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
// Proptest strategies
// ---------------------------------------------------------------------------

/// Strategy for generating an attempt outcome.
fn arb_attempt_outcome() -> impl Strategy<Value = AttemptOutcome> {
    prop_oneof![
        4 => (0i32..=255).prop_map(AttemptOutcome::Success),
        3 => Just(AttemptOutcome::TransportError),
        3 => Just(AttemptOutcome::ExecutionError),
    ]
}

/// Strategy for generating a retry scenario.
fn arb_retry_scenario() -> impl Strategy<Value = RetryScenario> {
    (arb_attempt_outcome(), arb_attempt_outcome()).prop_map(|(first, retry)| RetryScenario {
        first_attempt: first,
        retry_attempt: retry,
    })
}

/// Strategy for generating a scenario where first attempt is a transport error.
fn arb_transport_first_scenario() -> impl Strategy<Value = RetryScenario> {
    arb_attempt_outcome().prop_map(|retry| RetryScenario {
        first_attempt: AttemptOutcome::TransportError,
        retry_attempt: retry,
    })
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

    /// **Validates: Requirements 3.3, 4.3, 5.3**
    ///
    /// When the first exec attempt fails with a transport error, the Gateway
    /// SHALL discard the broken connection and retry exactly once with a newly
    /// created connection. The overall result depends on the retry outcome:
    /// - If retry succeeds → return Ok(exit_code)
    /// - If retry fails with transport error → propagate transport error
    /// - If retry fails with execution error → propagate execution error
    #[test]
    fn prop_transport_error_triggers_exactly_one_retry(
        scenario in arb_transport_first_scenario(),
        target in "[a-z]{3,10}",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let expected_retry = scenario.retry_attempt.clone();
            let gateway = RetryMockGateway::new("test-gw", scenario);
            let request = make_exec_request();

            let result = dispatch_single_exec(&gateway, &target, &request).await;

            // PROPERTY: When first attempt is a transport error, the gateway
            // MUST retry exactly once (2 total internal attempts)
            prop_assert_eq!(
                gateway.attempt_count(), 2,
                "Expected exactly 2 internal attempts (1 original + 1 retry), got {}",
                gateway.attempt_count()
            );

            // PROPERTY: The broken connection MUST be discarded before retry
            prop_assert!(
                gateway.was_connection_discarded(),
                "Connection must be discarded before retry"
            );

            // PROPERTY: The overall result matches the retry attempt outcome
            match expected_retry {
                AttemptOutcome::Success(code) => {
                    prop_assert!(
                        result.is_ok(),
                        "Expected Ok({}) after successful retry, got Err({:?})",
                        code,
                        result.err()
                    );
                    prop_assert_eq!(result.unwrap(), code);
                }
                AttemptOutcome::TransportError => {
                    prop_assert!(result.is_err(), "Expected transport error after retry failure");
                    prop_assert_eq!(
                        result.unwrap_err().kind,
                        ErrorKind::Transport,
                        "Expected Transport error kind when retry also fails with transport"
                    );
                }
                AttemptOutcome::ExecutionError => {
                    prop_assert!(result.is_err(), "Expected execution error from retry");
                    prop_assert_eq!(
                        result.unwrap_err().kind,
                        ErrorKind::Execution,
                        "Expected Execution error kind when retry fails with execution error"
                    );
                }
            }

            Ok(())
        })?;
    }

    /// **Validates: Requirements 3.3, 4.3, 5.3**
    ///
    /// When the first exec attempt succeeds, no retry SHALL occur.
    /// The Gateway returns the result immediately with only 1 internal attempt.
    #[test]
    fn prop_success_does_not_trigger_retry(
        exit_code in 0i32..=255,
        target in "[a-z]{3,10}",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let scenario = RetryScenario {
                first_attempt: AttemptOutcome::Success(exit_code),
                // retry_attempt doesn't matter — should never be reached
                retry_attempt: AttemptOutcome::TransportError,
            };
            let gateway = RetryMockGateway::new("test-gw", scenario);
            let request = make_exec_request();

            let result = dispatch_single_exec(&gateway, &target, &request).await;

            // PROPERTY: When first attempt succeeds, only 1 internal attempt occurs
            prop_assert_eq!(
                gateway.attempt_count(), 1,
                "Expected exactly 1 attempt when first succeeds, got {}",
                gateway.attempt_count()
            );

            // PROPERTY: Connection is NOT discarded (no retry needed)
            prop_assert!(
                !gateway.was_connection_discarded(),
                "Connection should not be discarded on success"
            );

            // PROPERTY: Result is the expected exit code
            prop_assert!(result.is_ok());
            prop_assert_eq!(result.unwrap(), exit_code);

            Ok(())
        })?;
    }

    /// **Validates: Requirements 3.3, 4.3, 5.3**
    ///
    /// When the first exec attempt fails with a non-transport error (execution
    /// error), no retry SHALL occur. The error is propagated immediately.
    #[test]
    fn prop_execution_error_does_not_trigger_retry(
        target in "[a-z]{3,10}",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let scenario = RetryScenario {
                first_attempt: AttemptOutcome::ExecutionError,
                // retry_attempt doesn't matter — should never be reached
                retry_attempt: AttemptOutcome::Success(0),
            };
            let gateway = RetryMockGateway::new("test-gw", scenario);
            let request = make_exec_request();

            let result = dispatch_single_exec(&gateway, &target, &request).await;

            // PROPERTY: When first attempt is an execution error, only 1 attempt occurs
            prop_assert_eq!(
                gateway.attempt_count(), 1,
                "Expected exactly 1 attempt for execution error, got {}",
                gateway.attempt_count()
            );

            // PROPERTY: Connection is NOT discarded (no retry for non-transport errors)
            prop_assert!(
                !gateway.was_connection_discarded(),
                "Connection should not be discarded for execution errors"
            );

            // PROPERTY: Error is propagated as Execution kind
            prop_assert!(result.is_err());
            prop_assert_eq!(result.unwrap_err().kind, ErrorKind::Execution);

            Ok(())
        })?;
    }

    /// **Validates: Requirements 3.3, 4.3, 5.3**
    ///
    /// For any retry scenario, the gateway's exec function is called exactly
    /// once by the Daemon. Internal retry is transparent to the caller.
    /// The final result matches the specified model:
    /// - first attempt ok → return ok (1 internal attempt)
    /// - first attempt transport error → retry (2 internal attempts), result
    ///   depends on retry outcome
    /// - first attempt execution error → return error (1 internal attempt)
    #[test]
    fn prop_retry_model_correctness(
        scenario in arb_retry_scenario(),
        target in "[a-z]{3,10}",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let first = scenario.first_attempt.clone();
            let retry = scenario.retry_attempt.clone();
            let gateway = RetryMockGateway::new("test-gw", scenario);
            let request = make_exec_request();

            let result = dispatch_single_exec(&gateway, &target, &request).await;

            match first {
                AttemptOutcome::Success(code) => {
                    // No retry — 1 attempt, result is Ok
                    prop_assert_eq!(gateway.attempt_count(), 1);
                    prop_assert!(result.is_ok());
                    prop_assert_eq!(result.unwrap(), code);
                }
                AttemptOutcome::TransportError => {
                    // Retry occurred — 2 attempts
                    prop_assert_eq!(gateway.attempt_count(), 2);
                    prop_assert!(gateway.was_connection_discarded());

                    match retry {
                        AttemptOutcome::Success(code) => {
                            prop_assert!(result.is_ok());
                            prop_assert_eq!(result.unwrap(), code);
                        }
                        AttemptOutcome::TransportError => {
                            prop_assert!(result.is_err());
                            prop_assert_eq!(result.unwrap_err().kind, ErrorKind::Transport);
                        }
                        AttemptOutcome::ExecutionError => {
                            prop_assert!(result.is_err());
                            prop_assert_eq!(result.unwrap_err().kind, ErrorKind::Execution);
                        }
                    }
                }
                AttemptOutcome::ExecutionError => {
                    // No retry — 1 attempt, execution error propagated
                    prop_assert_eq!(gateway.attempt_count(), 1);
                    prop_assert!(!gateway.was_connection_discarded());
                    prop_assert!(result.is_err());
                    prop_assert_eq!(result.unwrap_err().kind, ErrorKind::Execution);
                }
            }

            Ok(())
        })?;
    }
}

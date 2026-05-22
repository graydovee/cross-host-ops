use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tracing::warn;

use crate::protocol::AuthPromptMessage;

/// Routes authentication prompts upstream toward the CLI and delivers
/// responses back to the waiting caller by `prompt_id`.
///
/// The `ask` method sends an `AuthPromptMessage` upstream and blocks until
/// the corresponding response arrives via `deliver_response`.
pub struct AuthPromptRouter {
    upstream: UnboundedSender<AuthPromptMessage>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<String>>>>,
}

impl AuthPromptRouter {
    /// Create a new router that forwards prompts through `upstream`.
    pub fn new(upstream: UnboundedSender<AuthPromptMessage>) -> Self {
        Self {
            upstream,
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Send an authentication prompt upstream and wait for the response.
    ///
    /// Returns the user-provided response string, or an error if the upstream
    /// channel is closed or the response sender is dropped.
    pub async fn ask(&self, msg: AuthPromptMessage) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        let prompt_id = msg.prompt_id.clone();

        {
            let mut pending = self.pending.lock().await;
            pending.insert(prompt_id.clone(), tx);
        }

        self.upstream.send(msg).map_err(|_| {
            anyhow!("failed to send auth prompt upstream: channel closed")
        })?;

        rx.await.map_err(|_| {
            anyhow!(
                "auth prompt response channel dropped for prompt_id '{}'",
                prompt_id
            )
        })
    }

    /// Deliver a response for a previously issued prompt.
    ///
    /// If the `prompt_id` is not found in the pending map (e.g. it was already
    /// answered or never issued), a warning is logged and the value is dropped.
    pub async fn deliver_response(&self, prompt_id: &str, value: String) {
        let tx = {
            let mut pending = self.pending.lock().await;
            pending.remove(prompt_id)
        };

        match tx {
            Some(sender) => {
                if sender.send(value).is_err() {
                    warn!(
                        prompt_id = prompt_id,
                        "auth prompt receiver already dropped"
                    );
                }
            }
            None => {
                warn!(
                    prompt_id = prompt_id,
                    "deliver_response called for unknown prompt_id"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tokio::sync::mpsc;

    fn make_prompt(prompt_id: &str) -> AuthPromptMessage {
        AuthPromptMessage {
            prompt_id: prompt_id.to_string(),
            target_label: "test-target".to_string(),
            kind: "password".to_string(),
            secret: true,
            message: "Enter password:".to_string(),
        }
    }

    #[tokio::test]
    async fn ask_and_deliver_response_round_trip() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let router = AuthPromptRouter::new(tx);

        let prompt = make_prompt("p1");

        // Spawn ask in a background task
        let router_clone = Arc::new(router);
        let router_for_ask = Arc::clone(&router_clone);
        let handle = tokio::spawn(async move {
            router_for_ask.ask(prompt).await
        });

        // Receive the prompt upstream
        let received = rx.recv().await.expect("should receive prompt");
        assert_eq!(received.prompt_id, "p1");
        assert_eq!(received.kind, "password");
        assert!(received.secret);

        // Deliver the response
        router_clone.deliver_response("p1", "my-secret".to_string()).await;

        // The ask should resolve
        let result = handle.await.unwrap();
        assert_eq!(result.unwrap(), "my-secret");
    }

    #[tokio::test]
    async fn deliver_response_unknown_prompt_id_does_not_panic() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let router = AuthPromptRouter::new(tx);

        // Should not panic, just log a warning
        router.deliver_response("nonexistent", "value".to_string()).await;
    }

    #[tokio::test]
    async fn ask_fails_when_upstream_closed() {
        let (tx, rx) = mpsc::unbounded_channel::<AuthPromptMessage>();
        let router = AuthPromptRouter::new(tx);

        // Drop the receiver to close the channel
        drop(rx);

        let prompt = make_prompt("p2");
        let result = router.ask(prompt).await;
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("channel closed")
        );
    }

    #[tokio::test]
    async fn multiple_concurrent_prompts() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let router = Arc::new(AuthPromptRouter::new(tx));

        let r1 = Arc::clone(&router);
        let r2 = Arc::clone(&router);

        let h1 = tokio::spawn(async move {
            r1.ask(make_prompt("a")).await
        });
        let h2 = tokio::spawn(async move {
            r2.ask(make_prompt("b")).await
        });

        // Collect both prompts
        let msg1 = rx.recv().await.unwrap();
        let msg2 = rx.recv().await.unwrap();

        // Deliver responses (order may vary)
        router.deliver_response(&msg1.prompt_id, format!("reply-{}", msg1.prompt_id)).await;
        router.deliver_response(&msg2.prompt_id, format!("reply-{}", msg2.prompt_id)).await;

        let res1 = h1.await.unwrap().unwrap();
        let res2 = h2.await.unwrap().unwrap();

        assert_eq!(res1, "reply-a");
        assert_eq!(res2, "reply-b");
    }

    // -----------------------------------------------------------------------
    // Property-based tests
    // -----------------------------------------------------------------------

    // Feature: rhopd-jumpserver-architecture, Property 8: Auth-prompt forwarding identity

    /// Strategy for generating arbitrary AuthPromptMessage values.
    fn arb_auth_prompt_message() -> impl Strategy<Value = AuthPromptMessage> {
        (
            "[a-zA-Z0-9_\\-]{1,36}",   // prompt_id
            "[a-zA-Z0-9_\\-\\.]{1,30}", // target_label
            prop_oneof![
                Just("password".to_string()),
                Just("jump_mfa".to_string()),
                Just("host_key_trust".to_string()),
                Just("totp".to_string()),
            ],                           // kind
            any::<bool>(),               // secret
            "[ -~]{0,100}",              // message (printable ASCII)
        )
            .prop_map(|(prompt_id, target_label, kind, secret, message)| {
                AuthPromptMessage {
                    prompt_id,
                    target_label,
                    kind,
                    secret,
                    message,
                }
            })
    }

    /// Strategy for generating arbitrary response strings.
    fn arb_response_string() -> impl Strategy<Value = String> {
        "[ -~]{0,100}" // printable ASCII up to 100 chars
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

        /// **Validates: Requirements 8.1, 8.2, 8.4**
        ///
        /// For arbitrary `AuthPromptMessage p`, the message arriving at the
        /// upstream channel equals `p` byte-for-byte in
        /// `{prompt_id, target_label, kind, secret, message}`.
        /// For arbitrary response string `r`, a CLI reply with the same
        /// `prompt_id` is delivered to the originating daemon as a string
        /// equal to `r`.
        ///
        /// Tests at depth 1 (CLI ↔ local daemon): creates an AuthPromptRouter
        /// with an upstream channel, calls `ask(msg)`, verifies the message
        /// received upstream matches byte-for-byte, then delivers the response
        /// and verifies the `ask` call returns the exact value.
        #[test]
        fn prop_auth_prompt_forwarding_identity(
            msg in arb_auth_prompt_message(),
            response in arb_response_string(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let (tx, mut rx) = mpsc::unbounded_channel();
                let router = Arc::new(AuthPromptRouter::new(tx));

                let expected_prompt_id = msg.prompt_id.clone();
                let expected_target_label = msg.target_label.clone();
                let expected_kind = msg.kind.clone();
                let expected_secret = msg.secret;
                let expected_message = msg.message.clone();
                let expected_response = response.clone();

                // Spawn ask in a background task
                let router_for_ask = Arc::clone(&router);
                let handle = tokio::spawn(async move {
                    router_for_ask.ask(msg).await
                });

                // Receive the prompt on the upstream channel
                let received = rx.recv().await.expect("should receive prompt upstream");

                // Verify byte-for-byte identity of all fields
                prop_assert_eq!(&received.prompt_id, &expected_prompt_id);
                prop_assert_eq!(&received.target_label, &expected_target_label);
                prop_assert_eq!(&received.kind, &expected_kind);
                prop_assert_eq!(received.secret, expected_secret);
                prop_assert_eq!(&received.message, &expected_message);

                // Deliver the response using the same prompt_id
                router.deliver_response(&received.prompt_id, response).await;

                // Verify the ask call returns the exact response string
                let result = handle.await.unwrap();
                prop_assert!(result.is_ok(), "ask should succeed, got: {:?}", result.err());
                prop_assert_eq!(result.unwrap(), expected_response);

                Ok(())
            })?;
        }
    }
}

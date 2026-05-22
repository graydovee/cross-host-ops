use anyhow::{bail, Result};

/// Defaults applied when the input string omits user or port.
#[derive(Clone, Debug)]
pub struct AddressDefaults {
    pub user: String,
    pub port: u16,
}

/// A structured SSH-style remote address with explicit user, host, and port.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RemoteAddress {
    pub user: String,
    pub host: String,
    pub port: u16,
}

impl RemoteAddress {
    /// Parse `[user@]host[:port]`.
    ///
    /// - Empty input is rejected.
    /// - Empty host (e.g. `user@` or `user@:22`) is rejected.
    /// - If `user` is missing, fills `defaults.user`.
    /// - If `port` is missing, fills `defaults.port`.
    pub fn parse(input: &str, defaults: &AddressDefaults) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            bail!("address input is empty: {:?}", input);
        }

        let (user, host_port) = if let Some(at_pos) = trimmed.find('@') {
            let user_part = &trimmed[..at_pos];
            let rest = &trimmed[at_pos + 1..];
            (user_part.to_string(), rest)
        } else {
            (String::new(), trimmed)
        };

        let (host, port) = if let Some(colon_pos) = host_port.rfind(':') {
            let host_part = &host_port[..colon_pos];
            let port_str = &host_port[colon_pos + 1..];
            let port: u16 = port_str
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid port in address {:?}", input))?;
            (host_part.to_string(), Some(port))
        } else {
            (host_port.to_string(), None)
        };

        if host.is_empty() {
            bail!("empty host in address {:?}", input);
        }

        let effective_user = if user.is_empty() {
            defaults.user.clone()
        } else {
            user
        };

        let effective_port = port.unwrap_or(defaults.port);

        Ok(RemoteAddress {
            user: effective_user,
            host,
            port: effective_port,
        })
    }

    /// Produces the canonical `user@host:port` form.
    ///
    /// Round-trips with `parse` when `user` is non-empty.
    pub fn format(&self) -> String {
        format!("{}@{}:{}", self.user, self.host, self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn defaults() -> AddressDefaults {
        AddressDefaults {
            user: "default_user".to_string(),
            port: 22,
        }
    }

    #[test]
    fn parse_full_address() {
        let addr = RemoteAddress::parse("alice@example.com:2222", &defaults()).unwrap();
        assert_eq!(addr.user, "alice");
        assert_eq!(addr.host, "example.com");
        assert_eq!(addr.port, 2222);
    }

    #[test]
    fn parse_host_only_fills_defaults() {
        let addr = RemoteAddress::parse("example.com", &defaults()).unwrap();
        assert_eq!(addr.user, "default_user");
        assert_eq!(addr.host, "example.com");
        assert_eq!(addr.port, 22);
    }

    #[test]
    fn parse_user_and_host_fills_default_port() {
        let addr = RemoteAddress::parse("bob@myhost", &defaults()).unwrap();
        assert_eq!(addr.user, "bob");
        assert_eq!(addr.host, "myhost");
        assert_eq!(addr.port, 22);
    }

    #[test]
    fn parse_host_and_port_fills_default_user() {
        let addr = RemoteAddress::parse("myhost:8022", &defaults()).unwrap();
        assert_eq!(addr.user, "default_user");
        assert_eq!(addr.host, "myhost");
        assert_eq!(addr.port, 8022);
    }

    #[test]
    fn parse_rejects_empty_input() {
        let result = RemoteAddress::parse("", &defaults());
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("empty"), "error should mention empty: {msg}");
    }

    #[test]
    fn parse_rejects_whitespace_only() {
        let result = RemoteAddress::parse("   ", &defaults());
        assert!(result.is_err());
    }

    #[test]
    fn parse_rejects_empty_host_with_user() {
        let result = RemoteAddress::parse("user@", &defaults());
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("empty host"),
            "error should mention empty host: {msg}"
        );
    }

    #[test]
    fn parse_rejects_empty_host_with_user_and_port() {
        let result = RemoteAddress::parse("user@:22", &defaults());
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("empty host"),
            "error should mention empty host: {msg}"
        );
    }

    #[test]
    fn parse_rejects_invalid_port() {
        let result = RemoteAddress::parse("host:notaport", &defaults());
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("invalid port"),
            "error should mention invalid port: {msg}"
        );
    }

    #[test]
    fn format_canonical() {
        let addr = RemoteAddress {
            user: "alice".to_string(),
            host: "example.com".to_string(),
            port: 2222,
        };
        assert_eq!(addr.format(), "alice@example.com:2222");
    }

    #[test]
    fn round_trip_with_non_empty_user() {
        let original = RemoteAddress {
            user: "deploy".to_string(),
            host: "prod.internal".to_string(),
            port: 443,
        };
        let formatted = original.format();
        let parsed = RemoteAddress::parse(&formatted, &defaults()).unwrap();
        assert_eq!(parsed, original);
    }

    // Feature: rhopd-jumpserver-architecture, Property 9: RemoteAddress parser round-trip and default-filling

    /// Strategy for generating non-empty strings that are valid for user/host fields
    /// (no '@', ':', or whitespace).
    fn valid_identifier() -> impl Strategy<Value = String> {
        "[a-zA-Z0-9._-]{1,20}".prop_map(|s| s)
    }

    /// Strategy for generating a valid port number (1-65535).
    fn valid_port() -> impl Strategy<Value = u16> {
        1u16..=65535u16
    }

    /// Strategy for generating arbitrary `RemoteAddress` values with non-empty user,
    /// non-empty host, and valid port.
    fn arb_remote_address() -> impl Strategy<Value = RemoteAddress> {
        (valid_identifier(), valid_identifier(), valid_port()).prop_map(|(user, host, port)| {
            RemoteAddress { user, host, port }
        })
    }

    /// Strategy for generating arbitrary `AddressDefaults`.
    fn arb_address_defaults() -> impl Strategy<Value = AddressDefaults> {
        (valid_identifier(), valid_port()).prop_map(|(user, port)| AddressDefaults { user, port })
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

        /// **Validates: Requirements 11.1, 11.2, 11.3**
        ///
        /// Round-trip: For any `RemoteAddress` with non-empty user,
        /// `parse(addr.format(), any_defaults) == addr` regardless of defaults.
        #[test]
        fn prop_round_trip(addr in arb_remote_address(), defaults in arb_address_defaults()) {
            let formatted = addr.format();
            let parsed = RemoteAddress::parse(&formatted, &defaults).unwrap();
            prop_assert_eq!(parsed, addr);
        }

        /// **Validates: Requirements 11.1, 11.4, 11.5**
        ///
        /// Default-filling: For input strings that omit user and/or port,
        /// the parser fills `defaults.user` / `defaults.port` and remaining
        /// fields equal the input's fields.
        #[test]
        fn prop_default_filling(
            user in valid_identifier(),
            host in valid_identifier(),
            port in valid_port(),
            defaults in arb_address_defaults(),
            form in 0u8..4u8,
        ) {
            // Generate input strings in various forms:
            // 0: host only (omits user and port)
            // 1: host:port (omits user)
            // 2: user@host (omits port)
            // 3: user@host:port (full, no defaults needed)
            let (input, expected_user, expected_port) = match form {
                0 => {
                    // Just host — parser fills both defaults
                    (host.clone(), defaults.user.clone(), defaults.port)
                }
                1 => {
                    // host:port — parser fills default user
                    (format!("{}:{}", host, port), defaults.user.clone(), port)
                }
                2 => {
                    // user@host — parser fills default port
                    (format!("{}@{}", user, host), user.clone(), defaults.port)
                }
                _ => {
                    // user@host:port — no defaults needed
                    (format!("{}@{}:{}", user, host, port), user.clone(), port)
                }
            };

            let parsed = RemoteAddress::parse(&input, &defaults).unwrap();
            prop_assert_eq!(&parsed.host, &host);
            prop_assert_eq!(&parsed.user, &expected_user);
            prop_assert_eq!(parsed.port, expected_port);
        }
    }
}

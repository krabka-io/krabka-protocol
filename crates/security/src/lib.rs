//! Security primitives used by Krabka brokers, clients, and tooling.
//!
//! The crate owns the protocol-independent pieces of TLS, SASL, SCRAM,
//! OAuth/OIDC, mTLS principal extraction, delegation-token HMACs, Kerberos
//! exchange state, and principal modelling. Network I/O stays in the caller.
//! The async [`IntrospectionClient`] trait represents OAuth introspection, and
//! the broker and client crates supply the concrete transports and wire the
//! resulting validators into listener or connection handshakes.
//!
//! ## SASL/PLAIN verification
//!
//! ```rust
//! use std::collections::HashMap;
//!
//! use krabka_security::{AuthMethod, verify_plain};
//!
//! let mut users = HashMap::new();
//! users.insert("alice".to_string(), "wonderland".to_string());
//!
//! let principal = verify_plain(&users, "alice", b"wonderland").unwrap();
//! assert_eq!(principal.name, "alice");
//! assert_eq!(principal.auth_method, AuthMethod::SaslPlain);
//! ```
//!
//! ## Storing SCRAM credentials
//!
//! ```rust
//! use krabka_security::{SaslMechanism, hash_scram_password};
//!
//! let credential = hash_scram_password(
//!     b"correct horse battery staple",
//!     SaslMechanism::ScramSha512,
//!     4096,
//! );
//! assert_eq!(credential.iterations, 4096);
//! ```
//!
//! ## Cargo features
//!
//! - `gssapi` (off by default): SASL/GSSAPI (Kerberos), the `gssapi` module.
//!   It is native-only, and has no effect on `wasm32` targets.
//!
//! `gssapi` stays out of `default` on purpose. The Kerberos stack it pulls in,
//! `krabka-sspi`, depends on `rsa` 0.10, which carries RUSTSEC-2023-0071 (the
//! Marvin timing side channel) and has no patched release. A build without the
//! feature compiles no `rsa` at all. A build with it is still not exposed,
//! because Krabka's GSSAPI path uses only AES keytab keys and never
//! runs an RSA private-key operation; `docs/rustsec-2023-0071.md` in the
//! repository gives the reachability analysis.
//!
//! Without the feature, [`SaslMechanism::Gssapi`] still parses and still names
//! the mechanism in persisted metadata, but [`SaslMechanism::is_available`]
//! returns `false` for it and [`SaslMechanism::require_available`] returns
//! [`AuthError::UnsupportedMechanism`]. A broker maps that to Kafka's
//! `UNSUPPORTED_SASL_MECHANISM` (error code 33) in the `SaslHandshake` response,
//! and a client reports it before it opens a connection.

pub mod ca;
pub mod delegation_token;
#[cfg(all(feature = "gssapi", not(target_family = "wasm")))]
pub mod gssapi;
pub mod java_regex;
mod jwks;
mod jwks_trust;
mod listener;
mod mechanism;
mod mtls;
mod oauthbearer;
mod plain;
mod principal;
mod reload;
pub mod scram;
mod tls;

pub use delegation_token::{SecretBytes, compute_token_hmac};
pub use jwks::{Jwks, JwksHandle};
pub use jwks_trust::{JwksTrustError, build_client_config_from_pem};
pub use listener::ListenerProtocol;
pub use mechanism::SaslMechanism;
pub use mtls::extract_principal_from_cert;
pub use oauthbearer::{
    AuthOutcome, ClientInitialResponse, IntrospectionClient, IntrospectionError,
    IntrospectionValidator, InvalidScopeItem, OAuthBearerValidator, SignedJwsValidator,
    UnsecuredJwsValidator, oauth_error_json, parse_client_initial_response, parse_required_scope,
};
pub use plain::verify_plain;
pub use principal::{AuthError, AuthMethod, KafkaPrincipal, Principal};
pub use reload::DynamicServerConfig;
pub use scram::{
    ScramClientExchange, ScramClientFirst, ScramCredential, ScramServerExchange, StepResult,
    TOKEN_AUTH_EXTENSION, decode_saslname, derive_keys_from_salted, encode_saslname,
    hash_scram_password, pbkdf2_salted, scram_hash_len,
};
pub use tls::{ClientAuthMode, TlsConfig, TlsError};

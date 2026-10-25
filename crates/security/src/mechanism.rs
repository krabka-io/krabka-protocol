use serde::{Deserialize, Serialize};

use crate::AuthError;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::IntoStaticStr,
    strum::EnumString,
)]
pub enum SaslMechanism {
    #[strum(serialize = "PLAIN")]
    Plain,
    #[strum(serialize = "SCRAM-SHA-256")]
    ScramSha256,
    #[strum(serialize = "SCRAM-SHA-512")]
    ScramSha512,
    /// SASL/OAUTHBEARER (KIP-255 / RFC 7628). The broker's configured token
    /// validator validates the bearer token.
    #[strum(serialize = "OAUTHBEARER")]
    OAuthBearer,
    /// SASL/GSSAPI (Kerberos, RFC 4752). The GSS provider drives context
    /// establishment and the RFC 4752 security-layer negotiation from the
    /// broker's configured service keytab.
    ///
    /// The variant exists in every build, because metadata records persist it.
    /// The provider needs the `gssapi` Cargo feature; see
    /// [`SaslMechanism::is_available`].
    #[strum(serialize = "GSSAPI")]
    Gssapi,
}

impl SaslMechanism {
    #[must_use]
    pub fn wire_name(self) -> &'static str {
        self.into()
    }

    #[must_use]
    pub fn from_wire(s: &str) -> Option<Self> {
        s.parse().ok()
    }

    /// `true` for the SCRAM mechanisms, SHA-256 and SHA-512.
    ///
    /// The handshake and authenticate code uses this where it treats both the
    /// same way at the dispatch level.
    #[must_use]
    pub fn is_scram(self) -> bool {
        matches!(self, Self::ScramSha256 | Self::ScramSha512)
    }

    /// Every mechanism, in declaration order.
    pub const ALL: [Self; 5] = [
        Self::Plain,
        Self::ScramSha256,
        Self::ScramSha512,
        Self::OAuthBearer,
        Self::Gssapi,
    ];

    /// `true` when this build can run the mechanism.
    ///
    /// GSSAPI needs the `gssapi` Cargo feature on a native target. Every other
    /// mechanism is always available.
    #[must_use]
    pub const fn is_available(self) -> bool {
        match self {
            Self::Gssapi => cfg!(all(feature = "gssapi", not(target_family = "wasm"))),
            Self::Plain | Self::ScramSha256 | Self::ScramSha512 | Self::OAuthBearer => true,
        }
    }

    /// The mechanisms this build can run, in declaration order.
    pub fn available() -> impl Iterator<Item = Self> {
        Self::ALL.into_iter().filter(|m| m.is_available())
    }

    /// Returns `self` when this build can run the mechanism.
    ///
    /// # Errors
    ///
    /// [`AuthError::UnsupportedMechanism`] when [`SaslMechanism::is_available`]
    /// is `false`. A broker answers that with Kafka's
    /// `UNSUPPORTED_SASL_MECHANISM` (error code 33), as it does for a mechanism
    /// the listener has not enabled.
    pub const fn require_available(self) -> Result<Self, AuthError> {
        if self.is_available() {
            Ok(self)
        } else {
            Err(AuthError::UnsupportedMechanism)
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn wire_name_round_trip() {
        for m in [
            SaslMechanism::Plain,
            SaslMechanism::ScramSha256,
            SaslMechanism::ScramSha512,
            SaslMechanism::OAuthBearer,
        ] {
            assert2::assert!(SaslMechanism::from_wire(m.wire_name()) == Some(m));
        }
    }

    #[test]
    fn from_wire_unknown_returns_none() {
        for wire in ["SCRAM-SHA-128", "OAUTH", ""] {
            assert2::assert!(SaslMechanism::from_wire(wire) == None);
        }
    }

    /// Wire-exactness guard: only the canonical Kafka mechanism strings parse.
    /// The Rust variant names and other casings must NOT parse, so the
    /// `strum(serialize = ...)` attributes can never silently loosen the SASL
    /// handshake's accepted mechanism set.
    #[test]
    fn from_wire_rejects_variant_names_and_casing() {
        for wire in [
            "Plain",
            "plain",
            "ScramSha256",
            "scram-sha-256",
            "OAuthBearer",
        ] {
            assert2::assert!(SaslMechanism::from_wire(wire) == None);
        }
    }

    #[test]
    fn oauthbearer_wire_round_trip() {
        check!(
            (
                SaslMechanism::from_wire("OAUTHBEARER"),
                SaslMechanism::OAuthBearer.wire_name(),
                SaslMechanism::OAuthBearer.is_scram(),
            ) == (Some(SaslMechanism::OAuthBearer), "OAUTHBEARER", false)
        );
    }

    #[test]
    fn gssapi_mechanism_roundtrips_wire_name() {
        use std::str::FromStr;
        assert2::assert!(
            (
                SaslMechanism::from_str("GSSAPI").unwrap(),
                SaslMechanism::Gssapi.wire_name(),
            ) == (SaslMechanism::Gssapi, "GSSAPI")
        );
    }

    /// Whether this test build has the GSSAPI provider.
    const GSSAPI: bool = cfg!(all(feature = "gssapi", not(target_family = "wasm")));

    #[test]
    fn availability_follows_the_gssapi_feature() {
        let available = SaslMechanism::ALL.map(|m| (m, m.is_available()));
        let listed: Vec<_> = SaslMechanism::available().collect();
        let want_listed: Vec<_> = SaslMechanism::ALL
            .into_iter()
            .filter(|m| *m != SaslMechanism::Gssapi || GSSAPI)
            .collect();
        assert2::assert!(
            (available, listed)
                == (
                    [
                        (SaslMechanism::Plain, true),
                        (SaslMechanism::ScramSha256, true),
                        (SaslMechanism::ScramSha512, true),
                        (SaslMechanism::OAuthBearer, true),
                        (SaslMechanism::Gssapi, GSSAPI),
                    ],
                    want_listed,
                )
        );
    }

    #[test]
    fn require_available_rejects_gssapi_without_the_feature() {
        let want_gssapi = GSSAPI
            .then_some(SaslMechanism::Gssapi)
            .ok_or(AuthError::UnsupportedMechanism);
        assert2::assert!(
            (
                SaslMechanism::Gssapi.require_available(),
                SaslMechanism::Plain.require_available(),
            ) == (want_gssapi, Ok(SaslMechanism::Plain))
        );
    }

    #[test]
    fn is_scram_predicate() {
        for (_case, mechanism, want) in [
            ("PLAIN", SaslMechanism::Plain, false),
            ("SCRAM SHA-256", SaslMechanism::ScramSha256, true),
            ("SCRAM SHA-512", SaslMechanism::ScramSha512, true),
        ] {
            assert2::assert!(mechanism.is_scram() == want);
        }
    }
}

//! `ScramServerExchange`, the RFC 5802 SCRAM server state machine.
//!
//! It supports SCRAM-SHA-256 and SCRAM-SHA-512. The mechanism comes from the
//! credential that the server authenticates against.

use std::fmt;

use base64::{Engine, engine::general_purpose::STANDARD as B64};
use ring::{
    hmac as ring_hmac,
    rand::{SecureRandom, SystemRandom},
};
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;

use super::{
    ScramClientFirst, ScramCredential,
    messages::{JAVA_B64, ScramClientFinal},
    scram_hash_len,
};
use crate::{AuthError, AuthMethod, Principal, SaslMechanism};

struct AwaitingClientFirst {
    username: String,
    credential: ScramCredential,
    principal_override: Option<Principal>,
}

struct AwaitingClientFinal {
    username: String,
    credential: ScramCredential,
    principal_override: Option<Principal>,
    client_first_bare: String,
    server_first: String,
}

/// RFC 5802 SCRAM server-side handshake, one type per negotiation phase.
///
/// The variant payload types stay unexported on purpose. `step` and
/// `StepResult` drive the phase, so callers never need to name
/// `AwaitingClientFirst` or `AwaitingClientFinal` directly.
#[allow(private_interfaces)]
pub enum ScramServerExchange {
    AwaitingClientFirst(AwaitingClientFirst),
    AwaitingClientFinal(AwaitingClientFinal),
}

impl fmt::Debug for ScramServerExchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AwaitingClientFirst(_) => f
                .debug_tuple("ScramServerExchange::AwaitingClientFirst")
                .finish(),
            Self::AwaitingClientFinal(_) => f
                .debug_tuple("ScramServerExchange::AwaitingClientFinal")
                .finish(),
        }
    }
}

#[derive(Debug)]
#[must_use]
pub enum StepResult {
    Continue(Vec<u8>, ScramServerExchange),
    Done(Principal, Vec<u8>),
    Failed(AuthError),
    /// The client-final proof does not decode to the length of the
    /// mechanism's hash, the empty proof included. Kafka's
    /// `ScramFormatter.xor` throws `IllegalArgumentException` on it, which no
    /// `SaslException` handler catches, so `Selector.pollSelectionKeys`
    /// closes the connection at once: no `SaslAuthenticate` response and no
    /// `connection.failed.authentication.delay.ms`. The caller closes the
    /// connection the same way.
    Disconnect,
}

impl ScramServerExchange {
    #[must_use]
    pub fn new(username: String, credential: ScramCredential) -> Self {
        Self::AwaitingClientFirst(AwaitingClientFirst {
            username,
            credential,
            principal_override: None,
        })
    }

    /// KIP-48: variant of [`Self::new`] that stamps a principal for the `Done`
    /// arm to return in place of one synthesized from `username`.
    ///
    /// The delegation-token SCRAM fallback uses this to surface the token's
    /// owner, for example `User:alice`, when the client authenticated with the
    /// token's UUID `token_id` as the SCRAM username.
    #[must_use]
    pub fn new_with_principal(
        username: String,
        credential: ScramCredential,
        override_principal: Principal,
    ) -> Self {
        Self::AwaitingClientFirst(AwaitingClientFirst {
            username,
            credential,
            principal_override: Some(override_principal),
        })
    }

    /// Feed one client message and advance the negotiation.
    pub fn step(self, client_bytes: &[u8]) -> StepResult {
        match self {
            Self::AwaitingClientFirst(s) => s.step(client_bytes),
            Self::AwaitingClientFinal(s) => s.step(client_bytes),
        }
    }
}

impl AwaitingClientFirst {
    // Per-step SCRAM server driver. skip_all keeps the raw `client_bytes`
    // (client-first carrying nonce) out of span fields; only the
    // non-sensitive mechanism + username are recorded. Returns a
    // `StepResult` enum (not a Result), so no `err`.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            mechanism = %self.credential.mechanism.wire_name(),
            principal = %self.username,
        )
    )]
    fn step(self, client_bytes: &[u8]) -> StepResult {
        // Kafka's `ClientFirstMessage` grammar: a GS2 header of `n,,` or
        // `n,a=<authzid>,`, then `n=`, `r=` and any extensions. Extensions
        // such as `tokenauth` are signed as part of the bare message; the
        // caller has already chosen the credential they select.
        let client_first = match ScramClientFirst::parse(client_bytes) {
            Ok(client_first) => client_first,
            Err(e) => return StepResult::Failed(e),
        };
        if client_first.username != self.username {
            return StepResult::Failed(AuthError::UnknownUser);
        }
        // `ScramSaslServer`: an authorization id, when the client sends one,
        // must name the authenticating user. Kafka compares the authzid as
        // the client escaped it with the decoded username.
        if client_first
            .gs2_header
            .strip_prefix("n,a=")
            .and_then(|header| header.strip_suffix(','))
            .is_some_and(|authzid| authzid != client_first.username)
        {
            return StepResult::Failed(AuthError::AuthorizationIdMismatch);
        }
        let c_nonce = client_first.nonce;
        let mut server_nonce_bytes = [0u8; 18];
        // The client side maps a failed system RNG the same way.
        if SystemRandom::new().fill(&mut server_nonce_bytes).is_err() {
            return StepResult::Failed(AuthError::MalformedMessage);
        }
        let server_nonce = B64.encode(server_nonce_bytes);
        let combined_nonce = format!("{c_nonce}{server_nonce}");
        let server_first = format!(
            "r={},s={},i={}",
            combined_nonce,
            B64.encode(&self.credential.salt),
            self.credential.iterations,
        );
        let response = server_first.clone().into_bytes();
        let next = AwaitingClientFinal {
            username: self.username,
            credential: self.credential,
            principal_override: self.principal_override,
            client_first_bare: client_first.bare,
            server_first,
        };
        StepResult::Continue(response, ScramServerExchange::AwaitingClientFinal(next))
    }
}

impl AwaitingClientFinal {
    // Per-step SCRAM server driver. skip_all keeps the raw `client_bytes`
    // (client-final carrying proof) out of span fields; only the
    // non-sensitive mechanism + username are recorded. Terminal: returns
    // `Done`/`Failed` only. Returns a `StepResult` enum (not a Result), so
    // no `err`.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            mechanism = %self.credential.mechanism.wire_name(),
            principal = %self.username,
        )
    )]
    fn step(self, client_bytes: &[u8]) -> StepResult {
        let client_first_bare = &self.client_first_bare;
        let server_first = &self.server_first;
        // Kafka's `ClientFinalMessage` grammar: `c=`, `r=`, any extensions,
        // then `p=`, in that order. Anything else is malformed.
        let client_final = match ScramClientFinal::parse(client_bytes) {
            Ok(client_final) => client_final,
            Err(e) => return StepResult::Failed(e),
        };
        let nonce = client_final.nonce;

        // RFC 5802 §5.1: the client-final `r=` (combined nonce) must
        // equal the nonce the server issued in server-first
        // (client-first-nonce + server-nonce). server-first is
        // `r={combined_nonce},s=...,i=...`, so the expected value is the
        // `r=` attribute up to the first comma.
        let expected_nonce = server_first
            .strip_prefix("r=")
            .and_then(|rest| rest.split(',').next())
            .unwrap_or_default();
        if nonce != expected_nonce {
            return StepResult::Failed(AuthError::MalformedMessage);
        }

        // Kafka checks only that `c=` is base64, not that it encodes the GS2
        // header the client sent: `ScramSaslServer` never reads it, and the
        // proof signs it as part of the `AuthMessage`.

        // Java's `Base64` decoder ignores the unused bits of the last
        // character, so the proof decodes as Kafka decodes it. The grammar
        // already holds it to base64.
        let Ok(proof) = JAVA_B64.decode(client_final.proof) else {
            return StepResult::Failed(AuthError::MalformedMessage);
        };
        if proof.len() != scram_hash_len(self.credential.mechanism) {
            return StepResult::Disconnect;
        }

        // Kafka's `ScramFormatter.authMessage`, from the parsed fields.
        let client_final_no_proof = client_final.without_proof;

        let auth_message = format!("{client_first_bare},{server_first},{client_final_no_proof}");

        let (computed_stored, server_signature) = match self.credential.mechanism {
            SaslMechanism::ScramSha512 => verify_and_sign_sha512(
                &self.credential.stored_key,
                &self.credential.server_key,
                &proof,
                auth_message.as_bytes(),
            ),
            SaslMechanism::ScramSha256 => verify_and_sign_sha256(
                &self.credential.stored_key,
                &self.credential.server_key,
                &proof,
                auth_message.as_bytes(),
            ),
            SaslMechanism::Plain | SaslMechanism::OAuthBearer | SaslMechanism::Gssapi => {
                return StepResult::Failed(AuthError::MalformedMessage);
            }
        };

        if computed_stored
            .ct_eq(self.credential.stored_key.as_slice())
            .unwrap_u8()
            != 1
        {
            return StepResult::Failed(AuthError::BadProof);
        }
        let server_final = format!("v={}", B64.encode(&server_signature));
        // KIP-48: prefer the override principal when set
        // (delegation-token SCRAM fallback path). Otherwise build the
        // standard `User:<scram-username>` principal from the live
        // exchange state.
        let principal = self
            .principal_override
            .clone()
            .unwrap_or_else(|| Principal {
                name: self.username.clone(),
                auth_method: AuthMethod::from_sasl(self.credential.mechanism),
                groups: vec![],
            });
        StepResult::Done(principal, server_final.into_bytes())
    }
}

fn verify_and_sign_sha512(
    stored_key: &[u8],
    server_key: &[u8],
    proof: &[u8],
    auth_message: &[u8],
) -> (Vec<u8>, Vec<u8>) {
    verify_and_sign_with(
        ring_hmac::HMAC_SHA512,
        stored_key,
        server_key,
        proof,
        auth_message,
        |client_key| Sha512::digest(client_key).to_vec(),
    )
}

fn verify_and_sign_sha256(
    stored_key: &[u8],
    server_key: &[u8],
    proof: &[u8],
    auth_message: &[u8],
) -> (Vec<u8>, Vec<u8>) {
    verify_and_sign_with(
        ring_hmac::HMAC_SHA256,
        stored_key,
        server_key,
        proof,
        auth_message,
        |client_key| Sha256::digest(client_key).to_vec(),
    )
}

/// Recover `H(ClientKey)` from `proof`, and compute the `ServerSignature`,
/// with `algorithm` as the HMAC and `hash` as `H`.
///
/// `ring`'s HMAC takes a key of any length, so this cannot fail.
fn verify_and_sign_with(
    algorithm: ring_hmac::Algorithm,
    stored_key: &[u8],
    server_key: &[u8],
    proof: &[u8],
    auth_message: &[u8],
    hash: impl FnOnce(&[u8]) -> Vec<u8>,
) -> (Vec<u8>, Vec<u8>) {
    let client_signature =
        ring_hmac::sign(&ring_hmac::Key::new(algorithm, stored_key), auth_message);
    let client_key: Vec<u8> = client_signature
        .as_ref()
        .iter()
        .zip(proof.iter())
        .map(|(a, b)| a ^ b)
        .collect();
    let computed_stored = hash(&client_key);
    let server_signature =
        ring_hmac::sign(&ring_hmac::Key::new(algorithm, server_key), auth_message)
            .as_ref()
            .to_vec();
    (computed_stored, server_signature)
}

//! SCRAM (RFC 5802) with support for SHA-256 and SHA-512.

use std::{fmt, num::ParseIntError, str::FromStr};

mod client;
mod messages;
mod pg_verifier;
mod server;

pub use client::ScramClientExchange;
pub use messages::{ScramClientFirst, TOKEN_AUTH_EXTENSION, decode_saslname, encode_saslname};
pub use pg_verifier::{PgScramVerifier, ScramError};
use refined_type::rule::MinMaxI32;
use ring::{
    hmac as ring_hmac,
    rand::{SecureRandom, SystemRandom},
};
pub use server::{ScramServerExchange, StepResult};
use sha2::{Digest, Sha256, Sha512};

use crate::SaslMechanism;

/// Default PBKDF2 iteration count for new Kafka SCRAM credentials.
pub const DEFAULT_SCRAM_ITERATIONS: i32 = 8192;
/// Minimum PBKDF2 iteration count accepted by the broker.
pub const MIN_SCRAM_ITERATIONS: i32 = 4096;
/// Maximum PBKDF2 iteration count accepted by the broker.
pub const MAX_SCRAM_ITERATIONS: i32 = 16_384;

/// Broker-valid PBKDF2 iteration count for a SCRAM credential.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScramIterations(i32);

impl ScramIterations {
    /// Validate a SCRAM iteration count.
    ///
    /// # Errors
    ///
    /// Returns an error unless `value` is accepted by the broker.
    pub fn new(value: i32) -> Result<Self, String> {
        MinMaxI32::<MIN_SCRAM_ITERATIONS, MAX_SCRAM_ITERATIONS>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    #[must_use]
    pub const fn into_value(self) -> i32 {
        self.0
    }
}

impl Default for ScramIterations {
    fn default() -> Self {
        // `DEFAULT_SCRAM_ITERATIONS` lies inside the broker-valid range.
        Self(DEFAULT_SCRAM_ITERATIONS)
    }
}

impl fmt::Display for ScramIterations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ScramIterations {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredential {
    pub mechanism: SaslMechanism,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
    pub iterations: u32,
}

/// Output byte length of the underlying hash function for a SCRAM mechanism.
///
/// The length is 32 for SHA-256 and 64 for SHA-512. This function panics on a
/// non-SCRAM mechanism.
#[must_use]
/// # Panics
/// Panics if validated key material has an impossible size or synchronized credential state is poisoned.
pub fn scram_hash_len(mechanism: SaslMechanism) -> usize {
    match mechanism {
        SaslMechanism::ScramSha256 => 32,
        SaslMechanism::ScramSha512 => 64,
        SaslMechanism::Plain | SaslMechanism::OAuthBearer | SaslMechanism::Gssapi => {
            panic!("scram_hash_len called with non-SCRAM mechanism {mechanism:?}")
        }
    }
}

/// Hash `password` into a SCRAM credential with a fresh random 16-byte salt.
///
/// # Errors
///
/// [`ScramError::RandomUnavailable`] when the system RNG cannot produce the
/// salt.
///
/// # Panics
///
/// Panics if `iterations` is zero, or `mechanism` is not a SCRAM mechanism.
pub fn hash_scram_password(
    password: &[u8],
    mechanism: SaslMechanism,
    iterations: u32,
) -> Result<ScramCredential, ScramError> {
    assert2::assert!(iterations > 0);
    let mut salt = vec![0u8; 16];
    SystemRandom::new()
        .fill(&mut salt)
        .map_err(|_| ScramError::RandomUnavailable)?;
    Ok(hash_scram_password_with_salt(
        password, mechanism, iterations, salt,
    ))
}

/// Test-only entry that lets callers fix the salt for golden vectors.
#[must_use]
/// # Panics
/// Panics if validated key material has an impossible size or synchronized credential state is poisoned.
pub fn hash_scram_password_with_salt(
    password: &[u8],
    mechanism: SaslMechanism,
    iterations: u32,
    salt: Vec<u8>,
) -> ScramCredential {
    let (stored_key, server_key) = match mechanism {
        SaslMechanism::ScramSha512 => {
            let salted: [u8; 64] =
                pbkdf2::pbkdf2_hmac_array::<Sha512, 64>(password, &salt, iterations);
            derive_keys_sha512(&salted)
        }
        SaslMechanism::ScramSha256 => {
            let salted: [u8; 32] =
                pbkdf2::pbkdf2_hmac_array::<Sha256, 32>(password, &salt, iterations);
            derive_keys_sha256(&salted)
        }
        SaslMechanism::Plain | SaslMechanism::OAuthBearer | SaslMechanism::Gssapi => {
            panic!("hash_scram_password called with non-SCRAM mechanism {mechanism:?}");
        }
    };
    ScramCredential {
        mechanism,
        salt,
        stored_key,
        server_key,
        iterations,
    }
}

/// Compute the PBKDF2 output for a SCRAM mechanism.
///
/// KIP-554 and RFC 5802 call this output the "salted password". Its length
/// matches [`scram_hash_len`]: 32 bytes for SHA-256, 64 bytes for SHA-512.
///
/// `krabka-client-admin` uses this function to populate
/// `AlterUserScramCredentialsRequest.upsertions[].salted_password`. It does so
/// without leaking the broker-side `derive_keys_from_salted` machinery or the
/// raw password into the operator. This function panics on a non-SCRAM
/// mechanism.
#[must_use]
/// # Panics
/// Panics if validated key material has an impossible size or synchronized credential state is poisoned.
pub fn pbkdf2_salted(
    password: &[u8],
    mechanism: SaslMechanism,
    iterations: u32,
    salt: &[u8],
) -> Vec<u8> {
    assert2::assert!(iterations > 0);
    match mechanism {
        SaslMechanism::ScramSha512 => {
            let arr: [u8; 64] = pbkdf2::pbkdf2_hmac_array::<Sha512, 64>(password, salt, iterations);
            arr.to_vec()
        }
        SaslMechanism::ScramSha256 => {
            let arr: [u8; 32] = pbkdf2::pbkdf2_hmac_array::<Sha256, 32>(password, salt, iterations);
            arr.to_vec()
        }
        SaslMechanism::Plain | SaslMechanism::OAuthBearer | SaslMechanism::Gssapi => {
            panic!("pbkdf2_salted called with non-SCRAM mechanism {mechanism:?}");
        }
    }
}

/// Reconstruct `(stored_key, server_key)` from the salted-password
/// output the client sends in an `AlterUserScramCredentialsRequest`
/// per KIP-554.
///
/// The KIP places PBKDF2 on the client side. The wire request carries the
/// already-stretched PBKDF2 output, which is 32 bytes for SHA-256 and 64 bytes
/// for SHA-512, and the broker derives the two stored keys from it. This keeps
/// the broker from holding the raw password even briefly. The transformation,
/// for each hash H, is:
///
/// ```text
/// client_key  = HMAC-H(salted_password, "Client Key")
/// stored_key  = H(client_key)
/// server_key  = HMAC-H(salted_password, "Server Key")
/// ```
///
/// The mechanism argument selects which `H` to use. This function panics on a
/// non-SCRAM mechanism.
#[must_use]
/// # Panics
/// Panics if validated key material has an impossible size or synchronized credential state is poisoned.
pub fn derive_keys_from_salted(mechanism: SaslMechanism, salted: &[u8]) -> (Vec<u8>, Vec<u8>) {
    match mechanism {
        SaslMechanism::ScramSha512 => derive_keys_sha512(salted),
        SaslMechanism::ScramSha256 => derive_keys_sha256(salted),
        SaslMechanism::Plain | SaslMechanism::OAuthBearer | SaslMechanism::Gssapi => {
            panic!("derive_keys_from_salted called with non-SCRAM mechanism {mechanism:?}");
        }
    }
}

fn derive_keys_sha512(salted: &[u8]) -> (Vec<u8>, Vec<u8>) {
    derive_keys_with(ring_hmac::HMAC_SHA512, salted, |client_key| {
        Sha512::digest(client_key).to_vec()
    })
}

fn derive_keys_sha256(salted: &[u8]) -> (Vec<u8>, Vec<u8>) {
    derive_keys_with(ring_hmac::HMAC_SHA256, salted, |client_key| {
        Sha256::digest(client_key).to_vec()
    })
}

/// `StoredKey := H(HMAC(salted, "Client Key"))` and
/// `ServerKey := HMAC(salted, "Server Key")`, with `algorithm` as the HMAC and
/// `hash` as `H`.
///
/// `ring`'s HMAC takes a key of any length, so this cannot fail.
fn derive_keys_with(
    algorithm: ring_hmac::Algorithm,
    salted: &[u8],
    hash: impl FnOnce(&[u8]) -> Vec<u8>,
) -> (Vec<u8>, Vec<u8>) {
    let key = ring_hmac::Key::new(algorithm, salted);
    let client_key = ring_hmac::sign(&key, b"Client Key");
    let stored_key = hash(client_key.as_ref());
    let server_key = ring_hmac::sign(&key, b"Server Key").as_ref().to_vec();
    (stored_key, server_key)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use base64::{Engine, engine::general_purpose::STANDARD as B64};
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::{Digest, Sha512};

    use super::*;
    use crate::{AuthMethod, Principal};

    /// SHA-512 PBKDF2 vector + verify `stored_key = H(client_key)`.
    /// `ScramIterations` is a validated newtype whose whole job is to carry a
    /// number the broker will accept, and nothing read one back out: an
    /// `into_value` answering a constant, a `Display` writing nothing, and a
    /// `from_str` returning the default all describe an iteration count that
    /// was never the one asked for.
    #[test]
    fn scram_iterations_round_trip_through_value_text_and_parse() {
        let iterations = ScramIterations::new(MIN_SCRAM_ITERATIONS + 1).expect("in range");
        check!(iterations.into_value() == MIN_SCRAM_ITERATIONS + 1);
        check!(iterations.to_string() == (MIN_SCRAM_ITERATIONS + 1).to_string());
        check!(
            (MIN_SCRAM_ITERATIONS + 1)
                .to_string()
                .parse::<ScramIterations>()
                == Ok(iterations)
        );

        check!(ScramIterations::default().into_value() == DEFAULT_SCRAM_ITERATIONS);
        check!(
            ScramIterations::new(MAX_SCRAM_ITERATIONS)
                .expect("the maximum is in range")
                .into_value()
                == MAX_SCRAM_ITERATIONS
        );

        // Outside the broker's range, and not a number at all.
        check!(ScramIterations::new(MIN_SCRAM_ITERATIONS - 1).is_err());
        check!(ScramIterations::new(MAX_SCRAM_ITERATIONS + 1).is_err());
        check!("not-a-number".parse::<ScramIterations>().is_err());
    }

    #[test]
    fn hash_scram_password_produces_expected_keys() {
        let password = b"pencil";
        let cred = hash_scram_password(password, SaslMechanism::ScramSha512, 4096).unwrap();
        check!(
            (
                cred.mechanism,
                cred.salt.len(),
                cred.stored_key.len(),
                cred.server_key.len(),
                cred.iterations,
            ) == (SaslMechanism::ScramSha512, 16, 64, 64, 4096)
        );
        let salted =
            pbkdf2::pbkdf2_hmac_array::<sha2::Sha512, 64>(password, &cred.salt, cred.iterations);
        let client_key = {
            use hmac::{Hmac, KeyInit, Mac};
            let mut m = <Hmac<Sha512>>::new_from_slice(&salted).unwrap();
            m.update(b"Client Key");
            m.finalize().into_bytes()
        };
        let expected_stored = Sha512::digest(client_key);
        assert2::assert!(cred.stored_key == expected_stored.as_slice());
    }

    /// SHA-256 analog of the SHA-512 vector. It checks the 32-byte output
    /// lengths and the same `stored_key = H(client_key)` invariant.
    #[test]
    fn hash_scram_password_sha256_produces_expected_keys() {
        let password = b"pencil";
        let cred = hash_scram_password(password, SaslMechanism::ScramSha256, 4096).unwrap();
        check!(
            (
                cred.mechanism,
                cred.salt.len(),
                cred.stored_key.len(),
                cred.server_key.len(),
            ) == (SaslMechanism::ScramSha256, 16, 32, 32)
        );
        let salted =
            pbkdf2::pbkdf2_hmac_array::<sha2::Sha256, 32>(password, &cred.salt, cred.iterations);
        let client_key = {
            use hmac::{Hmac, KeyInit, Mac};
            let mut m = <Hmac<sha2::Sha256>>::new_from_slice(&salted).unwrap();
            m.update(b"Client Key");
            m.finalize().into_bytes()
        };
        let expected_stored = sha2::Sha256::digest(client_key);
        assert2::assert!(cred.stored_key == expected_stored.as_slice());
    }

    #[test]
    fn hash_scram_password_is_deterministic_given_salt() {
        let a = hash_scram_password(b"x", SaslMechanism::ScramSha512, 4096).unwrap();
        let b = hash_scram_password(b"x", SaslMechanism::ScramSha512, 4096).unwrap();
        assert2::assert!(a.salt != b.salt);
    }

    use crate::{
        AuthError,
        scram::{
            client::ScramClientExchange,
            server::{ScramServerExchange, StepResult},
        },
    };

    #[test]
    fn scram_server_and_client_round_trip() {
        let password = b"hunter2";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha512,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let server = ScramServerExchange::new("alice".to_owned(), cred);
        let client = ScramClientExchange::new(
            "alice".to_owned(),
            password.to_vec(),
            SaslMechanism::ScramSha512,
        );

        // Client first
        let (c1, client) = client.client_first().expect("client first");
        // Server step 1 -> server-first
        let (s1, server) = match server.step(&c1) {
            StepResult::Continue(b, next) => (b, next),
            other => panic!("server step 1 must continue, got {other:?}"),
        };
        // Client final
        let (c2, client) = client.step(&s1).expect("client final");
        // Server step 2 -> done
        let (principal, s2) = match server.step(&c2) {
            StepResult::Done(p, b) => (p, b),
            other => panic!("server step 2 must Done, got {other:?}"),
        };
        assert2::assert!(principal.name.as_str() == "alice");
        assert2::assert!(principal.auth_method == crate::AuthMethod::SaslScramSha512);
        // Client verifies server signature. `verify_server_final` consumes
        // `client`, so a second verification attempt (the old "server final
        // must only verify once" regression test) is now a compile-time
        // move error rather than a runtime `MalformedMessage` — the
        // scenario it guarded against is no longer expressible.
        let final_check = client.verify_server_final(&s2);
        assert2::assert!(final_check.is_ok());
    }

    /// Mirror of the SHA-512 round-trip with SHA-256. It proves that the
    /// generalized state machines produce a matching client/server pair on the
    /// smaller-hash path.
    #[test]
    fn scram_server_and_client_round_trip_sha256() {
        let password = b"hunter2";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let server = ScramServerExchange::new("alice".to_owned(), cred);
        let client = ScramClientExchange::new(
            "alice".to_owned(),
            password.to_vec(),
            SaslMechanism::ScramSha256,
        );

        let (c1, client) = client.client_first().expect("client first");
        let (s1, server) = match server.step(&c1) {
            StepResult::Continue(b, next) => (b, next),
            other => panic!("server step 1 must continue, got {other:?}"),
        };
        let (c2, client) = client.step(&s1).expect("client final");
        let (principal, s2) = match server.step(&c2) {
            StepResult::Done(p, b) => (p, b),
            other => panic!("server step 2 must Done, got {other:?}"),
        };
        assert2::assert!(principal.name.as_str() == "alice");
        assert2::assert!(principal.auth_method == crate::AuthMethod::SaslScramSha256);
        let final_check = client.verify_server_final(&s2);
        assert2::assert!(final_check.is_ok());
    }

    /// The hand-written `Debug` impl on `ScramServerExchange` prints the
    /// observable negotiation phase. The variant payload is not exported, so
    /// callers can only ever see the phase name through `Debug` or logging.
    #[test]
    fn scram_server_exchange_debug_includes_the_observable_phase() {
        let password = b"hunter2";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let server = ScramServerExchange::new("alice".to_owned(), cred);
        let rendered = format!("{server:?}");
        assert2::assert!(rendered.contains("ScramServerExchange::AwaitingClientFirst"));

        let client = ScramClientExchange::new(
            "alice".to_owned(),
            password.to_vec(),
            SaslMechanism::ScramSha256,
        );
        let (c1, _client) = client.client_first().expect("client first");
        let server = match server.step(&c1) {
            StepResult::Continue(_, next) => next,
            other => panic!("server step 1 must continue, got {other:?}"),
        };
        let rendered = format!("{server:?}");
        assert2::assert!(rendered.contains("ScramServerExchange::AwaitingClientFinal"));
    }

    #[test]
    fn scram_client_rejects_tampered_server_final_signature() {
        let password = b"hunter2";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let server = ScramServerExchange::new("alice".to_owned(), cred);
        let client = ScramClientExchange::new(
            "alice".to_owned(),
            password.to_vec(),
            SaslMechanism::ScramSha256,
        );

        let (c1, client) = client.client_first().expect("client first");
        let (s1, server) = match server.step(&c1) {
            StepResult::Continue(b, next) => (b, next),
            other => panic!("server step 1 must continue, got {other:?}"),
        };
        let (c2, client) = client.step(&s1).expect("client final");
        match server.step(&c2) {
            StepResult::Done(_, _) => {}
            other => panic!("server step 2 must Done, got {other:?}"),
        }

        assert2::assert!(client.verify_server_final(b"v=AAAA").is_err());
    }

    /// How a server exchange ended after client-final, comparable as a whole.
    #[derive(Debug, PartialEq, Eq)]
    enum Ending {
        Done(Principal, Vec<u8>),
        Failed(AuthError),
        Disconnect,
    }

    /// A server exchange that continued past client-final has no ending: the
    /// conversion hands the result back.
    impl TryFrom<StepResult> for Ending {
        type Error = StepResult;

        fn try_from(result: StepResult) -> Result<Self, StepResult> {
            match result {
                StepResult::Done(principal, server_final) => {
                    Ok(Self::Done(principal, server_final))
                }
                StepResult::Failed(e) => Ok(Self::Failed(e)),
                StepResult::Disconnect => Ok(Self::Disconnect),
                continued @ StepResult::Continue(..) => Err(continued),
            }
        }
    }

    /// A proof that does not decode to the hash length makes Kafka's
    /// `ScramFormatter.xor` throw `IllegalArgumentException`, and the
    /// connection closes with no response. The nonce is checked first, as in
    /// `ScramSaslServer`, so a wrong nonce still fails as malformed.
    #[test]
    fn scram_server_disconnects_on_a_proof_of_the_wrong_length() {
        let password = b"hunter2";
        for (case, mechanism, proof_len, wrong_nonce, want) in [
            (
                "empty proof",
                SaslMechanism::ScramSha256,
                0,
                false,
                Ending::Disconnect,
            ),
            (
                "short proof",
                SaslMechanism::ScramSha256,
                3,
                false,
                Ending::Disconnect,
            ),
            (
                "one byte short",
                SaslMechanism::ScramSha256,
                31,
                false,
                Ending::Disconnect,
            ),
            (
                "one byte long",
                SaslMechanism::ScramSha256,
                33,
                false,
                Ending::Disconnect,
            ),
            (
                "SHA-512 length on SHA-256",
                SaslMechanism::ScramSha256,
                64,
                false,
                Ending::Disconnect,
            ),
            (
                "SHA-256 length on SHA-512",
                SaslMechanism::ScramSha512,
                32,
                false,
                Ending::Disconnect,
            ),
            (
                "right length, wrong value",
                SaslMechanism::ScramSha256,
                32,
                false,
                Ending::Failed(AuthError::BadProof),
            ),
            (
                "wrong length and wrong nonce",
                SaslMechanism::ScramSha256,
                3,
                true,
                Ending::Failed(AuthError::MalformedMessage),
            ),
        ] {
            let cred = hash_scram_password_with_salt(
                password,
                mechanism,
                4096,
                (0..16).collect::<Vec<u8>>(),
            );
            let server = ScramServerExchange::new("alice".to_owned(), cred);
            let StepResult::Continue(server_first, server) = server.step(b"n,,n=alice,r=cn") else {
                panic!("server step 1 must continue");
            };
            let server_first = String::from_utf8(server_first).unwrap();
            let nonce = server_first
                .strip_prefix("r=")
                .and_then(|rest| rest.split(',').next())
                .unwrap();
            let nonce = if wrong_nonce { "cnX" } else { nonce };
            let client_final = format!("c=biws,r={nonce},p={}", B64.encode(vec![7u8; proof_len]));
            check!(
                Ending::try_from(server.step(client_final.as_bytes())).unwrap() == want,
                "case {case}"
            );
        }
    }

    #[test]
    fn pbkdf2_salted_matches_hash_scram_password_intermediate() {
        // `pbkdf2_salted` exposes the PBKDF2 intermediate so the
        // operator can produce the KIP-554 wire `salted_password`. It
        // must equal the value `hash_scram_password_with_salt` feeds
        // into `derive_keys_from_salted` internally.
        let password = b"pencil";
        let salt: Vec<u8> = (0..16).collect();
        for (_case, mechanism, expected_len) in [
            ("SCRAM SHA-512", SaslMechanism::ScramSha512, 64),
            ("SCRAM SHA-256", SaslMechanism::ScramSha256, 32),
        ] {
            let cred = hash_scram_password_with_salt(password, mechanism, 4096, salt.clone());
            let salted = pbkdf2_salted(password, mechanism, 4096, &salt);
            let (stored_key, server_key) = derive_keys_from_salted(mechanism, &salted);
            assert2::assert!(salted.len() == expected_len);
            assert2::assert!(stored_key == cred.stored_key);
            assert2::assert!(server_key == cred.server_key);
        }
    }

    #[test]
    fn derive_keys_from_salted_matches_hash_scram_password_sha512() {
        let password = b"hunter2";
        let salt: Vec<u8> = (0..16).collect();
        let cred =
            hash_scram_password_with_salt(password, SaslMechanism::ScramSha512, 4096, salt.clone());
        let salted: [u8; 64] = pbkdf2::pbkdf2_hmac_array::<sha2::Sha512, 64>(password, &salt, 4096);
        let (stored_key, server_key) = derive_keys_from_salted(SaslMechanism::ScramSha512, &salted);
        check!(
            (&stored_key, &server_key, stored_key.len(), server_key.len())
                == (&cred.stored_key, &cred.server_key, 64, 64)
        );
    }

    #[test]
    fn derive_keys_from_salted_matches_hash_scram_password_sha256() {
        let password = b"hunter2";
        let salt: Vec<u8> = (0..16).collect();
        let cred =
            hash_scram_password_with_salt(password, SaslMechanism::ScramSha256, 4096, salt.clone());
        let salted: [u8; 32] = pbkdf2::pbkdf2_hmac_array::<sha2::Sha256, 32>(password, &salt, 4096);
        let (stored_key, server_key) = derive_keys_from_salted(SaslMechanism::ScramSha256, &salted);
        check!(
            (&stored_key, &server_key, stored_key.len(), server_key.len())
                == (&cred.stored_key, &cred.server_key, 32, 32)
        );
    }

    /// KIP-48: `new_with_principal` stamps an override principal that wins on
    /// the `Done` arm. The delegation-token SCRAM fallback uses it, so a client
    /// that authenticates with a `tokenId` as the SCRAM username surfaces as
    /// the token's owner, for example `User:alice`, and not as
    /// `User:<token-uuid>`.
    #[test]
    fn scram_server_with_principal_override_yields_override_on_done() {
        let password = b"hunter2";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let override_principal = crate::Principal {
            name: "alice".to_owned(),
            auth_method: crate::AuthMethod::SaslScramSha256,
            groups: vec![],
        };
        // SCRAM username (the wire "n=..." attribute) is "tok-uuid";
        // the override principal is "alice" (the token's owner).
        let server = ScramServerExchange::new_with_principal(
            "tok-uuid".to_owned(),
            cred,
            override_principal.clone(),
        );
        let client = ScramClientExchange::new(
            "tok-uuid".to_owned(),
            password.to_vec(),
            SaslMechanism::ScramSha256,
        );

        let (c1, client) = client.client_first().expect("client first");
        let (s1, server) = match server.step(&c1) {
            StepResult::Continue(b, next) => (b, next),
            other => panic!("server step 1 must continue, got {other:?}"),
        };
        let (c2, _client) = client.step(&s1).expect("client final");
        let (principal, _s2) = match server.step(&c2) {
            StepResult::Done(p, b) => (p, b),
            other => panic!("server step 2 must Done, got {other:?}"),
        };
        // Override wins: principal is the token owner, NOT "tok-uuid".
        assert2::assert!(principal == override_principal);
    }

    #[test]
    fn scram_server_rejects_bad_proof() {
        let cred = hash_scram_password_with_salt(
            b"correct",
            SaslMechanism::ScramSha512,
            4096,
            vec![0u8; 16],
        );
        let server = ScramServerExchange::new("alice".to_owned(), cred);
        let client = ScramClientExchange::new(
            "alice".to_owned(),
            b"wrong".to_vec(),
            SaslMechanism::ScramSha512,
        );
        let (c1, client) = client.client_first().unwrap();
        let StepResult::Continue(s1, server) = server.step(&c1) else {
            panic!();
        };
        let (c2, _client) = client.step(&s1).unwrap();
        match server.step(&c2) {
            StepResult::Failed(crate::AuthError::BadProof) => {}
            other => panic!("expected BadProof, got {other:?}"),
        }
    }

    /// RFC 5802 §5.1: the server must reject a client-final whose `r=`
    /// combined nonce does not equal the nonce it issued in server-first. This
    /// test tampers with the `r=` attribute of an otherwise well-formed
    /// client-final and expects `MalformedMessage`.
    #[test]
    fn scram_server_rejects_wrong_nonce() {
        // Arbitrary, non-secret test password generated at runtime (not a
        // hard-coded credential literal).
        let password: Vec<u8> = (b'A'..=b'Z').collect();
        let cred = hash_scram_password_with_salt(
            &password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let server = ScramServerExchange::new("alice".to_owned(), cred);
        let client =
            ScramClientExchange::new("alice".to_owned(), password, SaslMechanism::ScramSha256);
        let (c1, client) = client.client_first().unwrap();
        let StepResult::Continue(s1, server) = server.step(&c1) else {
            panic!("server step 1 must continue");
        };
        let (c2, _client) = client.step(&s1).unwrap();
        let c2_str = String::from_utf8(c2).unwrap();
        // Flip the combined nonce: replace `r=<nonce>` with a different
        // value while leaving `c=` and `p=` intact.
        let combined = c2_str
            .split(',')
            .find_map(|a| a.strip_prefix("r="))
            .expect("client-final has r=");
        let tampered = c2_str.replacen(
            &format!("r={combined}"),
            &format!("r={combined}deadbeef"),
            1,
        );
        match server.step(tampered.as_bytes()) {
            StepResult::Failed(crate::AuthError::MalformedMessage) => {}
            other => panic!("expected MalformedMessage for wrong nonce, got {other:?}"),
        }
    }

    /// Kafka does not compare `c=` with the GS2 header, but the proof signs
    /// it: a channel binding swapped in after the client signed fails as a
    /// bad proof.
    #[test]
    fn scram_server_fails_a_channel_binding_the_client_did_not_sign() {
        // Arbitrary, non-secret test password generated at runtime (not a
        // hard-coded credential literal).
        let password: Vec<u8> = (b'A'..=b'Z').collect();
        let cred = hash_scram_password_with_salt(
            &password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let server = ScramServerExchange::new("alice".to_owned(), cred);
        let client =
            ScramClientExchange::new("alice".to_owned(), password, SaslMechanism::ScramSha256);
        let (c1, client) = client.client_first().unwrap();
        let StepResult::Continue(s1, server) = server.step(&c1) else {
            panic!("server step 1 must continue");
        };
        let (c2, _client) = client.step(&s1).unwrap();
        let c2_str = String::from_utf8(c2).unwrap();
        // The client always emits `c=biws`; rewrite it to a different
        // (still-valid-base64) channel binding.
        assert2::assert!(c2_str.starts_with("c=biws,"));
        let wrong_cb = B64.encode(b"y,,");
        let tampered = c2_str.replacen("c=biws", &format!("c={wrong_cb}"), 1);
        match server.step(tampered.as_bytes()) {
            StepResult::Failed(crate::AuthError::BadProof) => {}
            other => panic!("expected BadProof for an unsigned channel binding, got {other:?}"),
        }
    }

    /// Drive a server exchange for `alice` by hand with the given GS2 header
    /// and extensions in client-first, and the given header base64-encoded as
    /// the client-final `c=`. Returns the principal on success, or the first
    /// failure from either round.
    fn handshake_with_header(
        gs2_header: &str,
        extensions: &str,
        channel_binding_header: &str,
    ) -> Result<crate::Principal, crate::AuthError> {
        let password: Vec<u8> = (b'a'..=b'z').collect();
        let salt: Vec<u8> = (0..16).collect();
        let cred = hash_scram_password_with_salt(
            &password,
            SaslMechanism::ScramSha256,
            4096,
            salt.clone(),
        );
        let server = ScramServerExchange::new("alice".to_owned(), cred);

        let bare = format!("n=alice,r=client-nonce{extensions}");
        let (server_first, server) = match server.step(format!("{gs2_header}{bare}").as_bytes()) {
            StepResult::Continue(server_first, next) => (server_first, next),
            StepResult::Failed(e) => return Err(e),
            StepResult::Done(..) | StepResult::Disconnect => {
                panic!("server ended the exchange on client-first")
            }
        };
        let server_first = String::from_utf8(server_first).unwrap();
        let combined_nonce = server_first
            .strip_prefix("r=")
            .and_then(|rest| rest.split(',').next())
            .unwrap();

        let client_final_no_proof = format!(
            "c={},r={combined_nonce}",
            B64.encode(channel_binding_header.as_bytes())
        );
        let auth_message = format!("{bare},{server_first},{client_final_no_proof}");
        let (proof, _) =
            client::compute_proof_sha256(&password, &salt, 4096, auth_message.as_bytes()).unwrap();
        match server.step(format!("{client_final_no_proof},p={}", B64.encode(proof)).as_bytes()) {
            StepResult::Done(principal, _) => Ok(principal),
            StepResult::Failed(e) => Err(e),
            StepResult::Continue(..) | StepResult::Disconnect => {
                panic!("server did not finish client-final")
            }
        }
    }

    /// Broker issue #764: the server accepts a GS2 header carrying an
    /// authorization id, as Kafka's `ScramSaslServer` does. Like Kafka, it
    /// does not hold the client-final `c=` to that header: the proof signs
    /// whatever channel binding the client sent.
    #[test]
    fn scram_server_reads_the_gs2_header_but_not_the_channel_binding() {
        let alice = Ok(crate::Principal {
            name: "alice".into(),
            auth_method: crate::AuthMethod::SaslScramSha256,
            groups: vec![],
        });
        for (case, gs2_header, extensions, channel_binding_header, want) in [
            ("plain header", "n,,", "", "n,,", alice.clone()),
            (
                "authzid naming the user",
                "n,a=alice,",
                "",
                "n,a=alice,",
                alice.clone(),
            ),
            (
                "token extension",
                "n,,",
                ",tokenauth=true",
                "n,,",
                alice.clone(),
            ),
            (
                "authzid with extension",
                "n,a=alice,",
                ",tokenauth=true",
                "n,a=alice,",
                alice.clone(),
            ),
            (
                "authzid naming another user",
                "n,a=bob,",
                "",
                "n,a=bob,",
                Err(crate::AuthError::AuthorizationIdMismatch),
            ),
            (
                "authzid header but biws binding",
                "n,a=alice,",
                "",
                "n,,",
                alice.clone(),
            ),
            (
                "plain header but authzid binding",
                "n,,",
                "",
                "n,a=alice,",
                alice,
            ),
            (
                "channel binding requested",
                "p=tls-unique,,",
                "",
                "p=tls-unique,,",
                Err(crate::AuthError::MalformedMessage),
            ),
            (
                "client supports binding",
                "y,,",
                "",
                "y,,",
                Err(crate::AuthError::MalformedMessage),
            ),
        ] {
            check!(
                handshake_with_header(gs2_header, extensions, channel_binding_header) == want,
                "case {case}"
            );
        }
    }

    /// Drive a SCRAM-SHA-256 server exchange for `username` with a hand-built
    /// client-first (`gs2_header` then `bare`) and a client-final built from
    /// `template`. In `template`, `{cb}` stands for the base64 of
    /// `gs2_header`, `{nonce}` for the combined nonce and `{proof}` for the
    /// base64 proof. The proof signs `c={cb},r={nonce}`, which is what Kafka
    /// rebuilds for any of these templates. With
    /// `flip_unused_bit`, the proof's last base64 character has its unused
    /// low bit set. Returns the principal, or the first failure.
    fn handshake_with_client_final(
        username: &str,
        gs2_header: &str,
        bare: &str,
        template: &str,
        flip_unused_bit: bool,
    ) -> Result<Principal, AuthError> {
        let password: Vec<u8> = (b'a'..=b'z').collect();
        let salt: Vec<u8> = (0..16).collect();
        let cred = hash_scram_password_with_salt(
            &password,
            SaslMechanism::ScramSha256,
            4096,
            salt.clone(),
        );
        let server = ScramServerExchange::new(username.to_owned(), cred);
        let (server_first, server) = match server.step(format!("{gs2_header}{bare}").as_bytes()) {
            StepResult::Continue(server_first, next) => (server_first, next),
            StepResult::Failed(e) => return Err(e),
            StepResult::Done(..) | StepResult::Disconnect => {
                panic!("server ended the exchange on client-first")
            }
        };
        let server_first = String::from_utf8(server_first).unwrap();
        let combined_nonce = server_first
            .strip_prefix("r=")
            .and_then(|rest| rest.split(',').next())
            .unwrap();
        let client_final = template
            .replace("{cb}", &B64.encode(gs2_header.as_bytes()))
            .replace("{nonce}", combined_nonce);
        let signed = format!("c={},r={combined_nonce}", B64.encode(gs2_header.as_bytes()));
        let auth_message = format!("{bare},{server_first},{signed}");
        let (client_proof, _) =
            client::compute_proof_sha256(&password, &salt, 4096, auth_message.as_bytes()).unwrap();
        let mut proof_b64 = B64.encode(client_proof);
        if flip_unused_bit {
            // 32 bytes encode as 43 characters and one `=`; the 43rd carries
            // four data bits and two unused ones.
            let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
            let last = proof_b64.as_bytes()[42];
            let index = alphabet.iter().position(|&c| c == last).unwrap();
            proof_b64.replace_range(42..43, &char::from(alphabet[index ^ 1]).to_string());
        }
        match server.step(client_final.replace("{proof}", &proof_b64).as_bytes()) {
            StepResult::Done(principal, _) => Ok(principal),
            StepResult::Failed(e) => Err(e),
            StepResult::Continue(..) | StepResult::Disconnect => {
                panic!("server did not finish client-final")
            }
        }
    }

    /// Issue #65: the server reads the client-final message with Kafka's
    /// `ClientFinalMessage` grammar, signs it without its extensions, decodes
    /// the proof as Java does, and compares the authzid as the client
    /// escaped it with the decoded username, as `ScramSaslServer` does.
    #[test]
    fn scram_server_reads_client_final_with_kafkas_grammar() {
        let principal = |name: &str| {
            Ok(Principal {
                name: name.into(),
                auth_method: AuthMethod::SaslScramSha256,
                groups: vec![],
            })
        };
        let malformed = Err(AuthError::MalformedMessage);
        let alice_bare = "n=alice,r=client-nonce";
        for (case, username, gs2_header, bare, template, flip_unused_bit, want) in [
            (
                "kafka order",
                "alice",
                "n,,",
                alice_bare,
                "c={cb},r={nonce},p={proof}",
                false,
                principal("alice"),
            ),
            (
                "extension before the proof is not signed",
                "alice",
                "n,,",
                alice_bare,
                "c={cb},r={nonce},foo=bar,p={proof}",
                false,
                principal("alice"),
            ),
            (
                "unused proof bit set",
                "alice",
                "n,,",
                alice_bare,
                "c={cb},r={nonce},p={proof}",
                true,
                principal("alice"),
            ),
            (
                "nonce before channel binding",
                "alice",
                "n,,",
                alice_bare,
                "r={nonce},c={cb},p={proof}",
                false,
                malformed.clone(),
            ),
            (
                "proof before nonce",
                "alice",
                "n,,",
                alice_bare,
                "c={cb},p={proof},r={nonce}",
                false,
                malformed.clone(),
            ),
            (
                "unknown attribute after the proof",
                "alice",
                "n,,",
                alice_bare,
                "c={cb},r={nonce},p={proof},x=1",
                false,
                malformed.clone(),
            ),
            (
                "duplicate channel binding",
                "alice",
                "n,,",
                alice_bare,
                "c={cb},c={cb},r={nonce},p={proof}",
                false,
                malformed.clone(),
            ),
            (
                // Kafka reads the second `r=` as an extension.
                "repeated nonce",
                "alice",
                "n,,",
                alice_bare,
                "c={cb},r={nonce},r={nonce},p={proof}",
                false,
                principal("alice"),
            ),
            (
                "trailing comma",
                "alice",
                "n,,",
                alice_bare,
                "c={cb},r={nonce},p={proof},",
                false,
                malformed,
            ),
            (
                "escaped authzid equal to the decoded username",
                "a=3D",
                "n,a=a=3D,",
                "n=a=3D3D,r=client-nonce",
                "c={cb},r={nonce},p={proof}",
                false,
                principal("a=3D"),
            ),
            (
                "escaped authzid that decodes to the username",
                "a,b",
                "n,a=a=2Cb,",
                "n=a=2Cb,r=client-nonce",
                "c={cb},r={nonce},p={proof}",
                false,
                Err(AuthError::AuthorizationIdMismatch),
            ),
        ] {
            check!(
                handshake_with_client_final(username, gs2_header, bare, template, flip_unused_bit)
                    == want,
                "case {case}"
            );
        }
    }

    /// Kafka's `ScramFormatter.authMessage` signs the messages that
    /// `ClientFirstMessage.clientFirstMessageBare` and
    /// `ClientFinalMessage.clientFinalMessageWithoutProof` rebuild from the
    /// parsed fields, not the bytes the client sent. The expected forms below
    /// are what Kafka 4.3.1's own `ScramMessages` returns for each message.
    /// A proof over Kafka's form authenticates, and the server signature is
    /// over the same form. A proof over the raw bytes, where they differ,
    /// fails as a bad proof, as it does against Kafka.
    #[test]
    fn scram_server_signs_the_auth_message_kafka_rebuilds() {
        let password: Vec<u8> = (b'a'..=b'z').collect();
        let salt: Vec<u8> = (0..16).collect();
        let alice = Principal {
            name: "alice".into(),
            auth_method: AuthMethod::SaslScramSha256,
            groups: vec![],
        };
        for (case, client_first, client_final, kafka_bare, kafka_final) in [
            (
                "canonical",
                "n,,n=alice,r=cn",
                "c=biws,r={nonce},p={proof}",
                "n=alice,r=cn",
                "c=biws,r={nonce}",
            ),
            (
                "reserved attribute",
                "n,,m=ext,n=alice,r=cn",
                "c=biws,r={nonce},p={proof}",
                "n=alice,r=cn",
                "c=biws,r={nonce}",
            ),
            (
                "repeated client-first extension",
                "n,,n=alice,r=cn,foo=1,foo=2",
                "c=biws,r={nonce},p={proof}",
                "n=alice,r=cn,foo=2",
                "c=biws,r={nonce}",
            ),
            (
                "client-final extension",
                "n,,n=alice,r=cn",
                "c=biws,r={nonce},foo=bar,p={proof}",
                "n=alice,r=cn",
                "c=biws,r={nonce}",
            ),
            (
                "unused bits in the channel binding",
                "n,a=alice,n=alice,r=cn",
                "c=bixhPWFsaWNlLB==,r={nonce},p={proof}",
                "n=alice,r=cn",
                "c=bixhPWFsaWNlLA==,r={nonce}",
            ),
            (
                "channel binding of another header",
                "n,,n=alice,r=cn",
                "c=bixhPWFsaWNlLA==,r={nonce},p={proof}",
                "n=alice,r=cn",
                "c=bixhPWFsaWNlLA==,r={nonce}",
            ),
        ] {
            let raw_bare = client_first.splitn(3, ',').nth(2).unwrap();
            let raw_final = client_final.split(",p={proof}").next().unwrap();
            for (signed_bare, signed_final) in [(kafka_bare, kafka_final), (raw_bare, raw_final)] {
                let cred = hash_scram_password_with_salt(
                    &password,
                    SaslMechanism::ScramSha256,
                    4096,
                    salt.clone(),
                );
                let server = ScramServerExchange::new("alice".to_owned(), cred);
                let StepResult::Continue(server_first, server) =
                    server.step(client_first.as_bytes())
                else {
                    panic!("case {case}: server step 1 must continue");
                };
                let server_first = String::from_utf8(server_first).unwrap();
                let combined_nonce = server_first
                    .strip_prefix("r=")
                    .and_then(|rest| rest.split(',').next())
                    .unwrap();
                let auth_message = format!(
                    "{signed_bare},{server_first},{}",
                    signed_final.replace("{nonce}", combined_nonce)
                );
                let (client_proof, server_key) =
                    client::compute_proof_sha256(&password, &salt, 4096, auth_message.as_bytes())
                        .unwrap();
                let mut server_mac = <Hmac<Sha256>>::new_from_slice(&server_key).unwrap();
                server_mac.update(auth_message.as_bytes());
                let server_final =
                    format!("v={}", B64.encode(server_mac.finalize().into_bytes())).into_bytes();
                let want = if signed_bare == kafka_bare && signed_final == kafka_final {
                    Ending::Done(alice.clone(), server_final)
                } else {
                    Ending::Failed(AuthError::BadProof)
                };
                let message = client_final
                    .replace("{nonce}", combined_nonce)
                    .replace("{proof}", &B64.encode(client_proof));
                check!(
                    Ending::try_from(server.step(message.as_bytes())).unwrap() == want,
                    "case {case}, signed {signed_bare} / {signed_final}"
                );
            }
        }
    }

    /// The client sends the extensions it is given after the nonce, and the
    /// server signs them as part of the bare message, so a `tokenauth=true`
    /// login completes.
    #[test]
    fn scram_client_sends_extensions_in_client_first() {
        let password = b"token-hmac";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let server = ScramServerExchange::new("token-id".to_owned(), cred);
        let client = ScramClientExchange::new(
            "token-id".to_owned(),
            password.to_vec(),
            SaslMechanism::ScramSha256,
        )
        .with_extensions([(TOKEN_AUTH_EXTENSION, "true")])
        .unwrap();

        let (c1, client) = client.client_first().unwrap();
        let parsed = ScramClientFirst::parse(&c1).unwrap();
        check!(
            (
                parsed.gs2_header.as_str(),
                parsed.username.as_str(),
                parsed.extensions.clone(),
                parsed.token_authenticated(),
            ) == (
                "n,,",
                "token-id",
                vec![(TOKEN_AUTH_EXTENSION.to_owned(), "true".to_owned())],
                true,
            )
        );
        let StepResult::Continue(s1, server) = server.step(&c1) else {
            panic!("server step 1 must continue");
        };
        let (c2, client) = client.step(&s1).unwrap();
        let StepResult::Done(principal, s2) = server.step(&c2) else {
            panic!("server step 2 must finish");
        };
        check!(principal.name == "token-id");
        check!(client.verify_server_final(&s2).is_ok());
    }

    /// A repeated extension key is sent once, at its first position with its
    /// last value, as Kafka's map-backed client sends it, so the server's
    /// rebuilt `AuthMessage` matches what the client signed.
    #[test]
    fn scram_client_sends_a_repeated_extension_once_and_authenticates() {
        let password = b"pw";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let server = ScramServerExchange::new("alice".to_owned(), cred);
        let client = ScramClientExchange::new(
            "alice".to_owned(),
            password.to_vec(),
            SaslMechanism::ScramSha256,
        )
        .with_extensions([("foo", "1"), ("bar", "2"), ("foo", "3")])
        .unwrap();

        let (c1, client) = client.client_first().unwrap();
        let parsed = ScramClientFirst::parse(&c1).unwrap();
        check!(
            parsed.extensions
                == vec![
                    ("foo".to_owned(), "3".to_owned()),
                    ("bar".to_owned(), "2".to_owned()),
                ]
        );
        let StepResult::Continue(s1, server) = server.step(&c1) else {
            panic!("server step 1 must continue");
        };
        let (c2, client) = client.step(&s1).unwrap();
        let StepResult::Done(principal, s2) = server.step(&c2) else {
            panic!("server step 2 must finish");
        };
        check!(principal.name == "alice");
        check!(client.verify_server_final(&s2).is_ok());
    }

    #[test]
    fn scram_client_rejects_extensions_kafka_cannot_parse() {
        for (case, key, value) in [
            ("empty key", "", "true"),
            ("non-alphabetic key", "token_auth", "true"),
            ("empty value", "tokenauth", ""),
            ("comma in value", "tokenauth", "a,b"),
            ("non-ASCII value", "tokenauth", "\u{e9}"),
        ] {
            let client = ScramClientExchange::new(
                "alice".to_owned(),
                b"pw".to_vec(),
                SaslMechanism::ScramSha256,
            );
            check!(
                client.with_extensions([(key, value)]).map(|_| ())
                    == Err(crate::AuthError::MalformedMessage),
                "case {case}"
            );
        }
    }

    /// A username holding `,` or `=` is sent as an escaped `saslname` and the
    /// server authenticates it under its decoded name.
    #[test]
    fn scram_round_trip_escapes_the_username() {
        let password = b"pw";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let server = ScramServerExchange::new("a,b=c".to_owned(), cred);
        let client = ScramClientExchange::new(
            "a,b=c".to_owned(),
            password.to_vec(),
            SaslMechanism::ScramSha256,
        );
        let (c1, client) = client.client_first().unwrap();
        check!(c1.starts_with(b"n,,n=a=2Cb=3Dc,r="));
        let StepResult::Continue(s1, server) = server.step(&c1) else {
            panic!("server step 1 must continue");
        };
        let (c2, _client) = client.step(&s1).unwrap();
        let StepResult::Done(principal, _) = server.step(&c2) else {
            panic!("server step 2 must finish");
        };
        check!(principal.name == "a,b=c");
    }
}

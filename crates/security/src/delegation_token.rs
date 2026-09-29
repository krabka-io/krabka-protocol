//! KIP-48 delegation token primitives: the HMAC and a secret-key wrapper that
//! keeps the bytes out of Debug.

use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha512;

#[derive(Clone, PartialEq, Eq)]
pub struct SecretBytes(Bytes);

impl SecretBytes {
    #[must_use]
    pub fn new(bytes: impl Into<Bytes>) -> Self {
        Self(bytes.into())
    }
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::fmt::Debug for SecretBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretBytes(<{} bytes redacted>)", self.0.len())
    }
}

/// A delegation token's HMAC: `HmacSHA512` over the token id's UTF-8 bytes,
/// keyed with the secret key's bytes, as Kafka's
/// `DelegationTokenManager.createHmac` computes it. The result is 64 bytes,
/// and its base64 is the SCRAM password a client authenticates with.
///
/// # Panics
///
/// Never: HMAC accepts a key of any length.
#[must_use]
pub fn compute_token_hmac(secret_key: &[u8], token_id: &str) -> Vec<u8> {
    let mut mac =
        <Hmac<Sha512>>::new_from_slice(secret_key).expect("HMAC-SHA-512 accepts any key length");
    mac.update(token_id.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn secret_bytes_accessors() {
        let s = SecretBytes::new(Bytes::from_static(b"abc"));
        check!(
            (
                s.as_bytes(),
                s.len(),
                s.is_empty(),
                SecretBytes::new(Bytes::new()).is_empty(),
            ) == (b"abc".as_slice(), 3, false, true)
        );
    }

    /// The expected bytes are `printf tok-1 | openssl dgst -sha512 -hmac k`,
    /// which is what Kafka's `Mac.getInstance("HmacSHA512")` produces.
    #[test]
    fn hmac_is_hmac_sha512_of_the_token_id() {
        let expected = hex::decode(
            "0e527131aafda6c68f4161e8593b8e6eb591403471ef360854b63c883f5a3247\
             fde5b7720739ea603cb7226cd209ab14718a27b4d3c1b384da9beb197ee7cb3c",
        )
        .expect("valid hex");
        assert2::assert!(compute_token_hmac(b"k", "tok-1") == expected);
    }

    #[test]
    fn hmac_diverges_on_key_change() {
        let h1 = compute_token_hmac(b"k1", "tok-1");
        let h2 = compute_token_hmac(b"k2", "tok-1");
        assert2::assert!(h1 != h2);
    }

    #[test]
    fn secret_bytes_debug_does_not_leak_bytes() {
        let s = SecretBytes::new(b"super-secret-master-key".to_vec());
        let d = format!("{s:?}");
        assert2::assert!(d.contains("redacted"));
        assert2::assert!(!d.contains("super-secret"));
    }
}

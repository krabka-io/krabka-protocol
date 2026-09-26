//! RFC 5802 client-first parsing and the `saslname` and extension encodings,
//! shaped as Kafka's `ScramMessages.ClientFirstMessage` accepts them.
//!
//! Kafka's grammar is
//! `n,[a=<authzid>],[m=<value>,]n=<saslname>,r=<nonce>[,<alpha>=<value>]*`.
//! Only the `n` channel-binding flag is accepted: Kafka runs SCRAM without
//! channel binding, so a `y` or `p` GS2 header is malformed.

use crate::AuthError;

/// The SCRAM extension that marks a KIP-48 delegation-token authentication.
/// Its value is `true` when the SCRAM username is a token id.
pub const TOKEN_AUTH_EXTENSION: &str = "tokenauth";

/// A parsed SCRAM client-first message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramClientFirst {
    /// The GS2 header exactly as the client sent it, `n,,` or `n,a=<authzid>,`.
    /// The client-final `c=` attribute must be its base64 encoding.
    pub gs2_header: String,
    /// The decoded GS2 authorization id, `None` when the header carries none.
    pub authzid: Option<String>,
    /// The decoded SCRAM username (`n=`), with `=2C` and `=3D` unescaped.
    pub username: String,
    /// The client nonce (`r=`).
    pub nonce: String,
    /// The SCRAM extensions after the nonce, in the order the client sent
    /// them, for example `("tokenauth", "true")`.
    pub extensions: Vec<(String, String)>,
    /// `client-first-message-bare`, everything after the GS2 header. It is the
    /// first part of the `AuthMessage` both sides sign.
    pub bare: String,
}

impl ScramClientFirst {
    /// Parse a client-first message.
    ///
    /// # Errors
    /// [`AuthError::MalformedMessage`] when the message is not UTF-8, has a
    /// GS2 header other than `n,,` or `n,a=<authzid>,`, or does not follow
    /// Kafka's client-first grammar.
    pub fn parse(bytes: &[u8]) -> Result<Self, AuthError> {
        let message = std::str::from_utf8(bytes).map_err(|_| AuthError::MalformedMessage)?;
        let after_flag = message
            .strip_prefix("n,")
            .ok_or(AuthError::MalformedMessage)?;
        let (authzid_part, bare) = after_flag
            .split_once(',')
            .ok_or(AuthError::MalformedMessage)?;
        let authzid = if authzid_part.is_empty() {
            None
        } else {
            let raw = authzid_part
                .strip_prefix("a=")
                .ok_or(AuthError::MalformedMessage)?;
            Some(decode_saslname(raw)?)
        };
        let gs2_header = format!("n,{authzid_part},");

        let mut attributes = bare.split(',').peekable();
        // Kafka accepts, and ignores, a reserved `m=` attribute here.
        if attributes.peek().is_some_and(|a| a.starts_with("m=")) {
            let reserved = attributes.next().unwrap_or_default();
            if !is_value(&reserved[2..]) {
                return Err(AuthError::MalformedMessage);
            }
        }
        let username = attributes
            .next()
            .and_then(|a| a.strip_prefix("n="))
            .ok_or(AuthError::MalformedMessage)
            .and_then(decode_saslname)?;
        let nonce = attributes
            .next()
            .and_then(|a| a.strip_prefix("r="))
            .filter(|n| is_printable(n))
            .ok_or(AuthError::MalformedMessage)?
            .to_string();
        let extensions = attributes
            .map(|attribute| {
                attribute
                    .split_once('=')
                    .filter(|(key, value)| is_extension(key, value))
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .ok_or(AuthError::MalformedMessage)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            gs2_header,
            authzid,
            username,
            nonce,
            extensions,
            bare: bare.to_string(),
        })
    }

    /// The value of extension `key`, if the client sent it.
    #[must_use]
    pub fn extension(&self, key: &str) -> Option<&str> {
        self.extensions
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// True when the client sent `tokenauth=true`, Kafka's
    /// `ScramExtensions.tokenAuthenticated`.
    #[must_use]
    pub fn token_authenticated(&self) -> bool {
        self.extension(TOKEN_AUTH_EXTENSION)
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    }
}

/// Encode a username as an RFC 5802 `saslname`: `=` becomes `=3D` and `,`
/// becomes `=2C`, as Kafka's `ScramFormatter.saslName` does.
#[must_use]
pub fn encode_saslname(username: &str) -> String {
    username.replace('=', "=3D").replace(',', "=2C")
}

/// Decode an RFC 5802 `saslname`, as Kafka's `ScramFormatter.username` does.
///
/// # Errors
/// [`AuthError::MalformedMessage`] when the name is empty, holds a `,`, or
/// holds an `=` that does not start `=2C` or `=3D`.
pub fn decode_saslname(saslname: &str) -> Result<String, AuthError> {
    if saslname.is_empty() || saslname.contains(',') {
        return Err(AuthError::MalformedMessage);
    }
    let mut decoded = String::with_capacity(saslname.len());
    let mut rest = saslname;
    while let Some(at) = rest.find('=') {
        decoded.push_str(&rest[..at]);
        let escape = rest.get(at..at + 3).ok_or(AuthError::MalformedMessage)?;
        decoded.push(match escape {
            "=2C" => ',',
            "=3D" => '=',
            _ => return Err(AuthError::MalformedMessage),
        });
        rest = &rest[at + 3..];
    }
    decoded.push_str(rest);
    Ok(decoded)
}

/// Kafka's `EXTENSIONS` grammar: an ASCII-alphabetic key and a non-empty
/// value of ASCII characters other than NUL and `,`.
pub(super) fn is_extension(key: &str, value: &str) -> bool {
    !key.is_empty() && key.bytes().all(|b| b.is_ascii_alphabetic()) && is_value(value)
}

fn is_value(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| (0x01..=0x7F).contains(&b) && b != b',')
}

fn is_printable(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| (0x21..=0x7E).contains(&b) && b != b',')
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn client_first_parses_kafkas_grammar() {
        for (case, message, want) in [
            (
                "plain header",
                "n,,n=alice,r=abc",
                ScramClientFirst {
                    gs2_header: "n,,".into(),
                    authzid: None,
                    username: "alice".into(),
                    nonce: "abc".into(),
                    extensions: vec![],
                    bare: "n=alice,r=abc".into(),
                },
            ),
            (
                "authzid header",
                "n,a=alice,n=alice,r=abc",
                ScramClientFirst {
                    gs2_header: "n,a=alice,".into(),
                    authzid: Some("alice".into()),
                    username: "alice".into(),
                    nonce: "abc".into(),
                    extensions: vec![],
                    bare: "n=alice,r=abc".into(),
                },
            ),
            (
                "token extension",
                "n,,n=tok-id,r=abc,tokenauth=true",
                ScramClientFirst {
                    gs2_header: "n,,".into(),
                    authzid: None,
                    username: "tok-id".into(),
                    nonce: "abc".into(),
                    extensions: vec![("tokenauth".into(), "true".into())],
                    bare: "n=tok-id,r=abc,tokenauth=true".into(),
                },
            ),
            (
                "escaped names and reserved attribute",
                "n,a=a=2Cb=3Dc,m=ignored,n=a=2Cb=3Dc,r=x,foo=1,bar=2",
                ScramClientFirst {
                    gs2_header: "n,a=a=2Cb=3Dc,".into(),
                    authzid: Some("a,b=c".into()),
                    username: "a,b=c".into(),
                    nonce: "x".into(),
                    extensions: vec![("foo".into(), "1".into()), ("bar".into(), "2".into())],
                    bare: "m=ignored,n=a=2Cb=3Dc,r=x,foo=1,bar=2".into(),
                },
            ),
        ] {
            check!(
                ScramClientFirst::parse(message.as_bytes()) == Ok(want),
                "case {case}"
            );
        }
    }

    #[test]
    fn client_first_rejects_what_kafka_rejects() {
        for (case, message) in [
            (
                "channel binding requested",
                &b"p=tls-unique,,n=alice,r=abc"[..],
            ),
            ("client supports binding", b"y,,n=alice,r=abc"),
            ("no GS2 header", b"n=alice,r=abc"),
            ("authzid without a=", b"n,alice,n=alice,r=abc"),
            ("empty authzid", b"n,a=,n=alice,r=abc"),
            ("attributes out of order", b"n,,r=abc,n=alice"),
            ("missing nonce", b"n,,n=alice"),
            ("empty nonce", b"n,,n=alice,r="),
            ("bad escape", b"n,,n=a=2Xb,r=abc"),
            ("truncated escape", b"n,,n=ab=2,r=abc"),
            ("extension without value", b"n,,n=alice,r=abc,tokenauth"),
            (
                "non-alphabetic extension key",
                b"n,,n=alice,r=abc,t0ken=true",
            ),
            ("not UTF-8", b"n,,n=\xff,r=abc"),
        ] {
            check!(
                ScramClientFirst::parse(message) == Err(AuthError::MalformedMessage),
                "case {case}"
            );
        }
    }

    #[test]
    fn token_authenticated_reads_the_tokenauth_extension() {
        for (message, want) in [
            ("n,,n=t,r=a,tokenauth=true", true),
            ("n,,n=t,r=a,tokenauth=TRUE", true),
            ("n,,n=t,r=a,tokenauth=false", false),
            ("n,,n=t,r=a", false),
        ] {
            let parsed = ScramClientFirst::parse(message.as_bytes()).unwrap();
            check!(parsed.token_authenticated() == want, "message {message}");
        }
    }

    #[test]
    fn saslname_encoding_round_trips() {
        for (name, encoded) in [
            ("alice", "alice"),
            ("a,b", "a=2Cb"),
            ("a=b", "a=3Db"),
            ("=,", "=3D=2C"),
        ] {
            check!(
                (encode_saslname(name), decode_saslname(encoded))
                    == (encoded.to_string(), Ok(name.to_string()))
            );
        }
    }
}

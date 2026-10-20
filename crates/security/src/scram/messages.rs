//! RFC 5802 client-first parsing and the `saslname` and extension encodings,
//! shaped as Kafka's `ScramMessages.ClientFirstMessage` accepts them.
//!
//! Kafka's grammar is
//! `n,[a=<authzid>],[m=<value>,]n=<saslname>,r=<nonce>[,<alpha>=<value>]*`.
//! Only the `n` channel-binding flag is accepted: Kafka runs SCRAM without
//! channel binding, so a `y` or `p` GS2 header is malformed.
//!
//! Both sides sign an `AuthMessage` that Kafka's `ScramFormatter.authMessage`
//! rebuilds from the parsed fields rather than from the bytes the client
//! sent: `ClientFirstMessage.clientFirstMessageBare` and
//! `ClientFinalMessage.clientFinalMessageWithoutProof`. A client that signs
//! the bytes it sent, as RFC 5802 reads, authenticates only when the two
//! agree, as against Kafka.

use base64::{
    Engine, alphabet,
    engine::{GeneralPurpose, GeneralPurposeConfig, general_purpose::STANDARD as B64},
};

use crate::AuthError;

/// Standard base64 that, like Java's `Base64` decoder, ignores the unused
/// bits of the last character: Java decodes `bh==` to `n`, as it does `bg==`.
pub(super) const JAVA_B64: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_allow_trailing_bits(true),
);

/// The SCRAM extension that marks a KIP-48 delegation-token authentication.
/// Its value is `true` when the SCRAM username is a token id.
pub const TOKEN_AUTH_EXTENSION: &str = "tokenauth";

/// A parsed SCRAM client-first message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramClientFirst {
    /// The GS2 header exactly as the client sent it, `n,,` or `n,a=<authzid>,`.
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
    /// `client-first-message-bare` as Kafka's
    /// `ClientFirstMessage.clientFirstMessageBare` rebuilds it, the first part
    /// of the `AuthMessage` both sides sign: `n=<saslname as sent>,r=<nonce>`,
    /// then `,<key>=<value>` for each distinct extension key. A reserved `m=`
    /// attribute is dropped, and a repeated key keeps its last value, as the
    /// map that `ScramExtensions` builds does.
    ///
    /// Kafka iterates that map through `Map.copyOf`, whose order is fixed for
    /// one entry but randomized per JVM for two or more, so no order matches
    /// Kafka every time for a client that sends several distinct extensions.
    /// The keys keep the order in which the client first sent them, which is
    /// the order Kafka's own client signs.
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
        let saslname = attributes
            .next()
            .and_then(|a| a.strip_prefix("n="))
            .ok_or(AuthError::MalformedMessage)?;
        let username = decode_saslname(saslname)?;
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
        let bare = std::iter::once(format!("n={saslname},r={nonce}"))
            .chain(
                distinct_extensions(&extensions)
                    .into_iter()
                    .map(|(key, value)| format!("{key}={value}")),
            )
            .collect::<Vec<_>>()
            .join(",");

        Ok(Self {
            gs2_header,
            authzid,
            username,
            nonce,
            extensions,
            bare,
        })
    }

    /// The value of extension `key`, if the client sent it.
    ///
    /// When the client sent `key` more than once, the last value wins, as in
    /// the map that Kafka's `ScramExtensions` builds.
    #[must_use]
    pub fn extension(&self, key: &str) -> Option<&str> {
        self.extensions
            .iter()
            .rev()
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

/// Each distinct extension key once, in the order the client first sent it,
/// with the last value the client sent for it.
fn distinct_extensions(extensions: &[(String, String)]) -> Vec<(&str, &str)> {
    let mut distinct: Vec<(&str, &str)> = Vec::with_capacity(extensions.len());
    for (key, value) in extensions {
        match distinct.iter_mut().find(|(seen, _)| seen == key) {
            Some(entry) => entry.1 = value,
            None => distinct.push((key, value)),
        }
    }
    distinct
}

/// A SCRAM client-final message, split as Kafka's
/// `ScramMessages.ClientFinalMessage` matches it.
///
/// Kafka's grammar is `c=<base64>,r=<nonce>[,<alpha>=<value>]*,p=<base64>`,
/// in that order and nothing else. Every field but `without_proof` borrows
/// the raw message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ScramClientFinal<'a> {
    /// The base64 channel binding (`c=`), as the client sent it.
    pub channel_binding: &'a str,
    /// The combined nonce (`r=`).
    pub nonce: &'a str,
    /// The extensions between the nonce and the proof, in the order the
    /// client sent them.
    pub extensions: Vec<(&'a str, &'a str)>,
    /// The base64 client proof (`p=`), as the client sent it.
    pub proof: &'a str,
    /// `client-final-message-without-proof` as Kafka's
    /// `ClientFinalMessage.clientFinalMessageWithoutProof` rebuilds it, the
    /// last part of the `AuthMessage` both sides sign:
    /// `c=<channel binding>,r=<nonce>`. The extensions are dropped, and the
    /// channel binding is decoded and encoded again as Java's `Base64` does,
    /// so unused bits the client set in its last character are cleared.
    pub without_proof: String,
}

impl<'a> ScramClientFinal<'a> {
    /// Parse a client-final message with Kafka's grammar.
    ///
    /// # Errors
    /// [`AuthError::MalformedMessage`] when the message is not UTF-8 or does
    /// not match Kafka's client-final grammar as a whole.
    pub(super) fn parse(bytes: &'a [u8]) -> Result<Self, AuthError> {
        let message = std::str::from_utf8(bytes).map_err(|_| AuthError::MalformedMessage)?;
        // No base64 character is a `,`, so the proof is what follows the
        // last `,p=`. A later attribute leaves a `,` in it and fails below.
        let (before_proof, proof) = message
            .rsplit_once(",p=")
            .filter(|(_, proof)| is_base64(proof))
            .ok_or(AuthError::MalformedMessage)?;
        let mut attributes = before_proof.split(',');
        let channel_binding = attributes
            .next()
            .and_then(|a| a.strip_prefix("c="))
            .filter(|c| is_base64(c))
            .ok_or(AuthError::MalformedMessage)?;
        let nonce = attributes
            .next()
            .and_then(|a| a.strip_prefix("r="))
            .filter(|n| is_printable(n))
            .ok_or(AuthError::MalformedMessage)?;
        let extensions = attributes
            .map(|attribute| {
                attribute
                    .split_once('=')
                    .filter(|(key, value)| is_extension(key, value))
                    .ok_or(AuthError::MalformedMessage)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let channel_binding_bytes = JAVA_B64
            .decode(channel_binding)
            .map_err(|_| AuthError::MalformedMessage)?;
        let without_proof = format!("c={},r={nonce}", B64.encode(channel_binding_bytes));
        Ok(Self {
            channel_binding,
            nonce,
            extensions,
            proof,
            without_proof,
        })
    }
}

/// Kafka's `BASE64` grammar: groups of four base64 characters, the last of
/// which may end in `=` or `==`. It matches the empty string.
fn is_base64(value: &str) -> bool {
    let (body, remainder) = if let Some(body) = value.strip_suffix("==") {
        (body, 2)
    } else if let Some(body) = value.strip_suffix('=') {
        (body, 3)
    } else {
        (value, 0)
    };
    body.len() % 4 == remainder
        && body
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/')
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
                    bare: "n=a=2Cb=3Dc,r=x,foo=1,bar=2".into(),
                },
            ),
            (
                "repeated extension keeps its first place and last value",
                "n,,n=alice,r=x,a=1,b=2,a=3",
                ScramClientFirst {
                    gs2_header: "n,,".into(),
                    authzid: None,
                    username: "alice".into(),
                    nonce: "x".into(),
                    extensions: vec![
                        ("a".into(), "1".into()),
                        ("b".into(), "2".into()),
                        ("a".into(), "3".into()),
                    ],
                    bare: "n=alice,r=x,a=3,b=2".into(),
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
            ("n,,n=t,r=a,tokenauth=true,tokenauth=false", false),
            ("n,,n=t,r=a,tokenauth=false,tokenauth=true", true),
        ] {
            let parsed = ScramClientFirst::parse(message.as_bytes()).unwrap();
            check!(parsed.token_authenticated() == want, "message {message}");
        }
    }

    #[test]
    fn client_final_parses_kafkas_grammar() {
        for (case, message, want) in [
            (
                "plain",
                "c=biws,r=NS,p=AAAA",
                ScramClientFinal {
                    channel_binding: "biws",
                    nonce: "NS",
                    extensions: vec![],
                    proof: "AAAA",
                    without_proof: "c=biws,r=NS".into(),
                },
            ),
            (
                "padded base64",
                "c=bixhPWFsaWNlLA==,r=N!~,p=AAA=",
                ScramClientFinal {
                    channel_binding: "bixhPWFsaWNlLA==",
                    nonce: "N!~",
                    extensions: vec![],
                    proof: "AAA=",
                    without_proof: "c=bixhPWFsaWNlLA==,r=N!~".into(),
                },
            ),
            (
                "extensions before the proof",
                "c=biws,r=NS,foo=1,Bar=a=b,p=x,p=AA==",
                ScramClientFinal {
                    channel_binding: "biws",
                    nonce: "NS",
                    extensions: vec![("foo", "1"), ("Bar", "a=b"), ("p", "x")],
                    proof: "AA==",
                    without_proof: "c=biws,r=NS".into(),
                },
            ),
            (
                // Kafka reads a repeated `r=` or `p=` before the proof as an
                // extension.
                "repeated nonce and proof",
                "c=biws,r=NS,r=NS,p=AAAA,p=AAAA",
                ScramClientFinal {
                    channel_binding: "biws",
                    nonce: "NS",
                    extensions: vec![("r", "NS"), ("p", "AAAA")],
                    proof: "AAAA",
                    without_proof: "c=biws,r=NS".into(),
                },
            ),
            (
                "unused bits in the channel binding",
                "c=bixhPWFsaWNlLB==,r=NS,p=AAAA",
                ScramClientFinal {
                    channel_binding: "bixhPWFsaWNlLB==",
                    nonce: "NS",
                    extensions: vec![],
                    proof: "AAAA",
                    without_proof: "c=bixhPWFsaWNlLA==,r=NS".into(),
                },
            ),
            (
                "empty base64",
                "c=,r=NS,p=",
                ScramClientFinal {
                    channel_binding: "",
                    nonce: "NS",
                    extensions: vec![],
                    proof: "",
                    without_proof: "c=,r=NS".into(),
                },
            ),
        ] {
            check!(
                ScramClientFinal::parse(message.as_bytes()) == Ok(want),
                "case {case}"
            );
        }
    }

    #[test]
    fn client_final_rejects_what_kafka_rejects() {
        for (case, message) in [
            ("nonce first", &b"r=NS,c=biws,p=AAAA"[..]),
            ("proof before nonce", b"c=biws,p=AAAA,r=NS"),
            ("missing channel binding", b"r=NS,p=AAAA"),
            ("missing nonce", b"c=biws,p=AAAA"),
            ("missing proof", b"c=biws,r=NS"),
            (
                "unknown attribute after the proof",
                b"c=biws,r=NS,p=AAAA,x=1",
            ),
            ("duplicate channel binding", b"c=biws,c=biws,r=NS,p=AAAA"),
            ("extension before the nonce", b"c=biws,x=1,r=NS,p=AAAA"),
            ("empty extension value", b"c=biws,r=NS,x=,p=AAAA"),
            ("extension without value", b"c=biws,r=NS,x,p=AAAA"),
            ("non-alphabetic extension key", b"c=biws,r=NS,x1=a,p=AAAA"),
            (
                "non-ASCII extension value",
                "c=biws,r=NS,x=\u{e9},p=AAAA".as_bytes(),
            ),
            ("channel binding not base64", b"c=bi*s,r=NS,p=AAAA"),
            ("channel binding unpadded", b"c=biw,r=NS,p=AAAA"),
            ("proof not base64", b"c=biws,r=NS,p=AA-A"),
            ("proof unpadded", b"c=biws,r=NS,p=AAAAA"),
            ("proof over-padded", b"c=biws,r=NS,p=A==="),
            ("proof padding mid-string", b"c=biws,r=NS,p=AA==AAAA"),
            ("empty nonce", b"c=biws,r=,p=AAAA"),
            ("nonce with a space", b"c=biws,r=N S,p=AAAA"),
            ("nonce with a control byte", b"c=biws,r=N\x7f,p=AAAA"),
            ("trailing comma", b"c=biws,r=NS,p=AAAA,"),
            ("trailing newline", b"c=biws,r=NS,p=AAAA\n"),
            ("leading space", b" c=biws,r=NS,p=AAAA"),
            ("not UTF-8", b"c=biws,r=\xff,p=AAAA"),
        ] {
            check!(
                ScramClientFinal::parse(message) == Err(AuthError::MalformedMessage),
                "case {case}"
            );
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

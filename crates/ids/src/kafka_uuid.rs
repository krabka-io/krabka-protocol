//! Kafka's 128-bit identifier and its base64url string form.

use core::{cmp::Ordering, fmt, str::FromStr};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use derive_more::{From, Into};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use thiserror::Error;
use uuid::Uuid;

/// The longest input, in UTF-16 code units, that Kafka tries to decode.
const MAX_ENCODED_LEN: usize = 24;

/// The number of bytes in a UUID.
const UUID_LEN: usize = 16;

/// A Kafka `Uuid`: a cluster id, topic id, or log-directory id.
///
/// This is the value type of `org.apache.kafka.common.Uuid`. Kafka prints
/// these ids as 22 characters of unpadded base64url, for example
/// `UXyU9i5ARn6W00ON2taeWA`, not as the hyphenated hex form that
/// [`uuid::Uuid`] prints. [`Display`](fmt::Display) and [`FromStr`] use the
/// Kafka form. So does serde, which always writes the id as that string.
///
/// [`FromStr`] matches `Uuid.fromString` in Apache Kafka 4.3.1. It accepts
/// exactly the inputs that Kafka accepts and gives the same error messages.
/// Two consequences are not obvious:
///
/// - Kafka ignores the unused low bits of the last character, so
///   `_____________________x` parses to the same id as
///   `_____________________w`.
/// - Kafka also accepts the 22 characters followed by `==`.
///
/// Parsing never rejects a reserved id. Kafka's `fromString` does not reject
/// them, and `kafka-storage format --cluster-id` stores any string. Check
/// [`is_reserved`](Self::is_reserved) where a caller must refuse one.
///
/// The ordering is Kafka's `compareTo`: it compares the two halves as signed
/// 64-bit integers, most significant half first. This is not the unsigned
/// byte order of [`uuid::Uuid`].
///
/// # Examples
///
/// ```
/// use krabka_ids::KafkaUuid;
///
/// let id: KafkaUuid = "UXyU9i5ARn6W00ON2taeWA".parse()?;
/// assert2::assert!(id.to_string() == "UXyU9i5ARn6W00ON2taeWA");
/// assert2::assert!(KafkaUuid::METADATA_TOPIC_ID.to_string() == "AAAAAAAAAAAAAAAAAAAAAQ");
/// # Ok::<(), krabka_ids::KafkaUuidError>(())
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, From, Into)]
pub struct KafkaUuid(pub Uuid);

impl KafkaUuid {
    /// The id that stands for a null or empty id (Kafka's `ZERO_UUID`).
    pub const ZERO: Self = Self::from_bits(0, 0);

    /// The reserved id with value one (Kafka's `ONE_UUID`).
    pub const ONE: Self = Self::from_bits(0, 1);

    /// The topic id of the metadata topic, `__cluster_metadata`.
    pub const METADATA_TOPIC_ID: Self = Self::ONE;

    /// The ids that [`random`](Self::random) never returns (Kafka's
    /// `Uuid.RESERVED`).
    pub const RESERVED: [Self; 2] = [Self::ZERO, Self::ONE];

    /// The directory id of a replica that is in a log directory that the
    /// broker has not yet reported (Kafka's `DirectoryId.MIGRATING`).
    pub const DIRECTORY_MIGRATING: Self = Self::ZERO;

    /// The directory id of a replica that is not yet placed in a log directory
    /// (Kafka's `DirectoryId.UNASSIGNED`).
    pub const DIRECTORY_UNASSIGNED: Self = Self::ONE;

    /// The directory id of a replica in an offline log directory that has no
    /// known id (Kafka's `DirectoryId.LOST`).
    pub const DIRECTORY_LOST: Self = Self::from_bits(0, 2);

    /// Builds an id from its two halves, as Kafka's `Uuid(long, long)` does.
    #[must_use]
    pub const fn from_bits(most_significant: i64, least_significant: i64) -> Self {
        Self(Uuid::from_u64_pair(
            most_significant.cast_unsigned(),
            least_significant.cast_unsigned(),
        ))
    }

    /// The inner [`uuid::Uuid`].
    #[must_use]
    pub const fn get(self) -> Uuid {
        self.0
    }

    /// The most significant 64 bits, as Kafka's `getMostSignificantBits`
    /// returns them.
    #[must_use]
    pub const fn most_significant_bits(self) -> i64 {
        self.0.as_u64_pair().0.cast_signed()
    }

    /// The least significant 64 bits, as Kafka's `getLeastSignificantBits`
    /// returns them.
    #[must_use]
    pub const fn least_significant_bits(self) -> i64 {
        self.0.as_u64_pair().1.cast_signed()
    }

    /// Returns a random version 4 id, as Kafka's `Uuid.randomUuid` does.
    ///
    /// The result is never in [`RESERVED`](Self::RESERVED), and its string
    /// form never starts with `-`. A leading `-` would make a command-line
    /// parser read the id as an option.
    #[must_use]
    pub fn random() -> Self {
        first_accepted(Uuid::new_v4, Self::is_rejected_random)
    }

    /// Returns a random log-directory id, as Kafka's `DirectoryId.random`
    /// does.
    ///
    /// The result obeys the rules of [`random`](Self::random), and it is never
    /// one of the 100 reserved directory ids (see
    /// [`is_reserved_directory_id`](Self::is_reserved_directory_id)).
    #[must_use]
    pub fn random_directory_id() -> Self {
        first_accepted(Uuid::new_v4, Self::is_rejected_directory_id)
    }

    /// Returns `true` for an id in [`RESERVED`](Self::RESERVED).
    #[must_use]
    pub fn is_reserved(self) -> bool {
        Self::RESERVED.contains(&self)
    }

    /// Returns `true` for one of the 100 lowest ids, which Kafka reserves as
    /// directory ids (Kafka's `DirectoryId.reserved`).
    #[must_use]
    pub const fn is_reserved_directory_id(self) -> bool {
        self.most_significant_bits() == 0 && self.least_significant_bits() < 100
    }

    /// Parses the Kafka form, or the 36-character hyphenated hex form.
    ///
    /// Kafka's `Uuid.fromString` rejects the hyphenated form, so
    /// [`FromStr`] does too. Krabka's own tools also accept it, and they
    /// parse through this function. An input with a `-` at each of the four
    /// hyphen positions of a 36-character string is parsed as hex. Any other
    /// input is parsed as [`FromStr`] parses it.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaUuidError::Hyphenated`] when the input has the shape of
    /// the hyphenated form but is not valid hex. Otherwise returns the error
    /// of [`FromStr`].
    pub fn parse_kafka_or_hyphenated(input: &str) -> Result<Self, KafkaUuidError> {
        if has_hyphenated_shape(input) {
            return Uuid::try_parse(input)
                .map(Self)
                .map_err(|source| KafkaUuidError::Hyphenated {
                    input: input.to_owned(),
                    source,
                });
        }
        input.parse()
    }

    fn is_rejected_random(self) -> bool {
        self.is_reserved() || self.string_starts_with_dash()
    }

    fn is_rejected_directory_id(self) -> bool {
        self.is_rejected_random() || self.is_reserved_directory_id()
    }

    /// Returns `true` when the first base64url character is `-`, the
    /// character for the six-bit value 62.
    fn string_starts_with_dash(self) -> bool {
        self.0.as_bytes()[0] >> 2 == 62
    }
}

/// Draws ids from `next` until one is not rejected.
fn first_accepted(mut next: impl FnMut() -> Uuid, rejected: fn(KafkaUuid) -> bool) -> KafkaUuid {
    loop {
        let candidate = KafkaUuid(next());
        if !rejected(candidate) {
            return candidate;
        }
    }
}

fn has_hyphenated_shape(input: &str) -> bool {
    let bytes = input.as_bytes();
    bytes.len() == 36 && [8, 13, 18, 23].iter().all(|&i| bytes[i] == b'-')
}

impl fmt::Display for KafkaUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&URL_SAFE_NO_PAD.encode(self.0.as_bytes()))
    }
}

impl FromStr for KafkaUuid {
    type Err = KafkaUuidError;

    /// Parses the Kafka form, as Kafka's `Uuid.fromString` does.
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        // Kafka measures the length of a Java string, in UTF-16 code units.
        if input.encode_utf16().count() > MAX_ENCODED_LEN {
            return Err(KafkaUuidError::TooLong {
                prefix: java_prefix(input, MAX_ENCODED_LEN),
            });
        }
        let decoded = java_url_decode(&latin1_bytes(input))?;
        let bytes: [u8; UUID_LEN] =
            decoded
                .try_into()
                .map_err(|decoded: Vec<u8>| KafkaUuidError::WrongLength {
                    input: input.to_owned(),
                    decoded_len: decoded.len(),
                })?;
        Ok(Self(Uuid::from_bytes(bytes)))
    }
}

impl PartialOrd for KafkaUuid {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KafkaUuid {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.most_significant_bits(), self.least_significant_bits()).cmp(&(
            other.most_significant_bits(),
            other.least_significant_bits(),
        ))
    }
}

impl Serialize for KafkaUuid {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for KafkaUuid {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

/// A string that is not a valid [`KafkaUuid`].
///
/// Each message is the message of the `IllegalArgumentException` that
/// Kafka's `Uuid.fromString` throws for the same input. Kafka passes the
/// messages of `java.util.Base64` through unchanged, so some of them name a
/// byte array.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum KafkaUuidError {
    /// The input is longer than 24 UTF-16 code units.
    #[error("Input string with prefix `{prefix}` is too long to be decoded as a base64 UUID")]
    TooLong {
        /// The first 24 UTF-16 code units of the input.
        prefix: String,
    },

    /// The input is valid base64url, but not of 16 bytes.
    #[error(
        "Input string `{input}` decoded as {decoded_len} bytes, which is not equal to the \
         expected 16 bytes of a base64-encoded UUID"
    )]
    WrongLength {
        /// The input.
        input: String,
        /// The number of bytes that the input decodes to.
        decoded_len: usize,
    },

    /// The input is a single character.
    #[error("Input byte[] should at least have 2 bytes for base64 bytes")]
    TooShort,

    /// The input holds a byte that is not in the base64url alphabet.
    #[error("Illegal base64 character {}", JavaSignedHex(*.byte))]
    IllegalCharacter {
        /// The byte. A character above U+00FF is the byte `?`.
        byte: u8,
    },

    /// A `=` is in a position where base64 does not allow padding.
    #[error("Input byte array has wrong 4-byte ending unit")]
    WrongEndingUnit,

    /// The last base64 unit has only one character.
    #[error("Last unit does not have enough valid bits")]
    LastUnitTooShort,

    /// Bytes follow the padding.
    #[error("Input byte array has incorrect ending byte at {position}")]
    IncorrectEndingByte {
        /// The zero-based index of the first byte after the padding.
        position: usize,
    },

    /// The input has the shape of the hyphenated hex form but is not valid
    /// hex. Only [`KafkaUuid::parse_kafka_or_hyphenated`] returns it.
    #[error("Input string `{input}` is not a valid hyphenated UUID: {source}")]
    Hyphenated {
        /// The input.
        input: String,
        /// The error from the hex parser.
        source: uuid::Error,
    },
}

/// Formats a byte as Java's `Integer.toString(b, 16)` formats a signed
/// `byte`: `0xe9` is `-17`.
struct JavaSignedHex(u8);

impl fmt::Display for JavaSignedHex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = self.0.cast_signed();
        if value < 0 {
            write!(f, "-{:x}", value.unsigned_abs())
        } else {
            write!(f, "{value:x}")
        }
    }
}

/// Returns the first `max` UTF-16 code units of `input`, as Java's
/// `substring(0, max)` does.
///
/// When the cut splits a surrogate pair, Java keeps the lone high surrogate,
/// and the JVM prints it as `?`. This function writes that `?`.
fn java_prefix(input: &str, max: usize) -> String {
    let mut prefix = String::new();
    let mut units = 0;
    for c in input.chars() {
        let width = c.len_utf16();
        if units + width > max {
            if units < max {
                prefix.push('?');
            }
            break;
        }
        prefix.push(c);
        units += width;
    }
    prefix
}

/// Encodes `input` as Java's `String.getBytes(ISO_8859_1)` does: one byte per
/// character, and `?` for a character above U+00FF.
fn latin1_bytes(input: &str) -> Vec<u8> {
    input
        .chars()
        .map(|c| u8::try_from(c).unwrap_or(b'?'))
        .collect()
}

/// The six-bit value of a base64url character.
fn url_value(byte: u8) -> Option<u32> {
    let value = match byte {
        b'A'..=b'Z' => byte - b'A',
        b'a'..=b'z' => byte - b'a' + 26,
        b'0'..=b'9' => byte - b'0' + 52,
        b'-' => 62,
        b'_' => 63,
        _ => return None,
    };
    Some(u32::from(value))
}

/// Decodes base64url as `Base64.getUrlDecoder().decode(byte[])` does in the
/// JDK that Kafka 4.3.1 ships with.
///
/// This decoder exists so that [`KafkaUuid`] accepts and rejects the same
/// strings as Kafka, with the same messages. The `base64` crate is stricter:
/// it rejects the non-zero trailing bits that the JDK ignores. The caller
/// bounds the input to 24 bytes, so the output is at most 18 bytes.
fn java_url_decode(src: &[u8]) -> Result<Vec<u8>, KafkaUuidError> {
    // `decodedOutLength`: the only check that it makes before decoding.
    if src.len() == 1 {
        return Err(KafkaUuidError::TooShort);
    }

    // `decode0`, without the MIME branches.
    let mut dst = Vec::with_capacity(src.len() / 4 * 3 + 2);
    let mut bits = 0u32;
    let mut shift_to = 18i32;
    let mut sp = 0;
    while sp < src.len() {
        let byte = src[sp];
        sp += 1;
        if let Some(value) = url_value(byte) {
            bits |= value << shift_to;
            shift_to -= 6;
            if shift_to < 0 {
                dst.extend_from_slice(&bits.to_be_bytes()[1..]);
                shift_to = 18;
                bits = 0;
            }
            continue;
        }
        if byte != b'=' {
            return Err(KafkaUuidError::IllegalCharacter { byte });
        }
        // `=` ends the data. Only `xx==` and `xxx=` are complete units.
        let second_pad_missing = shift_to == 6
            && (sp == src.len() || {
                sp += 1;
                src[sp - 1] != b'='
            });
        if second_pad_missing || shift_to == 18 {
            return Err(KafkaUuidError::WrongEndingUnit);
        }
        break;
    }
    match shift_to {
        6 => dst.push(bits.to_be_bytes()[1]),
        0 => dst.extend_from_slice(&bits.to_be_bytes()[1..3]),
        12 => return Err(KafkaUuidError::LastUnitTooShort),
        _ => {}
    }
    if sp < src.len() {
        return Err(KafkaUuidError::IncorrectEndingByte { position: sp });
    }
    Ok(dst)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use uuid::Uuid;

    use super::{KafkaUuid, first_accepted};

    type Rejected = fn(KafkaUuid) -> bool;

    // The first id that the two generators accept, from a fixed sequence of
    // candidates. `-` leads the string form of any id whose first byte is in
    // `0xf8..=0xfb`.
    #[test]
    fn random_generators_skip_the_ids_that_kafka_skips() {
        let dash = Uuid::from_u128(0xf800_0000_0000_4000_8000_0000_0000_0000);
        let dash_last = Uuid::from_u128(0xfbff_ffff_ffff_4fff_bfff_ffff_ffff_ffff);
        let not_dash = Uuid::from_u128(0xfc00_0000_0000_4000_8000_0000_0000_0000);
        let reserved_dir = Uuid::from_u128(99);
        let first_free_dir = Uuid::from_u128(100);

        let cases: [(&str, Vec<Uuid>, Rejected, Uuid); 5] = [
            (
                "random skips ZERO and ONE",
                vec![Uuid::nil(), Uuid::from_u128(1), reserved_dir],
                KafkaUuid::is_rejected_random,
                reserved_dir,
            ),
            (
                "random skips a leading dash",
                vec![dash, dash_last, not_dash],
                KafkaUuid::is_rejected_random,
                not_dash,
            ),
            (
                "directory id skips the random rejects",
                vec![Uuid::nil(), dash, first_free_dir],
                KafkaUuid::is_rejected_directory_id,
                first_free_dir,
            ),
            (
                "directory id skips the first 100",
                vec![Uuid::from_u128(2), reserved_dir, first_free_dir],
                KafkaUuid::is_rejected_directory_id,
                first_free_dir,
            ),
            (
                "directory id takes the msb-nonzero id",
                vec![Uuid::from_u64_pair(1, 0)],
                KafkaUuid::is_rejected_directory_id,
                Uuid::from_u64_pair(1, 0),
            ),
        ];
        for (name, candidates, rejected, expected) in cases {
            let mut queue = VecDeque::from(candidates);
            let chosen = first_accepted(|| queue.pop_front().expect("ran out"), rejected);
            assert2::assert!(chosen == KafkaUuid(expected), "{name}");
        }
    }

    #[test]
    fn the_dash_check_agrees_with_the_string_form() {
        for first_byte in 0..=u8::MAX {
            let mut bytes = [0u8; 16];
            bytes[0] = first_byte;
            let id = KafkaUuid(Uuid::from_bytes(bytes));
            assert2::assert!(
                id.string_starts_with_dash() == id.to_string().starts_with('-'),
                "{first_byte:#x}"
            );
        }
    }
}

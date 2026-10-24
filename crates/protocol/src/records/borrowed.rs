//! Borrowed `RecordBatch<'a>`, `Record<'a>`, and `RecordHeader<'a>`.

use bytes::Bytes;
use krabka_compression::RecordDecompressionPolicy;
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use zerocopy::FromBytes as _;

use crate::{
    primitives::varint::{get_varint_from_slice, get_varlong_from_slice},
    records::{
        RecordsError,
        crc::crc32c,
        header::{Attributes, HEADER_LEN, RecordBatchHeader},
    },
};

// batch_length field semantics: bytes after itself.
// Header tail = partition_leader_epoch(4) + magic(1) + crc(4) +
//   attributes(2) + last_offset_delta(4) + base_timestamp(8) +
//   max_timestamp(8) + producer_id(8) + producer_epoch(2) +
//   base_sequence(4) + records_count(4) = 49 bytes.
const HEADER_TAIL_LEN: i32 = 49;

pub struct RecordBatch<'a> {
    pub(crate) header: &'a RecordBatchHeader,
    pub(crate) body: RecordBody<'a>,
}

pub(crate) enum RecordBody<'a> {
    Borrowed(&'a [u8]),
    Owned(Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<'a> {
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: RecordHeaders<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader<'a> {
    pub key: &'a str,
    pub value: Option<&'a [u8]>,
}

/// The headers of one borrowed [`Record`], read on demand from the record's
/// header bytes.
///
/// The record parser checks every header before it hands a record out, then
/// keeps only the header bytes and their count. Collecting them into a `Vec`
/// instead cost one allocation per record that carries headers, which the
/// produce path pays for every record of every client batch it validates.
/// Reading them again allocates nothing, and [`Self::find`], which a caller
/// looking for one header by name wants, does not check any name's UTF-8 a
/// second time.
#[derive(Clone, Copy, Default)]
pub struct RecordHeaders<'a> {
    /// The record's header region, checked by the parser.
    bytes: &'a [u8],
    count: usize,
}

impl<'a> RecordHeaders<'a> {
    /// The number of headers.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether the record carries no header.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The headers in wire order.
    #[inline]
    #[must_use]
    pub fn iter(&self) -> RecordHeaderIter<'a> {
        RecordHeaderIter {
            rest: self.bytes,
            left: self.count,
            names: HeaderNames::default(),
        }
    }

    /// The value of the first header named `name`: `Some(None)` for a header
    /// whose value is null, and `None` when no header has the name.
    ///
    /// It compares each name's bytes with `name`'s, so it reads no name as
    /// UTF-8: a name with the same bytes as a `&str` is that `&str`.
    #[inline]
    #[must_use]
    pub fn find(&self, name: &str) -> Option<Option<&'a [u8]>> {
        let mut rest = self.bytes;
        for _ in 0..self.count {
            // The parser checked these bytes, so the reads cannot fail; a
            // failure ends the search rather than panicking.
            let header = read_raw_header(rest)?;
            if same_bytes(header.key, name.as_bytes()) {
                return Some(header.value);
            }
            rest = header.rest;
        }
        None
    }
}

impl<'a> IntoIterator for RecordHeaders<'a> {
    type Item = RecordHeader<'a>;
    type IntoIter = RecordHeaderIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> IntoIterator for &RecordHeaders<'a> {
    type Item = RecordHeader<'a>;
    type IntoIter = RecordHeaderIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl std::fmt::Debug for RecordHeaders<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl PartialEq for RecordHeaders<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl Eq for RecordHeaders<'_> {}

/// An iterator over [`RecordHeaders`].
#[derive(Debug)]
pub struct RecordHeaderIter<'a> {
    rest: &'a [u8],
    left: usize,
    names: HeaderNames<'a>,
}

impl<'a> Iterator for RecordHeaderIter<'a> {
    type Item = RecordHeader<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        // The parser checked these bytes before it built the record, so the
        // read cannot fail. Ending the iteration is the answer if it did
        // anyway, rather than a panic.
        let Ok((header, rest)) = parse_header(self.rest, 0, &mut self.names) else {
            self.left = 0;
            return None;
        };
        self.rest = rest;
        Some(header)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

impl ExactSizeIterator for RecordHeaderIter<'_> {}

impl Record<'_> {
    /// Size of this record's body in bytes: what Kafka's `DefaultRecord` calls
    /// `sizeOfBodyInBytes`, and what its varlong length prefix declares.
    ///
    /// The body is the attributes byte, the two deltas, and the key, the value
    /// and the headers, each behind its own length prefix. A null key, value or
    /// header value counts as the one-byte `-1` marker. The length prefix
    /// itself is not counted. This is the size the owned
    /// [`crate::records::Record::body_len`] reports for the same record.
    #[must_use]
    pub fn body_len(&self) -> usize {
        super::owned::body_len_of(
            self.timestamp_delta,
            self.offset_delta,
            self.key.map(<[u8]>::len),
            self.value.map(<[u8]>::len),
            self.headers
                .iter()
                .map(|h| (h.key.len(), h.value.map(<[u8]>::len))),
        )
    }
}

impl RecordBatch<'_> {
    #[must_use]
    pub fn header(&self) -> &RecordBatchHeader {
        self.header
    }

    #[must_use]
    pub fn attributes(&self) -> Attributes {
        Attributes(self.header.attributes.get())
    }
}

impl<'a> Default for RecordBatch<'a> {
    /// Returns an empty record batch with a zeroed header and no records body.
    /// Use it in generated `Default` impls and round-trip tests. It is not
    /// suitable for a real Kafka record batch.
    fn default() -> Self {
        use zerocopy::FromZeros as _;
        // RecordBatchHeader derives zerocopy::FromZeros (via FromBytes), so zeroing is safe.
        let header: &'a RecordBatchHeader = Box::leak(Box::new(RecordBatchHeader::new_zeroed()));
        Self {
            header,
            body: RecordBody::Owned(bytes::Bytes::new()),
        }
    }
}

// ── Decode ────────────────────────────────────────────────────────────────────

impl<'de> crate::DecodeBorrow<'de> for RecordBatch<'de> {
    fn decode_borrow(buf: &mut &'de [u8], _version: i16) -> Result<Self, crate::ProtocolError> {
        Self::decode_borrow_with_policy(buf, RecordDecompressionPolicy::default())
            .map_err(Into::into)
    }
}

impl<'de> RecordBatch<'de> {
    /// Decodes a borrowed v2 record batch with explicit decompression limits.
    ///
    /// # Errors
    ///
    /// Returns the underlying records error for malformed, truncated, corrupt,
    /// or over-limit input.
    pub fn decode_borrow_with_policy(
        buf: &mut &'de [u8],
        policy: RecordDecompressionPolicy,
    ) -> Result<Self, RecordsError> {
        decode_borrow_impl(buf, policy)
    }
}

fn decode_borrow_impl<'de>(
    buf: &mut &'de [u8],
    policy: RecordDecompressionPolicy,
) -> Result<RecordBatch<'de>, RecordsError> {
    if buf.len() < HEADER_LEN {
        return Err(RecordsError::HeaderTooShort {
            needed: HEADER_LEN - buf.len(),
        });
    }
    // Split off the header slice — both slices remain tied to 'de.
    let (hdr_slice, rest) = buf.split_at(HEADER_LEN);
    let hdr: &'de RecordBatchHeader =
        RecordBatchHeader::ref_from_bytes(hdr_slice).map_err(|_| RecordsError::ZerocopyFailure)?;
    if hdr.magic != 2 {
        return Err(RecordsError::UnsupportedMagic { found: hdr.magic });
    }

    let body_len = i32::checked_sub(hdr.batch_length.get(), HEADER_TAIL_LEN)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| RecordsError::RecordParse("negative or oversized batch_length".into()))?;

    if rest.len() < body_len {
        return Err(RecordsError::BodyTooShort {
            needed: body_len - rest.len(),
        });
    }
    let (raw_body, after) = rest.split_at(body_len);
    // The covered header and raw body are contiguous in the input batch.
    let computed = crc32c(&buf[21..HEADER_LEN + body_len]);
    *buf = after;

    let expected = hdr.crc.get();
    if computed != expected {
        return Err(RecordsError::CrcMismatch { expected, computed });
    }

    let attributes = Attributes(hdr.attributes.get());
    let codec = attributes.compression();
    let body = if codec == krabka_compression::CompressionType::None {
        RecordBody::Borrowed(raw_body)
    } else {
        let decompressed = krabka_compression::decompress(
            codec,
            raw_body,
            policy.output_limit(ByteSize::from_bytes(raw_body.len() as u64)),
        )?;
        RecordBody::Owned(decompressed)
    };

    Ok(RecordBatch { header: hdr, body })
}

/// A single complete v2 batch located within a larger buffer, together with
/// its already-validated header.
///
/// [`validate_one_v2_batch`] returns this type.
#[derive(Debug)]
pub struct ValidatedBatch<'a> {
    /// The fixed 61-byte header, reinterpreted in place, zero-copy.
    pub header: &'a RecordBatchHeader,
    /// Total on-disk and wire length of this batch in bytes, which is
    /// `12 + batch_length`, that is the header plus the body.
    pub total_len: usize,
    raw_body: &'a [u8],
}

impl ValidatedBatch<'_> {
    /// Validates that the body contains exactly the number of structurally
    /// valid records declared by the batch header.
    ///
    /// Uncompressed records are parsed directly from the input slice. A
    /// compressed body is decompressed once under `policy`, parsed, and then
    /// discarded; the original compressed bytes remain available to callers
    /// for verbatim append.
    ///
    /// # Errors
    ///
    /// Returns a records or compression error when the body is malformed,
    /// exceeds the decompression policy, contains fewer records than declared,
    /// or has trailing bytes after the declared records.
    pub fn validate_records(&self, policy: RecordDecompressionPolicy) -> Result<(), RecordsError> {
        self.validate_records_with(policy, |_| {})
    }

    /// Validates the records and calls `visit` for each borrowed record.
    ///
    /// This combines structural validation with callers' metadata checks while
    /// keeping compressed batches on the borrowed path.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::validate_records`].
    pub fn validate_records_with(
        &self,
        policy: RecordDecompressionPolicy,
        mut visit: impl FnMut(&Record<'_>),
    ) -> Result<(), RecordsError> {
        let count = self.header.records_count.get();
        if count < 0 {
            return Err(RecordsError::RecordParse(format!(
                "negative records_count {count}"
            )));
        }

        let codec = Attributes(self.header.attributes.get()).compression();
        let decompressed = if codec == krabka_compression::CompressionType::None {
            None
        } else {
            Some(krabka_compression::decompress(
                codec,
                self.raw_body,
                policy.output_limit(ByteSize::from_bytes(self.raw_body.len() as u64)),
            )?)
        };
        let mut remaining = decompressed.as_deref().unwrap_or(self.raw_body);
        let mut names = HeaderNames::default();
        for i in 0..count {
            let record = parse_one_record(&mut remaining, &mut names)
                .map_err(|error| RecordsError::RecordParse(format!("record[{i}]: {error}")))?;
            visit(&record);
        }
        if !remaining.is_empty() {
            return Err(RecordsError::RecordParse(format!(
                "trailing bytes after records (left={})",
                remaining.len()
            )));
        }
        Ok(())
    }
}

/// Validates the header, declared length, and CRC of one v2 record batch at
/// the start of `buf`.
///
/// This function materializes no records and decompresses no body.
///
/// This is the first stage of the produce passthrough fast path. It
/// reinterprets the fixed header in place, zero-copy, checks `magic == 2`, and
/// verifies the producer's CRC over `header[21..61] ++ raw_body`, which is the
/// compressed body bytes exactly as stored. Call
/// [`ValidatedBatch::validate_records`] before accepting producer input.
///
/// Returns the CRC-validated header, which the caller needs for offset
/// stamping and for idempotent and transactional gating, and the batch's total
/// byte length, which lets the caller slice out the verbatim bytes.
///
/// # Errors
///
/// - [`RecordsError::HeaderTooShort`] or [`RecordsError::BodyTooShort`]
///   when `buf` does not contain a whole batch.
/// - [`RecordsError::UnsupportedMagic`] for a non-v2 batch.
/// - [`RecordsError::CrcMismatch`] when the stored CRC does not match.
pub fn validate_one_v2_batch(buf: &[u8]) -> Result<ValidatedBatch<'_>, RecordsError> {
    if buf.len() < HEADER_LEN {
        return Err(RecordsError::HeaderTooShort {
            needed: HEADER_LEN - buf.len(),
        });
    }
    let (hdr_slice, rest) = buf.split_at(HEADER_LEN);
    let hdr: &RecordBatchHeader =
        RecordBatchHeader::ref_from_bytes(hdr_slice).map_err(|_| RecordsError::ZerocopyFailure)?;
    if hdr.magic != 2 {
        return Err(RecordsError::UnsupportedMagic { found: hdr.magic });
    }
    let body_len = i32::checked_sub(hdr.batch_length.get(), HEADER_TAIL_LEN)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| RecordsError::RecordParse("negative or oversized batch_length".into()))?;
    if rest.len() < body_len {
        return Err(RecordsError::BodyTooShort {
            needed: body_len - rest.len(),
        });
    }
    let raw_body = &rest[..body_len];

    let expected = hdr.crc.get();
    let computed = crc32c(&buf[21..HEADER_LEN + body_len]);
    if computed != expected {
        return Err(RecordsError::CrcMismatch { expected, computed });
    }

    Ok(ValidatedBatch {
        header: hdr,
        total_len: HEADER_LEN + body_len,
        raw_body,
    })
}

/// Sums the `records_count` header field of every concatenated v2 batch in
/// `buf`, and decompresses or parses no record body.
///
/// This is intentionally a best-effort metrics helper. Non-v2 input returns 0.
/// Malformed or truncated input contributes every complete v2 batch before the
/// first bad boundary. A negative `records_count` contributes 0. Real append
/// validation still goes through [`validate_one_v2_batch`].
#[must_use]
pub fn count_records_in_v2_batches(buf: &[u8]) -> u64 {
    // Magic byte sits at offset 16; only v2 (magic == 2) carries a
    // `records_count` header. Legacy slices contribute 0 here.
    const MAGIC_OFFSET: usize = 16;
    if buf.len() <= MAGIC_OFFSET || buf[MAGIC_OFFSET] != 2 {
        return 0;
    }

    let mut total = 0u64;
    let mut remaining = buf;
    while remaining.len() >= HEADER_LEN {
        if remaining[MAGIC_OFFSET] != 2 {
            break;
        }
        let Ok(hdr) = RecordBatchHeader::ref_from_bytes(&remaining[..HEADER_LEN]) else {
            break;
        };
        let Ok(after_len) = usize::try_from(hdr.batch_length.get()) else {
            break;
        };
        let total_len = 12 + after_len;
        if total_len < HEADER_LEN || total_len > remaining.len() {
            break;
        }
        total += u64::try_from(hdr.records_count.get().max(0)).unwrap_or(0);
        remaining = &remaining[total_len..];
    }
    total
}

// ── Iteration ─────────────────────────────────────────────────────────────────

impl RecordBatch<'_> {
    /// Iterates over the records and parses each one lazily.
    ///
    /// The returned `Record<'b>` items borrow from `self` with lifetime `'b`,
    /// not from the original input buffer. For uncompressed batches the
    /// backing memory is the input buffer. For compressed batches it is the
    /// batch's internal decompressed `Bytes`.
    /// # Panics
    /// Panics if a value previously validated by the protocol type no longer satisfies its encoded-length or field-range invariant.
    pub fn iter(&self) -> RecordIter<'_> {
        let body: &[u8] = match &self.body {
            RecordBody::Borrowed(s) => s,
            RecordBody::Owned(b) => b.as_ref(),
        };
        let count = usize::try_from(self.header.records_count.get().max(0))
            .expect("non-negative record count must fit in usize");
        RecordIter {
            remaining: body,
            count,
            index: 0,
            names: HeaderNames::default(),
        }
    }
}

impl<'a> IntoIterator for &'a RecordBatch<'_> {
    type Item = Result<Record<'a>, RecordsError>;
    type IntoIter = RecordIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct RecordIter<'a> {
    remaining: &'a [u8],
    count: usize,
    index: usize,
    names: HeaderNames<'a>,
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = Result<Record<'a>, RecordsError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.count {
            return None;
        }
        self.index += 1;
        Some(parse_one_record(&mut self.remaining, &mut self.names))
    }
}

#[inline]
pub(crate) fn parse_one_record<'a>(
    buf: &mut &'a [u8],
    names: &mut HeaderNames<'a>,
) -> Result<Record<'a>, RecordsError> {
    let mut cur = *buf;
    let body_len = get_varlong_from_slice(&mut cur)
        .map_err(|e| RecordsError::RecordParse(format!("record length: {e}")))?;
    let body_len = usize::try_from(body_len).map_err(|_| {
        RecordsError::RecordParse(format!("record length negative or too large: {body_len}"))
    })?;
    let Some((body, rest)) = cur.split_at_checked(body_len) else {
        return Err(RecordsError::BodyTooShort {
            needed: body_len - cur.len(),
        });
    };
    *buf = rest;
    parse_body(body, names)
}

/// One record body, which must fill `body` exactly.
///
/// Every reader below takes its slice by value and hands back what is left,
/// so the cursor stays in a register: a cursor behind `&mut` that a call can
/// see is stored to memory after every field.
fn parse_body<'a>(body: &'a [u8], names: &mut HeaderNames<'a>) -> Result<Record<'a>, RecordsError> {
    let Some((&attributes, mut cur)) = body.split_first() else {
        return Err(RecordsError::RecordParse("record body empty".into()));
    };
    let attributes = i8::from_ne_bytes([attributes]);
    let timestamp_delta = get_varlong_from_slice(&mut cur)
        .map_err(|e| RecordsError::RecordParse(format!("timestamp_delta: {e}")))?;
    let offset_delta = get_varint_from_slice(&mut cur)
        .map_err(|e| RecordsError::RecordParse(format!("offset_delta: {e}")))?;

    let (key, cur) = read_nullable_slice(cur, || "key".into())?;
    let (value, mut cur) = read_nullable_slice(cur, || "value".into())?;

    let header_count = get_varint_from_slice(&mut cur)
        .map_err(|e| RecordsError::RecordParse(format!("header_count: {e}")))?;
    if header_count < 0 {
        return Err(RecordsError::RecordParse(format!(
            "negative header count {header_count}"
        )));
    }
    let header_count =
        usize::try_from(header_count).expect("non-negative header count must fit in usize");
    let header_bytes = cur;
    for index in 0..header_count {
        cur = parse_header(cur, index, names)?.1;
    }
    if !cur.is_empty() {
        return Err(RecordsError::RecordParse(format!(
            "trailing bytes inside record (left={})",
            cur.len()
        )));
    }
    let headers = RecordHeaders {
        bytes: header_bytes,
        count: header_count,
    };

    Ok(Record {
        attributes,
        timestamp_delta,
        offset_delta,
        key,
        value,
        headers,
    })
}

/// The header names a walk over one batch has already checked are UTF-8.
///
/// Every record of a batch usually carries the same header names, and
/// checking each name's UTF-8 again was a sixth of the cost of reading a
/// record with one header. A name whose bytes equal the last checked one is
/// answered with that checked `&str`: it points into the same buffer and holds
/// the same bytes, so nothing a reader sees changes.
#[derive(Debug, Default)]
pub(crate) struct HeaderNames<'a> {
    last: Option<&'a str>,
}

impl<'a> HeaderNames<'a> {
    fn name(&mut self, bytes: &'a [u8], index: usize) -> Result<&'a str, RecordsError> {
        if let Some(last) = self.last
            && same_bytes(last.as_bytes(), bytes)
        {
            return Ok(last);
        }
        let name = std::str::from_utf8(bytes)
            .map_err(|e| RecordsError::RecordParse(format!("header[{index}] key utf-8: {e}")))?;
        self.last = Some(name);
        Ok(name)
    }
}

/// Whether two byte strings are equal.
///
/// Header names are short, and a call to `memcmp` cost more than the rest of
/// reading a header. A name of up to 16 bytes is compared as two overlapping
/// words instead.
#[inline]
fn same_bytes(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    if let (Some(a_head), Some(a_tail), Some(b_head), Some(b_tail)) = (
        a.first_chunk::<8>(),
        a.last_chunk::<8>(),
        b.first_chunk::<8>(),
        b.last_chunk::<8>(),
    ) && a.len() <= 16
    {
        return u64::from_ne_bytes(*a_head) == u64::from_ne_bytes(*b_head)
            && u64::from_ne_bytes(*a_tail) == u64::from_ne_bytes(*b_tail);
    }
    if let (Some(a_head), Some(a_tail), Some(b_head), Some(b_tail)) = (
        a.first_chunk::<4>(),
        a.last_chunk::<4>(),
        b.first_chunk::<4>(),
        b.last_chunk::<4>(),
    ) && a.len() < 8
    {
        return u32::from_ne_bytes(*a_head) == u32::from_ne_bytes(*b_head)
            && u32::from_ne_bytes(*a_tail) == u32::from_ne_bytes(*b_tail);
    }
    if a.len() < 4 {
        return a.iter().zip(b).all(|(x, y)| x == y);
    }
    a == b
}

/// One header's raw name bytes and nullable value, and the bytes after it.
struct RawHeader<'a> {
    key: &'a [u8],
    value: Option<&'a [u8]>,
    rest: &'a [u8],
}

/// The [`RawHeader`] at the start of bytes the parser already checked; `None`
/// if they do not hold one.
#[inline]
fn read_raw_header(mut bytes: &[u8]) -> Option<RawHeader<'_>> {
    let key_len = usize::try_from(get_varint_from_slice(&mut bytes).ok()?).ok()?;
    let (key, mut rest) = bytes.split_at_checked(key_len)?;
    let value_len = get_varint_from_slice(&mut rest).ok()?;
    let Ok(value_len) = usize::try_from(value_len) else {
        return Some(RawHeader {
            key,
            value: None,
            rest,
        });
    };
    let (value, rest) = rest.split_at_checked(value_len)?;
    Some(RawHeader {
        key,
        value: Some(value),
        rest,
    })
}

/// One header of a record body, a UTF-8 key and a nullable value, and the
/// bytes after it. `index` is the header's position, which only an error
/// message reads.
#[inline]
fn parse_header<'a>(
    mut bytes: &'a [u8],
    index: usize,
    names: &mut HeaderNames<'a>,
) -> Result<(RecordHeader<'a>, &'a [u8]), RecordsError> {
    let key_len = get_varint_from_slice(&mut bytes)
        .map_err(|e| RecordsError::RecordParse(format!("header[{index}] key length: {e}")))?;
    if key_len < 0 {
        return Err(RecordsError::RecordParse(format!(
            "header[{index}] negative key length"
        )));
    }
    let n = usize::try_from(key_len).expect("non-negative key length must fit in usize");
    let Some((key_bytes, rest)) = bytes.split_at_checked(n) else {
        return Err(RecordsError::BodyTooShort {
            needed: n - bytes.len(),
        });
    };
    let key = names.name(key_bytes, index)?;
    let (value, rest) = read_nullable_slice(rest, || format!("header[{index}] value"))?;
    Ok((RecordHeader { key, value }, rest))
}

/// A length-prefixed byte string, where a negative length is null, and the
/// bytes after it. `label` names the field in an error message, and is only
/// built for one: the produce path reads every record of every batch through
/// here, and a label formatted for each successful read cost an allocation
/// per header.
#[inline]
fn read_nullable_slice(
    mut bytes: &[u8],
    label: impl FnOnce() -> String,
) -> Result<(Option<&[u8]>, &[u8]), RecordsError> {
    let len = get_varint_from_slice(&mut bytes)
        .map_err(|e| RecordsError::RecordParse(format!("{} length: {e}", label())))?;
    let Ok(n) = usize::try_from(len) else {
        return Ok((None, bytes));
    };
    let Some((head, rest)) = bytes.split_at_checked(n) else {
        return Err(RecordsError::BodyTooShort {
            needed: n - bytes.len(),
        });
    };
    Ok((Some(head), rest))
}

// ── to_owned bridge ───────────────────────────────────────────────────────────

impl RecordBatch<'_> {
    /// Materializes an owned `RecordBatch` and copies every byte slice into
    /// `Bytes` or `String`.
    /// # Errors
    /// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
    pub fn to_owned(&self) -> Result<super::owned::RecordBatch, RecordsError> {
        let mut records = Vec::new();
        for r in self {
            let r = r?;
            records.push(super::owned::Record {
                attributes: r.attributes,
                timestamp_delta: r.timestamp_delta,
                offset_delta: r.offset_delta,
                key: r.key.map(Bytes::copy_from_slice),
                value: r.value.map(Bytes::copy_from_slice),
                headers: r
                    .headers
                    .into_iter()
                    .map(|h| super::owned::RecordHeader {
                        key: h.key.to_string(),
                        value: h.value.map(Bytes::copy_from_slice),
                    })
                    .collect(),
            });
        }
        Ok(super::owned::RecordBatch {
            base_offset: self.header.base_offset.get(),
            partition_leader_epoch: self.header.partition_leader_epoch.get(),
            attributes: self.attributes(),
            last_offset_delta: self.header.last_offset_delta.get(),
            base_timestamp: self.header.base_timestamp.get(),
            max_timestamp: self.header.max_timestamp.get(),
            producer_id: self.header.producer_id.get(),
            producer_epoch: self.header.producer_epoch.get(),
            base_sequence: self.header.base_sequence.get(),
            records,
        })
    }
}

// ── Debug / Clone / PartialEq / Eq ────────────────────────────────────────────
//
// RecordBatch<'a> holds a header reference and a body slice/bytes, so we can't
// #[derive] these. We provide hand-rolled impls that delegate to the owned type.

impl std::fmt::Debug for RecordBatch<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.to_owned() {
            Ok(o) => o.fmt(f),
            Err(e) => write!(f, "RecordBatch(<decode error: {e}>)"),
        }
    }
}

impl Clone for RecordBatch<'_> {
    /// Shallow clone: both `header` and `body` share the same underlying data
    /// as `self`. For a `Borrowed` body this is a reference copy. For an
    /// `Owned` body, `Bytes::clone` is a cheap reference-count bump.
    fn clone(&self) -> Self {
        RecordBatch {
            header: self.header,
            body: match &self.body {
                RecordBody::Borrowed(s) => RecordBody::Borrowed(s),
                RecordBody::Owned(b) => RecordBody::Owned(b.clone()),
            },
        }
    }
}

impl PartialEq for RecordBatch<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self.to_owned(), other.to_owned()) {
            (Ok(a), Ok(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for RecordBatch<'_> {}

// ── Encode trait impl ─────────────────────────────────────────────────────────

impl crate::Encode for RecordBatch<'_> {
    fn encode<B: bytes::BufMut>(
        &self,
        buf: &mut B,
        version: i16,
    ) -> Result<(), crate::ProtocolError> {
        let owned = self.to_owned().map_err(crate::ProtocolError::from)?;
        crate::Encode::encode(&owned, buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        match self.to_owned() {
            Ok(o) => crate::Encode::encoded_len(&o, version),
            Err(_) => 0,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {

    use bytes::BytesMut;
    use krabka_compression::{CompressionError, CompressionType, RecordDecompressionPolicy};
    use krabka_units::{bytes, fraction};

    use super::*;
    use crate::DecodeBorrow;

    fn encode_owned_then_borrow(b: &super::super::owned::RecordBatch) -> Vec<u8> {
        let mut buf = BytesMut::new();
        b.encode(&mut buf).unwrap();
        buf.to_vec()
    }

    fn refresh_batch_crc(encoded: &mut [u8]) {
        let coverage_start = crate::records::CRC_COVERAGE_START;
        let crc = crc32c(&encoded[coverage_start..]);
        encoded[coverage_start - 4..coverage_start].copy_from_slice(&crc.to_be_bytes());
    }

    #[test]
    fn borrowed_roundtrip_cases() {
        for (_case, codec) in [
            ("none", CompressionType::None),
            ("gzip", CompressionType::Gzip),
            ("snappy", CompressionType::Snappy),
            ("lz4", CompressionType::Lz4),
            ("zstd", CompressionType::Zstd),
        ] {
            let mut owned = super::super::owned::RecordBatch::default();
            owned.attributes = owned.attributes.with_compression(codec);
            owned.records.push(super::super::owned::Record {
                key: Some(Bytes::from_static(b"key")),
                value: Some(Bytes::from_static(b"value")),
                ..Default::default()
            });

            let encoded = encode_owned_then_borrow(&owned);
            let mut cur: &[u8] = &encoded[..];
            let borrowed = RecordBatch::decode_borrow(&mut cur, 0).unwrap();
            assert2::assert!(cur.is_empty());
            assert2::assert!(borrowed.attributes() == owned.attributes);

            let records: Vec<_> = borrowed.iter().collect::<Result<_, _>>().unwrap();
            let expected_records = vec![Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: Some(b"key".as_slice()),
                value: Some(b"value".as_slice()),
                headers: RecordHeaders::default(),
            }];
            assert2::assert!(records == expected_records);

            let back_owned = borrowed.to_owned().unwrap();
            assert2::assert!(back_owned == owned);
        }
    }

    /// The borrowed record reports the body size the owned record does, for the
    /// null and the present forms of every field and on both sides of the
    /// length-prefix width boundary (a body of 63 bytes takes a one-byte
    /// prefix, one of 64 takes two).
    #[test]
    fn borrowed_body_len_matches_the_owned_record() {
        use super::super::owned::{Record as OwnedRecord, RecordHeader as OwnedHeader};

        let owned = super::super::owned::RecordBatch {
            records: vec![
                OwnedRecord::default(),
                OwnedRecord {
                    timestamp_delta: 17,
                    offset_delta: 2,
                    key: Some(Bytes::from_static(b"the-key")),
                    value: Some(Bytes::from_static(b"hello kafka")),
                    headers: vec![
                        OwnedHeader {
                            key: "trace-id".to_string(),
                            value: Some(Bytes::from_static(b"abc")),
                        },
                        OwnedHeader {
                            key: "null-val".to_string(),
                            value: None,
                        },
                    ],
                    ..Default::default()
                },
                OwnedRecord {
                    value: Some(Bytes::from(vec![7_u8; 57])),
                    ..Default::default()
                },
                OwnedRecord {
                    value: Some(Bytes::from(vec![7_u8; 58])),
                    ..Default::default()
                },
            ],
            last_offset_delta: 3,
            ..Default::default()
        };
        let encoded = encode_owned_then_borrow(&owned);
        let mut cur: &[u8] = &encoded[..];
        let borrowed = RecordBatch::decode_borrow(&mut cur, 0).unwrap();
        let records: Vec<_> = borrowed.iter().collect::<Result<_, _>>().unwrap();

        let lens: Vec<usize> = records.iter().map(Record::body_len).collect();
        assert2::assert!(lens == vec![6, 47, 63, 64]);
        let owned_lens: Vec<usize> = owned.records.iter().map(OwnedRecord::body_len).collect();
        assert2::assert!(lens == owned_lens);
    }

    #[test]
    fn decompression_policy_limits_borrowed_decode() {
        let mut owned = super::super::owned::RecordBatch {
            attributes: Attributes::default().with_compression(CompressionType::Lz4),
            records: vec![super::super::owned::Record {
                value: Some(Bytes::from(vec![b'x'; 4096])),
                ..Default::default()
            }],
            ..Default::default()
        };
        owned.last_offset_delta = 0;
        let encoded = encode_owned_then_borrow(&owned);

        let mut default_cur = &encoded[..];
        RecordBatch::decode_borrow(&mut default_cur, 0).unwrap();

        let policy = RecordDecompressionPolicy::new(fraction(1.0), bytes(1), bytes(32)).unwrap();
        let mut limited_cur = &encoded[..];
        assert2::assert!(matches!(
            RecordBatch::decode_borrow_with_policy(&mut limited_cur, policy),
            Err(RecordsError::Compression(CompressionError::TooLarge {
                limit: 32
            }))
        ));
    }

    #[test]
    fn zero_copy_for_uncompressed() {
        // Pointer-identity: record key/value slices must point into the
        // input buffer for uncompressed batches.
        let mut owned = super::super::owned::RecordBatch::default();
        owned.records.push(super::super::owned::Record {
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        });
        let encoded = encode_owned_then_borrow(&owned);
        let encoded_start = encoded.as_ptr() as usize;
        let encoded_end = encoded_start + encoded.len();

        let mut cur: &[u8] = &encoded[..];
        let borrowed = RecordBatch::decode_borrow(&mut cur, 0).unwrap();
        let records: Vec<_> = borrowed.iter().collect::<Result<_, _>>().unwrap();

        let v_ptr = records[0].value.unwrap().as_ptr() as usize;
        assert2::assert!(v_ptr >= encoded_start && v_ptr < encoded_end);
    }

    #[test]
    fn validate_one_v2_batch_reads_header_and_len() {
        let mut owned = super::super::owned::RecordBatch {
            base_offset: 7,
            partition_leader_epoch: 3,
            last_offset_delta: 0,
            producer_id: 99,
            producer_epoch: 1,
            base_sequence: 5,
            max_timestamp: 1_234,
            ..Default::default()
        };
        owned.records.push(super::super::owned::Record {
            value: Some(Bytes::from_static(b"payload")),
            ..Default::default()
        });
        let encoded = encode_owned_then_borrow(&owned);

        let v = validate_one_v2_batch(&encoded).unwrap();
        assert2::assert!(
            (
                v.total_len,
                v.header.base_offset.get(),
                v.header.partition_leader_epoch.get(),
                v.header.producer_id.get(),
                v.header.producer_epoch.get(),
                v.header.base_sequence.get(),
                v.header.max_timestamp.get(),
                v.header.magic,
            ) == (encoded.len(), 7, 3, 99, 1, 5, 1_234, 2)
        );
        assert2::assert!(
            v.validate_records(RecordDecompressionPolicy::default())
                .is_ok()
        );
    }

    #[test]
    fn borrowed_crc_stops_at_the_declared_batch_boundary() {
        for codec in [CompressionType::None, CompressionType::Lz4] {
            for value in [Bytes::new(), Bytes::from_static(b"payload")] {
                let mut owned = super::super::owned::RecordBatch {
                    records: vec![super::super::owned::Record {
                        value: Some(value),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                owned.attributes = owned.attributes.with_compression(codec);
                let mut encoded = encode_owned_then_borrow(&owned);
                let first_len = encoded.len();
                encoded.extend_from_within(..);
                // A corrupt following batch must not affect the first CRC.
                encoded[first_len + 21] ^= 1;

                let validated = validate_one_v2_batch(&encoded).unwrap();
                assert2::assert!(validated.total_len == first_len);
                let mut input = encoded.as_slice();
                let borrowed = RecordBatch::decode_borrow(&mut input, 0).unwrap();
                assert2::assert!(borrowed.to_owned().unwrap() == owned);
                assert2::assert!(input == &encoded[first_len..]);
                assert2::assert!(matches!(
                    validate_one_v2_batch(input),
                    Err(RecordsError::CrcMismatch { .. })
                ));
                assert2::assert!(matches!(
                    RecordBatch::decode_borrow_with_policy(
                        &mut input,
                        RecordDecompressionPolicy::default(),
                    ),
                    Err(RecordsError::CrcMismatch { .. })
                ));
            }
        }
    }

    #[test]
    fn validated_batch_rejects_crc_valid_malformed_record_body() {
        let owned = super::super::owned::RecordBatch {
            records: vec![super::super::owned::Record {
                value: Some(Bytes::from_static(b"value")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut encoded = encode_owned_then_borrow(&owned);
        encoded[HEADER_LEN] = 0; // zero-length first record body
        refresh_batch_crc(&mut encoded);

        let validated = validate_one_v2_batch(&encoded).expect("CRC-valid header");
        let error = validated
            .validate_records(RecordDecompressionPolicy::default())
            .unwrap_err();
        assert2::assert!(matches!(error, RecordsError::RecordParse(_)));
    }

    #[test]
    fn validated_batch_visits_records_during_validation() {
        let owned = super::super::owned::RecordBatch {
            attributes: Attributes::default().with_compression(CompressionType::Lz4),
            records: vec![
                super::super::owned::Record {
                    timestamp_delta: 3,
                    value: Some(Bytes::from_static(b"first")),
                    ..Default::default()
                },
                super::super::owned::Record {
                    timestamp_delta: 8,
                    value: Some(Bytes::from_static(b"second")),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let encoded = encode_owned_then_borrow(&owned);
        let validated = validate_one_v2_batch(&encoded).unwrap();
        let mut timestamps = Vec::new();

        validated
            .validate_records_with(RecordDecompressionPolicy::default(), |record| {
                timestamps.push(record.timestamp_delta);
            })
            .unwrap();

        assert2::assert!(timestamps == vec![3, 8]);
    }

    #[test]
    fn validated_batch_rejects_trailing_body_bytes() {
        let owned = super::super::owned::RecordBatch {
            records: vec![super::super::owned::Record::default()],
            ..Default::default()
        };
        let mut encoded = encode_owned_then_borrow(&owned);
        let batch_length = i32::from_be_bytes(encoded[8..12].try_into().unwrap());
        encoded[8..12].copy_from_slice(&(batch_length + 1).to_be_bytes());
        encoded.push(0);
        refresh_batch_crc(&mut encoded);

        let validated = validate_one_v2_batch(&encoded).expect("CRC-valid header");
        let error = validated
            .validate_records(RecordDecompressionPolicy::default())
            .unwrap_err();
        assert2::assert!(
            matches!(error, RecordsError::RecordParse(message) if message.contains("trailing bytes"))
        );
    }

    #[test]
    fn validated_batch_applies_decompression_policy() {
        let owned = super::super::owned::RecordBatch {
            attributes: Attributes::default().with_compression(CompressionType::Lz4),
            records: vec![super::super::owned::Record {
                value: Some(Bytes::from(vec![b'x'; 4096])),
                ..Default::default()
            }],
            ..Default::default()
        };
        let encoded = encode_owned_then_borrow(&owned);
        let validated = validate_one_v2_batch(&encoded).unwrap();
        let policy = RecordDecompressionPolicy::new(fraction(1.0), bytes(1), bytes(32)).unwrap();

        assert2::assert!(matches!(
            validated.validate_records(policy),
            Err(RecordsError::Compression(CompressionError::TooLarge {
                limit: 32
            }))
        ));
    }

    #[test]
    fn validated_batch_distinguishes_zero_and_negative_record_counts() {
        let mut encoded = encode_owned_then_borrow(&super::super::owned::RecordBatch::default());
        let zero = validate_one_v2_batch(&encoded).unwrap();
        assert2::assert!(
            zero.validate_records(RecordDecompressionPolicy::default())
                .is_ok()
        );

        encoded[57..61].copy_from_slice(&(-1i32).to_be_bytes());
        refresh_batch_crc(&mut encoded);
        let negative = validate_one_v2_batch(&encoded).unwrap();
        assert2::assert!(matches!(
            negative.validate_records(RecordDecompressionPolicy::default()),
            Err(RecordsError::RecordParse(message)) if message.contains("negative records_count")
        ));
    }

    #[test]
    fn validate_one_v2_batch_rejects_corrupt_crc() {
        let owned = super::super::owned::RecordBatch {
            records: vec![super::super::owned::Record {
                value: Some(Bytes::from_static(b"x")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut encoded = encode_owned_then_borrow(&owned);
        // Flip a body byte (after the 61-byte header) → CRC mismatch.
        encoded[HEADER_LEN] ^= 0xFF;
        let err = validate_one_v2_batch(&encoded).unwrap_err();
        assert2::assert!(matches!(err, RecordsError::CrcMismatch { .. }));
    }

    #[test]
    fn validate_one_v2_batch_rejects_truncated() {
        let owned = super::super::owned::RecordBatch {
            records: vec![super::super::owned::Record {
                value: Some(Bytes::from_static(b"value")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let encoded = encode_owned_then_borrow(&owned);
        let err = validate_one_v2_batch(&encoded[..encoded.len() - 2]).unwrap_err();
        assert2::assert!(matches!(err, RecordsError::BodyTooShort { .. }));
    }

    #[test]
    fn count_records_in_v2_batches_counts_single_and_concatenated_batches() {
        let one = encode_owned_then_borrow(&super::super::owned::RecordBatch {
            records: vec![super::super::owned::Record::default()],
            ..Default::default()
        });
        let three = encode_owned_then_borrow(&super::super::owned::RecordBatch {
            last_offset_delta: 2,
            records: vec![
                super::super::owned::Record::default(),
                super::super::owned::Record::default(),
                super::super::owned::Record::default(),
            ],
            ..Default::default()
        });
        let mut both = Vec::with_capacity(one.len() + three.len());
        both.extend_from_slice(&one);
        both.extend_from_slice(&three);

        for (_case, input, expected) in [("single", &one[..], 1), ("concatenated", &both[..], 4)] {
            assert2::assert!(count_records_in_v2_batches(input) == expected);
        }
    }

    #[test]
    fn count_records_in_v2_batches_stops_at_bad_or_non_v2_input() {
        let encoded = encode_owned_then_borrow(&super::super::owned::RecordBatch {
            records: vec![super::super::owned::Record::default()],
            ..Default::default()
        });
        let mut with_truncated_tail = encoded.clone();
        with_truncated_tail.extend_from_slice(&encoded[..HEADER_LEN - 1]);

        for (_case, input, expected) in [
            ("empty", &[][..], 0),
            ("non-v2", &[0u8; HEADER_LEN][..], 0),
            ("truncated tail", &with_truncated_tail[..], 1),
        ] {
            assert2::assert!(count_records_in_v2_batches(input) == expected);
        }
    }

    /// Builds a bare 61-byte v2 batch header with `magic == 2` and all other
    /// fields zero, plus the given big-endian `batch_length` and
    /// `records_count`. The helper drives the `total_len` boundary checks of
    /// `count_records_in_v2_batches` precisely, because `records_count` is read
    /// straight from the header and the body needs no real records.
    fn v2_header_only(batch_length: i32, records_count: i32) -> Vec<u8> {
        let mut buf = vec![0u8; HEADER_LEN];
        buf[16] = 2; // magic == v2
        buf[8..12].copy_from_slice(&batch_length.to_be_bytes());
        buf[57..61].copy_from_slice(&records_count.to_be_bytes());
        buf
    }

    #[test]
    fn count_records_in_v2_batches_accepts_batch_that_exactly_fills_buffer() {
        // total_len = 12 + 49 = 61 = HEADER_LEN, exactly filling the slice, so
        // the batch is accepted and its header count is summed. Guards the
        // `total_len < HEADER_LEN` lower bound: a `<`→`==`/`<=` mutation would
        // wrongly break here and report 0.
        let buf = v2_header_only(49, 7);
        assert2::assert!(buf.len() == HEADER_LEN);
        assert2::assert!(count_records_in_v2_batches(&buf) == 7);
    }

    #[test]
    fn count_records_in_v2_batches_rejects_batch_overrunning_buffer() {
        // total_len = 12 + 100 = 112 > 61 bytes available, so the batch is
        // rejected and contributes nothing. Guards the
        // `total_len > remaining.len()` arm: an `||`→`&&` mutation would skip
        // the break and index past the buffer.
        let buf = v2_header_only(100, 9);
        assert2::assert!(count_records_in_v2_batches(&buf) == 0);
    }

    #[test]
    fn borrowed_encode_via_trait_roundtrips() {
        use crate::Encode as _;
        let owned_in = super::super::owned::RecordBatch {
            records: vec![super::super::owned::Record {
                key: Some(Bytes::from_static(b"x")),
                value: Some(Bytes::from_static(b"y")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let bytes_in = encode_owned_then_borrow(&owned_in);
        let mut cur: &[u8] = &bytes_in[..];
        let borrowed = RecordBatch::decode_borrow(&mut cur, 0).unwrap();

        let mut out = BytesMut::new();
        borrowed.encode(&mut out, 0).unwrap();
        assert2::assert!(&out[..] == &bytes_in[..]);
    }

    /// Encodes one record carrying `headers` and returns its batch bytes.
    fn batch_with_headers(headers: &[(&str, Option<&'static [u8]>)]) -> Vec<u8> {
        encode_owned_then_borrow(&super::super::owned::RecordBatch {
            records: vec![super::super::owned::Record {
                headers: headers
                    .iter()
                    .map(|&(key, value)| super::super::owned::RecordHeader {
                        key: key.to_owned(),
                        value: value.map(Bytes::from_static),
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        })
    }

    /// The one record of the uncompressed batch `encoded`.
    fn only_record(encoded: &[u8]) -> Record<'_> {
        let mut cur = encoded;
        let batch = RecordBatch::decode_borrow(&mut cur, 0).unwrap();
        let RecordBody::Borrowed(mut body) = batch.body else {
            panic!("an uncompressed batch borrows its body");
        };
        parse_one_record(&mut body, &mut HeaderNames::default()).unwrap()
    }

    #[test]
    fn record_headers_find_cases() {
        // Names of 2, 5, 8, 12 and 20 bytes take each comparison `same_bytes`
        // makes: bytewise, two 4-byte words, two 8-byte words, and `memcmp`.
        let headers: &[(&str, Option<&'static [u8]>)] = &[
            ("ab", Some(b"two")),
            ("trace", None),
            ("dedup-id", Some(b"first")),
            ("dedup-id", Some(b"second")),
            ("idempotency1", Some(b"twelve")),
            ("a-twenty-byte-header", Some(b"twenty")),
        ];
        let cases: &[(&str, Option<Option<&[u8]>>)] = &[
            ("ab", Some(Some(b"two"))),
            ("ac", None),
            ("trace", Some(None)),
            ("tracf", None),
            ("dedup-id", Some(Some(b"first"))),
            ("dedup-ie", None),
            ("dedup-i", None),
            ("dedup-idx", None),
            ("idempotency1", Some(Some(b"twelve"))),
            ("idempotency2", None),
            ("a-twenty-byte-header", Some(Some(b"twenty"))),
            ("a-twenty-byte-headex", None),
            ("", None),
        ];
        let encoded = batch_with_headers(headers);
        let record = only_record(&encoded);
        let found: Vec<_> = cases
            .iter()
            .map(|&(name, _)| (name, record.headers.find(name)))
            .collect();

        assert2::assert!(found == cases);
    }

    #[test]
    fn record_headers_iterate_in_wire_order() {
        let encoded = batch_with_headers(&[
            ("dedup-id", Some(b"first")),
            ("trace", None),
            ("dedup-id", Some(b"second")),
        ]);
        let expected = vec![
            RecordHeader {
                key: "dedup-id",
                value: Some(b"first"),
            },
            RecordHeader {
                key: "trace",
                value: None,
            },
            RecordHeader {
                key: "dedup-id",
                value: Some(b"second"),
            },
        ];
        let record = only_record(&encoded);
        let seen = (
            record.headers.len(),
            record.headers.iter().size_hint(),
            record.headers.iter().collect::<Vec<_>>(),
        );

        assert2::assert!(seen == (3, (3, Some(3)), expected));
    }

    proptest::proptest! {
        #[test]
        fn same_bytes_agrees_with_slice_equality(
            a in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..24),
            flip in proptest::option::of(proptest::prelude::any::<proptest::sample::Index>()),
            other in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..24),
        ) {
            // Equal pairs, pairs of one length that differ in one byte, and
            // pairs of unrelated bytes.
            let mut b = a.clone();
            if let Some(flip) = flip.filter(|_| !b.is_empty()) {
                let at = flip.index(b.len());
                b[at] ^= 1;
            }
            assert2::assert!(same_bytes(&a, &b) == (a == b));
            assert2::assert!(same_bytes(&a, &other) == (a == other));
        }
    }
}

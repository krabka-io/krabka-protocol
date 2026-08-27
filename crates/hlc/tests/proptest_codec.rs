//! Round-trip properties of the stamp byte layout.
//!
//! Three claims hold for every input. An encode is always
//! [`HLC_ENCODED_LEN`] bytes and a decode reads back the same stamp. Every
//! 16-byte value is a stamp, and re-rendering it gives the same bytes. Every
//! other length is a [`HlcError::Length`], and no input panics.

use crabka_hlc::{HLC_ENCODED_LEN, Hlc, HlcError, WallMicros};
use crabka_ids::NodeId;
use proptest::prelude::*;

// A stamp over the whole range each field can hold on the wire.
fn arb_stamp() -> impl Strategy<Value = Hlc> {
    (any::<i64>(), any::<u32>(), any::<u32>()).prop_map(|(wall, counter, node)| {
        Hlc::new(WallMicros(wall), counter, NodeId(u64::from(node)))
    })
}

// A byte string of any length the wire can carry except the one valid length.
fn arb_wrong_length() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..64usize)
        .prop_filter("a valid length is not a wrong length", |bytes| {
            bytes.len() != HLC_ENCODED_LEN
        })
}

proptest! {
    #[test]
    fn encode_then_decode_returns_the_same_stamp(stamp in arb_stamp()) {
        let bytes = stamp.encode()?;
        prop_assert_eq!(bytes.len(), HLC_ENCODED_LEN);
        prop_assert_eq!(Hlc::decode(&bytes)?, stamp);
    }

    #[test]
    fn decode_then_encode_returns_the_same_bytes(bytes in any::<[u8; HLC_ENCODED_LEN]>()) {
        // Every 16-byte value is a stamp, because each field covers the whole
        // range of its type. The re-render is what lets a consumer forward a
        // peer's stamp without keeping the peer's bytes.
        let stamp = Hlc::decode(&bytes)?;
        prop_assert_eq!(stamp.encode()?, bytes);
    }

    #[test]
    fn decode_rejects_every_other_length_without_a_panic(bytes in arb_wrong_length()) {
        prop_assert_eq!(Hlc::decode(&bytes), Err(HlcError::Length(bytes.len())));
    }

    #[test]
    fn a_node_id_above_the_field_width_is_always_refused(node in (u64::from(u32::MAX) + 1)..=u64::MAX) {
        let stamp = Hlc::new(WallMicros(0), 0, NodeId(node));
        prop_assert_eq!(stamp.encode(), Err(HlcError::NodeIdTooLarge(node)));
    }
}

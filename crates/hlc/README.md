# krabka-hlc

[![Crates.io](https://img.shields.io/crates/v/krabka-hlc.svg)](https://crates.io/crates/krabka-hlc)
[![Docs.rs](https://docs.rs/krabka-hlc/badge.svg)](https://docs.rs/krabka-hlc)

Hybrid logical clock stamps that ride on Kafka record headers.

Part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

## Overview

A hybrid logical clock (HLC) gives every event in the cluster one timestamp that stays close to real time and that still respects cause and effect. The stamp holds a wall time, a logical counter, and the node that minted it, so it sorts totally and every node computes the same order. The algorithm is the one in Kulkarni et al., *Logical Physical Clocks and Consistent Snapshots in Globally Distributed Databases* (OPODIS 2014).

A stamp travels on one Kafka record header, `krabka.hlc`. Like `krabka-trace-context`, this crate does not depend on `krabka-protocol`. It stays type-erased over `(&str, impl AsRef<[u8]>)` header pairs, so any producer or consumer header type works, and the caller converts to and from its own `Header` at the edge.

## Features

- **A 16-byte fixed-width header value.** Big-endian, with 8 bytes of wall time, 4 bytes of counter, and 4 bytes of node id. There is no varint framing, because Java and Go clients read this header by hand.
- **A self-fence.** The clock refuses a stamp whose wall time is more than the configured maximum offset ahead of the local physical clock, 500 milliseconds by default. One broken clock then cannot drag the fleet into the future.
- **An observation seam.** `HlcClock::observation` reports the drift of the logical clock from the physical clock, how many records pulled the clock forward, how many stamps the fence refused, and the largest peer-ahead delta. A record that pulls the clock forward by 40 milliseconds is direct evidence of 40 milliseconds of skew, measured on the data path.
- **An injectable physical clock.** `PhysicalClock` has a `SystemClock` implementation for a broker and a `ManualClock` implementation for a deterministic test.
- **Safe ingress.** `extract_from_headers` gives `None` for an absent, short, or invalid value, so a consumer that cannot read a stamp still applies the record. `Hlc::decode` returns the reason for a caller that wants it. No error variant holds the peer's bytes.

## Usage

```rust
use krabka_hlc::{HlcClock, HlcError, extract_from_headers};
use krabka_ids::NodeId;

fn round_trip(clock: &HlcClock) -> Result<(), HlcError> {
    // A producer stamps the record it writes.
    let (key, value) = clock.now()?.header()?;

    // A consumer merges the stamp it read into its own clock.
    if let Some(stamp) = extract_from_headers([(key, value.as_slice())]) {
        clock.observe(stamp)?;
    }

    // An observability agent reads what the clock learned about skew.
    let skew = clock.observation();
    println!("drift {:?}, refused {}", skew.drift, skew.refusals);
    Ok(())
}

let clock = HlcClock::new(NodeId(1));
round_trip(&clock).unwrap();
```

## Documentation

- [API Documentation](https://docs.rs/krabka-hlc)
- [Krabka repository](https://github.com/robot-head/crabka)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).

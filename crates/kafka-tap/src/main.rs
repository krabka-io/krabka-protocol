//! Standalone tap: `kafka-tap <listen> <upstream> <spool.ndjson>`.
//!
//! The tap writes one JSON record per frame to the spool file.
use std::{
    env,
    fmt::Write as _,
    fs::File,
    io::{self, Write},
    process,
    sync::{Arc, Mutex, PoisonError},
    thread,
};

use krabka_kafka_tap::{Recorder, frame::CapturedFrame, spawn};

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let [_, listen, upstream, spool, ..] = args.as_slice() else {
        eprintln!("usage: kafka-tap <listen> <upstream> <spool.ndjson>");
        process::exit(2);
    };
    let file = Arc::new(Mutex::new(File::create(spool)?));
    let rec: Recorder = Arc::new(move |f: CapturedFrame| {
        let mut hex = String::with_capacity(f.body.len() * 2);
        for b in &f.body {
            let _ = write!(hex, "{b:02x}");
        }
        let line = format!(
            "{{\"api_key\":{},\"version\":{},\"is_request\":{},\"body_hex\":\"{}\"}}\n",
            f.api_key, f.version, f.is_request, hex
        );
        let written = file
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .write_all(line.as_bytes());
        // A spool that misses a frame is not a corpus: stop the tap.
        if let Err(error) = written {
            eprintln!("kafka-tap: cannot write to the spool: {error}");
            process::exit(1);
        }
    });
    let addr = spawn(listen.as_str(), upstream, rec)?;
    eprintln!("kafka-tap listening on {addr} -> {upstream}, spooling to {spool}");
    park_forever()
}

/// Parks the main thread for good, while the relay threads run.
fn park_forever() -> ! {
    loop {
        thread::park();
    }
}

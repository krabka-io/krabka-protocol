//! End-to-end relay test against a fake upstream "broker" that echoes a canned
//! response.
//!
//! The test verifies that the bytes pass through unmodified, and that the relay
//! records the frames with the correct classification.
#![cfg(test)]
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use krabka_ids::{ApiKey, ApiVersion};
use krabka_kafka_tap::{Recorder, frame::CapturedFrame, spawn};

fn framed(body: &[u8]) -> Vec<u8> {
    let mut v = i32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
    v.extend_from_slice(body);
    v
}

#[test]
fn relays_and_records() {
    let upstream = TcpListener::bind("127.0.0.1:0").unwrap();
    let up_addr = upstream.local_addr().unwrap();
    thread::spawn(move || {
        let (mut s, _) = upstream.accept().unwrap();
        let mut len = [0u8; 4];
        s.read_exact(&mut len).unwrap();
        let n = usize::try_from(i32::from_be_bytes(len)).unwrap();
        let mut body = vec![0u8; n];
        s.read_exact(&mut body).unwrap();
        let mut resp_body = 42i32.to_be_bytes().to_vec();
        resp_body.push(0x99);
        s.write_all(&framed(&resp_body)).unwrap();
    });

    let recorder = Arc::new(Mutex::new(Vec::<CapturedFrame>::new()));
    let rec_for_tap: Recorder = {
        let r = Arc::clone(&recorder);
        Arc::new(move |f: CapturedFrame| r.lock().unwrap().push(f))
    };
    let tap_addr = spawn("127.0.0.1:0", &up_addr.to_string(), rec_for_tap).unwrap();

    let mut req_body = Vec::new();
    req_body.extend_from_slice(&3i16.to_be_bytes());
    req_body.extend_from_slice(&12i16.to_be_bytes());
    req_body.extend_from_slice(&42i32.to_be_bytes());
    req_body.push(0x11);

    let mut c = TcpStream::connect(tap_addr).unwrap();
    c.write_all(&framed(&req_body)).unwrap();
    let mut len = [0u8; 4];
    c.read_exact(&mut len).unwrap();
    let n = usize::try_from(i32::from_be_bytes(len)).unwrap();
    let mut resp = vec![0u8; n];
    c.read_exact(&mut resp).unwrap();
    assert2::assert!(resp == vec![0, 0, 0, 42, 0x99]);

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let frames = recorder.lock().unwrap().clone();
        let req = frames
            .iter()
            .any(|f| f.is_request && f.api_key == 3 && f.version == 12);
        let resp = frames
            .iter()
            .any(|f| !f.is_request && f.api_key == 3 && f.version == 12);
        if (req && resp) || Instant::now() >= deadline {
            break;
        }
        // real-time wait (not a progress poll): synchronous #[test] with
        // std::thread::sleep — there is no async runtime here for
        // yield_now().await, and the polled frames are produced by a background
        // OS thread + the relay's own threads that need real time to run.
        thread::sleep(Duration::from_millis(10));
    }
    let frames = recorder.lock().unwrap().clone();
    assert2::assert!(
        frames
            .iter()
            .any(|f| f.is_request && f.api_key == 3 && f.version == 12)
    );
    assert2::assert!(
        frames
            .iter()
            .any(|f| !f.is_request && f.api_key == 3 && f.version == 12)
    );
}

#[test]
fn a_captured_frame_spools_as_one_json_line() {
    for (frame, want) in [
        (
            CapturedFrame {
                api_key: ApiKey(18),
                version: ApiVersion(3),
                is_request: true,
                body: vec![0x00, 0x0a, 0xff],
            },
            "{\"api_key\":18,\"version\":3,\"is_request\":true,\"body_hex\":\"000aff\"}\n",
        ),
        (
            CapturedFrame {
                api_key: ApiKey(0),
                version: ApiVersion(0),
                is_request: false,
                body: Vec::new(),
            },
            "{\"api_key\":0,\"version\":0,\"is_request\":false,\"body_hex\":\"\"}\n",
        ),
    ] {
        assert2::assert!(frame.spool_line() == want);
    }
}

//! Test-only Kafka wire tap.
//!
//! The tap is a TCP relay. It tees complete frames to a `Recorder`, and at the
//! same time it forwards the bytes byte-for-byte to a real broker.
pub mod frame;

use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::{Arc, Mutex},
    thread,
};

use frame::{CapturedFrame, Pending, parse_request_prefix, read_correlation_id};

/// Callback invoked once per fully-read frame in either direction.
pub type Recorder = Arc<dyn Fn(CapturedFrame) + Send + Sync>;

/// Bind a listener, accept connections, relay each one to `upstream`, and
/// record the frames.
///
/// The function returns the bound local address, which is useful when the
/// caller passes port 0. The accept loop runs on a background thread for the
/// lifetime of the process.
pub fn spawn(
    listen: impl ToSocketAddrs,
    upstream: &str,
    recorder: Recorder,
) -> io::Result<std::net::SocketAddr> {
    let listener = TcpListener::bind(listen)?;
    let addr = listener.local_addr()?;
    let upstream = upstream.to_string();
    thread::spawn(move || {
        for client in listener.incoming() {
            let Ok(client) = client else { continue };
            let upstream = upstream.clone();
            let recorder = recorder.clone();
            thread::spawn(move || {
                if let Err(e) = handle_conn(client, &upstream, recorder) {
                    eprintln!("tap conn error: {e}");
                }
            });
        }
    });
    Ok(addr)
}

fn handle_conn(client: TcpStream, upstream: &str, recorder: Recorder) -> io::Result<()> {
    let server = TcpStream::connect(upstream)?;
    let pending = Arc::new(Mutex::new(Pending::default()));

    let c2s_client = client.try_clone()?;
    let c2s_server = server.try_clone()?;
    let pend_req = pending.clone();
    let rec_req = recorder.clone();
    let t = thread::spawn(move || {
        let _ = pump(c2s_client, c2s_server, true, pend_req, rec_req);
    });

    pump(server, client, false, pending, recorder)?;
    let _ = t.join();
    Ok(())
}

/// Copy length-prefixed frames from `src` to `dst`, and tee each frame to the
/// recorder. `is_request` selects between header parsing and correlation
/// lookup.
fn pump(
    mut src: TcpStream,
    mut dst: TcpStream,
    is_request: bool,
    pending: Arc<Mutex<Pending>>,
    recorder: Recorder,
) -> io::Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        if let Err(e) = src.read_exact(&mut len_buf) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(());
            }
            return Err(e);
        }
        let n = i32::from_be_bytes(len_buf);
        if n < 0 {
            return Ok(());
        }
        let mut body = vec![0u8; n as usize];
        src.read_exact(&mut body)?;

        let request_prefix = if is_request {
            parse_request_prefix(&body).inspect(|p| {
                pending
                    .lock()
                    .unwrap()
                    .record(p.correlation_id, p.api_key, p.api_version);
            })
        } else {
            None
        };

        dst.write_all(&len_buf)?;
        dst.write_all(&body)?;
        dst.flush()?;

        if let Some(p) = request_prefix {
            recorder(CapturedFrame {
                api_key: p.api_key,
                version: p.api_version,
                is_request: true,
                body,
            });
        } else if let Some(corr) = read_correlation_id(&body)
            && let Some((api_key, version)) = pending.lock().unwrap().take(corr)
        {
            recorder(CapturedFrame {
                api_key,
                version,
                is_request: false,
                body,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crabka_ids::{ApiKey, ApiVersion};

    use super::*;

    fn framed(body: &[u8]) -> Vec<u8> {
        let mut out = i32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
        out.extend_from_slice(body);
        out
    }

    /// A request frame: api_key, api_version, correlation_id, then payload.
    fn request(api_key: i16, version: i16, corr: i32) -> Vec<u8> {
        let mut b = api_key.to_be_bytes().to_vec();
        b.extend_from_slice(&version.to_be_bytes());
        b.extend_from_slice(&corr.to_be_bytes());
        b
    }

    /// An upstream that reads one frame and answers with `response`.
    fn upstream_echoing(response: Vec<u8>) -> io::Result<std::net::SocketAddr> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        thread::spawn(move || {
            let Ok((mut server, _)) = listener.accept() else {
                return;
            };
            let mut len = [0u8; 4];
            if server.read_exact(&mut len).is_err() {
                return;
            }
            let mut body = vec![0u8; i32::from_be_bytes(len) as usize];
            if server.read_exact(&mut body).is_err() {
                return;
            }
            let _ = server.write_all(&framed(&response));
            let _ = server.flush();
        });
        Ok(addr)
    }

    /// The relay is the whole point of the crate and nothing exercised it: a
    /// `handle_conn` or `pump` replaced by `Ok(())` forwards nothing and
    /// records nothing, while still returning success. Driving one request and
    /// one response through real sockets is what tells those apart.
    ///
    /// It also pins the frame-length guard. `n < 0` read as `n > 0` returns
    /// before relaying anything at all.
    #[test]
    fn relays_frames_both_ways_and_records_them() {
        let response = 7i32.to_be_bytes().to_vec();
        let upstream = upstream_echoing(response.clone()).unwrap();

        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let addr = spawn(
            "127.0.0.1:0",
            &upstream.to_string(),
            Arc::new(move |f: CapturedFrame| sink.lock().unwrap().push(f)),
        )
        .unwrap();

        let mut client = TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        client.write_all(&framed(&request(18, 3, 7))).unwrap();
        client.flush().unwrap();

        let mut len = [0u8; 4];
        client.read_exact(&mut len).unwrap();
        let mut body = vec![0u8; i32::from_be_bytes(len) as usize];
        client.read_exact(&mut body).unwrap();

        // Forwarded byte-for-byte.
        assert2::check!(body == response);

        // `pump` writes the frame on before it hands it to the recorder, so
        // reading the response above does not mean the response has been
        // recorded yet. Wait for it rather than racing it.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while seen.lock().unwrap().len() < 2 && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }

        // Both directions reached the recorder, and the response was correlated
        // back to the api key and version the request carried.
        let kinds: Vec<_> = seen
            .lock()
            .unwrap()
            .iter()
            .map(|f| (f.api_key, f.version, f.is_request))
            .collect();
        assert2::check!(
            kinds
                == vec![
                    (ApiKey(18), ApiVersion(3), true),
                    (ApiKey(18), ApiVersion(3), false)
                ]
        );
    }

    /// The frame-length guard stops the relay only on a *negative* length. Zero
    /// is a legal empty frame and must be forwarded like any other: read as
    /// `n <= 0` or `n == 0`, the first empty frame ends the connection and
    /// everything after it is silently dropped.
    #[test]
    fn a_zero_length_frame_is_forwarded_rather_than_ending_the_relay() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let upstream = listener.local_addr().unwrap();

        // Collects the frame lengths the upstream actually receives.
        let got = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&got);
        thread::spawn(move || {
            let Ok((mut server, _)) = listener.accept() else {
                return;
            };
            for _ in 0..2 {
                let mut len = [0u8; 4];
                if server.read_exact(&mut len).is_err() {
                    return;
                }
                let n = i32::from_be_bytes(len);
                let mut body = vec![0u8; n as usize];
                if server.read_exact(&mut body).is_err() {
                    return;
                }
                sink.lock().unwrap().push(n);
            }
        });

        let addr = spawn("127.0.0.1:0", &upstream.to_string(), Arc::new(|_| {})).unwrap();
        let mut client = TcpStream::connect(addr).unwrap();
        client.write_all(&framed(&[])).unwrap();
        client.write_all(&framed(&request(18, 3, 7))).unwrap();
        client.flush().unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while got.lock().unwrap().len() < 2 && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert2::check!(*got.lock().unwrap() == vec![0, 8]);
    }

    /// A client that closes without sending is an ordinary end of stream, not a
    /// failure. The guard reads the error kind to decide; inverted, a clean
    /// close is reported as an error instead.
    #[test]
    fn clean_close_ends_the_pump_without_an_error() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();

        // Close the writing end, so the pump's first read hits end of stream.
        drop(client);

        let sink: Recorder = Arc::new(|_| {});
        let result = pump(
            server.try_clone().unwrap(),
            server,
            true,
            Arc::new(Mutex::new(Pending::default())),
            sink,
        );
        assert2::check!(result.is_ok());
    }
}

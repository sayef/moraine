//! A counting reverse proxy in front of the S3-compatible server, so every engine's
//! object-store requests are counted the same way, outside the engine.
//!
//! HTTP/1.1 only, as the S3 clients of all three engines speak it. Requests are parsed just
//! enough to find where each ends (headers, then a `Content-Length` body or chunked body) and
//! are forwarded byte for byte, `Host` included, so request signatures stay valid. Responses are
//! copied through unparsed; their bytes are counted as "bytes read".

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[derive(Default)]
pub struct Counters {
    pub by_method: Mutex<BTreeMap<String, u64>>,
    pub bytes_up: AtomicU64,
    pub bytes_down: AtomicU64,
}

impl Counters {
    pub fn requests(&self) -> u64 {
        self.by_method.lock().unwrap().values().sum()
    }

    pub fn snapshot(&self) -> (BTreeMap<String, u64>, u64, u64) {
        (
            self.by_method.lock().unwrap().clone(),
            self.bytes_up.load(Ordering::Relaxed),
            self.bytes_down.load(Ordering::Relaxed),
        )
    }
}

/// Starts the proxy on an ephemeral local port. Returns its address and counters.
pub async fn start(upstream: String) -> std::io::Result<(String, Arc<Counters>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    let counters = Arc::new(Counters::default());
    let c = counters.clone();
    tokio::spawn(async move {
        loop {
            let Ok((client, _)) = listener.accept().await else {
                break;
            };
            let up = upstream.clone();
            let c = c.clone();
            tokio::spawn(async move {
                if let Ok(server) = TcpStream::connect(&up).await {
                    let _ = relay(client, server, c).await;
                }
            });
        }
    });
    Ok((addr, counters))
}

async fn relay(client: TcpStream, server: TcpStream, c: Arc<Counters>) -> std::io::Result<()> {
    let _ = client.set_nodelay(true);
    let _ = server.set_nodelay(true);
    let (cr, mut cw) = client.into_split();
    let (mut sr, mut sw) = server.into_split();
    let down = {
        let c = c.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 << 10];
            loop {
                let n = sr.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                c.bytes_down.fetch_add(n as u64, Ordering::Relaxed);
                cw.write_all(&buf[..n]).await?;
            }
            cw.shutdown().await?;
            Ok::<_, std::io::Error>(())
        })
    };
    let mut cr = BufReader::with_capacity(64 << 10, cr);
    loop {
        // Request line and headers.
        let mut head = Vec::new();
        let mut content_length: u64 = 0;
        let mut chunked = false;
        loop {
            let mut line = Vec::new();
            let n = cr.read_until(b'\n', &mut line).await?;
            if n == 0 {
                let _ = sw.shutdown().await;
                let _ = down.await;
                return Ok(());
            }
            let text = String::from_utf8_lossy(&line).to_ascii_lowercase();
            if let Some(v) = text.strip_prefix("content-length:") {
                content_length = v.trim().parse().unwrap_or(0);
            }
            if text.starts_with("transfer-encoding:") && text.contains("chunked") {
                chunked = true;
            }
            head.extend_from_slice(&line);
            if line == b"\r\n" || line == b"\n" {
                break;
            }
        }
        let method = head
            .split(|b| *b == b' ')
            .next()
            .map(|m| String::from_utf8_lossy(m).into_owned())
            .unwrap_or_default();
        *c.by_method.lock().unwrap().entry(method).or_default() += 1;
        c.bytes_up.fetch_add(head.len() as u64, Ordering::Relaxed);
        sw.write_all(&head).await?;
        if chunked {
            loop {
                let mut size_line = Vec::new();
                cr.read_until(b'\n', &mut size_line).await?;
                sw.write_all(&size_line).await?;
                let size_text = String::from_utf8_lossy(&size_line);
                let size =
                    u64::from_str_radix(size_text.split(';').next().unwrap_or("0").trim(), 16)
                        .unwrap_or(0);
                copy_exact(&mut cr, &mut sw, size + 2, &c).await?;
                if size == 0 {
                    break;
                }
            }
        } else {
            copy_exact(&mut cr, &mut sw, content_length, &c).await?;
        }
    }
}

async fn copy_exact<R, W>(r: &mut R, w: &mut W, mut n: u64, c: &Counters) -> std::io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut buf = vec![0u8; 64 << 10];
    while n > 0 {
        let want = n.min(buf.len() as u64) as usize;
        let got = r.read(&mut buf[..want]).await?;
        if got == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        w.write_all(&buf[..got]).await?;
        c.bytes_up.fetch_add(got as u64, Ordering::Relaxed);
        n -= got as u64;
    }
    Ok(())
}

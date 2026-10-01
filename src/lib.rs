use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

pub fn now_nanos() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
}

fn header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

pub fn read_request(stream: &mut TcpStream) -> io::Result<HttpRequest> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 1024];
    let end = loop {
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed"));
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(i) = header_end(&buf) {
            break i;
        }
        if buf.len() > 64 * 1024 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "request headers too large"));
        }
    };

    let headers = String::from_utf8_lossy(&buf[..end]);
    let mut lines = headers.lines();
    let first = lines.next().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request line"))?;
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    let content_length = lines
        .find_map(|line| {
            let (k, v) = line.split_once(':')?;
            if k.eq_ignore_ascii_case("content-length") { v.trim().parse::<usize>().ok() } else { None }
        })
        .unwrap_or(0);

    let body_start = end + 4;
    while buf.len() < body_start + content_length {
        let n = stream.read(&mut tmp)?;
        if n == 0 { break; }
        buf.extend_from_slice(&tmp[..n]);
    }
    let body_end = (body_start + content_length).min(buf.len());
    Ok(HttpRequest { method, path, body: buf[body_start..body_end].to_vec() })
}

pub fn write_response(stream: &mut TcpStream, status: u16, body: &[u8], content_type: &str) -> io::Result<()> {
    let reason = match status {
        200 => "OK", 201 => "Created", 204 => "No Content", 400 => "Bad Request", 404 => "Not Found",
        405 => "Method Not Allowed", 409 => "Conflict", 503 => "Service Unavailable", _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nContent-Type: {content_type}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

pub fn send_http(addr: &str, method: &str, path: &str, body: &[u8]) -> io::Result<HttpResponse> {
    let socket: SocketAddr = addr.parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid backend address"))?;
    let mut stream = TcpStream::connect_timeout(&socket, Duration::from_millis(350))?;
    stream.set_read_timeout(Some(Duration::from_millis(700)))?;
    stream.set_write_timeout(Some(Duration::from_millis(700)))?;
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(request.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf)?;
    let end = header_end(&buf).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid HTTP response"))?;
    let head = String::from_utf8_lossy(&buf[..end]);
    let status = head.lines().next().and_then(|l| l.split_whitespace().nth(1)).and_then(|s| s.parse().ok()).unwrap_or(500);
    Ok(HttpResponse { status, body: buf[end + 4..].to_vec() })
}

pub fn valid_key(key: &str) -> bool {
    !key.is_empty() && key.len() <= 128 && key.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}

pub fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn hex_decode(input: &str) -> Option<Vec<u8>> {
    if !input.len().is_multiple_of(2) { return None; }
    (0..input.len()).step_by(2).map(|i| u8::from_str_radix(&input[i..i+2], 16).ok()).collect()
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Record {
    pub clock: u128,
    pub origin: String,
    pub value: Vec<u8>,
}

impl Record {
    pub fn newer_than(&self, other: &Record) -> bool {
        (self.clock, self.origin.as_str()) > (other.clock, other.origin.as_str())
    }

    pub fn encode_line(&self, key: &str) -> String {
        format!("{key}\t{}\t{}\t{}\n", self.clock, self.origin, hex_encode(&self.value))
    }
}

pub fn parse_record_line(line: &str) -> Option<(String, Record)> {
    let mut p = line.trim_end().splitn(4, '\t');
    let key = p.next()?.to_string();
    let clock = p.next()?.parse::<u128>().ok()?;
    let origin = p.next()?.to_string();
    let value = hex_decode(p.next()?)?;
    Some((key, Record { clock, origin, value }))
}

pub struct Store {
    records: Mutex<HashMap<String, Record>>,
    wal: PathBuf,
}

impl Store {
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        fs::create_dir_all(dir.as_ref())?;
        let wal = dir.as_ref().join("aegismesh.wal");
        let mut records = HashMap::new();
        if let Ok(text) = fs::read_to_string(&wal) {
            for line in text.lines() {
                if let Some((key, record)) = parse_record_line(line) {
                    match records.get(&key) {
                        Some(existing) if !record.newer_than(existing) => {}
                        _ => { records.insert(key, record); }
                    }
                }
            }
        }
        Ok(Self { records: Mutex::new(records), wal })
    }

    pub fn apply(&self, key: String, record: Record) -> io::Result<bool> {
        let mut map = self.records.lock().expect("store mutex poisoned");
        let should_apply = map.get(&key).map(|r| record.newer_than(r)).unwrap_or(true);
        if !should_apply { return Ok(false); }
        let line = record.encode_line(&key);
        let mut file = OpenOptions::new().create(true).append(true).open(&self.wal)?;
        file.write_all(line.as_bytes())?;
        file.sync_data()?;
        map.insert(key, record);
        Ok(true)
    }

    pub fn get(&self, key: &str) -> Option<Record> {
        self.records.lock().expect("store mutex poisoned").get(key).cloned()
    }

    pub fn snapshot(&self) -> String {
        let map = self.records.lock().expect("store mutex poisoned");
        let mut keys: Vec<_> = map.keys().cloned().collect();
        keys.sort();
        keys.into_iter().map(|k| map[&k].encode_line(&k)).collect()
    }

    pub fn merge_snapshot(&self, snapshot: &str) -> io::Result<usize> {
        let mut applied = 0;
        for line in snapshot.lines() {
            if let Some((key, record)) = parse_record_line(line) {
                if self.apply(key, record)? { applied += 1; }
            }
        }
        Ok(applied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let x = b"settled:$10.25";
        assert_eq!(hex_decode(&hex_encode(x)).unwrap(), x);
    }

    #[test]
    fn newer_version_wins() {
        let a = Record { clock: 10, origin: "n1".into(), value: b"a".to_vec() };
        let b = Record { clock: 11, origin: "n0".into(), value: b"b".to_vec() };
        assert!(b.newer_than(&a));
    }
}

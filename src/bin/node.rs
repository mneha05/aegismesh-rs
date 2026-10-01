use aegismesh::{now_nanos, parse_record_line, read_request, send_http, valid_key, write_response, Record, Store};
use std::env;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

struct App {
    node_id: String,
    peers: Vec<String>,
    quorum: usize,
    ready: AtomicBool,
    store: Store,
    requests: AtomicU64,
    replicated: AtomicU64,
    failed_replications: AtomicU64,
}

fn env_or(name: &str, default: &str) -> String { env::var(name).unwrap_or_else(|_| default.into()) }

fn replicate(app: &App, key: &str, record: &Record) -> usize {
    let body = format!("{}\t{}\t{}", record.clock, record.origin, aegismesh::hex_encode(&record.value));
    let mut acks = 0;
    for peer in &app.peers {
        match send_http(peer, "POST", &format!("/internal/replicate/{key}"), body.as_bytes()) {
            Ok(r) if r.status == 200 || r.status == 201 => { acks += 1; }
            _ => { app.failed_replications.fetch_add(1, Ordering::Relaxed); }
        }
    }
    acks
}

fn handle(mut stream: TcpStream, app: Arc<App>) -> io::Result<()> {
    let req = read_request(&mut stream)?;
    app.requests.fetch_add(1, Ordering::Relaxed);

    if req.method == "GET" && req.path == "/healthz" {
        return write_response(&mut stream, 200, b"ok\n", "text/plain");
    }
    if req.method == "GET" && req.path == "/readyz" {
        let status = if app.ready.load(Ordering::Acquire) { 200 } else { 503 };
        return write_response(&mut stream, status, if status == 200 { b"ready\n" } else { b"syncing\n" }, "text/plain");
    }
    if req.method == "GET" && req.path == "/metrics" {
        let body = format!(
            "aegis_requests_total {}\naegis_replications_applied_total {}\naegis_replication_failures_total {}\n",
            app.requests.load(Ordering::Relaxed),
            app.replicated.load(Ordering::Relaxed),
            app.failed_replications.load(Ordering::Relaxed)
        );
        return write_response(&mut stream, 200, body.as_bytes(), "text/plain");
    }
    if req.method == "GET" && req.path == "/internal/snapshot" {
        return write_response(&mut stream, 200, app.store.snapshot().as_bytes(), "text/plain");
    }
    if let Some(key) = req.path.strip_prefix("/internal/replicate/") {
        if req.method != "POST" || !valid_key(key) { return write_response(&mut stream, 400, b"bad replication request\n", "text/plain"); }
        let text = String::from_utf8_lossy(&req.body);
        let synthetic = format!("{key}\t{text}");
        if let Some((key, record)) = parse_record_line(&synthetic) {
            if app.store.apply(key, record)? { app.replicated.fetch_add(1, Ordering::Relaxed); }
            return write_response(&mut stream, 200, b"ack\n", "text/plain");
        }
        return write_response(&mut stream, 400, b"invalid record\n", "text/plain");
    }

    if !app.ready.load(Ordering::Acquire) {
        return write_response(&mut stream, 503, b"node not ready\n", "text/plain");
    }

    if let Some(key) = req.path.strip_prefix("/v1/kv/") {
        if !valid_key(key) { return write_response(&mut stream, 400, b"invalid key\n", "text/plain"); }
        if req.method == "GET" {
            return match app.store.get(key) {
                Some(record) => write_response(&mut stream, 200, &record.value, "application/octet-stream"),
                None => write_response(&mut stream, 404, b"not found\n", "text/plain"),
            };
        }
        if req.method == "PUT" {
            if req.body.len() > 64 * 1024 { return write_response(&mut stream, 400, b"value too large\n", "text/plain"); }
            let record = Record { clock: now_nanos(), origin: app.node_id.clone(), value: req.body };
            let remote_acks = replicate(&app, key, &record);
            let acknowledgements = 1 + remote_acks;
            if acknowledgements < app.quorum {
                return write_response(&mut stream, 503, format!("quorum unavailable: {acknowledgements}/{}\n", app.quorum).as_bytes(), "text/plain");
            }
            app.store.apply(key.to_string(), record)?;
            let body = format!("committed quorum={acknowledgements}/{}\n", app.quorum);
            return write_response(&mut stream, 201, body.as_bytes(), "text/plain");
        }
        return write_response(&mut stream, 405, b"method not allowed\n", "text/plain");
    }

    write_response(&mut stream, 404, b"not found\n", "text/plain")
}

fn main() -> io::Result<()> {
    let node_id = env_or("NODE_ID", "node-1");
    let bind = env_or("BIND", "127.0.0.1:9101");
    let peers: Vec<String> = env_or("PEERS", "").split(',').filter(|x| !x.is_empty()).map(str::to_string).collect();
    let cluster_size = env_or("CLUSTER_SIZE", "3").parse::<usize>().unwrap_or(3);
    let quorum = cluster_size / 2 + 1;
    let state_dir = env_or("STATE_DIR", &format!("./data/{node_id}"));
    let listener = TcpListener::bind(&bind)?;
    eprintln!("aegis-node id={node_id} bind={bind} peers={} quorum={quorum}", peers.len());

    let app = Arc::new(App {
        node_id,
        peers,
        quorum,
        ready: AtomicBool::new(false),
        store: Store::open(state_dir)?,
        requests: AtomicU64::new(0),
        replicated: AtomicU64::new(0),
        failed_replications: AtomicU64::new(0),
    });

    let sync_app = Arc::clone(&app);
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(250));
        for peer in &sync_app.peers {
            if let Ok(resp) = send_http(peer, "GET", "/internal/snapshot", b"") {
                if resp.status == 200 {
                    let text = String::from_utf8_lossy(&resp.body);
                    match sync_app.store.merge_snapshot(&text) {
                        Ok(n) if n > 0 => eprintln!("catch-up from {peer}: applied {n} records"),
                        _ => {}
                    }
                }
            }
        }
        sync_app.ready.store(true, Ordering::Release);
        eprintln!("node ready");
    });

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let app = Arc::clone(&app);
                thread::spawn(move || { if let Err(e) = handle(stream, app) { eprintln!("request error: {e}"); } });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
    Ok(())
}

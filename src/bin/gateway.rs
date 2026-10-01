use aegismesh::{read_request, send_http, write_response};
use std::env;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

struct Gateway {
    backends: Vec<String>,
    cursor: AtomicUsize,
}

fn handle(mut stream: TcpStream, gateway: Arc<Gateway>) -> io::Result<()> {
    let req = read_request(&mut stream)?;
    if req.method == "GET" && req.path == "/healthz" {
        return write_response(&mut stream, 200, b"ok\n", "text/plain");
    }
    let n = gateway.backends.len();
    if n == 0 {
        return write_response(&mut stream, 503, b"no backends configured\n", "text/plain");
    }
    let start = gateway.cursor.fetch_add(1, Ordering::Relaxed) % n;
    let mut last_status = 503;
    for offset in 0..n {
        let backend = &gateway.backends[(start + offset) % n];
        if let Ok(ready) = send_http(backend, "GET", "/readyz", b"") {
            if ready.status != 200 {
                continue;
            }
        } else {
            continue;
        }
        match send_http(backend, &req.method, &req.path, &req.body) {
            Ok(resp) if resp.status < 500 => {
                return write_response(
                    &mut stream,
                    resp.status,
                    &resp.body,
                    "application/octet-stream",
                );
            }
            Ok(resp) => last_status = resp.status,
            Err(_) => {}
        }
    }
    write_response(
        &mut stream,
        last_status,
        b"all ready backends failed\n",
        "text/plain",
    )
}

fn main() -> io::Result<()> {
    let bind = env::var("BIND").unwrap_or_else(|_| "127.0.0.1:9000".into());
    let backends: Vec<String> = env::var("BACKENDS")
        .unwrap_or_else(|_| "127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103".into())
        .split(',')
        .filter(|x| !x.is_empty())
        .map(str::to_string)
        .collect();
    let listener = TcpListener::bind(&bind)?;
    eprintln!("aegis-gateway bind={bind} backends={}", backends.join(","));
    let gateway = Arc::new(Gateway {
        backends,
        cursor: AtomicUsize::new(0),
    });
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let gateway = Arc::clone(&gateway);
                thread::spawn(move || {
                    if let Err(e) = handle(stream, gateway) {
                        eprintln!("gateway error: {e}");
                    }
                });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
    Ok(())
}

# AegisMesh

**A dependency-free Rust high-availability service lab with quorum writes, WAL persistence, replica catch-up, retrying traffic routing, and automated chaos verification.**

AegisMesh exists to make two claims concrete rather than keyword-level:

1. hands-on **Rust** systems programming;
2. designing and **running a service under replica failure** with explicit availability and quorum behavior.

It is a portfolio distributed-systems lab, not a production consensus database and not a claim of professional on-call experience.

## Architecture

```text
                         clients
                            |
                            v
                  +-------------------+
                  |  Rust gateway     |
                  | ready probe       |
                  | round robin       |
                  | retry on failure  |
                  +----+----+----+----+
                       |    |    |
               +-------+    |    +-------+
               v            v            v
          +----------+  +----------+  +----------+
          | Rust n1  |  | Rust n2  |  | Rust n3  |
          | WAL      |<->| WAL      |<->| WAL      |
          | /readyz  |  | /readyz  |  | /readyz  |
          +----------+  +----------+  +----------+
                quorum write = 2 of 3 acknowledgements
```

Each node exposes:

- `GET /healthz` — process health;
- `GET /readyz` — traffic readiness after startup catch-up;
- `GET /metrics` — request/replication counters;
- `PUT /v1/kv/:key` — versioned quorum write;
- `GET /v1/kv/:key` — local read;
- internal replication and snapshot endpoints for peer catch-up.

## Availability behavior actually tested

The GitHub Actions chaos test launches **three independent Rust node processes plus a Rust gateway** and performs this sequence:

```text
1. start n1 + n2 + n3
2. write payment-001 through gateway
3. kill n2
4. write payment-002 with only n1+n3 available
5. issue 100 gateway reads while n2 is dead
6. restart n2
7. verify n2 catches up payment-002 from a peer snapshot
8. kill n1+n3
9. verify reads remain available on surviving n2
10. verify a new write returns HTTP 503 because 1/3 cannot form quorum
```

That distinguishes **availability** from **unsafe acceptance of writes**: one replica failure preserves read/write service; loss of quorum preserves reads but refuses new commits.

## Rust implementation

The project intentionally uses the Rust standard library only—no web framework—to make the systems mechanics visible:

- `std::net::TcpListener` / `TcpStream` HTTP transport;
- threads + `Arc` for concurrent connections;
- `Mutex<HashMap<...>>` shared replica state;
- `AtomicBool` readiness and atomic metrics;
- append-only WAL with `sync_data()` before acknowledging local application;
- last-write-wins version ordering using `(timestamp, node_id)`;
- startup anti-entropy by fetching peer snapshots;
- bounded TCP connect/read/write timeouts;
- health-aware gateway retries.

## Run locally

```bash
cargo build --release
bash scripts/chaos-test.sh
```

Or with containers:

```bash
docker compose up --build
curl -X PUT --data-binary 'settled' http://localhost:8080/v1/kv/payment-42
curl http://localhost:8080/v1/kv/payment-42
```

## Failure semantics

AegisMesh is deliberately explicit about its boundary. It is **not Raft/Paxos**, does not claim linearizable consensus, and does not implement distributed transactions or membership changes. The quorum write path is a small availability lab: a 3-node deployment requires two acknowledgements, persists node-local state to WAL, and uses versioned replication plus startup catch-up.

The valid résumé claim is therefore along the lines of:

> Built and chaos-tested a 3-replica Rust service with quorum writes, WAL persistence, readiness probes, failover routing, and replica catch-up; verified continuous service through a single-replica failure and safe write rejection after quorum loss.

That is different from claiming production on-call ownership or operating a globally distributed database.

## Repository map

```text
src/lib.rs              HTTP primitives, WAL store, versioned records
src/bin/node.rs         replica server, quorum writes, replication, catch-up
src/bin/gateway.rs      health-aware round-robin gateway with retries
scripts/chaos-test.sh   real multi-process failure/recovery verification
.github/workflows/ci.yml Rust fmt/test/clippy + chaos test
docker-compose.yml      3 nodes + gateway locally
Dockerfile              production-style Rust build/runtime image
```

## License

MIT

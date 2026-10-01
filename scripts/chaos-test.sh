#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/release"
TMP="${TMPDIR:-/tmp}/aegismesh-chaos-$$"
mkdir -p "$TMP" "$ROOT/chaos-artifacts"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

start_node() {
  local id="$1" port="$2" peers="$3"
  NODE_ID="$id" BIND="127.0.0.1:$port" PEERS="$peers" CLUSTER_SIZE=3 STATE_DIR="$TMP/$id"     "$BIN/aegis-node" >"$ROOT/chaos-artifacts/$id.log" 2>&1 &
  echo $!
}

wait_ready() {
  local url="$1"
  for _ in $(seq 1 80); do
    if curl -fsS "$url/readyz" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "timed out waiting for $url" >&2
  return 1
}

cargo build --release

N1=$(start_node n1 9101 '127.0.0.1:9102,127.0.0.1:9103'); PIDS+=("$N1")
N2=$(start_node n2 9102 '127.0.0.1:9101,127.0.0.1:9103'); PIDS+=("$N2")
N3=$(start_node n3 9103 '127.0.0.1:9101,127.0.0.1:9102'); PIDS+=("$N3")
BACKENDS='127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103' BIND='127.0.0.1:9000'   "$BIN/aegis-gateway" >"$ROOT/chaos-artifacts/gateway.log" 2>&1 &
GW=$!; PIDS+=("$GW")

wait_ready http://127.0.0.1:9101
wait_ready http://127.0.0.1:9102
wait_ready http://127.0.0.1:9103

curl -fsS -X PUT --data-binary 'settled-before-failure' http://127.0.0.1:9000/v1/kv/payment-001 >/dev/null
[[ "$(curl -fsS http://127.0.0.1:9000/v1/kv/payment-001)" == 'settled-before-failure' ]]

# Kill one replica: reads AND quorum writes must continue.
kill "$N2"; wait "$N2" 2>/dev/null || true
curl -fsS -X PUT --data-binary 'written-with-one-replica-down' http://127.0.0.1:9000/v1/kv/payment-002 >/dev/null
for _ in $(seq 1 100); do
  [[ "$(curl -fsS http://127.0.0.1:9000/v1/kv/payment-002)" == 'written-with-one-replica-down' ]]
done

# Restart the failed node with the same WAL. Startup anti-entropy must fetch the missed write.
N2R=$(start_node n2 9102 '127.0.0.1:9101,127.0.0.1:9103'); PIDS+=("$N2R")
wait_ready http://127.0.0.1:9102
[[ "$(curl -fsS http://127.0.0.1:9102/v1/kv/payment-002)" == 'written-with-one-replica-down' ]]

# Lose a second replica: reads remain available on the surviving node, but writes refuse to commit without quorum.
kill "$N1" "$N3"; wait "$N1" 2>/dev/null || true; wait "$N3" 2>/dev/null || true
[[ "$(curl -fsS http://127.0.0.1:9000/v1/kv/payment-002)" == 'written-with-one-replica-down' ]]
STATUS=$(curl -sS -o "$TMP/noquorum.txt" -w '%{http_code}' -X PUT --data-binary 'must-not-commit' http://127.0.0.1:9102/v1/kv/payment-003)
[[ "$STATUS" == '503' ]]

echo 'AegisMesh chaos verification passed:'
echo '  3-node cluster started'
echo '  1 replica killed: 100/100 gateway reads succeeded'
echo '  quorum write succeeded with 2/3 nodes'
echo '  restarted replica caught up missed write'
echo '  1/3-node write correctly rejected with HTTP 503'

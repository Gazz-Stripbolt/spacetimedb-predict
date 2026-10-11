#!/usr/bin/env bash
# Publish the demo modules (TypeScript and Rust) and run the Rust client's prediction tests
# through a latency proxy.
#
#   scripts/test-rs.sh        (needs a local server: `spacetime start`)
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DB=${DB:-predict-rs}                    # the TypeScript demo module (hand-written predictions)
DB_RS_MODULE=${DB_RS_MODULE:-predict-rs-mod}   # the Rust demo module (shared task_logic)
PROXY=${PROXY:-3120}
DELAY=${DELAY:-150}

cd "$ROOT"
[[ -f demo/module/src/page.gen.ts ]] || npm run build:web --silent
echo "== publish demo/module as $DB"
spacetime publish -s local -y "$DB" -p demo/module --delete-data >/dev/null 2>&1
echo "== publish demo/rust/module as $DB_RS_MODULE"
spacetime publish -s local -y "$DB_RS_MODULE" -p demo/rust/module --delete-data >/dev/null 2>&1
echo "== proxy :$PROXY -> :3000, +$DELAY ms each way"
node --import tsx tests/proxy.mts "$PROXY" 3000 "$DELAY" >/dev/null &   # one process, so the trap kills the proxy itself
PROXY_PID=$!
trap 'kill "$PROXY_PID" 2>/dev/null || true' EXIT
for _ in $(seq 50); do (exec 3<>/dev/tcp/127.0.0.1/"$PROXY") 2>/dev/null && break; sleep 0.2; done
echo "== rust client tests"
cd rust
DB=$DB DB_RS_MODULE=$DB_RS_MODULE PROXY=$PROXY DELAY=$DELAY cargo run -q -p predict-tests-client

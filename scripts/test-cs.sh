#!/usr/bin/env bash
# The C# predictor's tests: publish the demo modules (TypeScript, and the C# one that shares its
# logic with the client), put a latency proxy in front, run the console tests. Needs a local
# server (`spacetime start`) and `npm run build:web` (for page.gen.ts).
#
#   scripts/test-cs.sh     DELAY=150 DB=predict-cs SHARED_DB=predict-cs-mod PROXY_PORT=3110 by default;
#                          MODE=ts|shared|all (all)
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
export DB=${DB:-predict-cs} SHARED_DB=${SHARED_DB:-predict-cs-mod} DELAY=${DELAY:-150} PROXY_PORT=${PROXY_PORT:-3110}
cd "$ROOT"
[[ -f demo/module/src/page.gen.ts ]] || npm run build:web --silent
echo "== publish demo/module as $DB"
spacetime publish -s local -y "$DB" -p demo/module --delete-data >/dev/null 2>&1
echo "== publish demo/csharp/module as $SHARED_DB"
spacetime publish -s local -y "$SHARED_DB" -p demo/csharp/module --delete-data >/dev/null 2>&1
echo "== proxy :$PROXY_PORT -> :3000, +$DELAY ms each way"
# One node process (no npx/tsx wrapper processes), so the trap can stop it by PID.
node --import tsx tests/proxy.mts "$PROXY_PORT" 3000 "$DELAY" >/dev/null 2>&1 &
PROXY=$!
trap 'kill "$PROXY" 2>/dev/null || true' EXIT
for i in $(seq 50); do (exec 3<>/dev/tcp/127.0.0.1/"$PROXY_PORT") 2>/dev/null && break; sleep 0.1; done
echo "== C# tests"
dotnet run --project csharp/tests -c Release

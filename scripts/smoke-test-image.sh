#!/usr/bin/env bash
# End-to-end check of a PyTaskQ Docker image, used by CI on every pull request.
#
#   scripts/smoke-test-image.sh pytaskq:ci
#
# Starts Redis + the dashboard + a worker as containers, then signs in, queues a
# job through the API and waits for the worker to finish it.
# To run only the API checks against a server you already started:
#   SMOKE_BASE_URL=http://localhost:8000 SMOKE_USER=… SMOKE_PASSWORD=… scripts/smoke-test-image.sh
set -euo pipefail

IMAGE="${1:-}"
USER_NAME="${SMOKE_USER:-smoke}"
PASSWORD="${SMOKE_PASSWORD:-smoke-password-123}"
JAR="$(mktemp)"

step() { printf '\n▶ %s\n' "$*"; }
fail() { printf '✗ %s\n' "$*" >&2; exit 1; }
json() { python3 -c "import sys, json; d = json.load(sys.stdin); print($1)"; }

if [ -z "${SMOKE_BASE_URL:-}" ]; then
  [ -n "$IMAGE" ] || fail "usage: $0 IMAGE"
  NET="pytaskq-smoke-$$"
  PORT="${PORT:-18000}"
  BASE="http://127.0.0.1:$PORT"
  cleanup() {
    if [ "${KEEP:-0}" != 1 ]; then
      docker logs "$NET-web" 2>&1 | tail -n 30 || true
      docker rm -f "$NET-redis" "$NET-web" "$NET-worker" >/dev/null 2>&1 || true
      docker network rm "$NET" >/dev/null 2>&1 || true
    fi
    rm -f "$JAR"
  }
  trap cleanup EXIT

  step "Starting Redis, dashboard and worker from $IMAGE"
  docker network create "$NET" >/dev/null
  docker run -d --name "$NET-redis" --network "$NET" redis:7-alpine >/dev/null
  docker run -d --name "$NET-web" --network "$NET" -p "$PORT:8000" \
    -e REDIS_HOST="$NET-redis" -e PYTASKQ_APP=example_app:queue \
    -e PYTASKQ_ADMIN_USER="$USER_NAME" -e PYTASKQ_ADMIN_PASSWORD="$PASSWORD" \
    --health-interval 2s "$IMAGE" >/dev/null
  docker run -d --name "$NET-worker" --network "$NET" -e REDIS_HOST="$NET-redis" \
    "$IMAGE" pytaskq worker --app example_app:queue --cpu-workers 1 --io-workers 2 >/dev/null

  step "Waiting for the image's own HEALTHCHECK to report healthy"
  for _ in $(seq 60); do
    status="$(docker inspect -f '{{.State.Health.Status}}' "$NET-web")"
    [ "$status" = healthy ] && break
    sleep 2
  done
  [ "$status" = healthy ] || fail "dashboard container is '$status'"
  echo "healthy"

  step "Container runs as a non-root user"
  uid="$(docker exec "$NET-web" id -u)"
  [ "$uid" != 0 ] || fail "running as root"
  echo "uid $uid"
else
  BASE="$SMOKE_BASE_URL"
  trap 'rm -f "$JAR"' EXIT
fi

step "Health and dashboard page"
curl -fsS "$BASE/health" | json 'd["status"]' | grep -qx ok || fail "/health is not ok"
curl -fsS "$BASE/" | grep -q '<div id="root">' || fail "dashboard page not served"
echo "ok"

step "Anonymous requests are refused"
code="$(curl -s -o /dev/null -w '%{http_code}' "$BASE/tasks")"
[ "$code" = 401 ] || fail "expected 401 for anonymous /tasks, got $code"
echo "401 as expected"

step "Sign in as $USER_NAME"
curl -fsS -c "$JAR" -H 'Content-Type: application/json' \
  -d "{\"username\": \"$USER_NAME\", \"password\": \"$PASSWORD\"}" "$BASE/auth/login" \
  | json 'd["user"]["role"]' | grep -qx admin || fail "sign-in failed"
echo "signed in"

step "Worker has registered its tasks"
for _ in $(seq 30); do
  if curl -fsS -b "$JAR" "$BASE/system/workers" | json 'd["count"]' | grep -qv '^0$'; then break; fi
  sleep 1
done
curl -fsS -b "$JAR" "$BASE/system/workers" | json 'd["count"]' | grep -qv '^0$' || fail "no worker registered"
curl -fsS -b "$JAR" "$BASE/tasks/registered" | json '",".join(t["name"] for t in d["tasks"])' | grep -q notify_user \
  || fail "notify_user task not registered"
echo "worker online, tasks registered"

step "Queue a job and wait for it to succeed"
task_id="$(curl -fsS -b "$JAR" -H 'Content-Type: application/json' \
  -d '{"task_name": "notify_user", "kwargs": {"user_id": 7, "message": "smoke test"}}' \
  "$BASE/task/enqueue" | json 'd["task_id"]')"
for _ in $(seq 30); do
  status="$(curl -fsS -b "$JAR" "$BASE/task/$task_id" | json 'd["result"]["status"]')"
  [ "$status" = Success ] && break
  sleep 1
done
[ "$status" = Success ] || fail "job $task_id ended as '$status'"
echo "job $task_id: $status"

printf '\n✓ Smoke test passed\n'

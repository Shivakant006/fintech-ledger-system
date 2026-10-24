"""
Manual verification script for the health endpoints.

Calls the LIVE API, so run it with:
    docker compose exec api python -m app.test_health

SCENARIO 1: GET /health returns 200 {"status": "ok"} (liveness).
SCENARIO 2: GET /health/ready returns 200 and reports database and redis ok.
SCENARIO 3: an unreachable Redis makes the ping RAISE, quickly. This is the
            failure path the readiness endpoint relies on to report 503.

The full failure behaviour (503 with a named dependency while /health stays
200) is proven by hand: stop Redis, call both endpoints, restart Redis.
"""
import json
import sys
import time
import urllib.error
import urllib.request

from .redis_client import ping_redis

BASE_URL = "http://localhost:8000"


def _get(path):
    try:
        with urllib.request.urlopen(f"{BASE_URL}{path}") as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def run_scenario_1():
    print("\n=== SCENARIO 1: liveness ===")
    code, body = _get("/health")
    if code != 200 or body != {"status": "ok"}:
        print(f"[VERIFY] !! FAIL: got {code} {body}")
        return False
    print(f"[VERIFY] 200 {body}")
    print("[RESULT] PASS")
    return True


def run_scenario_2():
    print("\n=== SCENARIO 2: readiness with healthy dependencies ===")
    code, body = _get("/health/ready")
    expected = {"status": "ok", "checks": {"database": "ok", "redis": "ok"}}
    if code != 200 or body != expected:
        print(f"[VERIFY] !! FAIL: got {code} {body}")
        return False
    print(f"[VERIFY] 200 {body}")
    print("[RESULT] PASS")
    return True


def run_scenario_3():
    print("\n=== SCENARIO 3: an unreachable Redis is reported as a failure, fast ===")
    start = time.monotonic()
    try:
        ping_redis(port=1, timeout=0.5)  # nothing listens on port 1
        raised = False
    except Exception as e:
        raised = True
        error_name = type(e).__name__
    elapsed = time.monotonic() - start
    if not raised:
        print("[VERIFY] !! FAIL: ping to an unreachable Redis did NOT raise.")
        return False
    if elapsed > 3:
        print(f"[VERIFY] !! FAIL: took {elapsed:.1f}s to fail; a probe must not hang.")
        return False
    print(f"[VERIFY] Raised {error_name} after {elapsed:.2f}s.")
    print("[RESULT] PASS")
    return True


def main():
    results = [
        ("Scenario 1 (liveness)                     ", run_scenario_1()),
        ("Scenario 2 (readiness, all dependencies ok)", run_scenario_2()),
        ("Scenario 3 (unreachable Redis fails fast)  ", run_scenario_3()),
    ]
    print("\n=== SUMMARY ===")
    for label, ok in results:
        print(f"{label}: {'PASS' if ok else 'FAIL'}")
    if not all(ok for _, ok in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
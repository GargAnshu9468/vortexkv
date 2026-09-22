#!/usr/bin/env python3
import socket
import sys
import time
import urllib.request
import json

def send(s, data):
    if isinstance(data, str):
        data = data.encode()
    s.sendall(data)
    resp = s.recv(4096)
    return resp

def run_tests():
    print("=================================================================")
    print("       VortexKV Container End-to-End Verification Suite          ")
    print("=================================================================")

    # 1. Healthz & Metrics
    print("\n[Phase 1] HTTP Healthz & Prometheus Metrics Verification")
    with urllib.request.urlopen("http://localhost:7380/healthz") as res:
        health = json.loads(res.read().decode())
        print(f"  -> Health payload: {health}")
        assert health["status"] == "healthy", f"Health status not healthy: {health}"
        assert health["engine"] == "VortexKV"

    with urllib.request.urlopen("http://localhost:7380/metrics") as res:
        metrics = res.read().decode()
        assert "vortex_up 1" in metrics, "Missing vortex_up 1 metric"
        print("  -> Metrics endpoint verified (vortex_up 1 found)")

    # 2. Redis RESP Protocol Core Commands
    print("\n[Phase 2] RESP Wire Protocol & Core Key-Value Operations")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", 7379))

    # Clean test keyspace
    send(s, "*1\r\n$7\r\nFLUSHDB\r\n")

    # PING
    resp = send(s, "*1\r\n$4\r\nPING\r\n")
    assert resp == b"+PONG\r\n", f"PING failed: {resp}"
    print("  -> PING: OK (+PONG)")

    # SET & GET
    resp = send(s, "*3\r\n$3\r\nSET\r\n$7\r\ntestkey\r\n$9\r\ntestvalue\r\n")
    assert resp == b"+OK\r\n", f"SET failed: {resp}"
    resp = send(s, "*2\r\n$3\r\nGET\r\n$7\r\ntestkey\r\n")
    assert resp == b"$9\r\ntestvalue\r\n", f"GET failed: {resp}"
    print("  -> SET / GET: OK")

    # INCR & INCRBY
    resp = send(s, "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n")
    assert resp == b":1\r\n", f"INCR failed: {resp}"
    resp = send(s, "*3\r\n$6\r\nINCRBY\r\n$7\r\ncounter\r\n$2\r\n10\r\n")
    assert resp == b":11\r\n", f"INCRBY failed: {resp}"
    print("  -> INCR / INCRBY: OK (:11)")

    # MSET & MGET
    resp = send(s, "*5\r\n$4\r\nMSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n")
    assert resp == b"+OK\r\n", f"MSET failed: {resp}"
    resp = send(s, "*3\r\n$4\r\nMGET\r\n$2\r\nk1\r\n$2\r\nk2\r\n")
    assert b"$2\r\nv1\r\n$2\r\nv2\r\n" in resp, f"MGET failed: {resp}"
    print("  -> MSET / MGET: OK")

    # EXPIRE & TTL
    send(s, "*3\r\n$3\r\nSET\r\n$8\r\ntemp_key\r\n$3\r\nval\r\n")
    resp = send(s, "*3\r\n$6\r\nEXPIRE\r\n$8\r\ntemp_key\r\n$2\r\n60\r\n")
    assert resp == b":1\r\n", f"EXPIRE failed: {resp}"
    resp = send(s, "*2\r\n$3\r\nTTL\r\n$8\r\ntemp_key\r\n")
    assert resp.startswith(b":"), f"TTL failed: {resp}"
    print("  -> EXPIRE / TTL: OK")

    # 3. Transaction MULTI / EXEC (verifying no buffer mutation corruption)
    print("\n[Phase 3] ACID Transaction MULTI / EXEC Verification")
    send(s, "*1\r\n$5\r\nMULTI\r\n")
    send(s, "*3\r\n$3\r\nSET\r\n$5\r\ntxkey\r\n$5\r\nvaltx\r\n")
    send(s, "*2\r\n$3\r\nGET\r\n$5\r\ntxkey\r\n")
    resp = send(s, "*1\r\n$4\r\nEXEC\r\n")
    assert resp == b"*2\r\n+OK\r\n$5\r\nvaltx\r\n", f"MULTI/EXEC failed: {resp}"
    print("  -> MULTI / EXEC: OK (Transaction isolation and buffer copy verified)")

    # 4. WATCH / UNWATCH Optimistic Locking Verification
    print("\n[Phase 4] Optimistic Locking WATCH / UNWATCH Verification")
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.connect(("localhost", 7379))

    # Clean state
    send(s, "*3\r\n$3\r\nSET\r\n$9\r\nsharedkey\r\n$7\r\ninitial\r\n")

    # s watches sharedkey and starts multi
    send(s, "*2\r\n$5\r\nWATCH\r\n$9\r\nsharedkey\r\n")
    send(s, "*1\r\n$5\r\nMULTI\r\n")
    send(s, "*3\r\n$3\r\nSET\r\n$9\r\nsharedkey\r\n$11\r\nmutated_by1\r\n")

    # Concurrent mutation from s2
    send(s2, "*3\r\n$3\r\nSET\r\n$9\r\nsharedkey\r\n$11\r\nmutated_by2\r\n")

    # s executes -> MUST abort with nil (RESP null array *-1\r\n or null bulk $-1\r\n)
    resp = send(s, "*1\r\n$4\r\nEXEC\r\n")
    assert resp in (b"*-1\r\n", b"$-1\r\n"), f"WATCH optimistic conflict abort failed: {resp}"
    print(f"  -> WATCH conflict abort: OK (Returned {resp.strip()!r})")

    # Verify UNWATCH clears dirty flag
    send(s, "*2\r\n$5\r\nWATCH\r\n$9\r\nsharedkey\r\n")
    send(s, "*1\r\n$7\r\nUNWATCH\r\n")
    send(s2, "*3\r\n$3\r\nSET\r\n$9\r\nsharedkey\r\n$11\r\nmutated_by3\r\n")
    send(s, "*1\r\n$5\r\nMULTI\r\n")
    send(s, "*3\r\n$3\r\nSET\r\n$9\r\nsharedkey\r\n$11\r\nmutated_by1\r\n")
    resp = send(s, "*1\r\n$4\r\nEXEC\r\n")
    assert resp == b"*1\r\n+OK\r\n", f"UNWATCH failed: {resp}"
    print("  -> UNWATCH reset: OK")

    s.close()
    s2.close()

    print("\n=================================================================")
    print("  🎉 ALL END-TO-END DOCKER CONTAINER VERIFICATIONS PASSED 100%   ")
    print("=================================================================\n")

if __name__ == "__main__":
    run_tests()

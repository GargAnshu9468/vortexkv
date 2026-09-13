# 📊 Observability & Metrics

VortexKV provides native Prometheus text exporter metrics and Kubernetes health checking probes out of the box with zero external sidecars.

---

## 1. Prometheus Metrics (`GET /metrics`)

Available directly on Web Studio port `7380`:
```bash
curl -s http://localhost:7380/metrics
```

Sample output:
```text
# HELP vortex_commands_total Total commands executed by VortexKV
# TYPE vortex_commands_total counter
vortex_commands_total 248910

# HELP vortex_ops_per_sec Instantaneous operations per second
# TYPE vortex_ops_per_sec gauge
vortex_ops_per_sec 210970

# HELP vortex_keys_total Total active keys in memory
# TYPE vortex_keys_total gauge
vortex_keys_total 14502

# HELP vortex_p99_latency_microseconds P99 command execution latency
# TYPE vortex_p99_latency_microseconds gauge
vortex_p99_latency_microseconds 135.2
```

---

## 2. Kubernetes Probes (`GET /healthz`)

Integrated endpoint for pod liveness and readiness probes:
```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 7380
  initialDelaySeconds: 2
  periodSeconds: 5
readinessProbe:
  httpGet:
    path: /healthz
    port: 7380
  initialDelaySeconds: 2
  periodSeconds: 5
```

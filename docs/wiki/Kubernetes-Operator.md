# ☸️ VortexKV Kubernetes Operator

The VortexKV Operator automates deployment, health checking, and dynamic scale-out of clustered VortexKV instances on Kubernetes.

---

## ⚡ Deployment Steps

### 1. Install CRDs & RBAC
```bash
kubectl apply -f deployments/operator/crd.yaml
kubectl apply -f deployments/operator/rbac.yaml
```

### 2. Launch Controller
```bash
kubectl apply -f deployments/operator/operator.yaml
```

### 3. Deploy a Clustered Instance
```yaml
apiVersion: vortex.io/v1alpha1
kind: VortexCluster
metadata:
  name: production-vortex
spec:
  replicas: 6
  clusterEnabled: true
  maxMemory: "2gb"
  persistence:
    aof: true
    rdb: true
  resources:
    limits:
      cpu: "2000m"
      memory: "4Gi"
```

```bash
kubectl apply -f deployments/operator/example-cluster.yaml
```

The operator automatically:
- Provisions 6 StatefulSet pods
- Initializes cluster topology with `CLUSTER MEET`
- Evenly distributes 16,384 hash slots across master nodes
- Configures replicas with `CLUSTER REPLICATE`

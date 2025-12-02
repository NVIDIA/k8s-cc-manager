# NVIDIA CC Manager - Python Implementation

## Overview

This is the **Python implementation** of the NVIDIA CC Manager for Kubernetes. It directly uses NVIDIA's `gpu-admin-tools` library to manage GPU Confidential Computing (CC) modes based on Kubernetes node labels.

### Docker Build

```bash
# Build distroless image
docker build -f deployments/container/Dockerfile.distroless -t k8s-cc-manager:python .

# Or use Makefile
make -f deployments/container/Makefile distroless
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  k8s-cc-manager Pod (DaemonSet)                         │
│                                                         │
│  ┌───────────────────────────────────────────────────┐  │
│  │  main.py (single-threaded)                        │  │
│  │                                                   │  │
│  │  1. Apply initial CC mode                         │  │
│  │  2. Watch node label: nvidia.com/cc.mode          │  │
│  │  3. On label change:                              │  │
│  │     ├─ [Optional] Evict GPU operator components   │  │
│  │     ├─ Apply CC mode via gpu-admin-tools          │  │
│  │     │   └─▶ Gpu.set_cc_mode()                     │  │
│  │     │   └─▶ Gpu.reset_with_os()                   │  │
│  │     └─ [Optional] Reschedule GPU operator         │  │
│  └───────────────────────────────────────────────────┘  │
│                          │                              │
│                          ▼                              │
│  ┌───────────────────────────────────────────────────┐  │
│  │  gpu-admin-tools/                                 │  │
│  │  └─▶ nvidia_gpu_tools.py (Gpu class)              │  │
│  │       ├─ Direct GPU register access               │  │
│  │       ├─ CC mode query/set                        │  │
│  │       └─ GPU reset                                │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `NODE_NAME` | Yes | - | Kubernetes node name (from fieldRef) |
| `DEFAULT_CC_MODE` | No | "on" | Default CC mode if label not set |
| `EVICT_OPERATOR_COMPONENTS` | No | "true" | Evict GPU operator components before CC mode change |
| `OPERATOR_NAMESPACE` | No | "gpu-operator" | Namespace where GPU operator is deployed |
| `KUBECONFIG` | No | "" | Path to kubeconfig (for local testing) |

### Node Labels

**Primary label:**
```bash
# Set CC mode
kubectl label node <node-name> nvidia.com/cc.mode=on --overwrite

# Supported values: "on", "off", "devtools"
```

**Status label (set by manager):**
```bash
# Check CC mode change status
kubectl get node <node-name> -o jsonpath='{.metadata.labels.nvidia\.com/cc\.mode\.state}'

# Values: "on/off/devtools" or "failed"
```

## Usage Examples

### Enable CC Mode

```bash
# Label the node
kubectl label node gpu-node-1 nvidia.com/cc.mode=on --overwrite

# Watch the logs
kubectl logs -n gpu-operator -l app=k8s-cc-manager -f
```

### Disable CC Mode

```bash
kubectl label node gpu-node-1 nvidia.com/cc.mode=off --overwrite
```

### Remove CC Mode Label (Use Default)

```bash
# Remove label
kubectl label node gpu-node-1 nvidia.com/cc.mode-

# Manager will apply DEFAULT_CC_MODE if set
```

# AS26: Beyond Containers — Securely Orchestrating AI Agents with Strong Isolation in Airflow

**Event:** Airflow Summit 2026

## Session summary

AI agents break Airflow's traditional trust model:

- Standard tasks are **deterministic**.
- Agents execute **dynamic logic** and invoke **external tools** — so untrusted
  code is now running inside standard containers that **share your host kernel**.

The session shows how to secure AI workloads in Airflow **without rewriting the
orchestrator or building a custom executor**: a policy-driven **`@agent`
TaskFlow abstraction** that uses KubernetesExecutor `executor_config` overrides
(e.g. `runtimeClassName`) to isolate workloads on the fly.

## Key takeaways

- **Threat model** — why a container is not a strong enough security boundary
  for AI agents (shared kernel, syscall surface).
- **Implementation** — an `@agent` decorator that routes tasks to sandboxed
  runtimes (**gVisor, Kata Containers, Peer Pods**) while the KubernetesExecutor
  stays unchanged.
- **Kubernetes in production** — achieving a **VM-per-pod** pattern with
  open-source tools, without nested node virtualization.
- **Operational realities** — execution flow, pod-spec mutation, and the
  latency / cost trade-offs of runtime isolation.

## My notes / observations

- **Framing shift:** an agent task is just another Airflow task in the DAG — but
  it's treated as an **untrusted task**. Same scheduling / deps / retries /
  XCom, different *trust class*. The DAG author decorates it `@agent`, the
  platform enforces the sandbox.
- So the trust boundary moves from "the whole Airflow deployment is trusted" to
  "per-task trust level", enforced at the pod-runtime layer rather than in
  Airflow code.
- Nice property: no fork of the executor. The `@agent` decorator just injects
  `executor_config` (pod overrides) — `runtimeClassName: gvisor` / `kata`, tight
  seccomp/AppArmor, dropped caps, no service-account token, locked-down
  egress/NetworkPolicy — and the KubernetesExecutor applies it like any other
  pod override.

### Why a container isn't a real boundary

- A container is just a process with namespaces + cgroups + seccomp/LSM filters.
  The task process still makes **syscalls straight to the one shared host
  kernel**, and ultimately needs the kernel to reach hardware.
- That shared kernel is the attack surface: one kernel LtR / privilege-escalation
  bug → container escape → the node and every other pod on it.
- seccomp/AppArmor narrow the syscall surface but don't remove the shared-kernel
  problem.

### What the sandboxed runtimes actually do

- **gVisor** (`runsc`) — the alternative walked through in the talk. Path:
  ```
  task process
    → Sentry   — a kernel written in Go, running in user space; implements the
                 Linux syscall surface itself (the task's syscalls are trapped
                 and served here, not by the host)
    → Gofer    — separate process that brokers filesystem access (Sentry has no
                 direct FS access)
    → node's kernel  (only a small, guarded set of host syscalls)
    → hardware
  ```
  So the guest app almost never touches the host kernel directly — the host
  kernel's exposed surface shrinks to what Sentry + Gofer need. Cost: syscall
  interception overhead + some compat gaps.
- **Kata Containers** — each pod runs in a **lightweight VM** with its own real
  kernel. Path:
  ```
  task process  (every syscall goes to...)
    → a guest kernel  — its own full Linux, per pod
    → the hypervisor  (QEMU / Cloud Hypervisor / Firecracker)
    → the node's kernel
    → hardware
  ```
  The boundary is hardware virtualization (VT-x/AMD-V), not syscall filtering —
  strongest of the three. Cost: VM boot latency, memory overhead per pod.
- **Peer Pods (Confidential Containers / cloud-api-adaptor)** — same Kata guest
  model, but the VM is a **separate cloud VM** created next to the node instead
  of on it. Path:
  ```
  containerd  → shim (kata / remote-hypervisor shim)
    → cloud-api-adaptor  — runs on the node; calls the cloud provider's API
                           (EC2 / GCE / Azure) to create a real VM as a "peer"
    → task process  (in that peer VM)
    → guest kernel
    → the peer VM's hypervisor (managed by the cloud) → hardware
  ```
  Point: VM-per-pod **without nested virtualization** on the k8s nodes — the
  node just orchestrates; the cloud does the virt. Matters on managed node pools
  (EKS/GKE/AKS) where nested virt isn't available. Trade-off: pod start now
  includes a cloud VM provision, and you pay for those VMs.

_(add more during/after the talk)_

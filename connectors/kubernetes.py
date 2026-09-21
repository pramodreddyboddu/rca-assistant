"""Real Kubernetes connector: workload diagnostics via the official client.

TRANSPORT
---------
Reads and the privileged rollout restart go through the official
``kubernetes`` Python client, imported LAZILY: this module imports cleanly
when the driver is absent; only the driver paths raise, with install
instructions. The client is configured either from a kubeconfig FILE
(``kubeconfig=<path>``, optional ``context=<name>``) or from the
in-cluster service account (``kubeconfig=None``). No raw tokens, bearer
material, or credential env vars exist anywhere in this connector: the
kubeconfig file / in-cluster identity carries auth, and tokens are never
inlined into constructor params, repr, exceptions, or logs.

PRINCIPAL / APPROVAL NOTE
-------------------------
The client identity (the user/entry in the kubeconfig, or the
ServiceAccount the in-cluster pod runs as) is the approval-relevant
principal: the rollout restart executes with whatever RBAC that identity
has. This connector performs no additional authorization of its own --
the ``restart_k8s_deployment`` tool is approval-gated upstream by the RCA
assistant (read() can never reach it; act() routing is enforced by the
framework against mcp_server.tools.PRIVILEGED_TOOLS). Make sure the
identity bound to this connector has at least: pods/events/deployments
list+get (reads) and deployments patch (restart), and nothing wider.

TOOL SURFACE (exact names/args/returns; this module is the contract):
- get_k8s_pod_status(namespace) -> dict: namespace, pods (list of {name,
  phase, restarts, ready, node}), ts. ``restarts`` sums restart_count over
  the pod's containers; ``ready`` is a "ready/total" string ("1/1"
  style); for pods with no container statuses yet (e.g. Pending) total
  falls back to the pod spec's container count.
- get_k8s_deployments(namespace) -> dict: namespace, deployments (list of
  {name, replicas_desired, replicas_ready, replicas_unavailable}), ts.
- get_k8s_events(namespace, limit=25) -> list of {ts, type, reason,
  object, message}. Newest first, truncated to ``limit``; ``object`` is
  "Kind/name" of the involved object.
- restart_k8s_deployment(namespace, deployment) -> PRIVILEGED dict:
  namespace, deployment, state ("restarted"), ts. Implemented as a
  strategic merge patch setting the
  ``kubectl.kubernetes.io/restartedAt`` pod-template annotation -- the
  genuine ``kubectl rollout restart`` mechanism (the changed template
  hash triggers a rolling update). Approval-gated upstream.

CREDENTIALS
-----------
There are no credential parameters by design (documented why): auth is
carried by the kubeconfig FILE (a path string, never token material) or
by the in-cluster service account, so there is nothing to inline and
nothing to leak. The constructor takes only ``kubeconfig`` (path or
None), ``context`` (kubeconfig context name or None), and ``timeout``.
``repr`` shows the kubeconfig path and context -- neither is secret.

STATUS (honest)
---------------
Fake-driver tested only (tests/test_kubernetes.py): a fake ``kubernetes``
module in sys.modules, no cluster touched. Live validation against a real
cluster happens in a customer pilot -- see docs/REAL_CONNECTOR_READINESS.md.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from connectors.base import (
    Connector,
    ConnectorError,
    ConnectorSpec,
    register_connector,
)

#: Pod-template annotation the genuine `kubectl rollout restart` sets; the
#: changed annotation bumps the template hash and triggers a rolling update.
_RESTART_ANNOTATION = "kubectl.kubernetes.io/restartedAt"


def _kubernetes() -> Any:
    """Import the kubernetes client lazily; ConnectorError w/ install help."""
    try:
        import kubernetes  # type: ignore
    except ImportError as exc:
        raise ConnectorError(
            "kubernetes: install kubernetes "
            "(pip install kubernetes>=28) to use the Kubernetes connector"
        ) from exc
    return kubernetes


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ts(value: Any) -> str | None:
    """Format a k8s timestamp (datetime) as UTC ISO-8601; None stays None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(value)


class KubernetesConnector(Connector):
    """Live Kubernetes diagnostics via the official python client."""

    name = "kubernetes"

    SPEC = ConnectorSpec(
        name="kubernetes",
        display_name="Kubernetes (official python client)",
        description="Live Kubernetes diagnostics via the official client: "
        "pod status (phase/restarts/ready/node), deployment replica "
        "state, and namespace events. Approval-gated deployment rollout "
        "restart via the kubectl restartedAt pod-template annotation.",
        required_config=(),
        optional_config=("kubeconfig", "context", "timeout"),
        credential_refs=(),
        notes="Auth is carried by the kubeconfig FILE (path string) or the "
        "in-cluster service account; there are no credential env vars or "
        "raw token parameters by design. The client identity (kubeconfig "
        "user/context or pod ServiceAccount) is the approval-relevant "
        "principal: restart_k8s_deployment runs with its RBAC and is "
        "approval-gated upstream. Fake-driver tested; live validation "
        "happens in a customer pilot.",
    )

    def __init__(
        self,
        kubeconfig: str | None = None,
        context: str | None = None,
        *,
        timeout: int = 10,
    ) -> None:
        # kubeconfig is a FILE PATH (or None for in-cluster), never token
        # material -- safe to hold on the instance and show in repr.
        self._kubeconfig = kubeconfig
        self._context = context
        self._timeout = int(timeout)
        self._core_v1: Any = None
        self._apps_v1: Any = None
        self._connected = False

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_k8s_pod_status",
            "get_k8s_deployments",
            "get_k8s_events",
            "restart_k8s_deployment",
        }

    def connect(self) -> None:
        """Load config (kubeconfig or in-cluster) and probe the API server.

        The probe is a ``list_namespace(limit=1)`` call: it proves both
        reachability and that the client identity can read. Failures wrap
        in ConnectorError (no token material ever surfaces).
        """
        k8s = _kubernetes()
        try:
            if self._kubeconfig:
                k8s.config.load_kube_config(
                    config_file=self._kubeconfig, context=self._context
                )
            else:
                k8s.config.load_incluster_config()
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: failed to load Kubernetes client config: {exc}"
            ) from exc
        core_v1 = k8s.client.CoreV1Api()
        apps_v1 = k8s.client.AppsV1Api()
        try:
            core_v1.list_namespace(limit=1, _request_timeout=self._timeout)
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: cannot reach the Kubernetes API server: {exc}"
            ) from exc
        self._core_v1 = core_v1
        self._apps_v1 = apps_v1
        self._connected = True

    def close(self) -> None:
        """Drop the cached API clients; safe to call when not connected."""
        self._core_v1 = None
        self._apps_v1 = None
        self._connected = False

    def __repr__(self) -> str:
        # kubeconfig is a file path and context is a name: non-secret.
        return (
            f"KubernetesConnector(kubeconfig={self._kubeconfig!r}, "
            f"context={self._context!r}, timeout={self._timeout!r})"
        )

    # ------------------------------------------------- driver plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    def _wrap(self, op: str, exc: Exception) -> ConnectorError:
        """Translate a driver exception into ConnectorError (no secrets)."""
        return ConnectorError(f"{self.name}: {op} failed: {exc}")

    # ------------------------------------------------- reads

    def get_k8s_pod_status(self, namespace: str) -> dict:
        """Pod phase, restart counts, ready/total, and node per pod.

        Returns dict {namespace, pods: [{name, phase, restarts, ready,
        node}], ts}. ``restarts`` sums restart_count across the pod's
        containers; ``ready`` is "ready/total" (e.g. "1/1"); for pods with
        no container statuses yet (Pending) the total falls back to the
        pod spec's container count.
        """
        self._require_connected()
        try:
            pod_list = self._core_v1.list_namespaced_pod(
                namespace, _request_timeout=self._timeout
            )
        except Exception as exc:
            raise self._wrap(
                f"get_k8s_pod_status({namespace!r})", exc
            ) from exc
        pods: list[dict] = []
        for item in pod_list.items or []:
            meta = item.metadata
            status = item.status
            spec = item.spec
            statuses = status.container_statuses or []
            restarts = sum(
                int(getattr(c, "restart_count", 0) or 0) for c in statuses
            )
            ready_n = sum(1 for c in statuses if getattr(c, "ready", False))
            total = len(statuses)
            if total == 0 and spec is not None:
                total = len(spec.containers or [])
            pods.append({
                "name": getattr(meta, "name", None),
                "phase": getattr(status, "phase", None),
                "restarts": restarts,
                "ready": f"{ready_n}/{total}",
                "node": getattr(spec, "node_name", None),
            })
        pods.sort(key=lambda p: p["name"] or "")
        return {"namespace": namespace, "pods": pods, "ts": _now()}

    def get_k8s_deployments(self, namespace: str) -> dict:
        """Desired/ready/unavailable replica counts per deployment.

        Returns dict {namespace, deployments: [{name, replicas_desired,
        replicas_ready, replicas_unavailable}], ts}.
        """
        self._require_connected()
        try:
            dep_list = self._apps_v1.list_namespaced_deployment(
                namespace, _request_timeout=self._timeout
            )
        except Exception as exc:
            raise self._wrap(
                f"get_k8s_deployments({namespace!r})", exc
            ) from exc
        deployments: list[dict] = []
        for item in dep_list.items or []:
            meta = item.metadata
            spec = item.spec
            status = item.status
            deployments.append({
                "name": getattr(meta, "name", None),
                "replicas_desired": int(getattr(spec, "replicas", 0) or 0),
                "replicas_ready": int(
                    getattr(status, "ready_replicas", 0) or 0),
                "replicas_unavailable": int(
                    getattr(status, "unavailable_replicas", 0) or 0),
            })
        deployments.sort(key=lambda d: d["name"] or "")
        return {
            "namespace": namespace,
            "deployments": deployments,
            "ts": _now(),
        }

    def get_k8s_events(self, namespace: str, limit: int = 25) -> list[dict]:
        """Newest-first namespace events, truncated to ``limit``.

        Returns a list of {ts, type, reason, object, message}; ``object``
        is "Kind/name" of the involved object. Timestamps fall back through
        last_timestamp -> event_time -> first_timestamp -> creation time.
        """
        self._require_connected()
        try:
            event_list = self._core_v1.list_namespaced_event(
                namespace, _request_timeout=self._timeout
            )
        except Exception as exc:
            raise self._wrap(
                f"get_k8s_events({namespace!r})", exc
            ) from exc

        def _event_ts(item: Any) -> datetime | None:
            for attr in ("last_timestamp", "event_time", "first_timestamp"):
                value = getattr(item, attr, None)
                if isinstance(value, datetime):
                    return value
            meta = getattr(item, "metadata", None)
            created = getattr(meta, "creation_timestamp", None)
            return created if isinstance(created, datetime) else None

        def _sort_key(item: Any) -> tuple[int, str]:
            ts = _event_ts(item)
            # None timestamps sort last; newest first within dated events.
            return (
                0 if ts is None else 1,
                ts.isoformat() if ts is not None else "",
            )

        items = sorted(
            event_list.items or [], key=_sort_key, reverse=True
        )[: max(0, int(limit))]
        events: list[dict] = []
        for item in items:
            involved = getattr(item, "involved_object", None)
            kind = getattr(involved, "kind", None)
            obj_name = getattr(involved, "name", None)
            events.append({
                "ts": _ts(_event_ts(item)),
                "type": getattr(item, "type", None),
                "reason": getattr(item, "reason", None),
                "object": (
                    f"{kind}/{obj_name}"
                    if kind or obj_name else None
                ),
                "message": getattr(item, "message", None),
            })
        return events

    # ------------------------------------------------- privileged actions

    def restart_k8s_deployment(self, namespace: str, deployment: str) -> dict:
        """PRIVILEGED: rollout-restart a deployment.

        Patches the pod template's ``kubectl.kubernetes.io/restartedAt``
        annotation to now -- the genuine ``kubectl rollout restart``
        mechanism; the changed template hash triggers a rolling update.
        Runs with the client identity's RBAC (the approval-relevant
        principal); approval-gated upstream via act() routing.

        Returns dict {namespace, deployment, state: "restarted", ts}.
        """
        self._require_connected()
        body = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {_RESTART_ANNOTATION: _now()}
                    }
                }
            }
        }
        try:
            self._apps_v1.patch_namespaced_deployment(
                name=deployment,
                namespace=namespace,
                body=body,
                _request_timeout=self._timeout,
            )
        except Exception as exc:
            raise self._wrap(
                f"restart_k8s_deployment({namespace!r}, {deployment!r})",
                exc,
            ) from exc
        return {
            "namespace": namespace,
            "deployment": deployment,
            "state": "restarted",
            "ts": _now(),
        }


register_connector(KubernetesConnector.SPEC, KubernetesConnector)

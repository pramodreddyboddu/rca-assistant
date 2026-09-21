"""Tests for the real Kubernetes connector (connectors/kubernetes.py).

The ``kubernetes`` client is NOT installed here and no cluster is touched.
A fake ``kubernetes`` module is injected into ``sys.modules`` (removed
after each test by monkeypatch), modelling the official client's public
surface: ``config.load_kube_config`` / ``config.load_incluster_config``,
``client.CoreV1Api`` (list_namespace / list_namespaced_pod /
list_namespaced_event) and ``client.AppsV1Api``
(list_namespaced_deployment / patch_namespaced_deployment). Covers pod
status shape, deployment shape, event sorting/truncation, the rollout
restart annotation patch, connect() failure wrapping, and repr hygiene.
"""

import json
import subprocess
import sys
import types
from datetime import datetime, timezone

import pytest

from connectors.base import ConnectorError
from connectors.kubernetes import KubernetesConnector, _RESTART_ANNOTATION


# ------------------------------------------------------------- fake driver


def _dt(hour):
    return datetime(2026, 9, 21, hour, 0, 0, tzinfo=timezone.utc)


def _make_fake_kubernetes(state, config_mode="kubeconfig",
                          fail_probe=False):
    """Build a fake `kubernetes` module modelling the public client API.

    ``state``: {"namespaces": [...], "pods": {ns: [SimpleNamespace...]},
    "deployments": {ns: [...]}, "events": {ns: [...]}}.
    ``config_mode``: "kubeconfig" (records the call), "incluster", or
    "config-broken" (load raises ConfigException). ``fail_probe`` makes
    list_namespace raise ApiException (unreachable / forbidden).
    """
    mod = types.ModuleType("kubernetes")

    class ConfigException(Exception):
        pass

    class ApiException(Exception):
        pass

    config = types.SimpleNamespace()
    config.ConfigException = ConfigException
    config.calls = []

    def _load_kube_config(config_file=None, context=None, **kwargs):
        if config_mode == "config-broken":
            raise ConfigException("expected kubeconfig not found")
        config.calls.append(("kubeconfig", config_file, context))

    def _load_incluster_config(**kwargs):
        if config_mode == "config-broken":
            raise ConfigException("no service account token mounted")
        config.calls.append(("incluster",))

    config.load_kube_config = _load_kube_config
    config.load_incluster_config = _load_incluster_config
    mod.config = config
    mod.ApiException = ApiException

    class _List:
        def __init__(self, items):
            self.items = items

    class FakeCoreV1Api:
        instances = []

        def __init__(self, **kwargs):
            FakeCoreV1Api.instances.append(self)

        def list_namespace(self, limit=None, **kwargs):
            if fail_probe:
                raise ApiException("(401) Unauthorized")
            return _List([types.SimpleNamespace(
                metadata=types.SimpleNamespace(name=n))
                for n in state["namespaces"]])

        def list_namespaced_pod(self, namespace, **kwargs):
            return _List(list(state["pods"].get(namespace, [])))

        def list_namespaced_event(self, namespace, **kwargs):
            return _List(list(state["events"].get(namespace, [])))

    class FakeAppsV1Api:
        instances = []
        patches = []

        def __init__(self, **kwargs):
            FakeAppsV1Api.instances.append(self)

        def list_namespaced_deployment(self, namespace, **kwargs):
            return _List(list(state["deployments"].get(namespace, [])))

        def patch_namespaced_deployment(self, name=None, namespace=None,
                                        body=None, **kwargs):
            FakeAppsV1Api.patches.append(
                {"name": name, "namespace": namespace, "body": body})
            deps = state["deployments"].get(namespace, [])
            if all(d.metadata.name != name for d in deps):
                raise ApiException(f'(404) deployments "{name}" not found')
            return types.SimpleNamespace(
                metadata=types.SimpleNamespace(name=name))

    mod.client = types.SimpleNamespace(
        CoreV1Api=FakeCoreV1Api, AppsV1Api=FakeAppsV1Api)
    return mod


def _pod(name, phase="Running", containers=(), node="node-1",
         spec_containers=None):
    """Build a fake pod object.

    ``containers``: (name, ready, restart_count) triples for container
    statuses. ``spec_containers``: names for the pod spec when statuses
    are absent (Pending pods); defaults to the statuses' names.
    """
    statuses = [
        types.SimpleNamespace(
            name=cname, ready=ready, restart_count=restarts)
        for cname, ready, restarts in containers
    ]
    spec_names = ([c[0] for c in containers] if spec_containers is None
                  else spec_containers)
    return types.SimpleNamespace(
        metadata=types.SimpleNamespace(name=name),
        status=types.SimpleNamespace(
            phase=phase, container_statuses=statuses),
        spec=types.SimpleNamespace(
            node_name=node,
            containers=[types.SimpleNamespace(name=n) for n in spec_names]),
    )


def _deployment(name, desired=3, ready=3, unavailable=0):
    return types.SimpleNamespace(
        metadata=types.SimpleNamespace(name=name),
        spec=types.SimpleNamespace(replicas=desired),
        status=types.SimpleNamespace(
            ready_replicas=ready, unavailable_replicas=unavailable),
    )


def _event(name, hour, etype="Normal", reason="Pulled",
           kind="Pod", message="msg", last=None):
    return types.SimpleNamespace(
        metadata=types.SimpleNamespace(name=name),
        type=etype,
        reason=reason,
        message=message,
        involved_object=types.SimpleNamespace(kind=kind, name=name),
        last_timestamp=_dt(hour) if last is None else last,
        event_time=None,
        first_timestamp=None,
    )


def _fresh_state():
    return {
        "namespaces": ["default", "prod"],
        "pods": {
            "prod": [
                _pod("web-7d9f", containers=[
                    ("app", True, 2), ("sidecar", False, 0)]),
                _pod("db-0", phase="Pending", containers=[],
                     spec_containers=["db"], node=None),
            ],
        },
        "deployments": {
            "prod": [
                _deployment("web", desired=3, ready=2, unavailable=1),
                _deployment("db", desired=1, ready=1, unavailable=0),
            ],
        },
        "events": {
            "prod": [
                _event("e1", 8, reason="Pulled", message="pulling image"),
                _event("e2", 10, etype="Warning", reason="BackOff",
                       message="back-off restarting"),
                _event("e3", 9, reason="Started", message="started"),
            ],
        },
    }


@pytest.fixture()
def fake_k8s(monkeypatch):
    state = _fresh_state()
    monkeypatch.setitem(
        sys.modules, "kubernetes", _make_fake_kubernetes(state))
    return state


@pytest.fixture()
def fake_k8s_incluster(monkeypatch):
    state = _fresh_state()
    monkeypatch.setitem(
        sys.modules, "kubernetes",
        _make_fake_kubernetes(state, config_mode="incluster"))
    return state


@pytest.fixture()
def fake_k8s_config_broken(monkeypatch):
    state = _fresh_state()
    monkeypatch.setitem(
        sys.modules, "kubernetes",
        _make_fake_kubernetes(state, config_mode="config-broken"))
    return state


@pytest.fixture()
def fake_k8s_probe_fails(monkeypatch):
    state = _fresh_state()
    monkeypatch.setitem(
        sys.modules, "kubernetes",
        _make_fake_kubernetes(state, fail_probe=True))
    return state


def _conn(**kwargs):
    return KubernetesConnector(kubeconfig="/tmp/fake-kubeconfig", **kwargs)


def _connected(conn):
    conn.connect()
    return conn


# ------------------------------------------------- guarded import


def test_module_imports_cleanly_without_driver():
    """The driver must never be needed at import time (subprocess proof)."""
    code = (
        "import sys; "
        "assert 'kubernetes' not in sys.modules, "
        "'kubernetes unexpectedly present'; "
        "from connectors.kubernetes import KubernetesConnector; "
        "assert 'kubernetes' not in sys.modules, "
        "'import pulled in kubernetes'; "
        "print('import-ok')"
    )
    proc = subprocess.run([sys.executable, "-c", code],
                          capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr
    assert "import-ok" in proc.stdout


def test_driver_absent_gives_install_instructions(monkeypatch):
    monkeypatch.delitem(sys.modules, "kubernetes", raising=False)
    conn = _conn()
    with pytest.raises(ConnectorError) as excinfo:
        conn.connect()
    assert "install kubernetes" in str(excinfo.value)
    assert "pip install kubernetes" in str(excinfo.value)


def test_connect_loads_kubeconfig_first(fake_k8s):
    conn = _connected(_conn())
    config = sys.modules["kubernetes"].config
    assert config.calls == [("kubeconfig", "/tmp/fake-kubeconfig", None)]


def test_connect_with_context_passes_it_through(fake_k8s):
    conn = _connected(_conn(context="prod-ctx"))
    config = sys.modules["kubernetes"].config
    assert config.calls == [("kubeconfig", "/tmp/fake-kubeconfig", "prod-ctx")]


def test_connect_without_kubeconfig_uses_incluster(fake_k8s_incluster):
    conn = _connected(KubernetesConnector())
    config = sys.modules["kubernetes"].config
    assert config.calls == [("incluster",)]


def test_connect_wraps_config_load_failure(fake_k8s_config_broken):
    conn = _conn()
    with pytest.raises(ConnectorError) as excinfo:
        conn.connect()
    assert "config" in str(excinfo.value).lower()


def test_connect_wraps_unreachable_api_server(fake_k8s_probe_fails):
    conn = _conn()
    with pytest.raises(ConnectorError) as excinfo:
        conn.connect()
    assert "cannot reach" in str(excinfo.value)


def test_connect_failure_leaves_connector_disconnected(fake_k8s_probe_fails):
    conn = _conn()
    with pytest.raises(ConnectorError):
        conn.connect()
    with pytest.raises(ConnectorError) as excinfo:
        conn.get_k8s_pod_status("prod")
    assert "not connected" in str(excinfo.value)


def test_methods_require_connect_first(fake_k8s):
    conn = _conn()
    with pytest.raises(ConnectorError):
        conn.get_k8s_pod_status("prod")
    with pytest.raises(ConnectorError):
        conn.get_k8s_deployments("prod")
    with pytest.raises(ConnectorError):
        conn.get_k8s_events("prod")
    with pytest.raises(ConnectorError):
        conn.restart_k8s_deployment("prod", "web")


def test_close_safe_when_not_connected(fake_k8s):
    conn = _conn()
    conn.close()  # must not raise


# ------------------------------------------------- pod status


def test_get_k8s_pod_status_shape(fake_k8s):
    conn = _connected(_conn())
    d = conn.get_k8s_pod_status("prod")
    assert set(d) == {"namespace", "pods", "ts"}
    assert d["namespace"] == "prod"
    pods = {p["name"]: p for p in d["pods"]}
    assert pods["web-7d9f"] == {
        "name": "web-7d9f", "phase": "Running", "restarts": 2,
        "ready": "1/2", "node": "node-1",
    }
    # Pending pod with no container statuses: total falls back to spec.
    assert pods["db-0"] == {
        "name": "db-0", "phase": "Pending", "restarts": 0,
        "ready": "0/1", "node": None,
    }
    json.dumps(d)


def test_get_k8s_pod_status_empty_namespace(fake_k8s):
    conn = _connected(_conn())
    d = conn.get_k8s_pod_status("default")
    assert d["namespace"] == "default"
    assert d["pods"] == []
    json.dumps(d)


# ------------------------------------------------- deployments


def test_get_k8s_deployments_shape(fake_k8s):
    conn = _connected(_conn())
    d = conn.get_k8s_deployments("prod")
    assert set(d) == {"namespace", "deployments", "ts"}
    deps = {x["name"]: x for x in d["deployments"]}
    assert deps["web"] == {
        "name": "web", "replicas_desired": 3,
        "replicas_ready": 2, "replicas_unavailable": 1,
    }
    assert deps["db"]["replicas_unavailable"] == 0
    json.dumps(d)


def test_get_k8s_deployments_empty_namespace(fake_k8s):
    conn = _connected(_conn())
    d = conn.get_k8s_deployments("default")
    assert d["deployments"] == []
    json.dumps(d)


# ------------------------------------------------- events


def test_get_k8s_events_newest_first_and_truncated(fake_k8s):
    conn = _connected(_conn())
    events = conn.get_k8s_events("prod", limit=2)
    assert [e["reason"] for e in events] == ["BackOff", "Started"]
    first = events[0]
    assert set(first) == {"ts", "type", "reason", "object", "message"}
    assert first["type"] == "Warning"
    assert first["object"] == "Pod/e2"
    assert first["message"] == "back-off restarting"
    assert first["ts"] == "2026-09-21T10:00:00Z"
    json.dumps(events)


def test_get_k8s_events_full_list_sorted(fake_k8s):
    conn = _connected(_conn())
    events = conn.get_k8s_events("prod")
    assert [e["ts"] for e in events] == [
        "2026-09-21T10:00:00Z",
        "2026-09-21T09:00:00Z",
        "2026-09-21T08:00:00Z",
    ]
    json.dumps(events)


def test_get_k8s_events_empty_namespace(fake_k8s):
    conn = _connected(_conn())
    assert conn.get_k8s_events("default") == []


# ------------------------------------------------- privileged restart


def test_restart_k8s_deployment_shape_and_patch_args(fake_k8s):
    conn = _connected(_conn())
    d = conn.restart_k8s_deployment("prod", "web")
    assert d == {
        "namespace": "prod", "deployment": "web",
        "state": "restarted", "ts": d["ts"],
    }
    assert set(d) == {"namespace", "deployment", "state", "ts"}
    patches = sys.modules["kubernetes"].client.AppsV1Api.patches
    assert len(patches) == 1
    patch = patches[0]
    assert patch["name"] == "web"
    assert patch["namespace"] == "prod"
    annotations = patch["body"]["spec"]["template"]["metadata"]["annotations"]
    assert _RESTART_ANNOTATION in annotations
    assert annotations[_RESTART_ANNOTATION]  # a real timestamp, not empty
    json.dumps(d)


def test_restart_k8s_deployment_unknown_deployment(fake_k8s):
    conn = _connected(_conn())
    with pytest.raises(ConnectorError) as excinfo:
        conn.restart_k8s_deployment("prod", "nope")
    assert "nope" in str(excinfo.value)


# ------------------------------------------------- repr hygiene / gating


def test_capabilities_exact_set():
    conn = _conn()
    assert conn.capabilities() == {
        "get_k8s_pod_status",
        "get_k8s_deployments",
        "get_k8s_events",
        "restart_k8s_deployment",
    }


def test_no_raw_token_constructor_argument():
    import inspect
    params = inspect.signature(KubernetesConnector.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned, (
            f"KubernetesConnector.__init__ takes a raw secret argument "
            f"{pname!r}"
        )


def test_repr_contains_no_secrets():
    conn = KubernetesConnector(
        kubeconfig="/home/user/.kube/config", context="prod-ctx")
    text = repr(conn)
    assert "password" not in text.lower()
    assert "token" not in text.lower()
    assert "secret" not in text.lower()
    assert "/home/user/.kube/config" in text
    assert "prod-ctx" in text


def test_read_rejects_privileged_action_name(fake_k8s):
    # Unconnected instance: read() raises ConnectorError on the routing
    # path when the parallel worker has registered the privileged tool
    # name, or on "not connected" when routing falls through to the
    # method. Either way it must never run the restart.
    conn = _conn()
    with pytest.raises(ConnectorError):
        conn.read("restart_k8s_deployment",
                  {"namespace": "prod", "deployment": "web"})
    assert sys.modules["kubernetes"].client.AppsV1Api.patches == []


def test_act_rejects_read_tools(fake_k8s):
    conn = _connected(_conn())
    for tool in ("get_k8s_pod_status", "get_k8s_deployments",
                 "get_k8s_events"):
        with pytest.raises(ConnectorError):
            conn.act(tool, {"namespace": "prod"})


def test_read_dispatch_reaches_tools(fake_k8s):
    conn = _connected(_conn())
    assert conn.read("get_k8s_pod_status",
                     {"namespace": "prod"})["pods"][0]["name"] == "db-0"
    assert conn.read("get_k8s_deployments",
                     {"namespace": "prod"})["deployments"][0]["name"] == "db"
    assert conn.read("get_k8s_events",
                     {"namespace": "prod", "limit": 1})[0]["reason"] == "BackOff"


def test_all_results_json_serializable(fake_k8s):
    conn = _connected(_conn())
    results = [
        conn.get_k8s_pod_status("prod"),
        conn.get_k8s_deployments("prod"),
        conn.get_k8s_events("prod"),
        conn.restart_k8s_deployment("prod", "web"),
    ]
    for r in results:
        json.dumps(r)

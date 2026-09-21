"""stdio MCP server exposing the RCA assistant tools.

Demo simplification: each registered tool reads the bearer token from
the environment variable named by `token_env` (default "RCA_TOKEN") at
call time, then passes it to the Gateway, which performs the real
token verification, scope authorization, and audit logging. There is no
per-request auth context from the MCP transport in this demo, so the env
var stands in for "the caller's credential". Production must replace
this with real transport-level authentication (e.g. OAuth bearer tokens
validated per request) and never rely on a shared process env var.

The sim (Estate) and audit (AuditLog) packages are built in parallel;
they are imported inside main() so importing this module never breaks
when they are absent.
"""

from __future__ import annotations

import os
from typing import Any

from mcp.server.mcpserver import MCPServer

from mcp_server.gateway import Gateway


def create_mcp_server(gateway: Gateway, token_env: str = "RCA_TOKEN") -> MCPServer:
    """Build the "rca-diagnostics" MCPServer with the 17 tools registered."""
    mcp = MCPServer("rca-diagnostics")

    def _token() -> str:
        return os.environ.get(token_env, "")

    @mcp.tool()
    def get_queue_depth(qmgr: str, queue: str) -> dict:
        """Current depth and status of an IBM MQ queue."""
        return gateway.call("get_queue_depth", {"qmgr": qmgr, "queue": queue}, _token())

    @mcp.tool()
    def get_channel_status(qmgr: str, channel: str) -> dict:
        """Current status of an IBM MQ channel."""
        return gateway.call("get_channel_status", {"qmgr": qmgr, "channel": channel}, _token())

    @mcp.tool()
    def read_error_log(qmgr: str, limit: int = 50) -> list:
        """Recent entries from a queue manager error log."""
        return gateway.call("read_error_log", {"qmgr": qmgr, "limit": limit}, _token())

    @mcp.tool()
    def get_kafka_consumer_lag(topic: str, group: str) -> dict:
        """Consumer lag for a Kafka topic/group."""
        return gateway.call("get_kafka_consumer_lag", {"topic": topic, "group": group}, _token())

    @mcp.tool()
    def get_host_metrics(host: str) -> dict:
        """CPU, memory, and disk metrics for a host."""
        return gateway.call("get_host_metrics", {"host": host}, _token())

    @mcp.tool()
    def get_config(qmgr: str, object_type: str, name: str) -> dict:
        """Configuration of an MQ object (queue, channel, ...)."""
        return gateway.call(
            "get_config", {"qmgr": qmgr, "object_type": object_type, "name": name}, _token()
        )

    @mcp.tool()
    def restart_channel(qmgr: str, channel: str) -> dict:
        """PRIVILEGED: restart an MQ channel (mutates state). Requires admin:write scope."""
        return gateway.call("restart_channel", {"qmgr": qmgr, "channel": channel}, _token())

    @mcp.tool()
    def read_app_log(app: str, limit: int = 50) -> list:
        """Recent entries from an application log."""
        return gateway.call("read_app_log", {"app": app, "limit": limit}, _token())

    @mcp.tool()
    def get_listener_status(name: str) -> dict:
        """Current status of an MQ listener."""
        return gateway.call("get_listener_status", {"name": name}, _token())

    @mcp.tool()
    def get_cert_status(qmgr: str, channel: str) -> dict:
        """TLS certificate status for an MQ channel."""
        return gateway.call("get_cert_status", {"qmgr": qmgr, "channel": channel}, _token())

    @mcp.tool()
    def archive_logs(host: str) -> dict:
        """PRIVILEGED: archive logs on a host, freeing disk (mutates state). Requires admin:write scope."""
        return gateway.call("archive_logs", {"host": host}, _token())

    @mcp.tool()
    def restart_app(app: str) -> dict:
        """PRIVILEGED: restart an application (mutates state). Requires admin:write scope."""
        return gateway.call("restart_app", {"app": app}, _token())

    @mcp.tool()
    def restart_consumer(topic: str, group: str) -> dict:
        """PRIVILEGED: restart a Kafka consumer group (mutates state). Requires admin:write scope."""
        return gateway.call("restart_consumer", {"topic": topic, "group": group}, _token())

    @mcp.tool()
    def renew_certificate(qmgr: str, channel: str) -> dict:
        """PRIVILEGED: renew the TLS certificate for an MQ channel (mutates state). Requires admin:write scope."""
        return gateway.call("renew_certificate", {"qmgr": qmgr, "channel": channel}, _token())

    @mcp.tool()
    def update_queue_config(qmgr: str, queue: str, max_depth: int) -> dict:
        """PRIVILEGED: change an MQ queue's MAXDEPTH (mutates state). Requires admin:write scope."""
        return gateway.call(
            "update_queue_config", {"qmgr": qmgr, "queue": queue, "max_depth": max_depth}, _token()
        )

    @mcp.tool()
    def start_listener(name: str) -> dict:
        """PRIVILEGED: start an MQ listener (mutates state). Requires admin:write scope."""
        return gateway.call("start_listener", {"name": name}, _token())

    @mcp.tool()
    def quarantine_message(qmgr: str, queue: str) -> dict:
        """PRIVILEGED: quarantine a poison message to SYSTEM.DLQ (mutates state). Requires admin:write scope."""
        return gateway.call("quarantine_message", {"qmgr": qmgr, "queue": queue}, _token())

    return mcp


def main() -> None:
    """Build Estate + AuditLog + Gateway from env/config and serve stdio."""
    from sim.estate import Estate
    from audit.log import AuditLog

    audit_path = os.environ.get("RCA_AUDIT_PATH", "runs/rca-audit.jsonl")
    gateway = Gateway(Estate(), AuditLog(audit_path))
    create_mcp_server(gateway).run()


if __name__ == "__main__":
    main()

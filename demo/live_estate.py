"""Replay-mode estate adapter over the REAL IBM MQ connector.

The incident room diagnoses one real incident (BATCH.CHL stopped,
BACKLOG.Q backlogged on QM1) through the real pipeline: the genuine
``IBMQConnector`` read paths in ``connectors/ibmmq.py`` serve the fixtures
recorded live against QM1 (IBM MQ 10.0.0.5, 2026-09-21) via the recorder in
replay mode. No driver, no network, no queue manager is touched: in replay
mode the recorder never executes the live thunk, so ``connect()`` is never
called and no credentials are needed.

Estate-compatible signatures: ``get_queue_depth(qmgr, queue)``,
``get_channel_status(qmgr, channel)``, ``read_error_log(qmgr, limit=50)``,
``get_config(qmgr, object_type, name)`` -- the same positional shapes the
MCP tool handlers in ``mcp_server/tools.py`` call.

MODE
----
Constructing this adapter pins ``RCA_RECORD_MODE=replay`` for the process.
The incident room is a replay demo by contract ("replay-demo"): production
use goes through the real connector, whose own default stays
``passthrough`` (see ``connectors/ibmmq.py``) so live behavior is unchanged
outside this demo.

Only READS are adapted. Privileged actions (restart_channel, ...) are
never wired here; the incident room rehearses approvals without executing
them.
"""

from __future__ import annotations

import os

from connectors.ibmmq import IBMQConnector


class RecordedMQEstate:
    """Estate-compatible read facade over IBMQConnector in replay mode."""

    def __init__(
        self,
        qmgr: str = "QM1",
        *,
        error_log_dir: str | None = None,
    ) -> None:
        # The incident room is replay-only by design: evidence must come
        # from the committed fixtures, never from a live queue manager.
        # The recorder reads this env var dynamically per call, so setting
        # it here -- when the room is actually constructed -- is enough.
        # The connector's own default_mode stays "passthrough" for
        # production; this process is the replay demo, nothing else.
        # NOTE: process-wide on purpose. Do not import this module in a
        # shared process (e.g. the sim demo server) expecting live MQ.
        os.environ["RCA_RECORD_MODE"] = "replay"
        # The SVRCONN channel/host below are only used to build the
        # connector object; connect() is never called on the replay path,
        # so no session -- and no credential -- is ever opened.
        self._qmgr = qmgr
        self._conn = IBMQConnector(
            qmgr=qmgr,
            channel="APP.SVRCONN",
            host="127.0.0.1",
            port=1414,
            error_log_dir=(
                error_log_dir
                or os.environ.get("MQ_ERROR_LOG_DIR")
                # Conventional MQ error-log location for the queue manager.
                # Never opened on the replay path (the recorder serves the
                # fixture); required only to pass the connector's config
                # check before the recorder runs.
                or "/var/mqm/qmgrs/QM1/errors"
            ),
        )

    def get_queue_depth(self, qmgr: str, queue: str) -> dict:
        return self._conn.get_queue_depth(qmgr, queue)

    def get_channel_status(self, qmgr: str, channel: str) -> dict:
        return self._conn.get_channel_status(qmgr, channel)

    def read_error_log(self, qmgr: str, limit: int = 50) -> list:
        return self._conn.read_error_log(qmgr, limit=limit)

    def get_config(self, qmgr: str, object_type: str, name: str) -> dict:
        return self._conn.get_config(qmgr, object_type, name)

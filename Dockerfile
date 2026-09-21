# RCA assistant — customer-hosted reference image.
#
# Build:  docker build -t rca-assistant .
# Run:    docker compose up -d        (see docker-compose.yml)
#
# SECURITY NOTE on --bind 0.0.0.0:
# Binding 0.0.0.0 here is correct INSIDE the container. The container's own
# network namespace is the trust boundary; the process must listen on all
# container interfaces so Docker can forward the published port. The
# published port is controlled by docker-compose.yml ("${RCA_PORT:-8765}:8765").
# Bare-host runs of `python -m demo.web` keep the loopback default
# (127.0.0.1) so the demo is not exposed to the LAN unintentionally.
#
# HEALTHCHECK note: /healthz is served by demo/web.py (observability
# workstream). The container reports "unhealthy" until that route is present.

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Create a non-root runtime user first; nothing in this image runs as root.
RUN groupadd --system appuser \
 && useradd --system --gid appuser --create-home --shell /usr/sbin/nologin appuser

WORKDIR /app

# Dependencies first for layer caching. The app itself is stdlib-only; this
# layer covers the optional MCP server extras (requirements.txt).
COPY --chown=appuser:appuser requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The rest of the repo (.dockerignore keeps build junk and local run
# history out of the image).
COPY --chown=appuser:appuser . .

USER appuser

EXPOSE 8765

# Liveness probe against the /healthz route (demo/web.py).
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD-SHELL python -c "import os,urllib.request;urllib.request.urlopen(f'http://127.0.0.1:{os.environ.get(\"RCA_PORT\",\"8765\")}/healthz',timeout=4)" || exit 1

# Bind 0.0.0.0 only inside the container (see note at top of this file).
# --port reads RCA_PORT at startup, so compose can remap the host port.
CMD ["sh", "-c", "python -m demo.web --bind 0.0.0.0 --port ${RCA_PORT:-8765}"]

# Installing the RCA Assistant

**Start with `docs/GETTING_STARTED.md`.** It is the primary install path:
a pip install and a five-minute walk from install to first diagnosis. This
page covers the other way to run it (Docker) and the config knobs.

## Docker (customer-hosted)

Nothing here phones home: all reasoning and data stay on your host.

### Prerequisites

- Docker Engine 24+ with the Compose v2 plugin
  (`docker compose version` should work).
- 1 vCPU / 1 GB RAM is plenty for the demo.
- Outbound HTTPS access from the host if you want the Jev reasoning layer
  (it calls the TypeSafe API). The app works fully offline without it.

### Run it

```bash
git clone <your-rca-assistant-repo-url> rca-assistant && cd rca-assistant
cp .env.example .env
docker compose up -d --build
```

Open **http://localhost:8765** (or `http://<host>:$RCA_PORT` if you changed
the port). To stop: `docker compose down`.

Note: the Docker image build and compose flow are **unverified** here,
because no Docker host is available in this build environment. If you hit
build errors on your machine, the pip install path in
`docs/GETTING_STARTED.md` is the supported one.

Run history and the audit trail persist in the `rca-assistant-runs` Docker
volume, so `docker compose down` / upgrades never lose them.

## Jev setup (recommended)

Jev (TypeSafe System One) is the default reasoning layer when a key is
configured. It powers typed hypothesis ranking, evidence-support
verification, incident triage, and remediation risk scoring, all shown
directly in the UI with confidence scores. Without a key the app runs in
**deterministic mode** (the same correlation engine, no model calls), and
the UI says so.

1. Get a key from your **TypeSafe dashboard / console** (API keys section).
2. Put it in `.env`:
   ```
   TYPESAFE_API_KEY=ts-your-key-here
   ```
   Keep `RCA_JEV_ENABLED=1` (the default). Never commit a filled `.env`.
3. Restart: `docker compose up -d` (compose picks up the changed `.env`).

No key is ever written to disk by the app beyond the `.env` you created;
it is passed as an environment variable only.

## Configuration reference

All settings are environment variables (via `.env` / `env_file`).

| Variable           | Default     | Meaning |
|--------------------|-------------|---------|
| `RCA_PORT`         | `8765`      | Host port published by compose. The container always listens on 8765 internally. |
| `RCA_BIND`         | `127.0.0.1` | Bind address for **bare-host** runs. Keep loopback on a host; the container binds 0.0.0.0 by design (its network namespace is the trust boundary). |
| `RCA_JEV_ENABLED`  | `1`         | `1` enables the Jev reasoning layer (requires a valid `TYPESAFE_API_KEY`); `0` forces deterministic mode. |
| `TYPESAFE_API_KEY` | *(empty)*   | TypeSafe API key. Empty or invalid: deterministic fallback with a visible UI banner; never a silent failure. |

## Putting it behind a TLS reverse proxy

The app itself serves plain HTTP; terminate TLS in front of it. In short:

- Point your reverse proxy (nginx, Caddy, Traefik) at
  `http://127.0.0.1:8765` on the Docker host and issue a certificate for
  your hostname (Let's Encrypt via the proxy is the usual path).
- Keep the compose `ports` mapping bound to loopback if the proxy runs on
  the same host: `"127.0.0.1:${RCA_PORT:-8765}:8765"`.
- Forward the usual `X-Forwarded-For` / `X-Forwarded-Proto` headers.
- Restrict who can reach the UI at the proxy or firewall level: the demo
  has no login. Do not expose it to the open internet without access
  control.

## Upgrading

```bash
cd rca-assistant
git pull
docker compose up -d --build
```

Your runs and audit history live in the named volume and survive the
rebuild.

## Troubleshooting

**Port already in use**: `docker compose up` fails with a bind error.
Either stop whatever owns the port, or pick another: set `RCA_PORT=8770`
in `.env` and `docker compose up -d` again, then open
`http://localhost:8770`.

**Key invalid (deterministic fallback banner)**: the UI shows an amber
"Deterministic mode" banner. Check `docker compose logs rca` for the Jev
client error (bad key, expired key, or no network route to the TypeSafe
API). Fix `.env` and restart; or set `RCA_JEV_ENABLED=0` to run
deterministic mode intentionally and silence the warning.

**Logs**: `docker compose logs -f rca` streams the app log. Each incident
run also writes its own record under `/app/runs` (the `rca-assistant-runs`
volume); inspect with `docker compose exec rca ls /app/runs`.

**Container unhealthy**: `docker compose ps` shows `unhealthy`. The
healthcheck probes `http://127.0.0.1:8765/healthz` inside the container.
Confirm the app started (`docker compose logs rca`) and that nothing else
in the container is bound to 8765.

**Starting over**: `docker compose down -v` removes the container *and*
the runs volume (you lose run history). Without `-v`, history is kept.

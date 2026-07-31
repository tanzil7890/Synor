"""Read-only local dashboard for run evidence and artifact ownership."""

from __future__ import annotations

import asyncio as _asyncio
import dataclasses as _dataclasses
import http.server as _http_server
import ipaddress as _ipaddress
import json as _json
import os as _os
import pathlib as _pathlib
import socketserver as _socketserver
import typing as _typing
import urllib.parse as _urlparse

from . import audit as _audit
from . import quarantine as _quarantine
from . import state as _state

__all__ = [
    "DashboardAddress",
    "DashboardServer",
    "build_dashboard_snapshot",
]

_INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Synor local runs</title>
  <style>
    :root { color-scheme: dark; --ink:#f2f4ee; --muted:#9ca69e;
      --panel:#151b18; --line:#2a332e; --mint:#79e2ad; --amber:#ffcf70;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    * { box-sizing:border-box; }
    body { margin:0; background:#0c110f; color:var(--ink); }
    header { padding:34px clamp(20px,5vw,72px); border-bottom:1px solid var(--line);
      background:radial-gradient(circle at 85% 20%,#183c2b 0,transparent 34%); }
    h1 { margin:0 0 8px; font-size:clamp(28px,5vw,52px); letter-spacing:-.05em; }
    header p { color:var(--muted); margin:0; }
    main { padding:28px clamp(20px,5vw,72px) 60px; display:grid;
      grid-template-columns:minmax(0,1.7fr) minmax(280px,1fr); gap:22px; }
    section { background:var(--panel); border:1px solid var(--line); border-radius:14px;
      overflow:hidden; min-height:180px; }
    h2 { font-size:13px; text-transform:uppercase; letter-spacing:.12em;
      color:var(--muted); margin:0; padding:15px 18px; border-bottom:1px solid var(--line); }
    .row { padding:14px 18px; border-bottom:1px solid var(--line); cursor:pointer; }
    .row:hover { background:#1c2521; }
    .row:last-child { border-bottom:0; }
    .meta { color:var(--muted); font-size:12px; margin-top:5px; }
    .ok { color:var(--mint); } .warn { color:var(--amber); }
    pre { margin:0; padding:18px; white-space:pre-wrap; overflow-wrap:anywhere;
      color:#cbd4ce; font:12px/1.6 inherit; }
    .empty { padding:18px; color:var(--muted); }
    @media(max-width:800px){ main{grid-template-columns:1fr;} }
  </style>
</head>
<body>
<header><h1>Synor / local runs</h1><p>Read-only evidence. No payloads, no remote calls.</p></header>
<main>
  <section><h2>Recent runs</h2><div id="runs"><div class="empty">Loading…</div></div></section>
  <section><h2>Selected evidence</h2><pre id="detail">Select a run.</pre></section>
  <section><h2>Quarantine</h2><div id="quarantine"><div class="empty">Loading…</div></div></section>
  <section><h2>Trust boundary</h2><pre>Metadata-only view
Loopback by default
Manual review never executes code
Artifact paths come from local engine ownership state</pre></section>
</main>
<script>
const runs=document.querySelector("#runs"), detail=document.querySelector("#detail");
const quarantine=document.querySelector("#quarantine");
function row(title, meta, status, click){
  const el=document.createElement("div"); el.className="row"; if(click) el.onclick=click;
  const name=document.createElement("div"); name.textContent=title;
  const info=document.createElement("div"); info.className="meta";
  info.textContent=meta; if(status) info.classList.add(status==="succeeded"?"ok":"warn");
  el.append(name,info); return el;
}
async function load(){
  const data=await (await fetch("/api/snapshot")).json();
  runs.textContent="";
  if(!data.runs.length) runs.append(row("No runs yet","Run synor plan or update."));
  for(const run of data.runs){
    runs.append(row(run.app_name+" · "+run.command,
      run.started_at+" · "+run.status,run.status,async()=>{
        const value=await (await fetch("/api/runs/"+encodeURIComponent(run.run_id))).json();
        detail.textContent=JSON.stringify(value,null,2);
      }));
  }
  quarantine.textContent="";
  if(!data.quarantine.length) quarantine.append(row("No open cases","Failures can be reviewed here."));
  for(const item of data.quarantine){
    quarantine.append(row(item.app_name+" · "+item.reason,
      item.created_at+" · "+item.status,item.status));
  }
}
load().catch(error=>{ detail.textContent="Dashboard error: "+error.name; });
</script>
</body></html>
"""


@_dataclasses.dataclass(frozen=True, slots=True)
class DashboardAddress:
    """Bound local dashboard address."""

    host: str
    port: int

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/"


async def build_dashboard_snapshot(
    *,
    audit_root: _os.PathLike[str] | str | None = None,
    store: _state.StateStore | None = None,
) -> dict[str, _typing.Any]:
    """Build redacted dashboard data from local evidence."""

    root = _audit.resolve_audit_root(audit_root)
    runs: list[dict[str, _typing.Any]] = []
    if root.is_dir():
        for run_dir in sorted(
            (path for path in root.iterdir() if path.is_dir()),
            reverse=True,
        ):
            try:
                runs.append(_audit.read_run_manifest(run_dir))
            except (OSError, ValueError, _json.JSONDecodeError):
                continue
    repository = _quarantine.QuarantineRepository(
        store if store is not None else _state.state_store_from_env()
    )
    cases = await repository.list()
    return {
        "runs": runs[:100],
        "quarantine": [item.to_dict() for item in cases[:100]],
    }


def _run_detail(root: _pathlib.Path, run_id: str) -> dict[str, _typing.Any]:
    if not run_id or any(
        char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-"
        for char in run_id
    ):
        raise ValueError("invalid run id")
    resolved_root = root.resolve()
    run_dir = (root / run_id).resolve()
    if run_id in {".", ".."} or run_dir.parent != resolved_root:
        raise ValueError("run id escapes the audit root")
    manifest = _audit.read_run_manifest(run_dir)
    provenance: list[_typing.Any] = []
    provenance_path = run_dir / "provenance.jsonl"
    if provenance_path.is_file():
        for line in provenance_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                provenance.append(_json.loads(line))
    replay: _typing.Any = None
    replay_path = run_dir / "replay.json"
    if replay_path.is_file():
        replay = _json.loads(replay_path.read_text(encoding="utf-8"))
    return {
        "manifest": manifest,
        "artifacts": provenance,
        "replay": replay,
    }


class _ThreadingHTTPServer(_socketserver.ThreadingMixIn, _http_server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def server_bind(self) -> None:
        _socketserver.TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name = str(host)
        self.server_port = int(port)


class DashboardServer:
    """Read-only dashboard server. Call :meth:`shutdown` from another thread."""

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 8765,
        audit_root: _os.PathLike[str] | str | None = None,
        store: _state.StateStore | None = None,
        allow_remote: bool = False,
    ) -> None:
        if not allow_remote:
            try:
                is_loopback = _ipaddress.ip_address(host).is_loopback
            except ValueError:
                is_loopback = host.lower() == "localhost"
            if not is_loopback:
                raise ValueError(
                    "dashboard must bind to loopback unless allow_remote=True"
                )
        root = _audit.resolve_audit_root(audit_root)
        active_store = store if store is not None else _state.state_store_from_env()

        class Handler(_http_server.BaseHTTPRequestHandler):
            def _headers(self, content_type: str, length: int) -> None:
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(length))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("X-Frame-Options", "DENY")
                self.send_header(
                    "Content-Security-Policy",
                    "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'",
                )
                self.end_headers()

            def _json(self, value: _typing.Any) -> None:
                payload = (
                    _json.dumps(_audit.redact_metadata(value), sort_keys=True) + "\n"
                ).encode()
                self._headers("application/json; charset=utf-8", len(payload))
                self.wfile.write(payload)

            def do_GET(self) -> None:
                path = _urlparse.urlsplit(self.path).path
                try:
                    if path == "/":
                        payload = _INDEX_HTML.encode()
                        self._headers("text/html; charset=utf-8", len(payload))
                        self.wfile.write(payload)
                    elif path == "/health":
                        self._json({"ok": True})
                    elif path == "/api/snapshot":
                        self._json(
                            _asyncio.run(
                                build_dashboard_snapshot(
                                    audit_root=root,
                                    store=active_store,
                                )
                            )
                        )
                    elif path.startswith("/api/runs/"):
                        run_id = _urlparse.unquote(path.removeprefix("/api/runs/"))
                        self._json(_run_detail(root, run_id))
                    else:
                        self.send_error(404)
                except (OSError, ValueError, KeyError, _json.JSONDecodeError):
                    self.send_error(404)

            def log_message(self, format: str, *args: _typing.Any) -> None:
                return

        self._server = _ThreadingHTTPServer((host, port), Handler)

    @property
    def address(self) -> DashboardAddress:
        """Return the actual bound address, including an ephemeral port."""

        host, port = self._server.server_address[:2]
        return DashboardAddress(host=str(host), port=int(port))

    def serve_forever(self) -> None:
        """Serve until interrupted or :meth:`shutdown` is called."""

        self._server.serve_forever()

    def shutdown(self) -> None:
        """Stop serving and close the listener."""

        self._server.shutdown()
        self._server.server_close()

"""Prometheus text endpoint for the generator. Same shape as the Spark one."""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Tuple

Label = Tuple[Tuple[str, str], ...]


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Dict[str, Dict[Label, float]] = {}
        self._help: Dict[str, str] = {}

    def help(self, name: str, text: str) -> None:
        self._help[name] = text

    def inc(self, name: str, amount: float = 1.0, **labels: str) -> None:
        with self._lock:
            bucket = self._counters.setdefault(name, {})
            key = tuple(sorted((str(k), str(v)) for k, v in labels.items()))
            bucket[key] = bucket.get(key, 0.0) + float(amount)

    def render(self) -> str:
        lines = []
        with self._lock:
            for name, samples in self._counters.items():
                if name in self._help:
                    lines.append("# HELP %s %s" % (name, self._help[name]))
                lines.append("# TYPE %s counter" % name)
                for labels, value in sorted(samples.items()):
                    body = ""
                    if labels:
                        body = "{%s}" % ",".join('%s="%s"' % (k, v.replace('"', '\\"')) for k, v in labels)
                    lines.append("%s%s %s" % (name, body, int(value) if value == int(value) else value))
        lines.append("")
        return "\n".join(lines)


METRICS = Metrics()


def start_metrics_server(port: int) -> None:
    metrics = METRICS

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path.split("?", 1)[0] != "/metrics":
                self.send_response(404)
                self.end_headers()
                return
            body = metrics.render().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            return

    def _serve() -> None:
        ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()

    threading.Thread(target=_serve, name="metrics", daemon=True).start()

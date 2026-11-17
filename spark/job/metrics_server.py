"""Tiny Prometheus text endpoint. No client library, so the Spark 3.8 image stays small."""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Tuple

Label = Tuple[Tuple[str, str], ...]


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._gauges: Dict[str, Dict[Label, float]] = {}
        self._counters: Dict[str, Dict[Label, float]] = {}
        self._help: Dict[str, str] = {}

    def help(self, name: str, text: str) -> None:
        self._help[name] = text

    def set_gauge(self, name: str, value: float, **labels: str) -> None:
        with self._lock:
            self._gauges.setdefault(name, {})[_key(labels)] = float(value)

    def inc(self, name: str, amount: float = 1.0, **labels: str) -> None:
        with self._lock:
            bucket = self._counters.setdefault(name, {})
            key = _key(labels)
            bucket[key] = bucket.get(key, 0.0) + float(amount)

    def render(self) -> str:
        lines = []
        with self._lock:
            families = [(name, "gauge", samples) for name, samples in self._gauges.items()]
            families += [(name, "counter", samples) for name, samples in self._counters.items()]
            for name, kind, samples in families:
                if name in self._help:
                    lines.append("# HELP %s %s" % (name, self._help[name]))
                lines.append("# TYPE %s %s" % (name, kind))
                for labels, value in sorted(samples.items()):
                    lines.append("%s%s %s" % (name, _fmt(labels), _num(value)))
        lines.append("")
        return "\n".join(lines)


METRICS = Metrics()


def start_metrics_server(port: int) -> None:
    handler = _handler(METRICS)

    def _serve() -> None:
        server = ThreadingHTTPServer(("0.0.0.0", port), handler)
        server.serve_forever()

    thread = threading.Thread(target=_serve, name="metrics", daemon=True)
    thread.start()


def _key(labels: Dict[str, str]) -> Label:
    return tuple(sorted((str(k), str(v)) for k, v in labels.items()))


def _fmt(labels: Label) -> str:
    if not labels:
        return ""
    body = ",".join('%s="%s"' % (key, _esc(value)) for key, value in labels)
    return "{%s}" % body


def _esc(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _num(value: float) -> str:
    if value == int(value):
        return str(int(value))
    return repr(float(value))


def _handler(metrics: Metrics):
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

        def log_message(self, fmt: str, *args) -> None:
            return

    return Handler

"""Read a Spark checkpoint offset log and compare it with Kafka end offsets.

Spark writes the resume offset (the end of the last committed batch) into
``offsets/<batchId>``. ``commits/<batchId>`` means that batch finished.
The file for batch ``max(commit) + 1`` is the offset the driver will read
next, which is the committed end offset. If that file is not there yet, the
latest committed batch's own offset file is a lower bound on progress.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Tuple

OffsetKey = Tuple[str, int]


def parse_offset_log(text: str) -> Dict[OffsetKey, int]:
    found: Dict[OffsetKey, int] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        for topic, partitions in payload.items():
            if not isinstance(partitions, dict):
                continue
            numeric = True
            parsed = {}
            for part, offset in partitions.items():
                try:
                    parsed[int(part)] = int(offset)
                except (TypeError, ValueError):
                    numeric = False
                    break
            if numeric and parsed:
                for part, offset in parsed.items():
                    found[(str(topic), part)] = offset
    return found


def latest_committed_offsets(checkpoint: Path) -> Dict[OffsetKey, int]:
    commits = _ids(checkpoint / "commits")
    if not commits:
        return {}
    newest = max(commits)
    preferred = checkpoint / "offsets" / str(newest + 1)
    fallback = checkpoint / "offsets" / str(newest)
    for path in (preferred, fallback):
        if path.is_file():
            return parse_offset_log(path.read_text(encoding="utf-8", errors="replace"))
    return {}


def lag_records(end_offset: int, committed: int) -> int:
    return max(0, int(end_offset) - int(committed))


def _ids(directory: Path):
    if not directory.is_dir():
        return []
    found = []
    for path in directory.iterdir():
        if path.is_file() and path.name.isdigit():
            found.append(int(path.name))
    return found

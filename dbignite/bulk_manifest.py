"""FHIR Bulk Data Access IG — export manifest helpers (local files).

The completed export body is a JSON object with ``transactionTime``, ``output`` (resource
files), and optional ``error``. NDJSON bodies are parsed with the same pipeline as
:attr:`dbignite.readers.FhirFormat.NDJSON`.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Optional


def is_fhir_bulk_export_manifest(obj: dict[str, Any]) -> bool:
    """Return True if ``obj`` looks like a Bulk Data completed export manifest."""
    if not isinstance(obj, dict):
        return False
    out = obj.get("output")
    if not isinstance(out, list) or len(out) == 0:
        return False
    first = out[0]
    if not isinstance(first, dict):
        return False
    return "url" in first


def compute_bulk_export_correlation_id(manifest: dict[str, Any]) -> str:
    """Stable id for one export (same manifest → same id)."""
    canonical = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def resolve_output_local_path(manifest_dir: Path, url: str) -> Optional[Path]:
    """Map manifest ``output[].url`` to a readable local path; skip remote URLs."""
    if not url or not isinstance(url, str):
        return None
    u = url.strip()
    if u.startswith("http://") or u.startswith("https://"):
        return None
    if u.startswith("file://"):
        u = u[7:]
        if u.startswith("/") and os.name == "nt" and len(u) > 2 and u[2] == ":":
            return Path(u.lstrip("/"))
        return Path(u)
    p = Path(u)
    if p.is_absolute():
        return p if p.exists() else None
    joined = (manifest_dir / p).resolve()
    return joined if joined.exists() else None


def parse_bulk_manifest_file(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def discover_bulk_manifest(directory: str | Path) -> Optional[Path]:
    """Find the first ``*.json`` in ``directory`` that parses as a Bulk export manifest."""
    d = Path(directory)
    if not d.is_dir():
        return None
    for p in sorted(d.glob("*.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            if is_fhir_bulk_export_manifest(data):
                return p
        except (json.JSONDecodeError, OSError):
            continue
    return None


def ndjson_paths_from_manifest(manifest_path: str | Path) -> tuple[str, list[str], dict[str, Any]]:
    """Return ``(correlation_id, list of absolute ndjson paths, manifest dict)``."""
    mp = Path(manifest_path)
    manifest = parse_bulk_manifest_file(mp)
    if not is_fhir_bulk_export_manifest(manifest):
        raise ValueError(f"Not a FHIR Bulk Data export manifest: {manifest_path}")
    manifest_dir = mp.parent
    cid = compute_bulk_export_correlation_id(manifest)
    paths: list[str] = []
    skipped_remote = 0
    for item in manifest.get("output", []):
        if not isinstance(item, dict):
            continue
        url = item.get("url")
        local = resolve_output_local_path(manifest_dir, url) if url else None
        if local is None:
            if isinstance(url, str) and (url.startswith("http://") or url.startswith("https://")):
                skipped_remote += 1
            continue
        paths.append(str(local))
    if not paths:
        raise ValueError(
            f"No local NDJSON paths resolved from manifest {manifest_path} "
            f"(remote entries skipped: {skipped_remote})."
        )
    return cid, paths, manifest

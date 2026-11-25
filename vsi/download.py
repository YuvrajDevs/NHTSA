"""Download NHTSA bulk files into data/landing/<load_date>/ with a manifest.

- Conditional: asks the server for the file's ETag / Last-Modified first and skips files that have not changed since
  the last successful download (NHTSA republishes daily, but a file often hasn't changed).
- Auditable: every file gets a SHA-256 and a manifest entry, so you can always prove which bytes a table came from.
- Safe: downloads go to a temp file and are renamed only when complete, so a crash never leaves half a zip behind.
- Retries with exponential backoff on network errors and 5xx responses.
"""
import hashlib
import json
import shutil
import time
from datetime import date, datetime, timezone
from pathlib import Path

import requests

from .config import LANDING
from .sources import ALL, BASE

STATE = LANDING / "_download_state.json"   # last ETag / Last-Modified per remote file


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _request(method: str, url: str, retries: int = 4, **kw):
    for attempt in range(retries + 1):
        try:
            r = requests.request(method, url, timeout=kw.pop("timeout", 60), **kw)
            if r.status_code < 500:
                return r
            err = f"HTTP {r.status_code}"
        except requests.RequestException as e:   # timeouts, resets
            err = str(e)
        if attempt == retries:
            raise RuntimeError(f"{method} {url} failed after {retries + 1} attempts: {err}")
        wait = 2 ** attempt
        print(f"  {err}; retrying in {wait}s")
        time.sleep(wait)


def download(load_date: str | None = None, sources=None, force: bool = False) -> Path:
    """Fetch every file of the given sources into data/landing/<load_date>/. Returns that folder."""
    load_date = load_date or date.today().isoformat()
    out = LANDING / load_date
    out.mkdir(parents=True, exist_ok=True)
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    manifest = []
    for src in (sources or ALL.values()):
        for rel in (*src.files, src.doc):
            url = f"{BASE}/{rel}"
            dest = out / Path(rel).name
            head = _request("HEAD", url)
            if head.status_code == 404:
                raise RuntimeError(f"{url} returned 404: NHTSA may have renamed the file (see build-log F2)")
            version = head.headers.get("ETag") or head.headers.get("Last-Modified") or ""
            prev = state.get(url, {})
            if not force and version and prev.get("version") == version and Path(prev.get("path", "")).exists():
                if not dest.exists():          # unchanged since an earlier load: reuse those bytes, no re-download
                    shutil.copy2(prev["path"], dest)
                print(f"  unchanged  {rel}  ({version})")
                status = "unchanged"
            else:
                tmp = dest.with_suffix(dest.suffix + ".part")
                with _request("GET", url, stream=True, timeout=600) as r:
                    r.raise_for_status()
                    with tmp.open("wb") as f:
                        for chunk in r.iter_content(1 << 20):
                            f.write(chunk)
                tmp.rename(dest)
                print(f"  downloaded {rel}  ({dest.stat().st_size / 1e6:.1f} MB)")
                status = "downloaded"
            sha = _sha256(dest)
            state[url] = {"version": version, "path": str(dest), "sha256": sha}
            manifest.append({"source": src.name, "url": url, "file": dest.name, "bytes": dest.stat().st_size,
                             "sha256": sha, "remote_version": version, "status": status})
    (out / "_manifest.json").write_text(json.dumps(
        {"load_date": load_date, "fetched_at": datetime.now(timezone.utc).isoformat(), "files": manifest}, indent=2))
    STATE.write_text(json.dumps(state, indent=2))
    return out

#!/usr/bin/env python3
#
# ia_upload: resilient bulk uploader for archive.org items.
# Copyright (C) 2026  Whyte
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
ia_upload.py: resilient bulk uploader for archive.org items.

Built for set-and-forget uploads of large collections over unreliable
connections. Safe to kill and re-run at any point: it picks up where it left
off and never re-sends a file that already landed intact.

Requires: pip install internetarchive
Credentials: run `ia configure` once, or set
IA_ACCESS_KEY_ID / IA_SECRET_ACCESS_KEY.

  ./ia_upload.py my-item ./files/ --dry-run           # always check first
  ./ia_upload.py my-item ./files/ --derive --log up.log

Exit codes: 0 ok, 1 some files failed, 2 bad usage/credentials, 130 interrupted.
"""

from __future__ import annotations

import argparse
import atexit
import fnmatch
import hashlib
import json
import logging
import os
import random
import re
import signal
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

try:
    import requests
    from internetarchive import get_item, get_session
    from internetarchive.utils import validate_s3_identifier
except ImportError as exc:
    sys.exit(
        f"Missing dependency ({exc.name}). Install with:\n"
        f"    pip install internetarchive"
    )


# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

# Transient network failures. Retry these.
NETWORK_ERRORS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,      # includes SSLError, ProxyError
    requests.exceptions.ChunkedEncodingError,
)

# 503 is IA's "node busy / out of space" signal and is the most common
# failure on large batches. 429 is rate limiting.
RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504, 507}

# No amount of retrying fixes these.
FATAL_STATUS = {400, 401, 403, 404, 409}

# IA's own generated files. Never treat these as ours. These are matched as
# exact names (<identifier> + suffix), not as suffixes: a map pack legitimately
# containing "levelpack_meta.xml" must not be mistaken for IA's own metadata,
# or it would look absent on every run and be re-uploaded forever.
IA_GENERATED_SUFFIXES = (
    "_meta.xml", "_files.xml", "_meta.sqlite",
    "_archive.torrent", "_reviews.xml", "_rules.conf",
)
IA_GENERATED_EXACT = ("__ia_thumb.jpg",)


def ia_generated_names(identifier: str) -> set[str]:
    """The files IA creates on an item, by exact name."""
    return {f"{identifier}{suffix}" for suffix in IA_GENERATED_SUFFIXES} | set(
        IA_GENERATED_EXACT
    )

STATE_VERSION = 3
MAX_REMOTE_NAME_BYTES = 240          # IA rejects very long keys
HEARTBEAT_SECONDS = 60
RATE_WINDOW_SECONDS = 600            # ETA looks at recent throughput, not the run's whole history

# Redact anything shaped like IA's S3 auth header before logging.
_SECRET_RE = re.compile(r"(LOW\s+)[A-Za-z0-9]+:[A-Za-z0-9]+", re.IGNORECASE)

log = logging.getLogger("ia_upload")


def safe(text: object) -> str:
    """Strip credentials out of anything before it reaches a log or console."""
    return _SECRET_RE.sub(r"\1<redacted>", str(text))


class Outcome(Enum):
    OK = "ok"
    RETRY_EXHAUSTED = "retry_exhausted"
    FATAL = "fatal"           # stop the whole run
    SKIPPED = "skipped"
    INTERRUPTED = "interrupted"


# --------------------------------------------------------------------------
# Graceful shutdown
# --------------------------------------------------------------------------

class Interrupt:
    """First signal: finish the file in flight, then stop. Second: leave now."""

    def __init__(self, on_abort=None) -> None:
        self.requested = False
        self._on_abort = on_abort
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, self._handle)
            except (ValueError, OSError):
                pass  # not the main thread, or platform lacks the signal

    def _handle(self, signum, frame) -> None:
        if self.requested:
            log.warning("Second interrupt, exiting now.")
            if self._on_abort:
                try:
                    self._on_abort()
                except Exception:
                    pass
            sys.exit(130)
        self.requested = True
        log.warning(
            "Interrupt received. Finishing the file in flight, then stopping "
            "cleanly. Press Ctrl-C again to abort immediately."
        )

    def sleep(self, seconds: float) -> None:
        """Sleep in short slices so a signal is noticed promptly."""
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline and not self.requested:
            # max(0.0, ...) because the deadline can pass between the loop
            # test and this call, and time.sleep rejects a negative value.
            time.sleep(max(0.0, min(1.0, deadline - time.monotonic())))


class Heartbeat:
    """Log progress during a long single-file transfer.

    Without this a multi-hour upload of one large file looks identical to a
    hung process, which is the worst possible experience for an unattended run.
    """

    def __init__(self, label: str, size: int, timeout: int) -> None:
        self._label, self._size, self._timeout = label, size, timeout
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        started = time.monotonic()
        while not self._stop.wait(HEARTBEAT_SECONDS):
            elapsed = time.monotonic() - started
            log.info(
                "    ... still sending %s (%s), %s elapsed, times out at %s",
                self._label, human(self._size),
                clock(elapsed), clock(self._timeout),
            )

    def __enter__(self) -> "Heartbeat":
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._thread.join(timeout=1)


# --------------------------------------------------------------------------
# Local file discovery
# --------------------------------------------------------------------------

@dataclass
class LocalFile:
    path: Path
    remote_name: str
    size: int
    mtime: float
    md5: str = ""


def _excluded(name: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(name, pat) for pat in patterns)


def discover(root: Path, exclude: list[str], include_hidden: bool) -> list[LocalFile]:
    """Walk root, returning files keyed by POSIX-style relative name."""
    found: list[LocalFile] = []
    symlinks = 0

    if root.is_file():
        # Apply the same rules as the directory walk, so that --exclude,
        # --include-hidden and the empty-file skip do not silently change
        # meaning depending on whether the source is a file or a directory.
        name = root.name
        if root.is_symlink():
            log.warning(
                "%s is a symlink; symlinks are never followed. Pass the "
                "real path if you want to upload its target.", root
            )
            return []
        if not include_hidden and name.startswith("."):
            log.warning("%s is hidden; pass --include-hidden to upload it.", root)
            return []
        if _excluded(name, exclude):
            log.warning("%s matches an --exclude pattern.", root)
            return []
        st = root.stat()
        if st.st_size == 0:
            log.warning("Skipping empty file: %s", root)
            return []
        return [LocalFile(root, name, st.st_size, st.st_mtime)]

    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        # Prune in place so we never descend into excluded trees.
        dirnames[:] = [
            d for d in dirnames
            if (include_hidden or not d.startswith("."))
            and not _excluded(d, exclude)
        ]
        for name in sorted(filenames):
            if not include_hidden and name.startswith("."):
                continue
            if _excluded(name, exclude):
                continue
            local = Path(dirpath) / name
            if local.is_symlink():
                symlinks += 1
                continue
            try:
                if not local.is_file():
                    continue
                st = local.stat()
            except OSError as e:
                log.warning("Cannot read %s: %s", local, e)
                continue
            if st.st_size == 0:
                log.warning("Skipping empty file: %s", local)
                continue
            found.append(
                LocalFile(
                    local,
                    local.relative_to(root).as_posix(),
                    st.st_size,
                    st.st_mtime,
                )
            )

    if symlinks:
        log.warning(
            "Skipped %d symlink(s). Symlinks are not followed, so nothing "
            "outside the source tree can be uploaded by accident.", symlinks
        )
    return sorted(found, key=lambda f: f.remote_name)


def check_names(files: list[LocalFile], identifier: str) -> list[str]:
    """Flag names IA will reject, before we waste hours discovering it."""
    problems = []
    seen: dict[str, str] = {}
    generated = ia_generated_names(identifier)
    for f in files:
        name = f.remote_name
        if name in generated:
            problems.append(
                f"{f.path}: remote name '{name}' matches a file IA generates "
                f"itself on this item; it will never be recognized as "
                f"uploaded and will be re-sent every run. Rename it."
            )
        if any(ord(c) < 32 or ord(c) == 127 for c in name):
            problems.append(f"{f.path}: contains control characters")
        if len(name.encode("utf-8")) > MAX_REMOTE_NAME_BYTES:
            problems.append(f"{f.path}: remote name is too long for IA")
        segments = name.split("/")
        if any(seg != seg.strip() or not seg for seg in segments):
            problems.append(
                f"{f.path}: a path segment is empty or has leading/trailing spaces"
            )
        # Case-insensitive collisions bite on IA even from a case-sensitive FS.
        lowered = name.lower()
        if lowered in seen and seen[lowered] != name:
            problems.append(f"{f.path}: collides with {seen[lowered]} (case-insensitive)")
        seen[lowered] = name
    return problems


def _md5() -> "hashlib._Hash":
    """MD5 here is IA's content checksum, not a security primitive.

    On a FIPS-enforcing build a bare hashlib.md5() raises, which would take
    the whole run down; usedforsecurity=False is the documented opt-out.
    """
    try:
        return hashlib.md5(usedforsecurity=False)
    except TypeError:          # Python < 3.9
        return hashlib.md5()


def md5_of(path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    h = _md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------
# Persistent state
# --------------------------------------------------------------------------

@dataclass
class State:
    """Cached md5s and confirmed uploads, so a re-run is cheap."""
    path: Path
    identifier: str = ""
    hashes: dict = field(default_factory=dict)
    completed: dict = field(default_factory=dict)
    _dirty: bool = False

    @staticmethod
    def _key(f: LocalFile) -> str:
        # Size + mtime invalidate the cache when a file is edited.
        return f"{f.path}|{f.size}|{int(f.mtime)}"

    @classmethod
    def load(cls, path: Path, identifier: str) -> "State":
        s = cls(path=path, identifier=identifier)
        if not path.exists():
            return s
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise ValueError("not an object")
        except (json.JSONDecodeError, OSError, ValueError, UnicodeDecodeError) as e:
            log.warning("State file unusable (%s); starting fresh.", e)
            return s
        if data.get("version") != STATE_VERSION:
            log.info("State file is from a different version; starting fresh.")
            return s
        if data.get("identifier") != identifier:
            log.warning(
                "%s holds state for a different item ('%s', expected '%s'); "
                "starting fresh. Its contents will be overwritten on save.",
                path, data.get("identifier"), identifier,
            )
            return s
        if isinstance(data.get("hashes"), dict):
            s.hashes = data["hashes"]
        if isinstance(data.get("completed"), dict):
            s.completed = data["completed"]
        log.info(
            "Resuming: %d cached hashes, %d files recorded as accepted by IA.",
            len(s.hashes), len(s.completed),
        )
        return s

    def save(self, force: bool = False) -> None:
        if not (self._dirty or force):
            return
        payload = {
            "version": STATE_VERSION,
            "identifier": self.identifier,
            "hashes": self.hashes,
            "completed": self.completed,
        }
        tmp = self.path.with_name(self.path.name + ".tmp")
        try:
            tmp.write_text(json.dumps(payload), encoding="utf-8")
            os.chmod(tmp, 0o600)
            os.replace(tmp, self.path)   # atomic; survives a kill mid-write
            self._dirty = False
        except OSError as e:
            log.warning("Could not save resume state: %s", e)
            tmp.unlink(missing_ok=True)

    def hash_for(self, f: LocalFile) -> str:
        key = self._key(f)
        cached = self.hashes.get(key)
        if cached:
            return cached
        digest = md5_of(f.path)
        self.hashes[key] = digest
        self._dirty = True
        return digest

    def mark(self, remote_name: str, md5: str) -> None:
        if self.completed.get(remote_name) != md5:
            self.completed[remote_name] = md5
            self._dirty = True

    def prune(self, files: list[LocalFile]) -> None:
        """Drop cache entries for files that are no longer part of this run."""
        live = {self._key(f) for f in files}
        stale = set(self.hashes) - live
        if stale:
            for key in stale:
                del self.hashes[key]
            self._dirty = True

        # Without this, `completed` accumulated an entry for every file ever
        # sent from this source tree and the state file grew without bound.
        live_names = {f.remote_name for f in files}
        stale_names = set(self.completed) - live_names
        if stale_names:
            for name in stale_names:
                del self.completed[name]
            self._dirty = True

    def confirmed(self, f: LocalFile) -> bool:
        """True if we previously saw IA accept exactly these bytes."""
        return self.completed.get(f.remote_name) == f.md5


def acquire_lock(path: Path) -> None:
    """Refuse to start if another run is already using this state file.

    Without this, an overlapping cron run and a manual run can race on the
    same state file, each silently discarding the other's progress records.
    Released automatically on interpreter exit via atexit, which also fires
    for sys.exit() (including from the interrupt handler), so no explicit
    release call is needed at main()'s many return points.
    """
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        stale = False
        try:
            pid = int(path.read_text().strip())
        except (OSError, ValueError):
            stale = True  # lock file unreadable or garbage; safe to reclaim
        else:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                stale = True          # that pid is definitely gone
            except OSError:
                pass                  # exists but we can't signal it; assume running
        if not stale:
            raise SystemExit(
                f"Another ia_upload run appears to be using this state file "
                f"already (lock: {path}).\n"
                f"  If that run is truly gone, delete the lock file and retry."
            )
        path.unlink(missing_ok=True)
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w") as fh:
        fh.write(str(os.getpid()))
    atexit.register(path.unlink, missing_ok=True)


# --------------------------------------------------------------------------
# Remote inspection
# --------------------------------------------------------------------------

class FatalRemoteError(RuntimeError):
    """Reading the item failed in a way retrying cannot fix."""


def remote_manifest(identifier: str, attempts: int = 5) -> dict:
    """Return {filename: md5} for everything currently on the item."""
    for attempt in range(1, attempts + 1):
        try:
            item = get_item(identifier)
            if not item.exists:
                return {}
            generated = ia_generated_names(identifier)
            return {
                f.name: (getattr(f, "md5", "") or "")
                for f in item.get_files()
                if f.name not in generated
            }
        except Exception as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            text = safe(e)
            if status in (401, 403):
                raise FatalRemoteError(
                    f"Access denied reading item '{identifier}'.\n"
                    f"  Check the identifier spelling, and that your keys have "
                    f"write access to it.\n  {text}"
                ) from e
            if attempt == attempts:
                break
            wait = min(60, 2 ** attempt) * (0.5 + random.random())
            log.warning(
                "Could not read the item's file list (attempt %d/%d): %s "
                "; retrying in %s", attempt, attempts, text, clock(wait),
            )
            time.sleep(wait)
    raise RuntimeError(f"Gave up reading the file list for '{identifier}'")


# --------------------------------------------------------------------------
# Upload
# --------------------------------------------------------------------------

def read_timeout_for(size: int, base: int, per_gb: int, cap: int) -> int:
    """Scale the read timeout with file size.

    The read timeout governs how long we wait for IA's *response* after the
    body has been sent. IA writes the file and checksums it before replying,
    so large files legitimately need long waits. This is exactly why the
    library's 120s default fails on big uploads.
    """
    return int(min(cap, base + per_gb * (size / (1024 ** 3))))


def classify(exc: Exception) -> tuple[bool, str]:
    """Return (retryable, human reason) for an exception from upload()."""
    if isinstance(exc, requests.exceptions.HTTPError):
        resp = getattr(exc, "response", None)
        status = getattr(resp, "status_code", None)
        body = ""
        try:
            body = (resp.text or "")[:400] if resp is not None else ""
        except Exception:
            pass
        if "appears to be spam" in body.lower():
            return False, (
                "IA rejected the upload as spam. This usually means the item "
                "needs descriptive metadata, or the account is rate limited. "
                "Add --metadata 'title:...' and 'description:...' and retry later"
            )
        if status in FATAL_STATUS:
            return False, f"HTTP {status} (not retryable): {safe(body)}"
        if status in RETRYABLE_STATUS:
            return True, f"HTTP {status}"
        return True, f"HTTP {status}"        # unknown status: worth one more go
    if isinstance(exc, NETWORK_ERRORS):
        return True, type(exc).__name__
    if isinstance(exc, requests.exceptions.RequestException):
        return True, type(exc).__name__
    # internetarchive sometimes re-raises a raw socket-level ConnectionError
    # (e.g. ConnectionResetError) instead of wrapping it in
    # requests.exceptions.ConnectionError. Both are transient network
    # conditions, not the local file error the OSError branch below is for.
    if isinstance(exc, ConnectionError):
        return True, type(exc).__name__
    if isinstance(exc, (OSError, IOError)):
        return False, f"local file error: {safe(exc)}"
    return False, f"{type(exc).__name__}: {safe(exc)}"


def upload_one(
    item,
    f: LocalFile,
    args,
    size_hint: int,
    metadata: dict | None,
    interrupt: Interrupt,
) -> tuple[Outcome, str]:
    """Upload a single file with its own retry loop."""
    read_to = read_timeout_for(
        f.size, args.read_timeout, args.timeout_per_gb, args.max_timeout
    )
    delay = args.retry_wait

    headers = {
        "x-archive-size-hint": str(size_hint),
        # Server-side integrity check using the md5 we already computed.
        # Passing it directly avoids verify=True, which would re-read the
        # entire file from disk on every single attempt.
        "Content-MD5": f.md5,
    }

    for attempt in range(1, args.retries + 1):
        if interrupt.requested and attempt > 1:
            return Outcome.INTERRUPTED, "stopped before retry"

        try:
            with Heartbeat(f.remote_name, f.size, read_to):
                item.upload(
                    {f.remote_name: str(f.path)},
                    metadata=metadata,
                    headers=headers,
                    queue_derive=False,     # queued once at the end instead
                    checksum=False,         # we dedupe ourselves, from cache
                    verify=False,           # Content-MD5 above does this
                    retries=0,              # we own the retry loop
                    verbose=False,
                    request_kwargs={"timeout": (args.connect_timeout, read_to)},
                )
            return Outcome.OK, "uploaded"

        except Exception as exc:
            retryable, reason = classify(exc)

            # A read timeout very often means the transfer succeeded and we
            # simply stopped waiting for the acknowledgement. Confirm before
            # spending another full upload on it.
            if isinstance(exc, requests.exceptions.Timeout):
                log.info("    %s, checking whether it landed anyway...", reason)
                try:
                    if remote_manifest(item.identifier, attempts=2).get(f.remote_name) == f.md5:
                        return Outcome.OK, "confirmed present after timeout"
                except Exception:
                    pass

            if not retryable:
                fatal = isinstance(exc, requests.exceptions.HTTPError) and \
                    getattr(getattr(exc, "response", None), "status_code", None) in (401, 403)
                return (Outcome.FATAL if fatal else Outcome.RETRY_EXHAUSTED), reason

            if attempt == args.retries:
                return Outcome.RETRY_EXHAUSTED, f"{reason} after {args.retries} attempts"

            sleep_for = min(args.max_retry_wait, delay) * (0.5 + random.random())
            log.warning(
                "    %s on attempt %d/%d, retrying in %s",
                reason, attempt, args.retries, clock(sleep_for),
            )
            interrupt.sleep(sleep_for)
            delay = min(args.max_retry_wait, delay * 2)

    return Outcome.RETRY_EXHAUSTED, "retries exhausted"


# --------------------------------------------------------------------------
# Formatting helpers
# --------------------------------------------------------------------------

def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{int(n)}B" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"


def rolling_rate(window: deque[tuple[float, int]], now: float, size: int, elapsed: float) -> float:
    """Record a completed file and return bytes/sec over a recent window.

    A plain lifetime average (total bytes / total elapsed since the run
    started) stays dragged down by an early retry storm for the rest of the
    run, long after conditions recover. This looks only at RATE_WINDOW_SECONDS
    of recent history instead, aging out anything older.
    """
    window.append((now, size))
    while window and now - window[0][0] > RATE_WINDOW_SECONDS:
        window.popleft()
    window_bytes = sum(s for _, s in window)
    window_span = max(now - window[0][0], elapsed, 0.001)
    return window_bytes / window_span


def clock(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def parse_metadata(pairs: list[str] | None) -> dict:
    meta: dict = {}
    for pair in pairs or []:
        if ":" not in pair:
            raise ValueError(
                f"Metadata must be KEY:VALUE, got {pair!r}\n"
                f"  e.g. --metadata 'title:Quake 4 CTF Maps'"
            )
        key, value = (part.strip() for part in pair.split(":", 1))
        if not key or not value:
            raise ValueError(f"Metadata key and value must both be non-empty: {pair!r}")
        if key in meta:
            existing = meta[key]
            meta[key] = (existing if isinstance(existing, list) else [existing]) + [value]
        else:
            meta[key] = value
    return meta


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Resilient bulk uploader for archive.org items.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("identifier", help="Target archive.org item identifier")
    p.add_argument("source", type=Path, help="File or directory to upload")

    g = p.add_argument_group("what to send")
    g.add_argument("--dry-run", action="store_true",
                   help="Show what would happen; upload nothing")
    g.add_argument("--metadata", "-m", action="append", metavar="KEY:VALUE",
                   help="Item metadata, repeatable (e.g. -m 'title:My Item')")
    g.add_argument("--exclude", action="append", default=[], metavar="GLOB",
                   help="Skip names matching this glob, repeatable (e.g. '*.tmp')")
    g.add_argument("--include-hidden", action="store_true",
                   help="Include dotfiles and dot-directories")

    g = p.add_argument_group("derivation")
    g.add_argument("--derive", action="store_true",
                   help="Queue IA's derivation task once all uploads finish")
    g.add_argument("--reduced-priority", action="store_true",
                   help="Submit the derive at low priority to dodge rate limits")

    g = p.add_argument_group("retries and pacing")
    g.add_argument("--retries", type=int, default=12, metavar="N",
                   help="Attempts per file (default: 12)")
    g.add_argument("--retry-wait", type=float, default=15, metavar="SEC",
                   help="Initial backoff (default: 15)")
    g.add_argument("--max-retry-wait", type=float, default=600, metavar="SEC",
                   help="Backoff ceiling (default: 600)")
    g.add_argument("--wait-between", type=float, default=0, metavar="SEC",
                   help="Pause between files (default: 0)")

    g = p.add_argument_group("timeouts")
    g.add_argument("--connect-timeout", type=int, default=30, metavar="SEC",
                   help="TCP connect timeout (default: 30)")
    g.add_argument("--read-timeout", type=int, default=600, metavar="SEC",
                   help="Base response timeout (default: 600)")
    g.add_argument("--timeout-per-gb", type=int, default=300, metavar="SEC",
                   help="Added response timeout per GB (default: 300)")
    g.add_argument("--max-timeout", type=int, default=7200, metavar="SEC",
                   help="Response timeout ceiling (default: 7200)")

    g = p.add_argument_group("bookkeeping")
    g.add_argument("--state", type=Path, default=None, metavar="PATH",
                   help="Resume-state file (default: .ia_upload_<identifier>.json)")
    g.add_argument("--log", type=Path, default=None, metavar="PATH",
                   help="Also append logs to this file")
    g.add_argument("--no-verify", action="store_true",
                   help="Skip the final verification pass")
    g.add_argument("--trust-state", action="store_true",
                   help="Skip files this state file records as accepted, even "
                        "if IA's file list has not caught up yet")
    g.add_argument("--yes", "-y", action="store_true",
                   help="Do not prompt before replacing existing remote files")
    return p


def setup_logging(logfile: Path | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if logfile:
        try:
            logfile.parent.mkdir(parents=True, exist_ok=True)
            handlers.append(logging.FileHandler(logfile, encoding="utf-8"))
        except OSError as e:
            print(f"Warning: cannot write log file {logfile}: {e}", file=sys.stderr)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )
    # The library logs its own copy of every error; we report them with
    # context, so suppress the duplicates.
    for noisy in ("internetarchive", "urllib3", "requests"):
        logging.getLogger(noisy).setLevel(logging.CRITICAL)


def validate_args(args) -> str | None:
    """Return an error message, or None if the arguments are usable."""
    try:
        validate_s3_identifier(args.identifier)
    except Exception as e:
        return f"Invalid archive.org identifier '{args.identifier}': {e}"
    if not args.source.exists():
        return f"Source does not exist: {args.source}"
    if args.retries < 1:
        return "--retries must be at least 1"
    for name in ("retry_wait", "max_retry_wait", "wait_between"):
        if getattr(args, name) < 0:
            return f"--{name.replace('_', '-')} cannot be negative"
    for name in ("connect_timeout", "read_timeout", "max_timeout"):
        if getattr(args, name) < 1:
            return f"--{name.replace('_', '-')} must be at least 1 second"
    if args.timeout_per_gb < 0:
        return "--timeout-per-gb cannot be negative"
    if args.max_timeout < args.read_timeout:
        return "--max-timeout cannot be lower than --read-timeout"
    return None


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main() -> int:
    args = build_parser().parse_args()
    setup_logging(args.log)

    problem = validate_args(args)
    if problem:
        log.error(problem)
        return 2

    try:
        metadata = parse_metadata(args.metadata)
    except ValueError as e:
        log.error(str(e))
        return 2

    if not args.dry_run:
        try:
            session = get_session()
            if not (session.access_key and session.secret_key):
                raise ValueError("no keys configured")
        except Exception:
            log.error(
                "No archive.org credentials found.\n"
                "  Run `ia configure`, or set IA_ACCESS_KEY_ID and "
                "IA_SECRET_ACCESS_KEY (both, or neither)."
            )
            return 2

    root = args.source.resolve()
    state_path = args.state or Path(f".ia_upload_{args.identifier}.json")
    acquire_lock(state_path.with_name(state_path.name + ".lock"))
    state = State.load(state_path, args.identifier)
    interrupt = Interrupt(on_abort=lambda: state.save(force=True))

    # ---- Scan -------------------------------------------------------------
    log.info("Scanning %s", root)
    files = discover(root, args.exclude, args.include_hidden)
    if not files:
        log.error("No files found to upload.")
        return 1

    problems = check_names(files, args.identifier)
    if problems:
        log.error("%d filename problem(s) that IA will reject:", len(problems))
        for line in problems[:20]:
            log.error("    %s", line)
        if len(problems) > 20:
            log.error("    ... and %d more", len(problems) - 20)
        log.error("Rename these files, or exclude them, then re-run.")
        return 2

    total_bytes = sum(f.size for f in files)
    log.info("Found %d files, %s total.", len(files), human(total_bytes))

    # ---- Compare against the item ----------------------------------------
    log.info("Reading current contents of item '%s'", args.identifier)
    try:
        remote = remote_manifest(args.identifier)
    except FatalRemoteError as e:
        log.error(str(e))
        return 2
    except RuntimeError as e:
        log.error(str(e))
        return 1
    log.info("Item currently holds %d files.", len(remote))

    log.info("Hashing local files (cached across runs)...")
    pending: list[LocalFile] = []
    skipped = 0
    assumed = 0
    for f in files:
        if interrupt.requested:
            log.warning("Interrupted while hashing.")
            state.save(force=True)
            return 130
        f.md5 = state.hash_for(f)
        if remote.get(f.remote_name) == f.md5:
            state.mark(f.remote_name, f.md5)
            skipped += 1
        elif args.trust_state and state.confirmed(f):
            # IA's file list can lag minutes behind an accepted upload. We only
            # record a file once IA returned success for it, so this is a sound
            # skip, but it is opt-in, because the file list is the only real
            # evidence the bytes are still there.
            assumed += 1
        else:
            pending.append(f)
    state.prune(files)
    state.save()

    pending_bytes = sum(f.size for f in pending)
    replacing = [f for f in pending if f.remote_name in remote]
    log.info(
        "%d already present and identical, %d to upload (%s).",
        skipped, len(pending), human(pending_bytes),
    )
    if assumed:
        log.warning(
            "%d file(s) skipped on --trust-state alone; IA's file list does "
            "not show them yet. Re-run without --trust-state once the item "
            "has settled to confirm them.", assumed,
        )

    # ---- Dry run ----------------------------------------------------------
    if args.dry_run:
        for f in pending:
            marker = "REPLACE" if f.remote_name in remote else "new"
            log.info("  [%-7s] %s (%s)", marker, f.remote_name, human(f.size))
        if replacing:
            log.warning(
                "%d file(s) would OVERWRITE a different version already on "
                "the item.", len(replacing),
            )
        log.info("Dry run complete. Nothing was uploaded.")
        return 0

    if not pending:
        log.info("Nothing to do; the item is already up to date.")
        return 0

    # Overwriting is destructive and irreversible on IA, so confirm it.
    if replacing and not args.yes:
        if not sys.stdin.isatty():
            log.error(
                "%d file(s) would overwrite different content already on the "
                "item. Re-run with --yes to allow this, or --dry-run to list "
                "them.", len(replacing),
            )
            return 2
        log.warning("%d file(s) will overwrite existing remote copies:", len(replacing))
        for f in replacing[:10]:
            log.warning("    %s", f.remote_name)
        if len(replacing) > 10:
            log.warning("    ... and %d more", len(replacing) - 10)
        try:
            if input("Continue? [y/N] ").strip().lower() not in ("y", "yes"):
                log.info("Aborted.")
                return 0
        except (EOFError, KeyboardInterrupt):
            log.info("Aborted.")
            return 0

    # ---- Upload -----------------------------------------------------------
    try:
        item = get_item(args.identifier)
    except Exception as e:
        log.error("Could not open item: %s", safe(e))
        return 1

    if metadata and remote:
        log.info("Item already exists; metadata is applied only at creation. "
                 "Use `ia metadata` to change it on an existing item.")

    succeeded: list[LocalFile] = []
    failed: list[tuple[LocalFile, str]] = []
    sent_bytes = 0
    started = time.time()
    # (timestamp, bytes) of recent completions, for a recency-weighted ETA.
    # A plain lifetime average would stay dragged down by an early retry
    # storm for the rest of the run, long after conditions recover.
    rate_window: deque[tuple[float, int]] = deque()
    stop_all = False

    for i, f in enumerate(pending, 1):
        if interrupt.requested or stop_all:
            break

        # The file may have changed since we hashed it; the cached md5 would
        # then be wrong and we would record a false success.
        try:
            st = f.path.stat()
        except OSError as e:
            log.error("[%d/%d] %s is gone or unreadable: %s", i, len(pending), f.remote_name, e)
            failed.append((f, f"unreadable: {e}"))
            continue
        if st.st_size != f.size or int(st.st_mtime) != int(f.mtime):
            log.warning("[%d/%d] %s changed on disk, re-hashing.", i, len(pending), f.remote_name)
            f.size, f.mtime = st.st_size, st.st_mtime
            f.md5 = state.hash_for(f)

        log.info("[%d/%d] %s (%s)", i, len(pending), f.remote_name, human(f.size))
        t0 = time.time()
        outcome, message = upload_one(
            item, f, args, total_bytes,
            # Metadata rides along with the first upload, which is what
            # creates the item; it is ignored afterwards.
            metadata=(metadata if (metadata and not remote and not succeeded) else None),
            interrupt=interrupt,
        )
        elapsed = max(time.time() - t0, 0.001)

        if outcome is Outcome.OK:
            succeeded.append(f)
            sent_bytes += f.size
            state.mark(f.remote_name, f.md5)
            overall = rolling_rate(rate_window, time.time(), f.size, elapsed)
            remaining = pending_bytes - sent_bytes
            log.info(
                "    done: %s in %s (%s/s). %s to go, ETA %s.",
                message, clock(elapsed), human(f.size / elapsed),
                human(remaining), clock(remaining / overall) if overall else "?",
            )
        else:
            failed.append((f, message))
            log.error("    FAILED: %s", message)
            if outcome is Outcome.FATAL:
                log.error("Credentials or permissions problem, stopping the run.")
                stop_all = True

        state.save()
        if args.wait_between and i < len(pending):
            interrupt.sleep(args.wait_between)

    state.save(force=True)

    # ---- Verify -----------------------------------------------------------
    unconfirmed: list[str] = []
    if succeeded and not args.no_verify and not interrupt.requested:
        log.info("Letting the item settle, then verifying...")
        interrupt.sleep(30)
        try:
            final = remote_manifest(args.identifier)
            unconfirmed = [f.remote_name for f in succeeded
                           if final.get(f.remote_name) != f.md5]
            if unconfirmed:
                log.warning(
                    "%d file(s) not confirmed on the item yet. IA's file list "
                    "lags behind uploads, so this is often just timing. "
                    "Re-run this command later and anything genuinely missing "
                    "will be re-sent:", len(unconfirmed),
                )
                for name in unconfirmed[:20]:
                    log.warning("    %s", name)
                if len(unconfirmed) > 20:
                    log.warning("    ... and %d more", len(unconfirmed) - 20)
            else:
                log.info("Verified: all %d uploaded files match.", len(succeeded))
        except (RuntimeError, FatalRemoteError) as e:
            log.warning("Verification skipped: %s", e)

    # ---- Derive -----------------------------------------------------------
    if args.derive and succeeded and not failed and not interrupt.requested:
        try:
            item.derive(reduced_priority=args.reduced_priority)
            log.info("Derivation task queued.")
        except Exception as e:
            log.warning(
                "Could not queue derivation: %s\n"
                "  Run `ia derive %s` when convenient.", safe(e), args.identifier
            )
    elif args.derive:
        log.info(
            "Skipping derivation: the run did not finish cleanly. "
            "Re-run to completion, or `ia derive %s` manually.", args.identifier
        )

    # ---- Summary ----------------------------------------------------------
    log.info("-" * 64)
    log.info(
        "Finished in %s. Uploaded %d (%s), already present %d, failed %d.",
        clock(time.time() - started), len(succeeded), human(sent_bytes),
        skipped, len(failed),
    )
    if failed:
        log.info("Failed:")
        for f, message in failed:
            log.info("    %s: %s", f.remote_name, message)
        log.info("Re-run the identical command to retry just these.")
    if interrupt.requested:
        log.info("Stopped early on request. Re-run to continue where it left off.")
    log.info("Item: https://archive.org/details/%s", args.identifier)

    if interrupt.requested:
        return 130
    return 1 if (failed or unconfirmed) else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        # An unexpected bug, not a handled failure. Get it into --log (not
        # just stderr) so a crash hours into an unattended run leaves a trace.
        logging.getLogger("ia_upload").exception("Unexpected error; this is a bug.")
        sys.exit(1)

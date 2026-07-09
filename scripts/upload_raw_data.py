"""
Uploads the 5 raw source files to a Unity Catalog Volume, streaming from disk
so it works for files well past the 5 GB cap — and checkpoints per-file
status locally so a re-run only retries what actually failed.

Why this exists: Catalog Explorer's drag-and-drop upload UI is capped at 5 GB
per file (a UI limit, not a Volume storage limit or a Free Edition data cap —
Volumes themselves support files up to your cloud storage provider's max
size). streets.csv is 7.8 GB, so it can't go through the UI. Databricks'
recommendation for files over 5 GB is the SDK's streaming upload, which is
what this script does.

Checkpointing — what it does and doesn't do:
  - A local JSON file (.upload_state.<catalog>.json, next to this script)
    records, per source file: status (success/failed), size + mtime at the
    time it succeeded, and a timestamp. On every run, a file already marked
    "success" with an unchanged size/mtime is skipped entirely — you never
    re-upload cars.csv (943MB, near-instant) again just because streets.csv
    (7.8GB) hiccuped on a later run.
  - It does NOT resume a single interrupted file from the byte it died at.
    Checked against the Databricks Python SDK docs: files.upload/upload_from
    take overwrite/part_size/use_parallel/parallelism — no resume/offset
    parameter exists. If streets.csv dies 6GB in, the next run restarts that
    one file from 0 (but skips the 4 small ones that already succeeded).
  - Each file gets up to 3 attempts with exponential backoff (2s/4s/8s)
    within a single run before being marked "failed" — this alone should
    absorb most transient network blips without you needing to intervene.

Usage:
    pip install databricks-sdk
    databricks auth login --host https://<your-workspace-url>.cloud.databricks.com

    python scripts/upload_raw_data.py --catalog vstone_traffic_dev --data-dir "D:\\v4c\\Databricks\\vstone\\traffic_simulator\\src\\data"

    # Re-run after a partial failure — already-succeeded files are skipped
    # automatically, only the failed one(s) are retried:
    python scripts/upload_raw_data.py --catalog vstone_traffic_dev --data-dir "D:\\v4c\\Databricks\\vstone\\traffic_simulator\\src\\data"

    # Force re-upload of everything, ignoring the checkpoint:
    python scripts/upload_raw_data.py --catalog vstone_traffic_dev --data-dir "..." --force
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

RAW_FILES = [
    "cars.csv",
    "streets.csv",
    "node_locations.csv",
    "streets_list.csv",
    "telegram.csv",
]

MAX_ATTEMPTS = 3
BACKOFF_SECONDS = (2, 4, 8)


def human_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}PB"


def load_state(state_path: Path) -> dict:
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text())
    except (json.JSONDecodeError, OSError):
        print(f"Warning: couldn't read {state_path}, starting with empty state", file=sys.stderr)
        return {}


def save_state(state_path: Path, state: dict) -> None:
    # Written after every file, not just at the end, so a crash mid-run
    # doesn't lose progress on files that already succeeded.
    state_path.write_text(json.dumps(state, indent=2, sort_keys=True))


def local_fingerprint(path: Path) -> dict:
    stat = path.stat()
    return {"size": stat.st_size, "mtime": stat.st_mtime}


def already_uploaded(entry: dict | None, fingerprint: dict) -> bool:
    if not entry or entry.get("status") != "success":
        return False
    return entry.get("size") == fingerprint["size"] and entry.get("mtime") == fingerprint["mtime"]


def upload_one(w, local_path: Path, remote_path: str) -> None:
    """Raises on final failure after MAX_ATTEMPTS."""
    last_err = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            # upload_from() takes a local PATH, not an open stream — each
            # worker thread opens its own file handle from the path, so
            # parallel multipart upload is actually safe here (unlike
            # upload(), which hands every thread the SAME open file object
            # and crashes large files with "Exception in thread ... producer").
            # This should be meaningfully faster than the old single-threaded
            # upload() fallback, especially for streets.csv (7+ GB).
            w.files.upload_from(
                file_path=remote_path,
                source_path=str(local_path),
                overwrite=True,
                use_parallel=True,
            )
            return
        except Exception as exc:  # noqa: BLE001 - want to retry on any transient error
            last_err = exc
            if attempt < MAX_ATTEMPTS:
                wait = BACKOFF_SECONDS[attempt - 1]
                print(f"  attempt {attempt}/{MAX_ATTEMPTS} failed ({exc}); retrying in {wait}s")
                time.sleep(wait)
    raise last_err  # type: ignore[misc]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--catalog", required=True, help="e.g. vstone_traffic_dev")
    parser.add_argument("--schema", default="raw")
    parser.add_argument("--volume", default="raw_volume")
    parser.add_argument("--data-dir", required=True, help="Local folder containing the 5 raw CSVs")
    parser.add_argument(
        "--files",
        nargs="*",
        default=RAW_FILES,
        help="Override the list of files to upload (default: all 5 raw source files)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore the local checkpoint and re-upload every file regardless of prior success",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help=(
            "Databricks CLI auth profile to use (see `databricks auth profiles`). "
            "If omitted, falls back to DATABRICKS_CONFIG_PROFILE / DEFAULT — if you "
            "have more than one profile configured, pass this explicitly so the "
            "upload doesn't silently hit the wrong workspace."
        ),
    )
    args = parser.parse_args()

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("Missing dependency. Run: pip install databricks-sdk", file=sys.stderr)
        return 1

    data_dir = Path(args.data_dir)
    volume_root = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}/incoming"
    state_path = Path(__file__).resolve().parent / f".upload_state.{args.catalog}.json"

    missing = [f for f in args.files if not (data_dir / f).exists()]
    if missing:
        print(f"Missing local files, aborting: {missing}", file=sys.stderr)
        return 1

    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    print(f"Connected to: {w.config.host} (profile: {args.profile or 'DEFAULT / env vars'})")
    state = {} if args.force else load_state(state_path)

    results = {"skipped": [], "success": [], "failed": []}

    for filename in args.files:
        local_path = data_dir / filename
        remote_path = f"{volume_root}/{filename}"
        fingerprint = local_fingerprint(local_path)
        size = fingerprint["size"]

        if already_uploaded(state.get(filename), fingerprint):
            print(f"Skipping {filename} ({human_size(size)}) — already uploaded, unchanged since last success")
            results["skipped"].append(filename)
            continue

        print(f"Uploading {filename} ({human_size(size)}) -> {remote_path}")
        start = time.time()
        try:
            upload_one(w, local_path, remote_path)
        except Exception as exc:  # noqa: BLE001
            elapsed = time.time() - start
            print(f"  FAILED after {elapsed:.0f}s and {MAX_ATTEMPTS} attempts: {exc}", file=sys.stderr)
            state[filename] = {
                **fingerprint,
                "status": "failed",
                "error": str(exc),
                "attempted_at": datetime.now(timezone.utc).isoformat(),
            }
            save_state(state_path, state)
            results["failed"].append(filename)
            continue

        elapsed = time.time() - start
        print(f"  done in {elapsed:.0f}s")
        state[filename] = {
            **fingerprint,
            "status": "success",
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
            "remote_path": remote_path,
        }
        save_state(state_path, state)
        results["success"].append(filename)

    print(
        f"\nSummary: {len(results['success'])} uploaded, "
        f"{len(results['skipped'])} skipped (already done), "
        f"{len(results['failed'])} failed"
    )
    if results["failed"]:
        print(f"Failed: {results['failed']}")
        print("Just re-run this same command — succeeded files will be skipped automatically,")
        print("only the failed one(s) above will be retried.")
        return 1

    print(f"\nAll files present in the volume. Verify in Catalog Explorer, or:")
    print(f"  databricks fs ls dbfs:{volume_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
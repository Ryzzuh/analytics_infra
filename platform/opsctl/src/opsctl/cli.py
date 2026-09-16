"""Command-line face of the golden-snapshot logic, used by infra/golden/golden.sh."""

from __future__ import annotations

import argparse
import hashlib
from datetime import UTC, datetime
from pathlib import Path

from .golden import REQUIRED_VOLUMES, GoldenManifest, restore_plan


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="opsctl")
    sub = parser.add_subparsers(dest="command", required=True)

    write = sub.add_parser("write-manifest")
    write.add_argument("--dir", required=True, type=Path)
    write.add_argument("--golden-at", required=True)
    write.add_argument("--stack-version", default="unknown")

    plan = sub.add_parser("plan-restore")
    plan.add_argument("--dir", required=True, type=Path)

    at = sub.add_parser("golden-at")
    at.add_argument("--dir", required=True, type=Path)

    args = parser.parse_args(argv)

    if args.command == "write-manifest":
        archives = {name: args.dir / f"{name}.tar.zst" for name in REQUIRED_VOLUMES}
        missing = [n for n, p in archives.items() if not p.exists()]
        if missing:
            parser.error(f"archives missing for: {', '.join(missing)}")
        manifest = GoldenManifest(
            golden_at=datetime.fromisoformat(args.golden_at),
            created_at=datetime.now(UTC),
            volumes={name: _sha256(path) for name, path in archives.items()},
            sizes_bytes={name: path.stat().st_size for name, path in archives.items()},
            stack_version=args.stack_version,
        )
        manifest.validate()
        manifest.write(args.dir / "manifest.json")
        print(args.dir / "manifest.json")
        return 0

    manifest = GoldenManifest.read(args.dir / "manifest.json")

    if args.command == "golden-at":
        print(manifest.golden_at.isoformat())
        return 0

    for step in restore_plan(manifest, datetime.now(UTC)):
        print(f"- {step}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

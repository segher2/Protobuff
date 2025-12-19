#!/usr/bin/env python3
"""
Cross-platform Protobuf code generation for Python.

Equivalent to (example):
  python -m grpc_tools.protoc -I proto --python_out=generated proto/sf/v1/*.proto

Works on Windows/macOS/Linux.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def _fail(msg: str, code: int = 1) -> None:
    print(f"[gen_proto] ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--proto-dir",
        default="proto",
        help="Root directory containing .proto files (default: proto)",
    )
    parser.add_argument(
        "--out-dir",
        default="src/sf/v1",
        help="Output directory for generated Python files (default: generated)",
    )
    parser.add_argument(
        "--pattern",
        default="**/*.proto",
        help="Glob pattern under proto-dir (default: **/*.proto)",
    )
    parser.add_argument(
        "--mypy",
        action="store_true",
        help="Also generate .pyi stubs (requires mypy-protobuf).",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    proto_dir = (repo_root / args.proto_dir).resolve()
    out_dir = (repo_root / args.out_dir).resolve()

    if not proto_dir.exists():
        _fail(f"proto dir not found: {proto_dir}")

    proto_files = sorted(proto_dir.glob(args.pattern))
    if not proto_files:
        _fail(f"No .proto files found under {proto_dir} with pattern {args.pattern!r}")

    out_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,  # ensures we use the same Python env you invoked the script with
        "-m",
        "grpc_tools.protoc",
        f"-I{proto_dir}",
        f"--python_out={out_dir}",
    ]

    if args.mypy:
        cmd.append(f"--mypy_out={out_dir}")

    cmd.extend(str(p) for p in proto_files)

    print("[gen_proto] Running:")
    print("  " + " ".join(cmd))

    try:
        subprocess.check_call(cmd)
    except FileNotFoundError as e:
        _fail(f"Could not run protoc via grpc_tools.protoc: {e}")
    except subprocess.CalledProcessError as e:
        _fail(f"protoc failed with exit code {e.returncode}", code=e.returncode)

    print(f"[gen_proto] Done. Generated files are in: {out_dir}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import time
import statistics
import csv
from pathlib import Path
from typing import Any, Dict, List, Callable, Tuple

# =========================================================
# Imports
# =========================================================

# BAG Pand v3
from sfproto.geojson.v3_BAG.geojson_bag import (
    geojson_pand_featurecollection_to_bytes,
    bytes_to_geojson_pand_featurecollection,
    DEFAULT_SRID as BAG_DEFAULT_SRID,
    DEFAULT_SCALE as BAG_DEFAULT_SCALE,
)

# Generic GeoJSON v4 + v7
from sfproto.geojson.v4.geojson import geojson_to_bytes_v4, bytes_to_geojson_v4
from sfproto.geojson.v7.geojson import geojson_to_bytes_v7, bytes_to_geojson_v7

GeoJSON = Dict[str, Any]

# =========================================================
# Helpers
# =========================================================

def _now_ns() -> int:
    return time.perf_counter_ns()

def _stats(samples_ns: List[int]) -> Dict[str, float]:
    ms = [s / 1e6 for s in samples_ns]
    return {
        "mean_ms": statistics.mean(ms),
        "median_ms": statistics.median(ms),
        "stdev_ms": statistics.pstdev(ms) if len(ms) > 1 else 0.0,
        "min_ms": min(ms),
        "max_ms": max(ms),
    }

def _compact_json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)

def _write_bytes(path: Path, data: bytes) -> None:
    path.write_bytes(data)

def _read_bytes(path: Path) -> bytes:
    return path.read_bytes()

def _write_text(path: Path, s: str) -> None:
    path.write_text(s, encoding="utf-8")

def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")

# =========================================================
# Core benchmark primitive
# =========================================================

def benchmark_codec(
    label: str,
    geojson_obj: GeoJSON,
    encode_fn: Callable[[GeoJSON], bytes],
    decode_fn: Callable[[bytes], GeoJSON],
    out_dir: Path,
    runs: int,
    warmup: int,
    fair_geojson: bool,
) -> List[Dict[str, Any]]:
    """
    Returns list of CSV rows
    """
    rows: List[Dict[str, Any]] = []

    # ---- prepare baseline ----
    pb_bytes = encode_fn(geojson_obj)
    decoded_obj = decode_fn(pb_bytes)
    geojson_str = _compact_json(decoded_obj if fair_geojson else geojson_obj)
    geojson_bytes = geojson_str.encode("utf-8")

    pb_path = out_dir / f"{label}.bin"
    gj_path = out_dir / f"{label}.geojson"

    # ---- warmup ----
    for _ in range(warmup):
        encode_fn(geojson_obj)
        decode_fn(pb_bytes)
        _write_bytes(pb_path, pb_bytes)
        _read_bytes(pb_path)
        _write_text(gj_path, geojson_str)
        _read_text(gj_path)

    # ---- CPU encode/decode ----
    for phase, fn in [("encode", lambda: encode_fn(geojson_obj)),
                      ("decode", lambda: decode_fn(pb_bytes))]:
        samples = []
        for _ in range(runs):
            t0 = _now_ns()
            fn()
            samples.append(_now_ns() - t0)

        rows.append({
            "codec": label,
            "phase": phase,
            "stage": "cpu",
            **_stats(samples),
            "size_bytes": len(pb_bytes),
        })

    # ---- raw IO ----
    for phase, fn, size in [
        ("write", lambda: _write_text(gj_path, geojson_str), len(geojson_bytes)),
        ("read", lambda: _read_text(gj_path), len(geojson_bytes)),
        ("write", lambda: _write_bytes(pb_path, pb_bytes), len(pb_bytes)),
        ("read", lambda: _read_bytes(pb_path), len(pb_bytes)),
    ]:
        samples = []
        for _ in range(runs):
            t0 = _now_ns()
            fn()
            samples.append(_now_ns() - t0)

        rows.append({
            "codec": label,
            "phase": phase,
            "stage": "raw_io",
            **_stats(samples),
            "size_bytes": size,
        })

    # ---- end-to-end ----
    for phase, fn, size in [
        ("write", lambda: _write_text(gj_path, _compact_json(geojson_obj)), len(geojson_bytes)),
        ("read", lambda: json.loads(_read_text(gj_path)), len(geojson_bytes)),
        ("write", lambda: _write_bytes(pb_path, encode_fn(geojson_obj)), len(pb_bytes)),
        ("read", lambda: decode_fn(_read_bytes(pb_path)), len(pb_bytes)),
    ]:
        samples = []
        for _ in range(runs):
            t0 = _now_ns()
            fn()
            samples.append(_now_ns() - t0)

        rows.append({
            "codec": label,
            "phase": phase,
            "stage": "end_to_end",
            **_stats(samples),
            "size_bytes": size,
        })

    return rows

# =========================================================
# Runner
# =========================================================

def run_all_benchmarks(
    geojson_path: Path,
    out_dir: Path = Path("bench/bench_out_bag"),
    csv_path: Path = Path("bench/bench_out_bag/results.csv"),
    runs: int = 200,
    warmup: int = 20,
):
    geojson_obj = json.loads(geojson_path.read_text(encoding="utf-8"))
    out_dir.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, Any]] = []

    # BAG Pand v3
    rows += benchmark_codec(
        label="bag_v3",
        geojson_obj=geojson_obj,
        encode_fn=lambda o: geojson_pand_featurecollection_to_bytes(
            o, srid=BAG_DEFAULT_SRID, scale=BAG_DEFAULT_SCALE
        ),
        decode_fn=bytes_to_geojson_pand_featurecollection,
        out_dir=out_dir,
        runs=runs,
        warmup=warmup,
        fair_geojson=True,
    )

    # Generic v4
    rows += benchmark_codec(
        label="v4",
        geojson_obj=geojson_obj,
        encode_fn=lambda o: geojson_to_bytes_v4(o, srid=28992),
        decode_fn=bytes_to_geojson_v4,
        out_dir=out_dir,
        runs=runs,
        warmup=warmup,
        fair_geojson=True,
    )

    # Generic v7
    rows += benchmark_codec(
        label="v7",
        geojson_obj=geojson_obj,
        encode_fn=lambda o: geojson_to_bytes_v7(o, srid=28992, scale=1000),
        decode_fn=bytes_to_geojson_v7,
        out_dir=out_dir,
        runs=runs,
        warmup=warmup,
        fair_geojson=True,
    )

    # ---- write CSV ----
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Benchmark complete. CSV written to: {csv_path.resolve()}")

# =========================================================
# Entry point
# =========================================================

if __name__ == "__main__":
    run_all_benchmarks(
        geojson_path=Path("data/bag_data/bag_pand_10000.geojson"),
        runs=200,
        warmup=20,
    )
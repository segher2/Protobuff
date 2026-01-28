from __future__ import annotations

import json
import os
import time
import statistics
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

from sfproto.sf.v2 import geometry_pb2

GeoJSON = Dict[str, Any]

DEFAULT_SCALE = 10_000_000  # 1e7 for cm accuracy


# =========================
# Your conversion functions
# =========================

def _require_scale(scale: int) -> int:
    scale = int(scale)
    if scale <= 0:
        raise ValueError("scale must be a positive integer (e.g., 10000000)")
    return scale

def _quantize(v: float, scale: int) -> int:
    return int(round(float(v) * scale))

def _dequantize(vi: int, scale: int) -> float:
    return float(vi) / float(scale)

def _quantize_ring(ring: List[List[float]], scale: int) -> List[Tuple[int, int]]:
    if not isinstance(ring, (list, tuple)) or len(ring) < 4:
        raise ValueError("LinearRing must have at least 4 coordinates")

    q: List[Tuple[int, int]] = []
    for j, coord in enumerate(ring):
        if not (isinstance(coord, (list, tuple)) and len(coord) >= 2):
            raise ValueError(f"Polygon coordinates must be [x, y], got {coord!r} at index {j}")
        if coord[0] is None or coord[1] is None:
            raise ValueError(f"Polygon coordinates cannot be null, got {coord!r} at index {j}")
        q.append((_quantize(coord[0], scale), _quantize(coord[1], scale)))

    if q[0] != q[-1]:
        q.append(q[0])

    if len(q) < 4:
        raise ValueError("LinearRing must have at least 4 coordinates (after closure)")

    return q

def _fill_delta_ring(pb_ring: geometry_pb2.DeltaRing, q: List[Tuple[int, int]]) -> None:
    x0, y0 = q[0]
    pb_ring.start.x = int(x0)
    pb_ring.start.y = int(y0)

    prev_x, prev_y = x0, y0
    for (x, y) in q[1:]:
        pb_ring.dx.append(int(x - prev_x))
        pb_ring.dy.append(int(y - prev_y))
        prev_x, prev_y = x, y

def _decode_delta_ring(pb_ring: geometry_pb2.DeltaRing, scale: int) -> List[List[float]]:
    if len(pb_ring.dx) != len(pb_ring.dy):
        raise ValueError(f"DeltaRing dx/dy length mismatch: {len(pb_ring.dx)} vs {len(pb_ring.dy)}")

    coords: List[List[float]] = []
    x = int(pb_ring.start.x)
    y = int(pb_ring.start.y)
    coords.append([_dequantize(x, scale), _dequantize(y, scale)])

    for dx, dy in zip(pb_ring.dx, pb_ring.dy):
        x += int(dx)
        y += int(dy)
        coords.append([_dequantize(x, scale), _dequantize(y, scale)])

    if coords[0] != coords[-1]:
        coords.append(coords[0])

    return coords

def geojson_polygon_to_pb(obj: GeoJSON, srid: int = 0, scale: int = DEFAULT_SCALE) -> geometry_pb2.Geometry:
    if obj.get("type") != "Polygon":
        raise ValueError(f"Expected GeoJSON type=Polygon, got: {obj.get('type')!r}")

    rings = obj.get("coordinates")
    if not isinstance(rings, list) or len(rings) == 0:
        raise ValueError("GeoJSON Polygon must have at least one linear ring")

    scale = _require_scale(scale)

    g = geometry_pb2.Geometry()
    g.crs.srid = int(srid)
    g.crs.scale = int(scale)

    for ring in rings:
        q = _quantize_ring(ring, scale)
        pb_ring = g.polygon.rings.add()
        _fill_delta_ring(pb_ring, q)

    return g

def pb_to_geojson_polygon(g: geometry_pb2.Geometry) -> GeoJSON:
    if not g.HasField("polygon"):
        raise ValueError(f"Expected Geometry.polygon, got oneof={g.WhichOneof('geom')!r}")

    scale = int(getattr(g.crs, "scale", 0)) or DEFAULT_SCALE
    scale = _require_scale(scale)

    coordinates: List[List[List[float]]] = []
    for pb_ring in g.polygon.rings:
        coordinates.append(_decode_delta_ring(pb_ring, scale))

    return {"type": "Polygon", "coordinates": coordinates}

def geojson_polygon_to_bytes_v2(obj_or_json: Union[GeoJSON, str], srid: int = 0, scale: int = DEFAULT_SCALE) -> bytes:
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json
    msg = geojson_polygon_to_pb(obj, srid=srid, scale=scale)
    return msg.SerializeToString()

def bytes_to_geojson_polygon_v2(data: bytes) -> GeoJSON:
    msg = geometry_pb2.Geometry.FromString(data)
    return pb_to_geojson_polygon(msg)


# =========================
# Benchmark helpers
# =========================

def _now_ns() -> int:
    return time.perf_counter_ns()

def _stats_ns(samples_ns: List[int]) -> Dict[str, float]:
    # returns in milliseconds
    ms = [s / 1e6 for s in samples_ns]
    return {
        "n": float(len(ms)),
        "mean_ms": statistics.mean(ms),
        "median_ms": statistics.median(ms),
        "stdev_ms": statistics.pstdev(ms) if len(ms) > 1 else 0.0,
        "min_ms": min(ms),
        "max_ms": max(ms),
    }

def _mb_per_s(total_bytes: int, total_ns: int) -> float:
    if total_ns <= 0:
        return float("inf")
    return (total_bytes / (1024 * 1024)) / (total_ns / 1e9)

def _write_bytes(path: Path, data: bytes) -> None:
    # buffered write
    with path.open("wb") as f:
        f.write(data)

def _read_bytes(path: Path) -> bytes:
    with path.open("rb") as f:
        return f.read()

def _write_text(path: Path, s: str) -> None:
    with path.open("w", encoding="utf-8") as f:
        f.write(s)

def _read_text(path: Path) -> str:
    with path.open("r", encoding="utf-8") as f:
        return f.read()


# =========================
# Main benchmark
# =========================

def benchmark_io(
    geojson_obj: GeoJSON,
    out_dir: Union[str, Path] = "bench_out",
    runs: int = 200,
    warmup: int = 20,
    srid: int = 0,
    scale: int = DEFAULT_SCALE,
) -> None:
    """
    Measures:
      - byte sizes
      - encode/decode times
      - raw disk write/read times
      - end-to-end write (encode+write) and read (read+decode)
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    geojson_str = json.dumps(geojson_obj, separators=(",", ":"), ensure_ascii=False)
    pb_bytes = geojson_polygon_to_bytes_v2(geojson_obj, srid=srid, scale=scale)

    geojson_path = out_dir / "test.geojson"
    pb_path = out_dir / "test.pb"

    # Report sizes
    geojson_bytes = geojson_str.encode("utf-8")
    print("=== Sizes ===")
    print(f"GeoJSON (utf-8 bytes): {len(geojson_bytes):,}")
    print(f"ProtoBuf (bytes):      {len(pb_bytes):,}")
    if len(pb_bytes) > 0:
        print(f"Size ratio (GeoJSON / PB): {len(geojson_bytes)/len(pb_bytes):.2f}x")
    print()

    # Warmup (helps avoid one-time overhead dominating)
    for _ in range(warmup):
        _ = geojson_polygon_to_bytes_v2(geojson_obj, srid=srid, scale=scale)
        _ = bytes_to_geojson_polygon_v2(pb_bytes)
        _write_text(geojson_path, geojson_str)
        _ = _read_text(geojson_path)
        _write_bytes(pb_path, pb_bytes)
        _ = _read_bytes(pb_path)

    # --- encode/decode CPU cost ---
    enc_samples = []
    dec_samples = []

    for _ in range(runs):
        t0 = _now_ns()
        b = geojson_polygon_to_bytes_v2(geojson_obj, srid=srid, scale=scale)
        t1 = _now_ns()
        enc_samples.append(t1 - t0)

        t0 = _now_ns()
        _ = bytes_to_geojson_polygon_v2(b)
        t1 = _now_ns()
        dec_samples.append(t1 - t0)

    print("=== CPU (encode/decode) ===")
    print("Proto encode:", _stats_ns(enc_samples))
    print("Proto decode:", _stats_ns(dec_samples))
    print()

    # --- raw disk IO times (write/read without conversion) ---
    gj_write_samples = []
    gj_read_samples = []
    pb_write_samples = []
    pb_read_samples = []

    for _ in range(runs):
        t0 = _now_ns()
        _write_text(geojson_path, geojson_str)
        t1 = _now_ns()
        gj_write_samples.append(t1 - t0)

        t0 = _now_ns()
        _ = _read_text(geojson_path)
        t1 = _now_ns()
        gj_read_samples.append(t1 - t0)

        t0 = _now_ns()
        _write_bytes(pb_path, pb_bytes)
        t1 = _now_ns()
        pb_write_samples.append(t1 - t0)

        t0 = _now_ns()
        _ = _read_bytes(pb_path)
        t1 = _now_ns()
        pb_read_samples.append(t1 - t0)

    # compute throughput using total bytes and total time
    gj_write_thr = _mb_per_s(len(geojson_bytes) * runs, sum(gj_write_samples))
    gj_read_thr = _mb_per_s(len(geojson_bytes) * runs, sum(gj_read_samples))
    pb_write_thr = _mb_per_s(len(pb_bytes) * runs, sum(pb_write_samples))
    pb_read_thr = _mb_per_s(len(pb_bytes) * runs, sum(pb_read_samples))

    print("=== Raw disk IO ===")
    print("GeoJSON write:", _stats_ns(gj_write_samples), f"throughput_MBps={gj_write_thr:.2f}")
    print("GeoJSON read: ", _stats_ns(gj_read_samples),  f"throughput_MBps={gj_read_thr:.2f}")
    print("PB write:     ", _stats_ns(pb_write_samples), f"throughput_MBps={pb_write_thr:.2f}")
    print("PB read:      ", _stats_ns(pb_read_samples),  f"throughput_MBps={pb_read_thr:.2f}")
    print()

    # --- end-to-end: encode+write and read+decode ---
    pb_e2e_write = []
    pb_e2e_read = []
    gj_e2e_write = []
    gj_e2e_read = []

    for _ in range(runs):
        # GeoJSON: json.dumps + write
        t0 = _now_ns()
        s = json.dumps(geojson_obj, separators=(",", ":"), ensure_ascii=False)
        _write_text(geojson_path, s)
        t1 = _now_ns()
        gj_e2e_write.append(t1 - t0)

        # GeoJSON: read + json.loads
        t0 = _now_ns()
        s2 = _read_text(geojson_path)
        _ = json.loads(s2)
        t1 = _now_ns()
        gj_e2e_read.append(t1 - t0)

        # PB: encode + write
        t0 = _now_ns()
        b = geojson_polygon_to_bytes_v2(geojson_obj, srid=srid, scale=scale)
        _write_bytes(pb_path, b)
        t1 = _now_ns()
        pb_e2e_write.append(t1 - t0)

        # PB: read + decode
        t0 = _now_ns()
        b2 = _read_bytes(pb_path)
        _ = bytes_to_geojson_polygon_v2(b2)
        t1 = _now_ns()
        pb_e2e_read.append(t1 - t0)

    print("=== End-to-end (serialize+IO) ===")
    print("GeoJSON write e2e:", _stats_ns(gj_e2e_write))
    print("GeoJSON read e2e: ", _stats_ns(gj_e2e_read))
    print("PB write e2e:     ", _stats_ns(pb_e2e_write))
    print("PB read e2e:      ", _stats_ns(pb_e2e_read))
    print()

    print(f"Files written to: {out_dir.resolve()}")


# =========================
# How to run
# =========================

if __name__ == "__main__":
    # Option A: load a GeoJSON polygon from a file
    # Put your test polygon in "test_polygon.geojson" or "test_polygon.json"
    # containing a single Polygon object: {"type":"Polygon","coordinates":[...]}
    p = Path("awd")
    if p.exists():
        geojson_obj = json.loads(p.read_text(encoding="utf-8"))
        benchmark_io(geojson_obj, runs=200, warmup=20)
    else:
        # Option B: minimal example polygon if no file is present
        geojson_obj = {
            "type": "Polygon",
            "coordinates": [
                [
                    [4.0, 52.0],
                    [4.1, 52.0],
                    [4.1, 52.1],
                    [4.0, 52.1],
                    [4.0, 52.0],
                ]
            ],
        }
        print("NOTE: test_polygon.geojson not found, using a tiny default polygon.")
        benchmark_io(geojson_obj, runs=200, warmup=20)

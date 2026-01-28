from __future__ import annotations

import json
import time
import statistics
from pathlib import Path
from typing import Any, Dict, List, Union, Callable, Tuple

# =========================
# Import v4 + v5 converters
# =========================
from sfproto.geojson.v4.geojson import geojson_to_bytes_v4, bytes_to_geojson_v4
from sfproto.geojson.v5.geojson import geojson_to_bytes_v5, bytes_to_geojson_v5

GeoJSON = Dict[str, Any]


# =========================
# Benchmark helpers
# =========================

def _now_ns() -> int:
    return time.perf_counter_ns()

def _stats_ns(samples_ns: List[int]) -> Dict[str, float]:
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

def _compact_json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


# =========================
# Fair baseline creation
# =========================

def _fair_geojson_baseline(
    src_obj: GeoJSON,
    encode_fn: Callable[[GeoJSON], bytes],
    decode_fn: Callable[[bytes], GeoJSON],
) -> Tuple[bytes, GeoJSON, bytes]:
    pb_bytes = encode_fn(src_obj)
    fair_geojson_obj = decode_fn(pb_bytes)
    fair_geojson_bytes = _compact_json(fair_geojson_obj).encode("utf-8")
    return pb_bytes, fair_geojson_obj, fair_geojson_bytes


# =========================
# Main benchmark (generic)
# =========================

def benchmark_io_generic(
    geojson_obj: GeoJSON,
    encode_fn: Callable[[GeoJSON], bytes],
    decode_fn: Callable[[bytes], GeoJSON],
    label: str,
    out_dir: Union[str, Path],
    runs: int = 200,
    warmup: int = 20,
    fair_geojson: bool = True,
) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- prepare bytes + baseline JSON ---
    if fair_geojson:
        pb_bytes, _, fair_json_bytes = _fair_geojson_baseline(geojson_obj, encode_fn, decode_fn)
        geojson_str = fair_json_bytes.decode("utf-8")
        geojson_bytes = fair_json_bytes
    else:
        pb_bytes = encode_fn(geojson_obj)
        geojson_str = _compact_json(geojson_obj)
        geojson_bytes = geojson_str.encode("utf-8")

    geojson_path = out_dir / f"{label}.geojson"
    pb_path = out_dir / f"{label}.bin"

    # --- sizes report ---
    print(f"=== Sizes ({label}) ===")
    print(f"GeoJSON (utf-8 bytes): {len(geojson_bytes):,} {'(fair baseline)' if fair_geojson else '(original)'}")
    print(f"Bytes ({label}):       {len(pb_bytes):,}")
    if len(pb_bytes) > 0:
        print(f"Size ratio (GeoJSON / {label}): {len(geojson_bytes)/len(pb_bytes):.2f}x")
    print()

    # --- warmup ---
    for _ in range(warmup):
        _ = encode_fn(geojson_obj)
        _ = decode_fn(pb_bytes)
        _write_text(geojson_path, geojson_str)
        _ = _read_text(geojson_path)
        _write_bytes(pb_path, pb_bytes)
        _ = _read_bytes(pb_path)

    # --- CPU encode/decode ---
    enc_samples: List[int] = []
    dec_samples: List[int] = []

    for _ in range(runs):
        t0 = _now_ns()
        b = encode_fn(geojson_obj)
        t1 = _now_ns()
        enc_samples.append(t1 - t0)

        t0 = _now_ns()
        _ = decode_fn(b)
        t1 = _now_ns()
        dec_samples.append(t1 - t0)

    print(f"=== CPU (encode/decode) ({label}) ===")
    print(f"{label} encode:", _stats_ns(enc_samples))
    print(f"{label} decode:", _stats_ns(dec_samples))
    print()

    # --- raw disk IO (no conversion) ---
    gj_write_samples: List[int] = []
    gj_read_samples: List[int] = []
    pb_write_samples: List[int] = []
    pb_read_samples: List[int] = []

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

    gj_write_thr = _mb_per_s(len(geojson_bytes) * runs, sum(gj_write_samples))
    gj_read_thr  = _mb_per_s(len(geojson_bytes) * runs, sum(gj_read_samples))
    pb_write_thr = _mb_per_s(len(pb_bytes) * runs, sum(pb_write_samples))
    pb_read_thr  = _mb_per_s(len(pb_bytes) * runs, sum(pb_read_samples))

    print(f"=== Raw disk IO ({label}) ===")
    print("GeoJSON write:", _stats_ns(gj_write_samples), f"throughput_MBps={gj_write_thr:.2f}")
    print("GeoJSON read: ", _stats_ns(gj_read_samples),  f"throughput_MBps={gj_read_thr:.2f}")
    print(f"{label} write:", _stats_ns(pb_write_samples), f"throughput_MBps={pb_write_thr:.2f}")
    print(f"{label} read: ", _stats_ns(pb_read_samples),  f"throughput_MBps={pb_read_thr:.2f}")
    print()

    # --- end-to-end: serialize+write and read+decode ---
    pb_e2e_write: List[int] = []
    pb_e2e_read: List[int] = []
    gj_e2e_write: List[int] = []
    gj_e2e_read: List[int] = []

    for _ in range(runs):
        # GeoJSON: dumps + write
        t0 = _now_ns()
        s = geojson_str if fair_geojson else _compact_json(geojson_obj)
        _write_text(geojson_path, s)
        t1 = _now_ns()
        gj_e2e_write.append(t1 - t0)

        # GeoJSON: read + loads
        t0 = _now_ns()
        s2 = _read_text(geojson_path)
        _ = json.loads(s2)
        t1 = _now_ns()
        gj_e2e_read.append(t1 - t0)

        # PB: encode + write
        t0 = _now_ns()
        b = encode_fn(geojson_obj)
        _write_bytes(pb_path, b)
        t1 = _now_ns()
        pb_e2e_write.append(t1 - t0)

        # PB: read + decode
        t0 = _now_ns()
        b2 = _read_bytes(pb_path)
        _ = decode_fn(b2)
        t1 = _now_ns()
        pb_e2e_read.append(t1 - t0)

    print(f"=== End-to-end (serialize+IO) ({label}) ===")
    print("GeoJSON write e2e:", _stats_ns(gj_e2e_write))
    print("GeoJSON read e2e: ", _stats_ns(gj_e2e_read))
    print(f"{label} write e2e: ", _stats_ns(pb_e2e_write))
    print(f"{label} read e2e:  ", _stats_ns(pb_e2e_read))
    print()


# =========================
# v4 + v5 benchmark wrapper
# =========================

def benchmark_v4_v5(
    geojson_obj: GeoJSON,
    out_dir: Union[str, Path] = "bench_out_v4_v5",
    runs: int = 200,
    warmup: int = 20,
    srid: int = 4326,
    scale_v5: int = 1000,     # IMPORTANT: safe for EPSG:28992; adjust if needed
    fair_geojson: bool = True,
) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    def enc_v4(obj: GeoJSON) -> bytes:
        return geojson_to_bytes_v4(obj, srid=srid)

    def dec_v4(b: bytes) -> GeoJSON:
        return bytes_to_geojson_v4(b)

    def enc_v5(obj: GeoJSON) -> bytes:
        return geojson_to_bytes_v5(obj, srid=srid, scale=scale_v5)

    def dec_v5(b: bytes) -> GeoJSON:
        return bytes_to_geojson_v5(b)

    benchmark_io_generic(
        geojson_obj=geojson_obj,
        encode_fn=enc_v4,
        decode_fn=dec_v4,
        label="v4",
        out_dir=out_dir,
        runs=runs,
        warmup=warmup,
        fair_geojson=fair_geojson,
    )

    benchmark_io_generic(
        geojson_obj=geojson_obj,
        encode_fn=enc_v5,
        decode_fn=dec_v5,
        label="v5",
        out_dir=out_dir,
        runs=runs,
        warmup=warmup,
        fair_geojson=fair_geojson,
    )

    print(f"Files written to: {out_dir.resolve()}")


# =========================
# How to run
# =========================

if __name__ == "__main__":
    # Use any GeoJSON your v4/v5 envelope supports:
    # Geometry | GeometryCollection | Feature | FeatureCollection
    p = Path("examples/data/osm_benchmark_10000.geojson")

    if p.exists():
        geojson_obj = json.loads(p.read_text(encoding="utf-8"))
    else:
        geojson_obj = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "A", "value": 1},
                    "geometry": {"type": "Point", "coordinates": [4.9, 52.37]},
                }
            ],
        }
        print("NOTE: input file not found, using a tiny default FeatureCollection.")

    # If your data is EPSG:28992, keep scale_v5 at 1000 (mm) or <= ~3500.
    benchmark_v4_v5(
        geojson_obj,
        out_dir="bench_out_v4_v5",
        runs=200,
        warmup=20,
        srid=4326,        # set to 28992 if that is your dataset CRS
        scale_v5=1000,    # crucial for int32 quantization in v5
        fair_geojson=True,
    )

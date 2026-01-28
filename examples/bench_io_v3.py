from __future__ import annotations

import json
import time
import statistics
from pathlib import Path
from typing import Any, Dict, List, Union, Callable, Tuple

# =========================
# Import your BAG Pand v1 converters (FeatureCollection <-> bytes)
# =========================
from sfproto.geojson.v3_BAG.geojson_bag import (
    geojson_pand_featurecollection_to_bytes,
    bytes_to_geojson_pand_featurecollection,
    DEFAULT_SRID as BAG_DEFAULT_SRID,
    DEFAULT_SCALE as BAG_DEFAULT_SCALE,
)

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
# Fair baseline creation (optional)
# =========================

def _fair_geojson_baseline(
    src_obj: GeoJSON,
    encode_fn: Callable[[GeoJSON], bytes],
    decode_fn: Callable[[bytes], GeoJSON],
) -> Tuple[bytes, GeoJSON, bytes]:
    """
    Returns:
      pb_bytes: protobuf bytes
      fair_geojson_obj: decoded object (what your schema represents)
      fair_geojson_bytes: compact utf-8 JSON bytes of that decoded object

    Use this if you want 'fair GeoJSON' = schema-representable JSON
    (very useful when some input fields are dropped/normalized).
    """
    pb_bytes = encode_fn(src_obj)
    fair_geojson_obj = decode_fn(pb_bytes)
    fair_geojson_bytes = _compact_json(fair_geojson_obj).encode("utf-8")
    return pb_bytes, fair_geojson_obj, fair_geojson_bytes


# =========================
# Main benchmark
# =========================

def benchmark_io_bag_pand_fc(
    geojson_fc: GeoJSON,
    out_dir: Union[str, Path] = "bench_out_bag_pand",
    runs: int = 200,
    warmup: int = 20,
    srid: int = BAG_DEFAULT_SRID,
    scale: int = BAG_DEFAULT_SCALE,
    fair_geojson: bool = True,
) -> None:
    """
    Benchmarks BAG PandFeatureCollection protobuf encoding.

    Measures:
      - byte sizes
      - encode/decode times
      - raw disk write/read times
      - end-to-end write (encode+write) and read (read+decode)

    Fairness option:
      - fair_geojson=True compares PB size/IO vs *round-tripped GeoJSON* (schema-representable)
      - fair_geojson=False compares PB vs original GeoJSON input (may be unfair if input has extra fields)
    """
    if geojson_fc.get("type") != "FeatureCollection":
        raise ValueError(f"Expected GeoJSON type=FeatureCollection, got: {geojson_fc.get('type')!r}")

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    def enc(obj: GeoJSON) -> bytes:
        return geojson_pand_featurecollection_to_bytes(obj, srid=srid, scale=scale)

    def dec(b: bytes) -> GeoJSON:
        return bytes_to_geojson_pand_featurecollection(b)

    # --- prepare bytes + baseline JSON ---
    if fair_geojson:
        pb_bytes, fair_obj, fair_json_bytes = _fair_geojson_baseline(geojson_fc, enc, dec)
        geojson_str = fair_json_bytes.decode("utf-8")
        geojson_bytes = fair_json_bytes
    else:
        pb_bytes = enc(geojson_fc)
        geojson_str = _compact_json(geojson_fc)
        geojson_bytes = geojson_str.encode("utf-8")

    geojson_path = out_dir / "test.geojson"
    pb_path = out_dir / "test.pb"

    # --- sizes report ---
    print("=== Sizes ===")
    print(f"GeoJSON (utf-8 bytes): {len(geojson_bytes):,} {'(fair baseline)' if fair_geojson else '(original)'}")
    print(f"ProtoBuf (bytes):      {len(pb_bytes):,}")
    if len(pb_bytes) > 0:
        print(f"Size ratio (GeoJSON / PB): {len(geojson_bytes)/len(pb_bytes):.2f}x")
    print()

    # --- warmup ---
    for _ in range(warmup):
        print(f"Warmup run {_+1}/{warmup} ...")
        _ = enc(geojson_fc)
        _ = dec(pb_bytes)
        _write_text(geojson_path, geojson_str)
        _ = _read_text(geojson_path)
        _write_bytes(pb_path, pb_bytes)
        _ = _read_bytes(pb_path)

    # --- CPU encode/decode ---
    enc_samples: List[int] = []
    dec_samples: List[int] = []

    for _ in range(runs):
        print(f"Run {_+1}/{runs} CPU benchmarking...")
        t0 = _now_ns()
        b = enc(geojson_fc)
        t1 = _now_ns()
        enc_samples.append(t1 - t0)

        t0 = _now_ns()
        _ = dec(b)
        t1 = _now_ns()
        dec_samples.append(t1 - t0)

    print("=== CPU (encode/decode) ===")
    print("PB encode:", _stats_ns(enc_samples))
    print("PB decode:", _stats_ns(dec_samples))
    print()

    # --- raw disk IO (no conversion) ---
    gj_write_samples: List[int] = []
    gj_read_samples: List[int] = []
    pb_write_samples: List[int] = []
    pb_read_samples: List[int] = []

    for _ in range(runs):
        print(f"Run {_+1}/{runs} raw IO benchmarking...")
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

    print("=== Raw disk IO ===")
    print("GeoJSON write:", _stats_ns(gj_write_samples), f"throughput_MBps={gj_write_thr:.2f}")
    print("GeoJSON read: ", _stats_ns(gj_read_samples),  f"throughput_MBps={gj_read_thr:.2f}")
    print("PB write:     ", _stats_ns(pb_write_samples), f"throughput_MBps={pb_write_thr:.2f}")
    print("PB read:      ", _stats_ns(pb_read_samples),  f"throughput_MBps={pb_read_thr:.2f}")
    print()

    # --- end-to-end: serialize+write and read+decode ---
    pb_e2e_write: List[int] = []
    pb_e2e_read: List[int] = []
    gj_e2e_write: List[int] = []
    gj_e2e_read: List[int] = []

    for _ in range(runs):
        # GeoJSON: dumps + write
        print(f"Run {_+1}/{runs} end-to-end benchmarking...")

        t0 = _now_ns()
        s = _compact_json(geojson_fc if not fair_geojson else json.loads(geojson_str))
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
        b = enc(geojson_fc)
        _write_bytes(pb_path, b)
        t1 = _now_ns()
        pb_e2e_write.append(t1 - t0)

        # PB: read + decode
        t0 = _now_ns()
        b2 = _read_bytes(pb_path)
        _ = dec(b2)
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
    # Put a BAG pand FeatureCollection file here:
    # examples/data/bag_pand_count_1000.geojson
    p = Path("examples/data/bag_pand_2k.geojson")

    if p.exists():
        geojson_obj = json.loads(p.read_text(encoding="utf-8"))
        benchmark_io_bag_pand_fc(
            geojson_obj,
            runs=200,
            warmup=20,
            srid=BAG_DEFAULT_SRID,
            scale=BAG_DEFAULT_SCALE,
            fair_geojson=True,   # recommended
        )
    else:
        # Minimal fallback FeatureCollection with 1 square polygon
        geojson_obj = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "id": "pand.4a7241b2-e5e6-4850-b084-687ab8f675c8",
                    "properties": {
                        "identificatie": "0123456789012345",
                        "bouwjaar": 1990,
                        "status": "Pand in gebruik",
                        "gebruiksdoel": "woonfunctie",
                        "aantal_verblijfsobjecten": 1,
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [100.0, 200.0],
                                [110.0, 200.0],
                                [110.0, 210.0],
                                [100.0, 210.0],
                                [100.0, 200.0],
                            ]
                        ],
                    },
                }
            ],
        }
        print("NOTE: input file not found, using a tiny default FeatureCollection.")
        benchmark_io_bag_pand_fc(
            geojson_obj,
            runs=200,
            warmup=20,
            srid=BAG_DEFAULT_SRID,
            scale=BAG_DEFAULT_SCALE,
            fair_geojson=True,
        )

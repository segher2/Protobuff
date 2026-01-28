from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any

# sfproto
from sfproto.geojson.v4.geojson import geojson_to_bytes_v4
from sfproto.geojson.v7.geojson import geojson_to_bytes_v7

GeoJSON = Dict[str, Any]

# =========================
# Configuration
# =========================

DATA_DIR = Path("examples/data/benchmarks")
FGB_DIR = Path("examples/data/benchmarks_fgb_no_index")
OUT_DIR = Path("bench_out/size")
OUT_DIR.mkdir(parents=True, exist_ok=True)

SRID = 4326
SCALE_V5 = 1000

# =========================
# Helpers
# =========================

def compact_geojson_bytes(obj: GeoJSON) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

# =========================
# Benchmark
# =========================

print("=== Benchmark 01: Representation size ===\n")

results = []

for geojson_path in sorted(DATA_DIR.glob("*.geojson")):
    name = geojson_path.stem
    fgb_path = FGB_DIR / f"{name}.fgb"

    if not fgb_path.exists():
        raise FileNotFoundError(f"Missing FlatGeobuf file: {fgb_path}")

    geojson_obj = json.loads(geojson_path.read_text(encoding="utf-8"))

    # ---- GeoJSON ----
    size_gj = len(compact_geojson_bytes(geojson_obj))

    # ---- sfproto v4 ----
    size_v4 = len(geojson_to_bytes_v4(geojson_obj, srid=SRID))

    # ---- sfproto v5 ----
    size_v5 = len(
        geojson_to_bytes_v7(
            geojson_obj,
            srid=SRID,
            scale=SCALE_V5,
        )
    )

    # ---- FlatGeobuf (reference) ----
    size_fgb = fgb_path.stat().st_size

    print(name)
    print(f"  GeoJSON: {size_gj:>12,} bytes")
    print(f"  v4:      {size_v4:>12,} bytes  ({size_gj/size_v4:6.2f}× smaller)")
    print(f"  v7:      {size_v5:>12,} bytes  ({size_gj/size_v5:6.2f}× smaller)")
    print(f"  FGB:     {size_fgb:>12,} bytes  ({size_gj/size_fgb:6.2f}× smaller)")
    print()

    results.append(
        {
            "dataset": name,
            "geojson": size_gj,
            "v4": size_v4,
            "v5": size_v5,
            "fgb": size_fgb,
        }
    )

# Optional CSV
summary = OUT_DIR / "size_summary.csv"
with summary.open("w", encoding="utf-8") as f:
    f.write("dataset,geojson,v4,v7,fgb\n")
    for r in results:
        f.write(f"{r['dataset']},{r['geojson']},{r['v4']},{r['v5']},{r['fgb']}\n")

print(f"Summary written to {summary.resolve()}")

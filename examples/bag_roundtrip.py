from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Union

# =========================
# Import your BAG Pand v1 converters
# =========================
from sfproto.geojson.v3_BAG.geojson_bag import (
    geojson_pand_featurecollection_to_bytes,
    bytes_to_geojson_pand_featurecollection,
    DEFAULT_SRID as BAG_DEFAULT_SRID,
    DEFAULT_SCALE as BAG_DEFAULT_SCALE,
)

GeoJSON = Dict[str, Any]


def roundtrip_bag_pand_geojson(
    input_geojson: Union[str, Path, GeoJSON],
    out_dir: Union[str, Path] = "roundtrip_out",
    srid: int = BAG_DEFAULT_SRID,
    scale: int = BAG_DEFAULT_SCALE,
) -> None:
    """
    GeoJSON → Protobuf → GeoJSON round-trip.
    Writes both the .pb and the reconstructed .geojson to disk.
    """

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------
    # Load input GeoJSON
    # -------------------------
    if isinstance(input_geojson, (str, Path)):
        input_path = Path(input_geojson)
        print(f"Reading GeoJSON: {input_path}")
        geojson_obj = json.loads(input_path.read_text(encoding="utf-8"))
        base_name = input_path.stem
    else:
        geojson_obj = input_geojson
        base_name = "in_memory"

    if geojson_obj.get("type") != "FeatureCollection":
        raise ValueError("Input must be a GeoJSON FeatureCollection")

    # -------------------------
    # Encode → Protobuf
    # -------------------------
    print("Encoding GeoJSON → Protobuf...")
    pb_bytes = geojson_pand_featurecollection_to_bytes(
        geojson_obj,
        srid=srid,
        scale=scale,
    )

    pb_path = out_dir / f"{base_name}.pb"
    pb_path.write_bytes(pb_bytes)
    print(f"Wrote Protobuf: {pb_path} ({len(pb_bytes):,} bytes)")

    # -------------------------
    # Decode → GeoJSON
    # -------------------------
    print("Decoding Protobuf → GeoJSON...")
    decoded_geojson = bytes_to_geojson_pand_featurecollection(pb_bytes)

    decoded_path = out_dir / f"{base_name}_roundtrip.geojson"
    decoded_path.write_text(
        json.dumps(decoded_geojson, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"Wrote round-tripped GeoJSON: {decoded_path}")
    print()
    print("Round-trip complete.")
    print("Recommended checks:")
    print(f"  • Visual: load both files into QGIS")
    print(f"  • Diff:   jq -S . original.geojson > a.json")
    print(f"           jq -S . {decoded_path.name} > b.json && diff a.json b.json")


# =========================
# CLI entry point
# =========================

if __name__ == "__main__":
    # Example usage:
    # python roundtrip_bag_pand_geojson.py examples/data/bag_pand_2k.geojson

    import argparse

    parser = argparse.ArgumentParser(
        description="Round-trip BAG pand GeoJSON ↔ Protobuf"
    )
    parser.add_argument(
        "input",
        help="Input BAG pand GeoJSON file",
    )
    parser.add_argument(
        "-o",
        "--out",
        default="roundtrip_out",
        help="Output directory (default: roundtrip_out)",
    )
    parser.add_argument(
        "--srid",
        type=int,
        default=BAG_DEFAULT_SRID,
        help="SRID to store in Protobuf (default: EPSG:28992)",
    )
    parser.add_argument(
        "--scale",
        type=int,
        default=BAG_DEFAULT_SCALE,
        help="Coordinate scale factor (default: 1000)",
    )

    args = parser.parse_args()

    roundtrip_bag_pand_geojson(
        input_geojson=args.input,
        out_dir=args.out,
        srid=args.srid,
        scale=args.scale,
    )

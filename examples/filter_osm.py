import json
from collections import defaultdict
from pathlib import Path

# =============================
# User configuration
# =============================

INPUT_GEOJSON = "examples/data/osm_mixed.geojson"
OUT_DIR = Path("examples/data/benchmarks")

SIZES = [10, 100, 1000, 10_000, 100_000]
ATTRIBUTE_PROFILES = ["none", "few", "medium", "many"]
GEOMETRY_REGIMES = ["mixed", "geometry_heavy", "attribute_heavy"]

# =============================
# Attribute schema
# =============================

ATTRIBUTE_SCHEMA = {
    "id": lambda f: f.get("id"),
    "name": lambda f: f["properties"].get("name"),
    "kind": lambda f: (
        f["properties"].get("amenity")
        or f["properties"].get("highway")
        or f["properties"].get("building")
    ),
    "source": lambda f: f["properties"].get("source"),
    "surface": lambda f: f["properties"].get("surface"),
    "lanes": lambda f: f["properties"].get("lanes"),
}

ATTRIBUTE_PROFILE_MAP = {
    "none": [],
    "few": ["id", "kind"],
    "medium": ["id", "kind", "name"],
    "many": list(ATTRIBUTE_SCHEMA.keys()),
}

# =============================
# Geometry regimes
# =============================

GEOMETRY_RATIOS = {
    "mixed": {
        "Point": 1 / 3,
        "LineString": 1 / 3,
        "Polygon": 1 / 3,
    },
    "geometry_heavy": {
        "Point": 0.10,
        "LineString": 0.20,
        "Polygon": 0.70,
    },
    "attribute_heavy": {
        "Point": 0.80,
        "LineString": 0.10,
        "Polygon": 0.10,
    },
}

# =============================
# Load input GeoJSON
# =============================

with open(INPUT_GEOJSON, "r", encoding="utf-8") as f:
    data = json.load(f)

features = data["features"]

# Deterministic ordering safeguard
features.sort(
    key=lambda f: (
        f["geometry"]["type"],
        f.get("id", "")
    )
)

# Group by geometry type
by_geom = defaultdict(list)
for f in features:
    g = f["geometry"]["type"]
    if g in ("Point", "LineString", "Polygon"):
        by_geom[g].append(f)

# =============================
# Dataset construction
# =============================

OUT_DIR.mkdir(parents=True, exist_ok=True)

for size in SIZES:
    for regime in GEOMETRY_REGIMES:
        ratios = GEOMETRY_RATIOS[regime]

        # Determine counts per geometry
        counts = {
            g: int(size * ratios[g])
            for g in ratios
        }

        # Fix rounding leftovers
        while sum(counts.values()) < size:
            counts["Point"] += 1

        # Select features
        base_selection = []
        remaining = size

        for g in ["Point", "LineString", "Polygon"]:
            requested = counts.get(g, 0)
            available = len(by_geom[g])

            take = min(requested, available)
            base_selection.extend(by_geom[g][:take])
            remaining -= take

        # Fill remainder deterministically
        if remaining > 0:
            for g in ["Point", "LineString", "Polygon"]:
                extras = by_geom[g][counts.get(g, 0):]
                take = min(len(extras), remaining)
                base_selection.extend(extras[:take])
                remaining -= take
                if remaining == 0:
                    break

        if len(base_selection) != size:
            raise RuntimeError(
                f"Could only construct {len(base_selection)} features "
                f"out of requested {size} (regime={regime})"
            )


        assert len(base_selection) == size

        for attr_profile in ATTRIBUTE_PROFILES:
            active_attrs = ATTRIBUTE_PROFILE_MAP[attr_profile]

            out_features = []
            for f in base_selection:
                props = {
                    key: ATTRIBUTE_SCHEMA[key](f)
                    for key in active_attrs
                }

                out_features.append({
                    "type": "Feature",
                    "geometry": f["geometry"],
                    "properties": props,
                })

            out = {
                "type": "FeatureCollection",
                "features": out_features,
            }

            filename = (
                f"osm_{regime}_{size}_{attr_profile}.geojson"
            )
            path = OUT_DIR / filename

            with path.open("w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False)

            print(
                f"Wrote {path.name} | "
                f"size={size}, regime={regime}, attrs={attr_profile}"
            )

print("\nAll benchmark datasets generated.")

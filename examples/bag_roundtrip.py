"""
Example test script for BAG pand GeoJSON <-> Protobuf (typed schema)

Assumptions:
- You have compiled the BAG pand .proto into Python, e.g.:
    protoc --python_out=. bag_pand_v1.proto
  which generates something like: bag_pand_v1_pb2.py

- You saved a BAG pand FeatureCollection GeoJSON file in examples/data/, e.g.:
    examples/data/bag_pand_count_1000.geojson

This script mirrors your previous testing style:
- load GeoJSON
- GeoJSON -> bytes (compact)
- GeoJSON -> Protobuf bytes
- Protobuf bytes -> GeoJSON
- GeoJSON -> bytes (fair, compact) from decoded output
- print size comparisons

You can extend it with timing + gzip later for benchmarks.
"""

from __future__ import annotations

import json
from pathlib import Path
import uuid
from typing import Any, Dict, List, Union, Tuple

# Adjust this import to match your generated protobuf module name
from sfproto.sf.v3_BAG import geometry_pb2 as pand_pb2

GeoJSON = Dict[str, Any]

DEFAULT_SRID = 28992
DEFAULT_SCALE = 1000  # mm precision for RD New (EPSG:28992)

# --------------------------- Enum mapping (extend as needed) ---------------------------

STATUS_MAP = {
    "Pand in gebruik": pand_pb2.PAND_IN_GEBRUIK,
    "Verbouwing pand": pand_pb2.VERBOUWING_PAND,
}
STATUS_MAP_REV = {v: k for k, v in STATUS_MAP.items()}

GEBRUIKSDOEL_MAP = {
    "": pand_pb2.GEBRUIKSDOEL_UNSPECIFIED,
    "woonfunctie": pand_pb2.WOONFUNCTIE,
    # add more if encountered
}
GEBRUIKSDOEL_MAP_REV = {v: k for k, v in GEBRUIKSDOEL_MAP.items()}


# --------------------------- Helpers ---------------------------

def load_geojson(relative_path: str) -> GeoJSON:
    base_dir = Path(__file__).parent
    path = base_dir / relative_path
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _q(val: float, scale: int) -> int:
    return int(round(val * scale))


def _uq(val_q: int, scale: int) -> float:
    return val_q / float(scale)


def _feature_id_to_uuid_bytes(feature_id: str) -> bytes:
    """
    GeoJSON id example: "pand.4a7241b2-e5e6-4850-b084-687ab8f675c8"
    Store only UUID bytes (16 bytes).
    """
    if feature_id.startswith("pand."):
        feature_id = feature_id[len("pand."):]
    u = uuid.UUID(feature_id)
    return u.bytes


def _uuid_bytes_to_feature_id(u_bytes: bytes) -> str:
    return "pand." + str(uuid.UUID(bytes=u_bytes))


def _bbox_to_bboxq(bbox: List[float], scale: int) -> pand_pb2.BBoxQ:
    if len(bbox) != 4:
        raise ValueError("bbox must have length 4")
    return pand_pb2.BBoxQ(
        minx=_q(bbox[0], scale),
        miny=_q(bbox[1], scale),
        maxx=_q(bbox[2], scale),
        maxy=_q(bbox[3], scale),
    )


def _bboxq_to_bbox(b: pand_pb2.BBoxQ, scale: int) -> List[float]:
    return [_uq(b.minx, scale), _uq(b.miny, scale), _uq(b.maxx, scale), _uq(b.maxy, scale)]


def _ring_drop_closing_point(coords: List[List[float]]) -> List[List[float]]:
    if len(coords) >= 2 and coords[0][0] == coords[-1][0] and coords[0][1] == coords[-1][1]:
        return coords[:-1]
    return coords


def _encode_delta_ring(ring_coords: List[List[float]], scale: int) -> pand_pb2.DeltaRing:
    ring_coords = _ring_drop_closing_point(ring_coords)

    if len(ring_coords) < 3:
        raise ValueError("Ring must have at least 3 points (excluding closure)")

    qpts: List[Tuple[int, int]] = [(_q(x, scale), _q(y, scale)) for x, y in ring_coords]

    startx, starty = qpts[0]
    dx: List[int] = []
    dy: List[int] = []

    prevx, prevy = startx, starty
    for (x, y) in qpts[1:]:
        dx.append(x - prevx)
        dy.append(y - prevy)
        prevx, prevy = x, y

    return pand_pb2.DeltaRing(
        start=pand_pb2.CoordinateQ(x=startx, y=starty),
        dx=dx,
        dy=dy,
    )


def _decode_delta_ring(r: pand_pb2.DeltaRing, scale: int) -> List[List[float]]:
    if len(r.dx) != len(r.dy):
        raise ValueError("DeltaRing dx/dy length mismatch")

    pts_q: List[Tuple[int, int]] = []
    x = r.start.x
    y = r.start.y
    pts_q.append((x, y))

    for i in range(len(r.dx)):
        x += r.dx[i]
        y += r.dy[i]
        pts_q.append((x, y))

    # close ring for GeoJSON output
    pts_q.append(pts_q[0])

    return [[_uq(px, scale), _uq(py, scale)] for px, py in pts_q]


def _encode_polygon(geom: GeoJSON, scale: int) -> pand_pb2.Polygon:
    if geom.get("type") != "Polygon":
        raise ValueError(f"Expected Polygon, got {geom.get('type')!r}")

    coords = geom.get("coordinates")
    if not isinstance(coords, list) or not coords:
        raise ValueError("Polygon.coordinates must be a non-empty list")

    rings: List[pand_pb2.DeltaRing] = []
    for ring in coords:
        if not isinstance(ring, list) or not ring:
            raise ValueError("Polygon ring must be a non-empty list")
        rings.append(_encode_delta_ring(ring, scale=scale))

    return pand_pb2.Polygon(rings=rings)


def _decode_polygon(poly: pand_pb2.Polygon, scale: int) -> GeoJSON:
    return {
        "type": "Polygon",
        "coordinates": [_decode_delta_ring(r, scale=scale) for r in poly.rings],
    }


def _encode_properties(props: GeoJSON) -> pand_pb2.PandProperties:
    ident_str = props.get("identificatie")
    if not isinstance(ident_str, str) or not ident_str.isdigit():
        raise ValueError("properties.identificatie must be a numeric string")
    ident_u64 = int(ident_str)

    bouwjaar = props.get("bouwjaar")
    if not isinstance(bouwjaar, int):
        raise ValueError("properties.bouwjaar must be an int")

    status_str = props.get("status", "")
    status_enum = STATUS_MAP.get(status_str, pand_pb2.PAND_STATUS_UNSPECIFIED)

    gebruiksdoel_str = props.get("gebruiksdoel", "")
    gebruiksdoel_enum = GEBRUIKSDOEL_MAP.get(gebruiksdoel_str, pand_pb2.GEBRUIKSDOEL_UNSPECIFIED)

    aantal_vo = props.get("aantal_verblijfsobjecten", 0)
    if not isinstance(aantal_vo, int):
        raise ValueError("properties.aantal_verblijfsobjecten must be an int")

    out = pand_pb2.PandProperties(
        identificatie=ident_u64,
        bouwjaar=bouwjaar,
        status=status_enum,
        gebruiksdoel=gebruiksdoel_enum,
        aantal_verblijfsobjecten=aantal_vo,
    )

    # optional fields
    if "oppervlakte_min" in props and props["oppervlakte_min"] is not None:
        out.oppervlakte_min = int(props["oppervlakte_min"])
    if "oppervlakte_max" in props and props["oppervlakte_max"] is not None:
        out.oppervlakte_max = int(props["oppervlakte_max"])

    return out


def _decode_properties(p: pand_pb2.PandProperties) -> GeoJSON:
    props: GeoJSON = {
        "identificatie": f"{p.identificatie:016d}",
        "rdf_seealso": f"http://bag.basisregistraties.overheid.nl/bag/id/pand/{p.identificatie:016d}",
        "bouwjaar": int(p.bouwjaar),
        "status": STATUS_MAP_REV.get(p.status, ""),
        "gebruiksdoel": GEBRUIKSDOEL_MAP_REV.get(p.gebruiksdoel, ""),
        "aantal_verblijfsobjecten": int(p.aantal_verblijfsobjecten),
    }

    if p.HasField("oppervlakte_min"):
        props["oppervlakte_min"] = int(p.oppervlakte_min)
    if p.HasField("oppervlakte_max"):
        props["oppervlakte_max"] = int(p.oppervlakte_max)

    return props


# --------------------------- Encode / Decode (Public) ---------------------------

def geojson_bag_pand_featurecollection_to_bytes(
    obj_or_json: Union[GeoJSON, str],
    srid: int = DEFAULT_SRID,
    scale: int = DEFAULT_SCALE,
) -> bytes:
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    if obj.get("type") != "FeatureCollection":
        raise ValueError(f"Expected FeatureCollection, got {obj.get('type')!r}")

    features = obj.get("features")
    if not isinstance(features, list):
        raise ValueError("FeatureCollection.features must be a list")

    fc = pand_pb2.PandFeatureCollection()
    fc.crs.srid = int(srid)
    fc.crs.scale = int(scale)

    if "bbox" in obj and isinstance(obj["bbox"], list) and len(obj["bbox"]) == 4:
        fc.bbox.CopyFrom(_bbox_to_bboxq(obj["bbox"], scale=scale))

    for feat in features:
        if not isinstance(feat, dict) or feat.get("type") != "Feature":
            raise ValueError("Each features[] item must be a GeoJSON Feature")

        pf = pand_pb2.PandFeature()

        # id -> uuid bytes (optional)
        fid = feat.get("id")
        if isinstance(fid, str):
            pf.uuid = _feature_id_to_uuid_bytes(fid)

        # properties
        props = feat.get("properties", {})
        pf.properties.CopyFrom(_encode_properties(props))

        # bbox (optional)
        if "bbox" in feat and isinstance(feat["bbox"], list) and len(feat["bbox"]) == 4:
            pf.bbox.CopyFrom(_bbox_to_bboxq(feat["bbox"], scale=scale))

        # geometry (Polygon)
        geom = feat.get("geometry")
        if not isinstance(geom, dict):
            raise ValueError("Feature.geometry must be an object")
        pf.geometry.CopyFrom(_encode_polygon(geom, scale=scale))

        fc.features.append(pf)

    return fc.SerializeToString()


def bytes_to_geojson_bag_pand_featurecollection(data: bytes) -> GeoJSON:
    fc = pand_pb2.PandFeatureCollection()
    fc.ParseFromString(data)

    scale = int(fc.crs.scale) if fc.HasField("crs") else DEFAULT_SCALE
    srid = int(fc.crs.srid) if fc.HasField("crs") else 0

    out: GeoJSON = {
        "type": "FeatureCollection",
        "name": "pand",
        "crs": {
            "type": "name",
            "properties": {"name": f"urn:ogc:def:crs:EPSG::{srid}"} if srid else {"name": "unknown"},
        },
        "features": [],
    }

    if fc.HasField("bbox"):
        out["bbox"] = _bboxq_to_bbox(fc.bbox, scale=scale)

    for pf in fc.features:
        feat: GeoJSON = {
            "type": "Feature",
            "properties": _decode_properties(pf.properties),
            "geometry": _decode_polygon(pf.geometry, scale=scale),
        }

        if pf.uuid:
            feat["id"] = _uuid_bytes_to_feature_id(pf.uuid)

        if pf.HasField("bbox"):
            feat["bbox"] = _bboxq_to_bbox(pf.bbox, scale=scale)

        out["features"].append(feat)

    return out


# --------------------------- Test (mirrors your previous script) ---------------------------

if __name__ == "__main__":
    # Load BAG pand FeatureCollection GeoJSON
    geojson_pand_fc = load_geojson("data/bag_pand_50k.geojson")

    # GeoJSON → bytes (compact, no whitespace)
    geojson_bytes_fc = json.dumps(geojson_pand_fc, separators=(",", ":")).encode("utf-8")

    # GeoJSON → Protobuf bytes
    data_fc = geojson_bag_pand_featurecollection_to_bytes(geojson_pand_fc, srid=DEFAULT_SRID, scale=DEFAULT_SCALE)

    # Protobuf bytes → GeoJSON
    out_fc = bytes_to_geojson_bag_pand_featurecollection(data_fc)

    # GeoJSON → bytes fair comparison (compact, no whitespace)
    geojson_bytes_fc_fair = json.dumps(out_fc, separators=(",", ":")).encode("utf-8")

    print(" ======================= BAG PAND FEATURECOLLECTION ============================== ")
    print("geojson featurecollection bytes length:", len(geojson_bytes_fc))
    print("protobuf bag-pands bytes length:", len(data_fc), "vs fair geojson bytes length:", len(geojson_bytes_fc_fair))
    print("================================================")

    # Optional: sanity check one feature
    if out_fc.get("features"):
        print("Sample decoded feature id:", out_fc["features"][0].get("id"))
        print("Sample decoded identificatie:", out_fc["features"][0].get("properties", {}).get("identificatie"))

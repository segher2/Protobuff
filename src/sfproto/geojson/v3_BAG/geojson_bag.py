from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Union, Tuple, Optional

# This module name depends on how you compile your .proto.
# Example:
#   protoc --python_out=. bag_pand_v1.proto
# might generate: bag_pand_v1_pb2.py
#
# Adjust the import below to match your generated filename.
from sfproto.sf.v3_BAG import geometry_pb2 as pand_pb2


GeoJSON = Dict[str, Any]

DEFAULT_SRID = 28992
DEFAULT_SCALE = 1000  # mm precision for EPSG:28992

# --- Enum mapping ---

STATUS_MAP = {
    "Pand in gebruik": pand_pb2.PAND_IN_GEBRUIK,
    "Verbouwing pand": pand_pb2.VERBOUWING_PAND,
    "Sloopvergunning verleend": pand_pb2.SLOOPVERGUNNING_VERLEEND,
    "Bouw gestart": pand_pb2.BOUW_GESTART,
    "Bouwvergunning verleend": pand_pb2.BOUWVERGUNNING_VERLEEND,
}

STATUS_MAP_REV = {v: k for k, v in STATUS_MAP.items()}


GEBRUIKSDOEL_MAP = {
    "woonfunctie": pand_pb2.WOONFUNCTIE,
    "kantoorfunctie": pand_pb2.KANTOORFUNCTIE,
    "winkelfunctie": pand_pb2.WINKELFUNCTIE,
    "industriefunctie": pand_pb2.INDUSTRIEFUNCTIE,
    "bijeenkomstfunctie": pand_pb2.BIJEENKOMSTFUNCTIE,
    "onderwijsfunctie": pand_pb2.ONDERWIJSFUNCTIE,
    "gezondheidszorgfunctie": pand_pb2.GEZONDHEIDSZORGFUNCTIE,
    "sportfunctie": pand_pb2.SPORTFUNCTIE,
    "logiesfunctie": pand_pb2.LOGIESFUNCTIE,
    "overige gebruiksfunctie": pand_pb2.OVERIGE_GEBRUIKSFUNCTIE,
}

GEBRUIKSDOEL_MAP_REV = {v: k for k, v in GEBRUIKSDOEL_MAP.items()}



# --- Quantization helpers ---

def _q(val: float, scale: int) -> int:
    return int(round(val * scale))


def _uq(val_q: int, scale: int) -> float:
    return val_q / float(scale)


def _bbox_to_bboxq(bbox: List[float], scale: int) -> pand_pb2.BBoxQ:
    if len(bbox) != 4:
        raise ValueError(f"Expected bbox length 4, got {len(bbox)}")
    return pand_pb2.BBoxQ(
        minx=_q(bbox[0], scale),
        miny=_q(bbox[1], scale),
        maxx=_q(bbox[2], scale),
        maxy=_q(bbox[3], scale),
    )


def _bboxq_to_bbox(b: pand_pb2.BBoxQ, scale: int) -> List[float]:
    return [_uq(b.minx, scale), _uq(b.miny, scale), _uq(b.maxx, scale), _uq(b.maxy, scale)]


# --- UUID helpers ---

def _feature_id_to_uuid_bytes(feature_id: str) -> bytes:
    """
    GeoJSON id example: "pand.4a7241b2-e5e6-4850-b084-687ab8f675c8"
    Store only the UUID portion as 16 bytes.
    """
    if not isinstance(feature_id, str):
        raise ValueError("Feature.id must be a string")
    if feature_id.startswith("pand."):
        feature_id = feature_id[len("pand."):]
    u = uuid.UUID(feature_id)
    return u.bytes


def _uuid_bytes_to_feature_id(u_bytes: bytes) -> str:
    if not isinstance(u_bytes, (bytes, bytearray)) or len(u_bytes) != 16:
        raise ValueError("uuid bytes must be 16 bytes")
    return "pand." + str(uuid.UUID(bytes=bytes(u_bytes)))


# --- Geometry helpers (Polygon only) ---

def _ring_drop_closing_point(coords: List[List[float]]) -> List[List[float]]:
    """
    coords: [[x,y], [x,y], ..., [x,y]] possibly closed.
    If last equals first, drop last.
    """
    if len(coords) >= 2 and coords[0][0] == coords[-1][0] and coords[0][1] == coords[-1][1]:
        return coords[:-1]
    return coords


def _encode_delta_ring(ring_coords: List[List[float]], scale: int) -> pand_pb2.DeltaRing:
    """
    Encode a ring as start + dx/dy, with implicit closure:
    closing point is omitted if present in input.
    """
    ring_coords = _ring_drop_closing_point(ring_coords)

    if len(ring_coords) < 3:
        raise ValueError("Ring must have at least 3 distinct points (excluding closure)")

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
    """
    Decode ring and CLOSE it for GeoJSON by appending start at the end.
    """
    pts_q: List[Tuple[int, int]] = []
    x = r.start.x
    y = r.start.y
    pts_q.append((x, y))

    if len(r.dx) != len(r.dy):
        raise ValueError("DeltaRing dx/dy length mismatch")

    for i in range(len(r.dx)):
        x += r.dx[i]
        y += r.dy[i]
        pts_q.append((x, y))

    # close ring for GeoJSON
    pts_q.append(pts_q[0])

    return [[_uq(px, scale), _uq(py, scale)] for px, py in pts_q]


def _encode_polygon(geojson_geom: GeoJSON, scale: int) -> pand_pb2.Polygon:
    if geojson_geom.get("type") != "Polygon":
        raise ValueError(f"Expected geometry type Polygon, got {geojson_geom.get('type')!r}")

    coords = geojson_geom.get("coordinates")
    if not isinstance(coords, list) or not coords:
        raise ValueError("Polygon.coordinates must be a non-empty list")

    rings: List[pand_pb2.DeltaRing] = []
    for ring in coords:
        if not isinstance(ring, list) or not ring:
            raise ValueError("Polygon ring must be a non-empty list")
        rings.append(_encode_delta_ring(ring, scale=scale))

    return pand_pb2.Polygon(rings=rings)


def _decode_polygon(poly: pand_pb2.Polygon, scale: int) -> GeoJSON:
    rings_coords: List[List[List[float]]] = []
    for ring in poly.rings:
        rings_coords.append(_decode_delta_ring(ring, scale=scale))
    return {"type": "Polygon", "coordinates": rings_coords}


# --- Properties helpers (PandProperties) ---

def _encode_properties(props: GeoJSON) -> pand_pb2.PandProperties:
    if not isinstance(props, dict):
        raise ValueError("Feature.properties must be an object")

    ident_str = props.get("identificatie")
    if not isinstance(ident_str, str) or not ident_str.isdigit():
        raise ValueError("properties.identificatie must be a numeric string")
    ident_u64 = int(ident_str)

    bouwjaar = props.get("bouwjaar")
    if not isinstance(bouwjaar, int):
        raise ValueError("properties.bouwjaar must be an int")

    status_str = str(props.get("status", ""))
    status_enum = STATUS_MAP.get(status_str, pand_pb2.PAND_STATUS_UNSPECIFIED)

    # --- FIX: multi-valued gebruiksdoel ---
    gebruiksdoel_raw = props.get("gebruiksdoel", "")
    doelen: List[int] = []

    if isinstance(gebruiksdoel_raw, str) and gebruiksdoel_raw.strip():
        for token in gebruiksdoel_raw.split(","):
            token = token.strip()
            enum_val = GEBRUIKSDOEL_MAP.get(token)
            if enum_val is not None:
                doelen.append(enum_val)

    aantal_vo = props.get("aantal_verblijfsobjecten", 0)
    if not isinstance(aantal_vo, int):
        raise ValueError("properties.aantal_verblijfsobjecten must be an int")

    out = pand_pb2.PandProperties(
        identificatie=ident_u64,
        bouwjaar=bouwjaar,
        status=status_enum,
        aantal_verblijfsobjecten=aantal_vo,
    )

    # add repeated enums
    out.gebruiksdoelen.extend(doelen)

    # optional fields
    if props.get("oppervlakte_min") is not None:
        out.oppervlakte_min = int(props["oppervlakte_min"])
    if props.get("oppervlakte_max") is not None:
        out.oppervlakte_max = int(props["oppervlakte_max"])

    return out



def _decode_properties(p: pand_pb2.PandProperties) -> GeoJSON:
    props: GeoJSON = {
        "identificatie": f"{p.identificatie:016d}",
        "bouwjaar": int(p.bouwjaar),
        "status": STATUS_MAP_REV.get(p.status, ""),
        "aantal_verblijfsobjecten": int(p.aantal_verblijfsobjecten),
        "rdf_seealso": f"http://bag.basisregistraties.overheid.nl/bag/id/pand/{p.identificatie:016d}",
    }

    # --- FIX: repeated gebruiksdoelen ---
    if p.gebruiksdoelen:
        doelen = [
            GEBRUIKSDOEL_MAP_REV.get(d, "")
            for d in p.gebruiksdoelen
            if d in GEBRUIKSDOEL_MAP_REV
        ]
        props["gebruiksdoel"] = ",".join(doelen)
    else:
        props["gebruiksdoel"] = ""

    if p.HasField("oppervlakte_min"):
        props["oppervlakte_min"] = int(p.oppervlakte_min)
    if p.HasField("oppervlakte_max"):
        props["oppervlakte_max"] = int(p.oppervlakte_max)

    return props



# --- Public API (mirrors your previous style) ---

def geojson_pand_featurecollection_to_bytes(
    obj_or_json: Union[GeoJSON, str],
    srid: int = DEFAULT_SRID,
    scale: int = DEFAULT_SCALE,
) -> bytes:
    """
    Convert BAG 'pand' GeoJSON FeatureCollection -> PandFeatureCollection Protobuf bytes.

    Assumptions:
    - CRS is EPSG:28992 (srid argument is stored in output regardless of input string)
    - Geometry is Polygon
    - Polygon ring closure is implicit in Protobuf (closing point omitted)
    - identificatie stored as uint64
    """
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    if obj.get("type") != "FeatureCollection":
        raise ValueError(f"Expected GeoJSON type=FeatureCollection, got: {obj.get('type')!r}")

    features = obj.get("features")
    if not isinstance(features, list):
        raise ValueError("FeatureCollection.features must be a list")

    fc = pand_pb2.PandFeatureCollection()
    fc.crs.srid = int(srid)
    fc.crs.scale = int(scale)

    # collection bbox (optional)
    if "bbox" in obj and isinstance(obj["bbox"], list) and len(obj["bbox"]) == 4:
        fc.bbox.CopyFrom(_bbox_to_bboxq(obj["bbox"], scale=scale))

    for feat in features:
        if not isinstance(feat, dict) or feat.get("type") != "Feature":
            raise ValueError("Each item in features must be a GeoJSON Feature object")

        f = pand_pb2.PandFeature()

        # id -> uuid bytes
        fid = feat.get("id")
        if fid is not None:
            f.uuid = _feature_id_to_uuid_bytes(fid)

        # properties
        f.properties.CopyFrom(_encode_properties(feat.get("properties", {})))

        # feature bbox (optional)
        if "bbox" in feat and isinstance(feat["bbox"], list) and len(feat["bbox"]) == 4:
            f.bbox.CopyFrom(_bbox_to_bboxq(feat["bbox"], scale=scale))

        # geometry
        geom = feat.get("geometry")
        if not isinstance(geom, dict):
            raise ValueError("Feature.geometry must be an object")
        f.geometry.CopyFrom(_encode_polygon(geom, scale=scale))

        fc.features.append(f)

    return fc.SerializeToString()


def bytes_to_geojson_pand_featurecollection(data: bytes) -> GeoJSON:
    """
    Convert PandFeatureCollection Protobuf bytes -> GeoJSON FeatureCollection.
    Reconstructs:
    - ring closure (appends start point)
    - identificatie as 16-digit string
    - rdf_seealso from identificatie
    """
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

    for f in fc.features:
        feat: GeoJSON = {
            "type": "Feature",
            "properties": _decode_properties(f.properties),
            "geometry": _decode_polygon(f.geometry, scale=scale),
        }

        if f.uuid:
            feat["id"] = _uuid_bytes_to_feature_id(f.uuid)

        if f.HasField("bbox"):
            feat["bbox"] = _bboxq_to_bbox(f.bbox, scale=scale)

        out["features"].append(feat)

    return out

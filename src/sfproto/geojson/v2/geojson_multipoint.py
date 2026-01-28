from __future__ import annotations

import json
from typing import Any, Dict, List, Union

from sfproto.sf.v2 import geometry_pb2

GeoJSON = Dict[str, Any]

DEFAULT_SCALE = 1000 #10^7 -> gets cm accuracy

def _require_scale(scale: int) -> int:
    scale = int(scale)
    if scale <= 0:
        raise ValueError("scale must be a positive integer (e.g., 10000000)")
    return scale

def _quantize(value: float, scale: int) -> int:
    # round half away from zero isn't needed; Python's round is fine for this use.
    return int(round(float(value) * scale))

def _dequantize(value_i: int, scale: int) -> float:
    return float(value_i) / float(scale)

def geojson_multipoint_to_pb( obj: GeoJSON, srid: int = 0, scale: int = DEFAULT_SCALE) -> geometry_pb2.Geometry:
    """
    Convert a GeoJSON MultiPoint dict -> Protobuf Geometry message.
    """
    if obj.get("type") != "MultiPoint":
        raise ValueError(
            f"Expected GeoJSON type=MultiPoint, got: {obj.get('type')!r}"
        )

    coords = obj.get("coordinates")
    if not isinstance(coords, list):
        raise ValueError("MultiPoint coordinates must be a list")

    g = geometry_pb2.Geometry()
    g.crs.srid = int(srid)
    g.crs.scale = int(scale)

    for coord in coords:
        if not (isinstance(coord, (list, tuple)) and len(coord) >= 2):
            raise ValueError("Each MultiPoint coordinate must be [x, y]")

        p = g.multipoint.points.add()
        p.coord.x = _quantize(coord[0], scale)
        p.coord.y = _quantize(coord[1], scale)

    return g


def pb_to_geojson_multipoint(g: geometry_pb2.Geometry) -> GeoJSON:
    """
    Convert Protobuf Geometry message -> GeoJSON MultiPoint dict.
    """
    if not g.HasField("multipoint"):
        raise ValueError(
            f"Expected Geometry.multipoint, got oneof={g.WhichOneof('geom')!r}"
        )

    scale = int(getattr(g.crs, "scale", 0)) or DEFAULT_SCALE
    scale = _require_scale(scale)
    coordinates: List[List[float]] = []

    for p in g.multipoint.points:
        coordinates.append([_dequantize(p.coord.x,scale), _dequantize(p.coord.y,scale)])

    return {
        "type": "MultiPoint",
        "coordinates": coordinates,
    }


def geojson_multipoint_to_bytes_v2(obj_or_json: Union[GeoJSON, str], srid: int = 0, scale: int = DEFAULT_SCALE) -> bytes:
    """
    Accepts a GeoJSON dict OR JSON string, returns Protobuf-encoded bytes.
    """
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    msg = geojson_multipoint_to_pb(obj, srid=srid, scale=scale)
    return msg.SerializeToString()


def bytes_to_geojson_multipoint_v2(data: bytes) -> GeoJSON:
    """
    Protobuf-encoded bytes -> GeoJSON MultiPoint dict.
    """
    msg = geometry_pb2.Geometry.FromString(data)
    return pb_to_geojson_multipoint(msg)

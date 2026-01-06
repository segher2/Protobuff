from __future__ import annotations

import json
from typing import Any, Dict, Union

from sfproto.sf.v2 import geometry_pb2



GeoJSON = Dict[str, Any]

DEFAULT_SCALE = 10000000 #10^7 -> gets cm accuracy


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

def geojson_point_to_pb(obj: GeoJSON, srid: int = 0, scale: int = DEFAULT_SCALE,) -> geometry_pb2.Geometry:
    """
    Convert a GeoJSON Point dict -> Protobuf Geometry message.
    GeoJSON spec doesn't mandate CRS; srid is an explicit parameter here.
    """
    if obj.get("type") != "Point":
        raise ValueError(f"Expected GeoJSON type=Point, got: {obj.get('type')!r}")

    coords = obj.get("coordinates")
    if not (isinstance(coords, (list, tuple)) and len(coords) >= 2):
        raise ValueError("GeoJSON Point coordinates must be [x, y]")

    x, y = coords[0], coords[1]
    if x is None or y is None:
        raise ValueError("GeoJSON Point coordinates cannot be null")

    scale = _require_scale(scale)

    g = geometry_pb2.Geometry()
    g.crs.srid = int(srid)
    g.crs.scale = int(scale)

    g.point.coord.x = _quantize(x, scale)
    g.point.coord.y = _quantize(y, scale)
    return g


def pb_to_geojson_point(g: geometry_pb2.Geometry) -> GeoJSON:
    """
    Convert Protobuf Geometry message -> GeoJSON Point dict.
    """
    if not g.HasField("point"):
        raise ValueError(f"Expected Geometry.point, got oneof={g.WhichOneof('geom')!r}")

    scale = int(getattr(g.crs, "scale", 0)) or DEFAULT_SCALE
    scale = _require_scale(scale)

    c = g.point.coord
    return {"type": "Point", "coordinates": [_dequantize(c.x,scale), _dequantize(c.y,scale)]}


def geojson_point_to_bytes_v2(obj_or_json: Union[GeoJSON, str], srid: int = 0, scale: int = DEFAULT_SCALE,) -> bytes:
    """
    Accepts a GeoJSON dict OR a JSON string, returns Protobuf-encoded bytes.
    """
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    msg = geojson_point_to_pb(obj, srid=srid, scale=scale)
    return msg.SerializeToString()


def bytes_to_geojson_point_v2(data: bytes) -> GeoJSON:
    """
    Protobuf-encoded bytes -> GeoJSON Point dict.
    """
    msg = geometry_pb2.Geometry.FromString(data)
    return pb_to_geojson_point(msg)

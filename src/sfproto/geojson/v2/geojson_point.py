from __future__ import annotations

import json
from typing import Any, Dict, Union

from sfproto.sf.v2 import geometry_pb2



GeoJSON = Dict[str, Any]

DEFAULT_SCALE = 1000 #parameter for accuacy
# -> strongly relies on which srid, formula to get 'cm' accuracy scaler is in geojson_roundtrip.py file

# scaler necessary for integer storing of coords
def _require_scale(scale: int) -> int:
    scale = int(scale)
    if scale <= 0:
        raise ValueError("scale must be a positive integer (e.g., 10000000)")
    return scale

def _quantize(value: float, scale: int) -> int:
    # multiply the floating number with scaler value and round to a integer
    return int(round(float(value) * scale))

def _dequantize(value_i: int, scale: int) -> float:
    # divide by scaler to get 'normal' float number back again (less precision)
    return float(value_i) / float(scale)

def geojson_point_to_pb(obj: GeoJSON, srid: int = 0, scale: int = DEFAULT_SCALE,) -> geometry_pb2.Geometry:
    """
    Convert a GeoJSON Point dict -> Protobuf Geometry message.
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
    # output GeoJSON Point format
    return {"type": "Point", "coordinates": [_dequantize(c.x,scale), _dequantize(c.y,scale)]}


def geojson_point_to_bytes_v2(obj_or_json: Union[GeoJSON, str], srid: int = 0, scale: int = DEFAULT_SCALE,) -> bytes:
    """
    GeoJSON Point (dict or JSON string) -> Protobuf bytes.
    """
    # if input geojson is string, convert to dict
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    # use message to encode to binary format
    msg = geojson_point_to_pb(obj, srid=srid, scale=scale)
    return msg.SerializeToString()


def bytes_to_geojson_point_v2(data: bytes) -> GeoJSON:
    """
    Protobuf-encoded bytes -> GeoJSON Point dict.
    """
    # use message to decode to GeoJSON format
    msg = geometry_pb2.Geometry.FromString(data)
    return pb_to_geojson_point(msg)

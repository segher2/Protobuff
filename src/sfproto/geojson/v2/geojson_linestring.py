from __future__ import annotations

import json
from typing import Any, Dict, Union, List, Tuple

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

# ============================================================
# GeoJSON LineString -> Protobuf Geometry
# ============================================================

def geojson_linestring_to_pb(obj: GeoJSON, srid: int = 0, scale: int = DEFAULT_SCALE) -> geometry_pb2.Geometry:
    """
    Convert a GeoJSON LineString dict -> Protobuf Geometry message.
    Uses ONLY Point objects internally.
    """
    if obj.get("type") != "LineString":
        raise ValueError(
            f"Expected GeoJSON type=LineString, got {obj.get('type')!r}"
        )

    coords = obj.get("coordinates")
    if not (isinstance(coords, (list, tuple)) and len(coords) >= 2):
        raise ValueError(
            "GeoJSON LineString coordinates must be a list of at least two points"
        )

    scale = _require_scale(scale)

    q: List[Tuple[int,int]] = []
    for i, pair in enumerate(coords):
        if (
            not isinstance(pair, (list, tuple))
            or len(pair) < 2
            or pair[0] is None
            or pair[1] is None
        ):
            raise ValueError(f"Invalid coordinate at index {i}: {pair!r}")
        x, y = pair
        q.append((_quantize(x,scale), _quantize(y,scale)))

        g = geometry_pb2.Geometry()
        g.crs.srid = int(srid)
        g.crs.scale = int(scale)

        ls = g.line_string # delta line_string
        x0,y0 = q[0]
        ls.start.x = x0
        ls.start.y = y0

        # store as delta values from each other
        prev_x, prev_y = x0,y0
        for (x, y) in q[1:]:
            ls.dx.append(int(x-prev_x))
            ls.dy.append(int(y-prev_y))
            prev_x, prev_y = x, y

    return g


# ============================================================
# Protobuf Geometry -> GeoJSON LineString
# ============================================================

def pb_to_geojson_linestring(g: geometry_pb2.Geometry) -> GeoJSON:
    """
    Convert Protobuf Geometry message -> GeoJSON LineString dict.
    """
    if not g.HasField("line_string"):
        raise ValueError(
            f"Expected Geometry.line_string, got oneof={g.WhichOneof('geom')!r}"
        )

    scale = int(getattr(g.crs, "scale", 0)) or DEFAULT_SCALE
    scale = _require_scale(scale)

    ls = g.line_string # delta line_string

    coords_out: List[List[float]] = []

    #start point
    x = int(ls.start.x)
    y = int(ls.start.y)
    coords_out.append([_dequantize(x,scale), _dequantize(y,scale)])

    for dx, dy in zip(ls.dx, ls.dy):
        x += int(dx)
        y += int(dy)
        coords_out.append([_dequantize(x,scale), _dequantize(y,scale)])

    return {
        "type": "LineString",
        "coordinates": coords_out
    }


# ============================================================
# Bytes helpers
# ============================================================

def geojson_linestring_to_bytes_v2(obj_or_json: Union[GeoJSON, str], srid: int = 0, scale: int = DEFAULT_SCALE) -> bytes:
    """
    GeoJSON LineString (dict or JSON string) -> Protobuf bytes.
    """
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    msg = geojson_linestring_to_pb(obj, srid=srid, scale=scale)
    return msg.SerializeToString()


def bytes_to_geojson_linestring_v2(data: bytes) -> GeoJSON:
    """
    Protobuf-encoded bytes -> GeoJSON LineString dict.
    """
    msg = geometry_pb2.Geometry.FromString(data)
    return pb_to_geojson_linestring(msg)



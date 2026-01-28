from __future__ import annotations

import json
from typing import Any, Dict, Union

from sfproto.sf.v1 import geometry_pb2

GeoJSON = Dict[str, Any]


# ============================================================
# GeoJSON LineString -> Protobuf Geometry
# ============================================================

def geojson_linestring_to_pb( obj: GeoJSON, srid: int = 0) -> geometry_pb2.Geometry:
    """
    Convert a GeoJSON LineString dict -> Protobuf Geometry message.
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

    g = geometry_pb2.Geometry()
    g.crs.srid = int(srid)

    for i, pair in enumerate(coords):
        if (
            not isinstance(pair, (list, tuple))
            or len(pair) < 2
            or pair[0] is None
            or pair[1] is None
        ):
            raise ValueError(f"Invalid coordinate at index {i}: {pair!r}")

        x, y = pair
        p = g.line_string.points.add()
        p.coord.x = float(x)
        p.coord.y = float(y)

    return g


# ============================================================
# Protobuf Geometry -> GeoJSON LineString
# ============================================================

def pb_to_geojson_linestring( g: geometry_pb2.Geometry) -> GeoJSON:
    """
    Convert Protobuf Geometry message -> GeoJSON LineString dict.
    """
    if not g.HasField("line_string"):
        raise ValueError(
            f"Expected Geometry.line_string, got oneof={g.WhichOneof('geom')!r}"
        )

    return {
        "type": "LineString",
        "coordinates": [
            [p.coord.x, p.coord.y]
            for p in g.line_string.points
        ],
    }


def geojson_linestring_to_bytes(obj_or_json: Union[GeoJSON, str],srid: int = 0) -> bytes:
    """
    GeoJSON LineString (dict or JSON string) -> Protobuf bytes.
    """
    # if input geojson is string, convert to dict
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    msg = geojson_linestring_to_pb(obj, srid=srid)
    return msg.SerializeToString()


def bytes_to_geojson_linestring(data: bytes) -> GeoJSON:
    """
    Protobuf-encoded bytes -> GeoJSON LineString dict.
    """
    msg = geometry_pb2.Geometry.FromString(data)
    return pb_to_geojson_linestring(msg)

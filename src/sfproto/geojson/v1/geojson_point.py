from __future__ import annotations

import json
from typing import Any, Dict, Union

from sfproto.sf.v1 import geometry_pb2



GeoJSON = Dict[str, Any]


def geojson_point_to_pb(obj: GeoJSON, srid: int = 0) -> geometry_pb2.Geometry:
    """
    Convert a GeoJSON Point dict -> Protobuf Geometry message.
    """
    if obj.get("type") != "Point":
        raise ValueError(f"Expected GeoJSON type=Point, got: {obj.get('type')!r}")

    coords = obj.get("coordinates")
    if not (isinstance(coords, (list, tuple)) and len(coords) >= 2):
        raise ValueError("GeoJSON Point coordinates must be [x, y]")

    # use Coordinate and Point messages to create Geometry message
    x, y = coords[0], coords[1]
    if x is None or y is None:
        raise ValueError("GeoJSON Point coordinates cannot be null")

    g = geometry_pb2.Geometry()
    g.crs.srid = int(srid)
    g.point.coord.x = float(x)
    g.point.coord.y = float(y)
    return g


def pb_to_geojson_point(g: geometry_pb2.Geometry) -> GeoJSON:
    """
    Convert Protobuf Geometry message -> GeoJSON Point dict.
    """
    if not g.HasField("point"):
        raise ValueError(f"Expected Geometry.point, got oneof={g.WhichOneof('geom')!r}")

    c = g.point.coord
    # output GeoJSON Point format
    return {"type": "Point", "coordinates": [c.x, c.y]}


def geojson_point_to_bytes(obj_or_json: Union[GeoJSON, str], srid: int = 0) -> bytes:
    """
    GeoJSON Point (dict or JSON string) -> Protobuf bytes.
    """
    # if input geojson is string, convert to dict
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    # use message to encode to binary format
    msg = geojson_point_to_pb(obj, srid=srid)
    return msg.SerializeToString()


def bytes_to_geojson_point(data: bytes) -> GeoJSON:
    """
    Protobuf-encoded bytes -> GeoJSON Point dict.
    """
    # use message to decode to GeoJSON format
    msg = geometry_pb2.Geometry.FromString(data)
    return pb_to_geojson_point(msg)

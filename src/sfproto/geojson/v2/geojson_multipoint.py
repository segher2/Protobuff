from __future__ import annotations

import json
from typing import Any, Dict, List, Union

from sfproto.sf.v1 import geometry_pb2

GeoJSON = Dict[str, Any]


def geojson_multipoint_to_pb(
    obj: GeoJSON, srid: int = 0
) -> geometry_pb2.Geometry:
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

    for coord in coords:
        if not (isinstance(coord, (list, tuple)) and len(coord) >= 2):
            raise ValueError("Each MultiPoint coordinate must be [x, y]")

        p = g.multipoint.points.add()
        p.coord.x = float(coord[0])
        p.coord.y = float(coord[1])

    return g


def pb_to_geojson_multipoint(g: geometry_pb2.Geometry) -> GeoJSON:
    """
    Convert Protobuf Geometry message -> GeoJSON MultiPoint dict.
    """
    if not g.HasField("multipoint"):
        raise ValueError(
            f"Expected Geometry.multipoint, got oneof={g.WhichOneof('geom')!r}"
        )

    coordinates: List[List[float]] = []

    for p in g.multipoint.points:
        coordinates.append([p.coord.x, p.coord.y])

    return {
        "type": "MultiPoint",
        "coordinates": coordinates,
    }


def geojson_multipoint_to_bytes(
    obj_or_json: Union[GeoJSON, str], srid: int = 0
) -> bytes:
    """
    Accepts a GeoJSON dict OR JSON string, returns Protobuf-encoded bytes.
    """
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    msg = geojson_multipoint_to_pb(obj, srid=srid)
    return msg.SerializeToString()


def bytes_to_geojson_multipoint(data: bytes) -> GeoJSON:
    """
    Protobuf-encoded bytes -> GeoJSON MultiPoint dict.
    """
    msg = geometry_pb2.Geometry.FromString(data)
    return pb_to_geojson_multipoint(msg)

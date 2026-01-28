from __future__ import annotations

import json
from typing import Any, Dict, List, Union

from sfproto.sf.v1 import geometry_pb2

GeoJSON = Dict[str, Any]


def geojson_polygon_to_pb(obj: GeoJSON, srid: int = 0) -> geometry_pb2.Geometry:
    """
    Convert a GeoJSON Polygon dict -> Protobuf Geometry message.
    """
    if obj.get("type") != "Polygon":
        raise ValueError(f"Expected GeoJSON type=Polygon, got: {obj.get('type')!r}")

    rings = obj.get("coordinates")
    if not isinstance(rings, list) or len(rings) == 0:
        raise ValueError("GeoJSON Polygon must have at least one linear ring")

    g = geometry_pb2.Geometry()
    g.crs.srid = int(srid)

    # use Coordinate, LinearRing and Polygon messages to create a Geometry message
    for ring in rings:
        if not isinstance(ring, list) or len(ring) < 4:
            raise ValueError("LinearRing must have at least 4 coordinates")

        pb_ring = g.polygon.rings.add()
        for coord in ring:
            if not (isinstance(coord, (list, tuple)) and len(coord) >= 2):
                raise ValueError("Polygon coordinates must be [x, y]")

            c = pb_ring.coords.add()
            c.x = float(coord[0])
            c.y = float(coord[1])

    return g


def pb_to_geojson_polygon(g: geometry_pb2.Geometry) -> GeoJSON:
    """
    Convert Protobuf Geometry message -> GeoJSON Polygon dict.
    """
    if not g.HasField("polygon"):
        raise ValueError(
            f"Expected Geometry.polygon, got oneof={g.WhichOneof('geom')!r}"
        )

    coordinates: List[List[List[float]]] = []

    for ring in g.polygon.rings:
        ring_coords = []
        for c in ring.coords:
            ring_coords.append([c.x, c.y])
        coordinates.append(ring_coords)

    # output GeoJSON Polygon format
    return {
        "type": "Polygon",
        "coordinates": coordinates,
    }


def geojson_polygon_to_bytes(obj_or_json: Union[GeoJSON, str], srid: int = 0) -> bytes:
    """
    GeoJSON Polygon (dict or JSON string) -> Protobuf bytes.
    """
    # if input geojson is string, convert to dict
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    # use message to encode to binary format
    msg = geojson_polygon_to_pb(obj, srid=srid)
    return msg.SerializeToString()


def bytes_to_geojson_polygon(data: bytes) -> GeoJSON:
    """
    Protobuf-encoded bytes -> GeoJSON Polygon dict.
    """
    # use message to decode to GeoJSON format
    msg = geometry_pb2.Geometry.FromString(data)
    return pb_to_geojson_polygon(msg)

from __future__ import annotations

import json
from typing import Any, Dict, List, Union

from sfproto.sf.v1 import geometry_pb2

GeoJSON = Dict[str, Any]


def geojson_multipolygon_to_pb(obj: GeoJSON, srid: int = 0) -> geometry_pb2.Geometry:
    """
    Convert GeoJSON MultiPolygon -> Protobuf Geometry
    """
    if obj.get("type") != "MultiPolygon":
        raise ValueError(
            f"Expected GeoJSON type=MultiPolygon, got {obj.get('type')!r}"
        )

    polygons = obj.get("coordinates")
    if not isinstance(polygons, list):
        raise ValueError("MultiPolygon coordinates must be a list")

    g = geometry_pb2.Geometry()
    g.crs.srid = int(srid)

    # use Coordinate, LinearRing, Polygon and MultiPolygon messages to create a Geometry message
    for poly in polygons:
        pb_poly = g.multipolygon.polygons.add()

        if not isinstance(poly, list):
            raise ValueError("Polygon must be a list of linear rings")

        for ring in poly:
            pb_ring = pb_poly.rings.add()

            if not isinstance(ring, list) or len(ring) < 4:
                raise ValueError("LinearRing must have at least 4 points")

            for coord in ring:
                if not (isinstance(coord, (list, tuple)) and len(coord) >= 2):
                    raise ValueError("Coordinates must be [x, y]")

                c = pb_ring.coords.add()
                c.x = float(coord[0])
                c.y = float(coord[1])

    return g


def pb_to_geojson_multipolygon(g: geometry_pb2.Geometry) -> GeoJSON:
    """
    Convert Protobuf Geometry -> GeoJSON MultiPolygon
    """
    if not g.HasField("multipolygon"):
        raise ValueError(
            f"Expected Geometry.multipolygon, got {g.WhichOneof('geom')!r}"
        )

    coordinates: List[List[List[List[float]]]] = []

    for poly in g.multipolygon.polygons:
        poly_coords = []

        for ring in poly.rings:
            ring_coords = []
            for c in ring.coords:
                ring_coords.append([c.x, c.y])
            poly_coords.append(ring_coords)

        coordinates.append(poly_coords)

    # output Multipolygon GeoJSON format
    return {
        "type": "MultiPolygon",
        "coordinates": coordinates,
    }


def geojson_multipolygon_to_bytes(obj_or_json: Union[GeoJSON, str], srid: int = 0) -> bytes:
    """
    GeoJSON MultiPolygon (dict or JSON string) -> Protobuf bytes.
    """
    # if input geojson is string, convert to dict
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    # use message to encode to binary format
    msg = geojson_multipolygon_to_pb(obj, srid=srid)
    return msg.SerializeToString()


def bytes_to_geojson_multipolygon(data: bytes) -> GeoJSON:
    """
    Protobuf-encoded bytes -> GeoJSON MultiPolygon dict.
    """
    # use message to decode to GeoJSON format
    msg = geometry_pb2.Geometry.FromString(data)
    return pb_to_geojson_multipolygon(msg)

from __future__ import annotations

import json
from typing import Any, Dict, List, Union

from sfproto.geojson.v2.geojson_point import geojson_point_to_bytes_v2, bytes_to_geojson_point_v2
from sfproto.geojson.v2.geojson_multipoint import geojson_multipoint_to_bytes_v2, bytes_to_geojson_multipoint_v2
from sfproto.geojson.v2.geojson_linestring import geojson_linestring_to_bytes_v2, bytes_to_geojson_linestring_v2
from sfproto.geojson.v2.geojson_multilinestring import geojson_multilinestring_to_bytes_v2, bytes_to_geojson_multilinestring_v2
from sfproto.geojson.v2.geojson_polygon import geojson_polygon_to_bytes_v2, bytes_to_geojson_polygon_v2
from sfproto.geojson.v2.geojson_multipolygon import geojson_multipolygon_to_bytes_v2, bytes_to_geojson_multipolygon_v2

GeoJSON = Dict[str, Any]

DEFAULT_SCALE = 1000 #10^7 -> gets cm accuracy

def geojson_geometry_to_bytes(geometry: GeoJSON, srid: int = 0, scale: int = DEFAULT_SCALE) -> bytes:
    """
    Convert a GeoJSON *geometry object* -> Protobuf Geometry bytes.
    (This is used internally for GeometryCollection.)
    """
    if not isinstance(geometry, dict):
        raise ValueError("Geometry must be a dict")

    gtype = geometry.get("type")
    if gtype is None:
        raise ValueError("Geometry.type is required")

    if gtype == "Point":
        return geojson_point_to_bytes_v2(geometry, srid=srid, scale=scale)

    if gtype == "MultiPoint":
        return geojson_multipoint_to_bytes_v2(geometry, srid=srid, scale=scale)

    if gtype == "LineString":
        return geojson_linestring_to_bytes_v2(geometry, srid=srid, scale=scale)

    if gtype == "MultiLineString":
        return geojson_multilinestring_to_bytes_v2(geometry, srid=srid, scale=scale)

    if gtype == "Polygon":
        return geojson_polygon_to_bytes_v2(geometry, srid=srid, scale=scale)

    if gtype == "MultiPolygon":
        return geojson_multipolygon_to_bytes_v2(geometry, srid=srid, scale=scale)

    # Spec allows nested GeometryCollections, but in many Simple Features contexts
    # it is excluded. Keep it explicit and safe.
    if gtype == "GeometryCollection":
        raise ValueError("Nested GeometryCollection is not supported")

    raise ValueError(f"Unsupported geometry type: {gtype!r}")


def bytes_to_geojson_geometry(data: bytes) -> GeoJSON:
    """
    Convert Protobuf Geometry bytes -> GeoJSON *geometry object*.
    Tries each supported geometry decoder.
    """
    for decoder in (
        bytes_to_geojson_point_v2,
        bytes_to_geojson_multipoint_v2,
        bytes_to_geojson_linestring_v2,
        bytes_to_geojson_multilinestring_v2,
        bytes_to_geojson_polygon_v2,
        bytes_to_geojson_multipolygon_v2,
    ):
        try:
            return decoder(data)
        except Exception:
            pass

    raise ValueError("Bytes do not contain a supported Geometry")


def geojson_geometrycollection_to_bytes_v2( obj_or_json: Union[GeoJSON, str], srid: int = 0, scale: int = DEFAULT_SCALE) -> List[bytes]:
    """
    Convert GeoJSON GeometryCollection -> list of Protobuf Geometry bytes.
    Each geometry is encoded separately (like your FeatureCollection approach).
    """
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    if obj.get("type") != "GeometryCollection":
        raise ValueError(
            f"Expected GeoJSON type=GeometryCollection, got: {obj.get('type')!r}"
        )

    geometries = obj.get("geometries")
    if not isinstance(geometries, list):
        raise ValueError("GeometryCollection.geometries must be a list")

    data: List[bytes] = []
    for geom in geometries:
        data.append(geojson_geometry_to_bytes(geom, srid=srid, scale=scale))

    return data


def bytes_to_geojson_geometrycollection_v2(data: List[bytes]) -> GeoJSON:
    """
    Convert list of Protobuf Geometry bytes -> GeoJSON GeometryCollection.
    """
    if not isinstance(data, list):
        raise ValueError("Expected a list of bytes for GeometryCollection")

    geometries: List[GeoJSON] = []
    for item in data:
        geometries.append(bytes_to_geojson_geometry(item))

    return {
        "type": "GeometryCollection",
        "geometries": geometries,
    }

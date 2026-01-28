from __future__ import annotations

import json
from typing import Any, Dict, List, Union

from sfproto.geojson.v1.geojson_point import geojson_point_to_bytes, bytes_to_geojson_point
from sfproto.geojson.v1.geojson_multipoint import geojson_multipoint_to_bytes, bytes_to_geojson_multipoint
from sfproto.geojson.v1.geojson_linestring import geojson_linestring_to_bytes, bytes_to_geojson_linestring
from sfproto.geojson.v1.geojson_multilinestring import geojson_multilinestring_to_bytes, bytes_to_geojson_multilinestring
from sfproto.geojson.v1.geojson_polygon import geojson_polygon_to_bytes, bytes_to_geojson_polygon
from sfproto.geojson.v1.geojson_multipolygon import geojson_multipolygon_to_bytes, bytes_to_geojson_multipolygon

GeoJSON = Dict[str, Any]

# get a geometry type and encode 1 geometry to bytes
def geojson_geometry_to_bytes(geometry: GeoJSON, srid: int = 0) -> bytes:
    """
    Convert a GeoJSON *geometry object* -> Protobuf Geometry bytes.
    """
    if not isinstance(geometry, dict):
        raise ValueError("Geometry must be a dict")

    gtype = geometry.get("type")
    if gtype is None:
        raise ValueError("Geometry.type is required")

    if gtype == "Point":
        return geojson_point_to_bytes(geometry, srid=srid)

    if gtype == "MultiPoint":
        return geojson_multipoint_to_bytes(geometry, srid=srid)

    if gtype == "LineString":
        return geojson_linestring_to_bytes(geometry, srid=srid)

    if gtype == "MultiLineString":
        return geojson_multilinestring_to_bytes(geometry, srid=srid)

    if gtype == "Polygon":
        return geojson_polygon_to_bytes(geometry, srid=srid)

    if gtype == "MultiPolygon":
        return geojson_multipolygon_to_bytes(geometry, srid=srid)

    if gtype == "GeometryCollection":
        raise ValueError("Nested GeometryCollection is not supported")

    raise ValueError(f"Unsupported geometry type: {gtype!r}")

# decode 1 geometry
def bytes_to_geojson_geometry(data: bytes) -> GeoJSON:
    """
    Convert Protobuf Geometry bytes -> GeoJSON *geometry object*.
    Tries each supported geometry decoder.
    """
    for decoder in (
        bytes_to_geojson_point,
        bytes_to_geojson_multipoint,
        bytes_to_geojson_linestring,
        bytes_to_geojson_multilinestring,
        bytes_to_geojson_polygon,
        bytes_to_geojson_multipolygon,
    ):
        try:
            return decoder(data)
        except Exception:
            pass

    raise ValueError("Bytes do not contain a supported Geometry")


def geojson_geometrycollection_to_bytes(obj_or_json: Union[GeoJSON, str], srid: int = 0) -> List[bytes]:
    """
    Convert GeoJSON GeometryCollection -> list of Protobuf Geometry bytes.
    Each geometry is encoded separately (like your FeatureCollection approach).
    """
    # if input geojson is string, convert to dict
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    # only use this function if input type is geometry collection
    if obj.get("type") != "GeometryCollection":
        raise ValueError(
            f"Expected GeoJSON type=GeometryCollection, got: {obj.get('type')!r}"
        )

    geometries = obj.get("geometries")
    if not isinstance(geometries, list):
        raise ValueError("GeometryCollection.geometries must be a list")

    # make a list for encoding geometries
    data: List[bytes] = []
    for geom in geometries:
        # append the geometries to a byte list
        data.append(geojson_geometry_to_bytes(geom, srid=srid))

    return data


def bytes_to_geojson_geometrycollection(data: List[bytes]) -> GeoJSON:
    """
    Convert list of Protobuf Geometry bytes -> GeoJSON GeometryCollection.
    """
    if not isinstance(data, list):
        raise ValueError("Expected a list of bytes for GeometryCollection")

    geometries: List[GeoJSON] = []
    for item in data:
        geometries.append(bytes_to_geojson_geometry(item))

    # output geometry collection format
    return {
        "type": "GeometryCollection",
        "geometries": geometries,
    }

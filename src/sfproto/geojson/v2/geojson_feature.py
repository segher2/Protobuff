from __future__ import annotations

import json
from typing import Any, Dict, Union
from sfproto.geojson.v2.geojson_point import geojson_point_to_bytes_v2, bytes_to_geojson_point_v2
from sfproto.geojson.v2.geojson_polygon import geojson_polygon_to_bytes_v2, bytes_to_geojson_polygon_v2
from sfproto.geojson.v2.geojson_multipolygon import geojson_multipolygon_to_bytes_v2, bytes_to_geojson_multipolygon_v2
from sfproto.geojson.v2.geojson_multipoint import geojson_multipoint_to_bytes_v2, bytes_to_geojson_multipoint_v2
from sfproto.geojson.v2.geojson_linestring import geojson_linestring_to_bytes_v2, bytes_to_geojson_linestring_v2
from sfproto.geojson.v2.geojson_multilinestring import geojson_multilinestring_to_bytes_v2, bytes_to_geojson_multilinestring_v2

GeoJSON = Dict[str, Any]

DEFAULT_SCALE = 1000 #parameter for accuacy
# -> strongly relies on which srid, formula to get 'cm' accuracy scaler is in geojson_roundtrip.py file


def geojson_feature_to_bytes_v2(obj_or_json: Union[GeoJSON, str], srid: int = 0, scale: int = DEFAULT_SCALE) -> bytes:
    """
    Convert GeoJSON Feature -> Protobuf Geometry bytes.
    Properties are ignored (always null).
    """
    # if input geojson is string, convert to dict
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    # only use this function if input type is feature
    if obj.get("type") != "Feature":
        raise ValueError(
            f"Expected GeoJSON type=Feature, got: {obj.get('type')!r}"
        )
    # get geometry for encoding
    geometry = obj.get("geometry")
    if geometry is None:
        raise ValueError("Feature.geometry cannot be null")

    gtype = geometry.get("type")

    # get the geoemtry type and use the correct function (with scaling factor) for that type
    if gtype == "Point":
        return geojson_point_to_bytes_v2(geometry, srid=srid, scale=scale)

    if gtype == "MultiPoint":
        return geojson_multipoint_to_bytes_v2(geometry, srid=srid, scale=scale)

    if gtype == "Polygon":
        return geojson_polygon_to_bytes_v2(geometry, srid=srid, scale=scale)

    if gtype == "MultiPolygon":
        return geojson_multipolygon_to_bytes_v2(geometry, srid=srid, scale=scale)

    if gtype == "LineString":
        return geojson_linestring_to_bytes_v2(geometry, srid=srid, scale=scale)

    if gtype == "MultiLineString":
        return geojson_multilinestring_to_bytes_v2(geometry, srid=srid, scale=scale)

    raise ValueError(f"Unsupported Feature geometry type: {gtype!r}")


def bytes_to_geojson_feature_v2(data: bytes) -> GeoJSON:
    """
    Convert Protobuf Geometry bytes -> GeoJSON Feature.
    Properties are always null.
    """
    # Try each geometry decoder
    for decoder in (
        bytes_to_geojson_point_v2,
        bytes_to_geojson_multipoint_v2,
        bytes_to_geojson_polygon_v2,
        bytes_to_geojson_multipolygon_v2,
        bytes_to_geojson_linestring_v2,
        bytes_to_geojson_multilinestring_v2,
    ):
        try:
            # try the geoemtry decoder function, the function with the same geometry type should then work
            geometry = decoder(data)
            # output geojson Feature format
            return {
                "type": "Feature",
                "geometry": geometry,
                "properties": None,
            }
        except Exception:
            pass

    raise ValueError("Bytes do not contain a supported Geometry")
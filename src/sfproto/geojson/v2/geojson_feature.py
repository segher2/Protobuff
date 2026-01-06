from __future__ import annotations

import json
from typing import Any, Dict, Union
from sfproto.geojson.v1.geojson_point import geojson_point_to_bytes, bytes_to_geojson_point
from sfproto.geojson.v1.geojson_polygon import (geojson_polygon_to_bytes, bytes_to_geojson_polygon, )
from sfproto.geojson.v1.geojson_multipolygon import (geojson_multipolygon_to_bytes, bytes_to_geojson_multipolygon, )
from sfproto.geojson.v1.geojson_multipoint import (geojson_multipoint_to_bytes, bytes_to_geojson_multipoint, )
from sfproto.geojson.v1.geojson_linestring import (geojson_linestring_to_bytes, bytes_to_geojson_linestring, )
from sfproto.geojson.v1.geojson_multilinestring import (geojson_multilinestring_to_bytes, bytes_to_geojson_multilinestring, )

GeoJSON = Dict[str, Any]

def geojson_feature_to_bytes(
    obj_or_json: Union[GeoJSON, str], srid: int = 0
) -> bytes:
    """
    Convert GeoJSON Feature -> Protobuf Geometry bytes.
    Properties are ignored (always null).
    """
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    if obj.get("type") != "Feature":
        raise ValueError(
            f"Expected GeoJSON type=Feature, got: {obj.get('type')!r}"
        )

    geometry = obj.get("geometry")
    if geometry is None:
        raise ValueError("Feature.geometry cannot be null")

    gtype = geometry.get("type")

    if gtype == "Point":
        return geojson_point_to_bytes(geometry, srid=srid)

    if gtype == "MultiPoint":
        return geojson_multipoint_to_bytes(geometry, srid=srid)

    if gtype == "Polygon":
        return geojson_polygon_to_bytes(geometry, srid=srid)

    if gtype == "MultiPolygon":
        return geojson_multipolygon_to_bytes(geometry, srid=srid)

    if gtype == "LineString":
        return geojson_linestring_to_bytes(geometry, srid=srid)

    if gtype == "MultiLineString":
        return geojson_multilinestring_to_bytes(geometry, srid=srid)

    raise ValueError(f"Unsupported Feature geometry type: {gtype!r}")


def bytes_to_geojson_feature(data: bytes) -> GeoJSON:
    """
    Convert Protobuf Geometry bytes -> GeoJSON Feature.
    Properties are always null.
    """
    # Try each geometry decoder
    for decoder in (
        bytes_to_geojson_point,
        bytes_to_geojson_multipoint,
        bytes_to_geojson_polygon,
        bytes_to_geojson_multipolygon,
        bytes_to_geojson_linestring,
        bytes_to_geojson_multilinestring,
    ):
        try:
            geometry = decoder(data)
            return {
                "type": "Feature",
                "geometry": geometry,
                "properties": None,
            }
        except Exception:
            pass

    raise ValueError("Bytes do not contain a supported Geometry")
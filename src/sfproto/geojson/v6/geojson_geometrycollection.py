from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple, Union

from sfproto.sf.v6 import geometry_pb2

GeoJSON = Dict[str, Any]
GeoJSONInput = Union[GeoJSON, str]

from sfproto.geojson.v6.geojson_featurecollection import (
    _q, _uq, _first_coord_of_geometry,
    _encode_stream_geometry, _decode_stream_geometry,
)


def geojson_geometrycollection_to_bytes_v6(obj_or_json: GeoJSONInput, srid: int, scale: int) -> bytes:
    obj = json.loads(obj_or_json) if isinstance(obj_or_json, str) else obj_or_json

    if obj.get("type") != "GeometryCollection":
        raise ValueError(f"Expected GeometryCollection, got {obj.get('type')!r}")

    geoms = obj.get("geometries")
    if not isinstance(geoms, list) or not geoms:
        raise ValueError("GeometryCollection.geometries must be a non-empty list")

    gc = geometry_pb2.GeometryCollectionStream()
    gc.crs.srid = int(srid)
    gc.crs.scale = int(scale)

    x0, y0 = _first_coord_of_geometry(geoms[0])
    gc.global_start.x = _q(x0, scale)
    gc.global_start.y = _q(y0, scale)

    global_start_xy = (int(gc.global_start.x), int(gc.global_start.y))

    for g in geoms:
        if not isinstance(g, dict):
            raise ValueError("Each geometry must be an object")
        pb_geom = _encode_stream_geometry(g, global_start_xy, scale)
        gc.geometries.append(pb_geom)

    return gc.SerializeToString()


def bytes_to_geojson_geometrycollection_v6(data: bytes) -> GeoJSON:
    gc = geometry_pb2.GeometryCollectionStream.FromString(data)
    scale = int(gc.crs.scale)

    global_start_xy = (int(gc.global_start.x), int(gc.global_start.y))

    geometries: List[GeoJSON] = []
    for pb_geom in gc.geometries:
        geom = _decode_stream_geometry(pb_geom, global_start_xy, scale)
        geometries.append(geom)

    return {"type": "GeometryCollection", "geometries": geometries}


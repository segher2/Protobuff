from __future__ import annotations

import json
from typing import Any, Dict, List, Union

from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict

from sfproto.sf.v7 import geometry_pb2

from sfproto.geojson.v7.geojson_featurecollection import (
    _q, _uq, _first_coord_of_geometry,
    _encode_stream_geometry, _decode_stream_geometry,
)

GeoJSON = Dict[str, Any]
GeoJSONInput = Union[GeoJSON, str]

_RESERVED_GCOL = {"type", "geometries", "bbox", "crs"}

def _loads_if_needed(obj_or_json: GeoJSONInput) -> GeoJSON:
    return json.loads(obj_or_json) if isinstance(obj_or_json, str) else obj_or_json

def _dict_to_struct(d):
    s = Struct()
    if d:
        s.update(d)
    return s

def _struct_to_dict(s: Struct) -> Dict[str, Any]:
    return MessageToDict(s)

def geojson_geometrycollection_to_bytes_v7(obj_or_json: GeoJSONInput, srid: int, scale: int) -> bytes:
    obj = _loads_if_needed(obj_or_json)
    if obj.get("type") != "GeometryCollection":
        raise ValueError(f"Expected GeometryCollection, got {obj.get('type')!r}")

    geoms = obj.get("geometries")
    if not isinstance(geoms, list) or not geoms:
        raise ValueError("GeometryCollection.geometries must be a non-empty list")

    gc = geometry_pb2.GeometryCollection()
    gc.crs.srid = int(srid)
    gc.crs.scale = int(scale)

    x0, y0 = _first_coord_of_geometry(geoms[0])
    gc.global_start.x = _q(x0, scale)
    gc.global_start.y = _q(y0, scale)
    global_start_xy = (int(gc.global_start.x), int(gc.global_start.y))

    for g in geoms:
        if not isinstance(g, dict):
            raise ValueError("Each geometry must be an object")
        gc.geometries.append(_encode_stream_geometry(g, global_start_xy, scale))

    bbox = obj.get("bbox")
    if isinstance(bbox, list) and len(bbox) in (4, 6) and all(isinstance(x, (int, float)) for x in bbox):
        gc.bbox.extend([float(x) for x in bbox])

    extra = {k: v for k, v in obj.items() if k not in _RESERVED_GCOL}
    if extra:
        gc.extra.CopyFrom(_dict_to_struct(extra))

    return gc.SerializeToString()

def bytes_to_geojson_geometrycollection_v7(data: bytes) -> GeoJSON:
    gc = geometry_pb2.GeometryCollection.FromString(data)
    scale = int(gc.crs.scale)
    global_start_xy = (int(gc.global_start.x), int(gc.global_start.y))

    geoms: List[GeoJSON] = []
    for pb_geom in gc.geometries:
        geoms.append(_decode_stream_geometry(pb_geom, global_start_xy, scale))

    out: GeoJSON = {"type": "GeometryCollection", "geometries": geoms}

    if getattr(gc, "bbox", None) and len(gc.bbox) in (4, 6):
        out["bbox"] = list(gc.bbox)

    extra = _struct_to_dict(gc.extra) if hasattr(gc, "extra") else {}
    for k, v in extra.items():
        if k not in out:
            out[k] = v

    return out

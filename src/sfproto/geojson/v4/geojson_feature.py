from __future__ import annotations

import json
from typing import Any, Dict, Optional, Union

from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict

from sfproto.sf.v4 import geometry_pb2

# Reuse v1 geometry encoders/decoders (geometry bytes -> sf.v4.Geometry parses because schema matches)
from sfproto.geojson.v1.geojson_point import geojson_point_to_bytes, bytes_to_geojson_point
from sfproto.geojson.v1.geojson_polygon import geojson_polygon_to_bytes, bytes_to_geojson_polygon
from sfproto.geojson.v1.geojson_multipolygon import geojson_multipolygon_to_bytes, bytes_to_geojson_multipolygon
from sfproto.geojson.v1.geojson_multipoint import geojson_multipoint_to_bytes, bytes_to_geojson_multipoint
from sfproto.geojson.v1.geojson_linestring import geojson_linestring_to_bytes, bytes_to_geojson_linestring
from sfproto.geojson.v1.geojson_multilinestring import (
    geojson_multilinestring_to_bytes,
    bytes_to_geojson_multilinestring,
)

GeoJSON = Dict[str, Any]
GeoJSONInput = Union[GeoJSON, str]

_RESERVED_TOPLEVEL = {"type", "geometry", "properties", "id", "bbox"}


def _loads_if_needed(obj_or_json: GeoJSONInput) -> GeoJSON:
    return json.loads(obj_or_json) if isinstance(obj_or_json, str) else obj_or_json


def _dict_to_struct(d: Optional[Dict[str, Any]]) -> Struct:
    s = Struct()
    if d is None:
        return s  # empty struct as "null" convention
    s.update(d)
    return s


def _struct_to_dict(s: Struct) -> Dict[str, Any]:
    # Struct -> python dict (JSON-ish)
    return MessageToDict(s)


def _extract_extra(obj: GeoJSON) -> Dict[str, Any]:
    """
    Collect any top-level Feature keys that are not standard/reserved.
    These will be round-tripped via Feature.extra.
    """
    return {k: v for k, v in obj.items() if k not in _RESERVED_TOPLEVEL}


def _encode_geometry_to_bytes(geometry: GeoJSON, srid: int) -> bytes:
    gtype = geometry.get("type")

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

    raise ValueError(f"Unsupported Feature geometry type: {gtype!r}")


def geojson_feature_to_bytes_v4(obj_or_json: GeoJSONInput, srid: int = 0) -> bytes:
    """
    Convert GeoJSON Feature -> Protobuf sf.v4.Feature bytes.

    Encodes:
      - geometry (via existing v1 geometry encoders)
      - properties (Struct)
      - id (stored as string if present)
      - bbox (repeated double if present)
      - any other top-level keys in Feature.extra (Struct)
    """
    obj = _loads_if_needed(obj_or_json)

    if obj.get("type") != "Feature":
        raise ValueError(f"Expected GeoJSON type=Feature, got: {obj.get('type')!r}")

    geometry = obj.get("geometry")
    if geometry is None:
        raise ValueError("Feature.geometry cannot be null")
    if not isinstance(geometry, dict):
        raise ValueError("Feature.geometry must be an object")

    props = obj.get("properties")  # may be dict or None
    if props is not None and not isinstance(props, dict):
        raise ValueError("Feature.properties must be an object or null")

    # geometry -> bytes -> sf.v4.Geometry msg
    geom_bytes = _encode_geometry_to_bytes(geometry, srid=srid)
    geom_msg = geometry_pb2.Geometry.FromString(geom_bytes)

    feat = geometry_pb2.Feature()
    feat.geometry.CopyFrom(geom_msg)

    # properties
    feat.properties.CopyFrom(_dict_to_struct(props))

    # id (optional): GeoJSON allows string/number; store as string
    fid = obj.get("id")
    if fid is not None:
        feat.id = str(fid)

    # bbox (optional): accept length 4 or 6 numeric list
    bbox = obj.get("bbox")
    if isinstance(bbox, list) and len(bbox) in (4, 6) and all(isinstance(x, (int, float)) for x in bbox):
        feat.bbox.extend([float(x) for x in bbox])

    # extra (optional): any other top-level keys
    extra = _extract_extra(obj)
    if extra:
        feat.extra.CopyFrom(_dict_to_struct(extra))

    return feat.SerializeToString()


def bytes_to_geojson_feature_v4(data: bytes) -> GeoJSON:
    """
    Convert Protobuf sf.v4.Feature bytes -> GeoJSON Feature.

    Decodes:
      - geometry
      - properties
      - id (if set)
      - bbox (if present)
      - extra (merged into top-level, without overwriting reserved keys)
    """
    feat = geometry_pb2.Feature.FromString(data)

    # Decode geometry by re-serializing embedded Geometry message and using existing decoders
    geom_bytes = feat.geometry.SerializeToString()

    geometry: Optional[GeoJSON] = None
    for decoder in (
        bytes_to_geojson_point,
        bytes_to_geojson_multipoint,
        bytes_to_geojson_polygon,
        bytes_to_geojson_multipolygon,
        bytes_to_geojson_linestring,
        bytes_to_geojson_multilinestring,
    ):
        try:
            geometry = decoder(geom_bytes)
            break
        except Exception:
            pass

    if geometry is None:
        raise ValueError("Feature.geometry contains an unsupported Geometry")

    props_dict = _struct_to_dict(feat.properties)
    properties = None if props_dict == {} else props_dict  # "empty struct == null" convention

    out: GeoJSON = {
        "type": "Feature",
        "geometry": geometry,
        "properties": properties,
    }

    # id (only include if non-empty)
    if getattr(feat, "id", ""):
        out["id"] = feat.id

    # bbox (only include if present)
    if getattr(feat, "bbox", None) and len(feat.bbox) in (4, 6):
        out["bbox"] = list(feat.bbox)

    # extra (merge into top-level)
    if hasattr(feat, "extra"):
        extra_dict = _struct_to_dict(feat.extra)
        for k, v in extra_dict.items():
            if k not in out:  # don't overwrite reserved keys
                out[k] = v

    return out

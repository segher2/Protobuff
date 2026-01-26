from __future__ import annotations

import json
from typing import Any, Dict, Optional, Union

from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict

from sfproto.sf.v5 import geometry_pb2  # generated from your sf.v5 geometry.proto

from sfproto.geojson.v2.geojson_point import geojson_point_to_bytes_v2, bytes_to_geojson_point_v2
from sfproto.geojson.v2.geojson_polygon import geojson_polygon_to_bytes_v2, bytes_to_geojson_polygon_v2
from sfproto.geojson.v2.geojson_multipolygon import geojson_multipolygon_to_bytes_v2, bytes_to_geojson_multipolygon_v2
from sfproto.geojson.v2.geojson_multipoint import geojson_multipoint_to_bytes_v2, bytes_to_geojson_multipoint_v2
from sfproto.geojson.v2.geojson_linestring import geojson_linestring_to_bytes_v2, bytes_to_geojson_linestring_v2
from sfproto.geojson.v2.geojson_multilinestring import geojson_multilinestring_to_bytes_v2, bytes_to_geojson_multilinestring_v2

GeoJSON = Dict[str, Any]
GeoJSONInput = Union[GeoJSON, str]

DEFAULT_SCALE = 10000000  # 1e7 -> ~cm in EPSG:4326

_RESERVED_TOPLEVEL = {"type", "geometry", "properties", "id", "bbox"}


def _loads_if_needed(obj_or_json: GeoJSONInput) -> GeoJSON:
    return json.loads(obj_or_json) if isinstance(obj_or_json, str) else obj_or_json

def _dict_to_struct(d: Optional[Dict[str, Any]]) -> Struct:
    s = Struct()
    if d is None:
        return s  # empty struct represents null (your v4 convention)
    s.update(d)
    return s


def _struct_to_dict(s: Struct) -> Dict[str, Any]:
    return MessageToDict(s)

def _extract_extra(obj: GeoJSON) -> Dict[str, Any]:
    return {k: v for k, v in obj.items() if k not in _RESERVED_TOPLEVEL}


def _encode_geometry_v2_bytes(geometry: GeoJSON, srid: int, scale: int) -> bytes:
    gtype = geometry.get("type")

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

def geojson_feature_to_bytes_v5(
    obj_or_json: GeoJSONInput,
    srid: int = 0,
    scale: int = DEFAULT_SCALE,
) -> bytes:
    """
    Convert GeoJSON Feature -> Protobuf sf.v5.Feature bytes.
    Properties are encoded (unlike v2).
    """
    obj = _loads_if_needed(obj_or_json)
    if obj.get("type") != "Feature":
        raise ValueError(f"Expected GeoJSON type=Feature, got: {obj.get('type')!r}")

    geometry = obj.get("geometry")
    if geometry is None:
        raise ValueError("Feature.geometry cannot be null")

    props = obj.get("properties")  # may be dict or None
    if props is not None and not isinstance(props, dict):
        raise ValueError("Feature.properties must be an object or null")

    geom_bytes_v2 = _encode_geometry_v2_bytes(geometry, srid=srid, scale=scale)
    geom_msg_v5 = geometry_pb2.Geometry.FromString(geom_bytes_v2)

    feat = geometry_pb2.Feature()
    feat.geometry.CopyFrom(geom_msg_v5)

    feat.properties.CopyFrom(_dict_to_struct(props))

    fid = obj.get("id")
    if fid is not None:
        feat.id = str(fid)

    bbox = obj.get("bbox")
    if isinstance(bbox, list) and len(bbox) in (4,6) and all(isinstance(x,(int,float)) for x in bbox):
        feat.bbox.extend([float(x) for x in bbox])

    extra = _extract_extra(obj)
    if extra:
        feat.extra.CopyFrom(_dict_to_struct(extra))
    return feat.SerializeToString()


def bytes_to_geojson_feature_v5(data: bytes) -> GeoJSON:
    """
    Convert Protobuf sf.v5.Feature bytes -> GeoJSON Feature.
    """
    feat = geometry_pb2.Feature.FromString(data)

    # Decode geometry by re-serializing embedded v5 Geometry message to bytes.
    # v2 decoders can parse it because fields are identical.
    geom_bytes = feat.geometry.SerializeToString()

    geometry: Optional[GeoJSON] = None
    for decoder in (
        bytes_to_geojson_point_v2,
        bytes_to_geojson_multipoint_v2,
        bytes_to_geojson_polygon_v2,
        bytes_to_geojson_multipolygon_v2,
        bytes_to_geojson_linestring_v2,
        bytes_to_geojson_multilinestring_v2,
    ):
        try:
            geometry = decoder(geom_bytes)
            break
        except Exception:
            pass

    if geometry is None:
        raise ValueError("Feature.geometry contains an unsupported Geometry")

    props_dict = _struct_to_dict(feat.properties)
    properties = None if props_dict == {} else props_dict  # v4-style null convention

    out: GeoJSON = {
        "type": "Feature",
        "geometry": geometry,
        "properties": properties,
    }

    # id
    if getattr(feat, "id", ""):
        out["id"] = feat.id

    # bbox
    if getattr(feat, "bbox", None) and len(feat.bbox) in (4, 6):
        out["bbox"] = list(feat.bbox)

    # extra (merge without overwriting reserved keys)
    if hasattr(feat, "extra"):
        extra_dict = _struct_to_dict(feat.extra)
        for k, v in extra_dict.items():
            if k not in out:
                out[k] = v

    return out

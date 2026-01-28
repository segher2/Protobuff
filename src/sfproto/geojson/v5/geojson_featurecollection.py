from __future__ import annotations

import json
from typing import Any, Dict, List, Union, Optional

from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict

from sfproto.sf.v5 import geometry_pb2

from sfproto.geojson.v5.geojson_feature import geojson_feature_to_bytes_v5, bytes_to_geojson_feature_v5

GeoJSON = Dict[str, Any]
GeoJSONInput = Union[GeoJSON, str]

DEFAULT_SCALE = 1000  # 1e7 -> ~cm accuracy for EPSG:4326

_RESERVED_FCOL = {"type", "features", "bbox", "name", "crs"}

def _loads_if_needed(obj_or_json: GeoJSONInput) -> GeoJSON:
    return json.loads(obj_or_json) if isinstance(obj_or_json, str) else obj_or_json


def _dict_to_struct(d: Optional[Dict[str, Any]]) -> Struct:
    s = Struct()
    if d is None:
        return s
    s.update(d)
    return s


def _struct_to_dict(s: Struct) -> Dict[str, Any]:
    return MessageToDict(s)


def _extract_extra_fcol(obj: GeoJSON) -> Dict[str, Any]:
    return {k: v for k, v in obj.items() if k not in _RESERVED_FCOL}


def _geojson_crs_obj_to_srid(crs_obj: Any) -> int:
    """
    Accepts old-style GeoJSON CRS object:
      {"type":"name","properties":{"name":"urn:ogc:def:crs:EPSG::28992"}}
    Returns EPSG code int if parsable, else 0.
    """
    if not isinstance(crs_obj, dict):
        return 0
    props = crs_obj.get("properties")
    if not isinstance(props, dict):
        return 0
    name = props.get("name")
    if not isinstance(name, str):
        return 0
    if "EPSG" not in name:
        return 0
    try:
        return int(name.split(":")[-1])
    except ValueError:
        return 0


def _srid_to_geojson_crs_obj(srid: int) -> GeoJSON:
    return {
        "type": "name",
        "properties": {"name": f"urn:ogc:def:crs:EPSG::{int(srid)}"},
    }


def geojson_featurecollection_to_bytes_v5(obj_or_json: GeoJSONInput, srid: int = 0, scale: int = DEFAULT_SCALE,) -> List[bytes]:
    """
    Convert GeoJSON FeatureCollection -> list of sf.v5.Feature bytes.
    Properties ARE encoded (unlike v2).
    """
    obj = _loads_if_needed(obj_or_json)

    if obj.get("type") != "FeatureCollection":
        raise ValueError(
            f"Expected GeoJSON type=FeatureCollection, got: {obj.get('type')!r}"
        )

    feats = obj.get("features")
    if not isinstance(feats, list):
        raise ValueError("FeatureCollection.features must be a list")

    fc = geometry_pb2.FeatureCollection()

    # --- features ---
    for f in feats:
        feat_bytes = geojson_feature_to_bytes_v5(f, srid=srid, scale=scale)
        fc.features.append(geometry_pb2.Feature.FromString(feat_bytes))

    # --- bbox (optional) ---
    bbox = obj.get("bbox")
    if isinstance(bbox, list) and len(bbox) in (4, 6) and all(isinstance(x, (int, float)) for x in bbox):
        fc.bbox.extend([float(x) for x in bbox])

    # --- name (optional) ---
    name = obj.get("name")
    if isinstance(name, str) and name:
        fc.name = name

    # --- crs (optional) ---
    crs_obj = obj.get("crs")
    srid_from_geojson = _geojson_crs_obj_to_srid(crs_obj)
    if srid_from_geojson:
        fc.crs.srid = int(srid_from_geojson)
    elif srid:
        # If input GeoJSON has no CRS member, you can still store the srid you used.
        fc.crs.srid = int(srid)

    # --- extra (optional) ---
    extra = _extract_extra_fcol(obj)
    if extra:
        fc.extra.CopyFrom(_dict_to_struct(extra))

    return fc.SerializeToString()

def bytes_to_geojson_featurecollection_v5(data: List[bytes]) -> GeoJSON:
    """
    Convert list of sf.v5.Feature bytes -> GeoJSON FeatureCollection.
    Properties ARE decoded (unlike v2).
    """
    fc = geometry_pb2.FeatureCollection.FromString(data)

    out: GeoJSON = {
        "type": "FeatureCollection",
        "features": [bytes_to_geojson_feature_v5(f.SerializeToString()) for f in fc.features],
    }

    # bbox
    if getattr(fc, "bbox", None) and len(fc.bbox) in (4, 6):
        out["bbox"] = list(fc.bbox)

    # name
    if getattr(fc, "name", ""):
        out["name"] = fc.name

    # crs (reconstruct old-style member if srid present)
    if fc.HasField("crs") and fc.crs.srid:
        out["crs"] = _srid_to_geojson_crs_obj(fc.crs.srid)

    # extra (merge without overwriting reserved keys)
    if hasattr(fc, "extra"):
        extra_dict = _struct_to_dict(fc.extra)
        for k, v in extra_dict.items():
            if k not in out:
                out[k] = v

    return out

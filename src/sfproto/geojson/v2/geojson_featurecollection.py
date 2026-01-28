from __future__ import annotations

import json
from typing import Any, Dict, List, Union, Tuple
from sfproto.geojson.v2.geojson_feature import geojson_feature_to_bytes_v2, bytes_to_geojson_feature_v2

GeoJSON = Dict[str, Any]

DEFAULT_SCALE = 1000 #10^7 -> gets cm accuracy

def geojson_featurecollection_to_bytes_v2(obj_or_json: Union[GeoJSON, str], srid: int = 0, scale: int = DEFAULT_SCALE) -> List[bytes]:
    """
    Convert GeoJSON FeatureCollection -> Protobuf Geometry bytes.
    Properties are ignored (always null).
    """
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    if obj.get("type") != "FeatureCollection":
        raise ValueError(
            f"Expected GeoJSON type=Feature, got: {obj.get('type')!r}"
        )

    features = obj.get("features")
    if not isinstance(features, list):
        raise ValueError("FeatureCollection.features must be a list")

    data: List[bytes] = []

    for feature in features:
        data.append(geojson_feature_to_bytes_v2(feature, srid=srid, scale=scale))

    return data


def bytes_to_geojson_featurecollection_v2(
    data: List[bytes],
) -> GeoJSON:
    """
    Convert list of Protobuf Geometry bytes -> GeoJSON FeatureCollection.
    Properties are always null.
    """
    features = []

    for item in data:
        # IMPORTANT: decode EACH bytes blob into a Feature dict
        feature = bytes_to_geojson_feature_v2(item)
        features.append(feature)

    return {
        "type": "FeatureCollection",
        "features": features,
    }
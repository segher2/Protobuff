from __future__ import annotations

import json
from typing import Any, Dict, List, Union
from sfproto.geojson.v1.geojson_feature import (geojson_feature_to_bytes, bytes_to_geojson_feature, )

GeoJSON = Dict[str, Any]

def geojson_featurecollection_to_bytes(
    obj_or_json: Union[GeoJSON, str], srid: int = 0
) -> bytes:
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
        data.append(geojson_feature_to_bytes(feature, srid=srid))

    return data


def bytes_to_geojson_featurecollection(
    data: List[bytes],
) -> GeoJSON:
    """
    Convert list of Protobuf Geometry bytes -> GeoJSON FeatureCollection.
    Properties are always null.
    """
    features = []

    for item in data:
        # IMPORTANT: decode EACH bytes blob into a Feature dict
        feature = bytes_to_geojson_feature(item)
        features.append(feature)

    return {
        "type": "FeatureCollection",
        "features": features,
    }
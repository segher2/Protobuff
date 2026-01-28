from __future__ import annotations

import json
from typing import Any, Dict, List, Union, Tuple
from sfproto.geojson.v2.geojson_feature import geojson_feature_to_bytes_v2, bytes_to_geojson_feature_v2

GeoJSON = Dict[str, Any]

DEFAULT_SCALE = 1000 #parameter for accuacy
# -> strongly relies on which srid, formula to get 'cm' accuracy scaler is in geojson_roundtrip.py file

def geojson_featurecollection_to_bytes_v2(obj_or_json: Union[GeoJSON, str], srid: int = 0, scale: int = DEFAULT_SCALE) -> List[bytes]:
    """
    Convert GeoJSON FeatureCollection -> Protobuf Geometry bytes.
    Properties are ignored (always null).
    """
    # if input geojson is string, convert to dict
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    # only use this function if input type is feature collection
    if obj.get("type") != "FeatureCollection":
        raise ValueError(
            f"Expected GeoJSON type=Feature, got: {obj.get('type')!r}"
        )

    # get features
    features = obj.get("features")
    if not isinstance(features, list):
        raise ValueError("FeatureCollection.features must be a list")

    # make list in which the features will be stored
    data: List[bytes] = []
    # loop through the features in the feature collection and append each feature to the 'features' list
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
    # append all feature items to the 'features' list
    for item in data:
        feature = bytes_to_geojson_feature_v2(item)
        features.append(feature)

    # output feature collection format
    return {
        "type": "FeatureCollection",
        "features": features,
    }
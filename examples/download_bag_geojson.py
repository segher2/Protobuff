import requests
import json
import os
from typing import Dict, Any, List


def download_bag_pand_geojson_paginated(
    total_count: int,
    output_path: str,
    page_size: int = 1000,
):
    """
    Download BAG panden as GeoJSON from PDOK WFS using pagination
    and concatenate into a single valid FeatureCollection.

    Parameters
    ----------
    total_count : int
        Total number of features to download (e.g. 1_000_000)
    output_path : str
        Path to output GeoJSON file
    page_size : int
        Number of features per request (PDOK max = 1000)
    """

    base_url = "https://service.pdok.nl/lv/bag/wfs/v2_0"

    all_features: List[Dict[str, Any]] = []
    crs = None
    name = None
    global_bbox = None

    start_index = 0

    while start_index < total_count:
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": "bag:pand",
            "outputFormat": "application/json",
            "count": page_size,
            "startIndex": start_index,
        }

        print(f"Downloading features {start_index} → {start_index + page_size} ...")

        response = requests.get(base_url, params=params)
        response.raise_for_status()

        data = response.json()

        features = data.get("features", [])
        if not features:
            print("No more features returned, stopping.")
            break

        # Store metadata from first page
        if crs is None:
            crs = data.get("crs")
        if name is None:
            name = data.get("name")

        # Update bbox
        page_bbox = data.get("bbox")
        if page_bbox:
            if global_bbox is None:
                global_bbox = page_bbox
            else:
                global_bbox = [
                    min(global_bbox[0], page_bbox[0]),
                    min(global_bbox[1], page_bbox[1]),
                    max(global_bbox[2], page_bbox[2]),
                    max(global_bbox[3], page_bbox[3]),
                ]

        all_features.extend(features)

        start_index += page_size

        # Safety break if server returns fewer than requested
        if len(features) < page_size:
            print("Last page reached.")
            break

    print(f"Total features downloaded: {len(all_features)}")

    feature_collection = {
        "type": "FeatureCollection",
        "name": name,
        "crs": crs,
        "features": all_features,
        "bbox": global_bbox,
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(feature_collection, f)

    print(f"Saved combined GeoJSON to {output_path}")


if __name__ == "__main__":
    download_bag_pand_geojson_paginated(
        total_count=10_000,
        output_path="examples/data/bag_pand_10k.geojson",
        page_size=1000,
    )

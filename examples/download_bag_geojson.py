import requests

def download_bag_pand_geojson(count: int, output_dir: str = "."):
    """
    Download BAG panden as GeoJSON from PDOK WFS with a given count.
    The output filename includes the count.
    """

    base_url = "https://service.pdok.nl/lv/bag/wfs/v2_0"
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": "bag:pand",
        "outputFormat": "application/json",
        "count": count,
    }

    print(f"Downloading BAG panden (count={count})...")
    response = requests.get(base_url, params=params)
    response.raise_for_status()

    filename = f"{output_dir}/bag_pand_count_{count}.geojson"

    with open(filename, "wb") as f:
        f.write(response.content)

    print(f"Saved to {filename}")


if __name__ == "__main__":
    # the number of building features to download
    COUNT = 1000000

    download_bag_pand_geojson(COUNT, output_dir="data")

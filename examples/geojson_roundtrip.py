import json
# --------------------------------------- v1 ------------------------------------------
from sfproto.geojson.v1.geojson import geojson_to_bytes, bytes_to_geojson #hopefully rest can now be removed
# --------------------------------------- v2 ------------------------------------------
from sfproto.geojson.v2.geojson import geojson_to_bytes_v2, bytes_to_geojson_v2
# --------------------------------------- v4 ------------------------------------------
from sfproto.geojson.v4.geojson import geojson_to_bytes_v4, bytes_to_geojson_v4
# --------------------------------------- v5 ------------------------------------------
from sfproto.geojson.v5.geojson import geojson_to_bytes_v5, bytes_to_geojson_v5

from pathlib import Path

# function to load the geojson from the file
def load_geojson(relative_path):
    base_dir = Path(__file__).parent   # examples/
    path = base_dir / relative_path    # examples/data/Point.geojson
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

# ===================================================================================
# =================================== DATA ==========================================
# ===================================================================================
geojson_point = load_geojson('data/Point.geojson')
geojson_linestring = load_geojson('data/Linestring.geojson')
geojson_polygon = load_geojson('data/Polygon_with_holes.geojson')
geojson_multipoint = load_geojson('data/MultiPoint.geojson')
geojson_multilinestring = load_geojson('data/MultiLineString.geojson')
geojson_multipolygon = load_geojson('data/MultiPolygon.geojson')
geojson_geometrycollection = load_geojson('data/GeometryCollection.geojson')
geojson_feature = load_geojson('data/Feature.geojson')
geojson_featurecollection = load_geojson('data/bag_pand_count_10.geojson')

# ===================================================================================
# ================================ DATA LENGTH ======================================
# ===================================================================================
def roundtrip(input_geojson, version):
    data_length = json.dumps(input_geojson, separators=(",", ":")).encode("utf-8")
    print(f'data length: = {len(data_length)}')
    if version == 1:
        binary_representation = geojson_to_bytes(input_geojson, srid=4326)
        to_geojson = bytes_to_geojson(binary_representation)
    elif version == 2:
        binary_representation = geojson_to_bytes_v2(input_geojson, srid=4326)
        to_geojson = bytes_to_geojson_v2(binary_representation)
    elif version == 4:
        binary_representation = geojson_to_bytes_v4(input_geojson, srid=4326)
        to_geojson = bytes_to_geojson_v4(binary_representation)
    elif version == 5:
        binary_representation = geojson_to_bytes_v5(input_geojson, srid=4326)
        to_geojson = bytes_to_geojson_v5(binary_representation)
    else:
        print(f'version = {version} does not exist')
        return
    geojson_bytes_fair = json.dumps(to_geojson, separators=(",", ":")).encode("utf-8")
    print(f'protobuf v{version} bytes length: {len(binary_representation)} vs fair geojson byte length: {len(geojson_bytes_fair)}')
    print("protobuf  bytes length:", len(binary_representation), "vs fair geojson bytes length:", len(geojson_bytes_fair))
    print(f'output geojson after roundtrip: {to_geojson}')

roundtrip(geojson_featurecollection, 4)
print(geojson_featurecollection.keys())
print(geojson_featurecollection.get("name"))
print(geojson_featurecollection.get("crs"))

import json
from sfproto.geojson_point import geojson_point_to_bytes, bytes_to_geojson_point
from pathlib import Path

# function to load the geojson from the file
def load_geojson(relative_path):
    base_dir = Path(__file__).parent   # examples/
    path = base_dir / relative_path    # examples/data/Point.geojson
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

# simple GeoJSON --> Point
geojson_point = load_geojson('data/Point.geojson')
# geojson_point =  {"type": "Point", "coordinates": [4.9, 52.37]}

# simple GeoJSON --> LineString
geojson_linestring = load_geojson('data/Linestring.geojson')

# simple GeoJSON --> Polygon
geojson_polygon = load_geojson('data/Polygon.geojson')

# simple GeoJSON --> MultiPoint
geojson_multipoint = load_geojson('data/MultiPoint.geojson')

# simple GeoJSON --> MultiLineString
geojson_multilinestring = load_geojson('data/MultiLineString.geojson')

# simple GeoJSON --> MultiPolygon
geojson_multipolygon = load_geojson('data/MultiPolygon.geojson')

# simple GeoJSON --> Feature
# TODO

# simple GeoJSON --> FeatureCollection
# TODO

# GeoJSON → bytes (compact, no whitespace)
geojson_bytes = json.dumps(geojson_point, separators=(",", ":")).encode("utf-8")

data = geojson_point_to_bytes(geojson_point, srid=4326)
out = bytes_to_geojson_point(data)

print("geojson bytes length:", len(geojson_bytes))
print("protobuf bytes length:", len(data))
print("out:", json.dumps(out))

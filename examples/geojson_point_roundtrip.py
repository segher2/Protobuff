import json
from sfproto.geojson_point import geojson_point_to_bytes, bytes_to_geojson_point
from sfproto.geojson_polygon import (geojson_polygon_to_bytes, bytes_to_geojson_polygon,)
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
geojson_polygon = load_geojson('data/Polygon_with_holes.geojson')

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
geojson_bytes_point = json.dumps(geojson_point, separators=(",", ":")).encode("utf-8")
geojson_bytes_polygon = json.dumps(geojson_polygon, separators=(",", ":")).encode("utf-8")

data_point = geojson_point_to_bytes(geojson_point, srid=4326)
data_polygon = geojson_polygon_to_bytes(geojson_polygon, srid=4326)
out_point = bytes_to_geojson_point(data_point)
out_polygon = bytes_to_geojson_polygon(data_polygon)

print("geojson point bytes length:", len(geojson_bytes_point))
print("protobuf point bytes length:", len(data_point))
print("out point geojson:", json.dumps(out_point))
print("================================================")
print("geojson polygon bytes length", len(geojson_bytes_polygon))
print("protobuf polygon bytes length", len(data_polygon))
print("out polygon geojson:", json.dumps(out_polygon))

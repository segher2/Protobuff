import json
# --------------------------------------- v1 ------------------------------------------
from sfproto.geojson.v1.geojson_point import geojson_point_to_bytes, bytes_to_geojson_point
from sfproto.geojson.v1.geojson_polygon import (geojson_polygon_to_bytes, bytes_to_geojson_polygon, )
from sfproto.geojson.v1.geojson_multipolygon import (geojson_multipolygon_to_bytes, bytes_to_geojson_multipolygon, )
from sfproto.geojson.v1.geojson_linestring import (geojson_linestring_to_bytes, bytes_to_geojson_linestring)
from sfproto.geojson.v1.geojson_multilinestring import (geojson_multilinestring_to_bytes, bytes_to_geojson_multilinestring)
from sfproto.geojson.v1.geojson_multipoint import (geojson_multipoint_to_bytes, bytes_to_geojson_multipoint)
from sfproto.geojson.v1.geojson_feature import (geojson_feature_to_bytes, bytes_to_geojson_feature, )
from sfproto.geojson.v1.geojson_featurecollection import (geojson_featurecollection_to_bytes, bytes_to_geojson_featurecollection, )

# --------------------------------------- v2 ------------------------------------------
from sfproto.geojson.v2.geojson_point import geojson_point_to_bytes_v2, bytes_to_geojson_point_v2

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
geojson_feature = load_geojson('data/Feature.geojson')

# simple GeoJSON --> FeatureCollection
geojson_featurecollection = load_geojson('data/FeatureCollection.geojson')

# GeoJSON → bytes (compact, no whitespace)
geojson_bytes_point = json.dumps(geojson_point, separators=(",", ":")).encode("utf-8")
geojson_bytes_polygon = json.dumps(geojson_polygon, separators=(",", ":")).encode("utf-8")
geojson_bytes_multipolygon = json.dumps(geojson_multipolygon, separators=(",", ":")).encode("utf-8")
geojson_bytes_linestring = json.dumps(geojson_linestring, separators=(",", ":")).encode("utf-8")
geojson_bytes_multilinestring = json.dumps(geojson_multilinestring, separators=(",", ":")).encode("utf-8")
geojson_bytes_multipoint = json.dumps(geojson_multipoint, separators=(",", ":")).encode("utf-8")
geojson_bytes_feature = json.dumps(geojson_feature, separators=(",", ":")).encode("utf-8")
geojson_bytes_featurecollection = json.dumps(geojson_featurecollection, separators=(",", ":")).encode("utf-8")

# ------------------------- v1 point to bytes --------------------------------
data_point = geojson_point_to_bytes(geojson_point, srid=4326)
data_polygon = geojson_polygon_to_bytes(geojson_polygon, srid=4326)
data_multipolygon = geojson_multipolygon_to_bytes(geojson_multipolygon, srid=4326)
data_linestring = geojson_linestring_to_bytes(geojson_linestring, srid=4326)
data_multilinestring = geojson_multilinestring_to_bytes(geojson_multilinestring, srid=4326)
data_multipoint = geojson_multipoint_to_bytes(geojson_multipoint, srid=4326)
data_feature = geojson_feature_to_bytes(geojson_feature, srid=4326)
data_featurecollection = geojson_featurecollection_to_bytes(geojson_featurecollection, srid=4326)

# ------------------------- v2 geojson to bytes --------------------------------
data_point_v2 = geojson_point_to_bytes_v2(geojson_point, srid=4326)


# -------------------------- v1 bytes to geojson -------------------------------
out_point = bytes_to_geojson_point(data_point)
out_polygon = bytes_to_geojson_polygon(data_polygon)
out_multipolygon = bytes_to_geojson_multipolygon(data_multipolygon)
out_linestring = bytes_to_geojson_linestring(data_linestring)
out_multilinestring = bytes_to_geojson_multilinestring(data_multilinestring)
out_multipoint = bytes_to_geojson_multipoint(data_multipoint)
out_feature = bytes_to_geojson_feature(data_feature)
out_featurecollection = bytes_to_geojson_featurecollection(data_featurecollection)

# -------------------------- v2 bytes to geojson -------------------------------
out_point_v2 = bytes_to_geojson_point_v2(data_point_v2)

print("geojson point bytes length:", len(geojson_bytes_point))
print("protobuf v1 point bytes length:", len(data_point))
print("protobuf v2 point bytes length:", len(data_point_v2))
print("out point v1 geojson:", json.dumps(out_point))
print("out point v2 geojson:", json.dumps(out_point_v2))
print("================================================")
print("geojson polygon bytes length", len(geojson_bytes_polygon))
print("protobuf polygon bytes length", len(data_polygon))
print("out polygon geojson:", json.dumps(out_polygon))
print("================================================")
print("geojson multipolygon bytes length", len(geojson_bytes_multipolygon))
print("protobuf multipolygon bytes length", len(data_multipolygon))
print("out multipolygon geojson:", json.dumps(out_multipolygon))
print("================================================")
print("geojson linestring bytes length", len(geojson_bytes_linestring))
print("protobuf linestring bytes length", len(data_linestring))
print("out linestring:", json.dumps(out_linestring))
print("================================================")
print("geojson multilinestring bytes length", len(geojson_bytes_multilinestring))
print("protobuf multilinestring bytes length", len(data_multilinestring))
print("out multilinestring:", json.dumps(out_multilinestring))
print("================================================")
print("geojson multipoint bytes length", len(geojson_bytes_multipoint))
print("protobuf multipoint bytes length", len(data_multipoint))
print("out multipoint:", json.dumps(out_multipoint))
print("================================================")
protobuf_fc_bytes = sum(len(b) for b in data_featurecollection) # because now
print("geojson featurecollection bytes length", len(geojson_bytes_featurecollection))
print("protobuf featurecollection bytes length", protobuf_fc_bytes)
print("out featurecollection geojson:", json.dumps(out_featurecollection))
print("================================================")
print("geojson feature bytes length", len(geojson_bytes_feature))
print("protobuf feature bytes length", len(data_feature))
print("out feature geojson:", json.dumps(out_feature))
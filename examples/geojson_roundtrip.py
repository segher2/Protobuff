import json
# --------------------------------------- v1 ------------------------------------------
from sfproto.geojson.v1.geojson import geojson_to_bytes, bytes_to_geojson #hopefully rest can now be removed

from sfproto.geojson.v1.geojson_point import geojson_point_to_bytes, bytes_to_geojson_point
from sfproto.geojson.v1.geojson_multipoint import geojson_multipoint_to_bytes, bytes_to_geojson_multipoint
from sfproto.geojson.v1.geojson_linestring import geojson_linestring_to_bytes, bytes_to_geojson_linestring
from sfproto.geojson.v1.geojson_multilinestring import geojson_multilinestring_to_bytes, bytes_to_geojson_multilinestring
from sfproto.geojson.v1.geojson_polygon import geojson_polygon_to_bytes, bytes_to_geojson_polygon
from sfproto.geojson.v1.geojson_multipolygon import geojson_multipolygon_to_bytes, bytes_to_geojson_multipolygon
from sfproto.geojson.v1.geojson_feature import geojson_feature_to_bytes, bytes_to_geojson_feature
from sfproto.geojson.v1.geojson_featurecollection import geojson_featurecollection_to_bytes, bytes_to_geojson_featurecollection
from sfproto.geojson.v1.geojson_geometrycollection import geojson_geometrycollection_to_bytes, bytes_to_geojson_geometrycollection


# --------------------------------------- v2 ------------------------------------------
from sfproto.geojson.v2.geojson import geojson_to_bytes_v2, bytes_to_geojson_v2

from sfproto.geojson.v2.geojson_point import geojson_point_to_bytes_v2, bytes_to_geojson_point_v2
from sfproto.geojson.v2.geojson_multipoint import geojson_multipoint_to_bytes_v2, bytes_to_geojson_multipoint_v2
from sfproto.geojson.v2.geojson_linestring import geojson_linestring_to_bytes_v2, bytes_to_geojson_linestring_v2
from sfproto.geojson.v2.geojson_multilinestring import geojson_multilinestring_to_bytes_v2, bytes_to_geojson_multilinestring_v2
from sfproto.geojson.v2.geojson_polygon import geojson_polygon_to_bytes_v2, bytes_to_geojson_polygon_v2
from sfproto.geojson.v2.geojson_multipolygon import geojson_multipolygon_to_bytes_v2, bytes_to_geojson_multipolygon_v2
from sfproto.geojson.v2.geojson_geometrycollection import geojson_geometrycollection_to_bytes_v2, bytes_to_geojson_geometrycollection_v2
from sfproto.geojson.v2.geojson_feature import geojson_feature_to_bytes_v2, bytes_to_geojson_feature_v2
from sfproto.geojson.v2.geojson_featurecollection import geojson_featurecollection_to_bytes_v2, bytes_to_geojson_featurecollection_v2

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

# simple GeoJSON -> GeometryCollection
geojson_geometrycollection = load_geojson('data/GeometryCollection.geojson')

# simple GeoJSON --> Feature
geojson_feature = load_geojson('data/Feature.geojson')

# simple GeoJSON --> FeatureCollection
geojson_featurecollection = load_geojson('data/FeatureCollection.geojson')

# GeoJSON → bytes (compact, no whitespace)
geojson_bytes_point = json.dumps(geojson_point, separators=(",", ":")).encode("utf-8")
geojson_bytes_multipoint = json.dumps(geojson_multipoint, separators=(",", ":")).encode("utf-8")
geojson_bytes_linestring = json.dumps(geojson_linestring, separators=(",", ":")).encode("utf-8")
geojson_bytes_multilinestring = json.dumps(geojson_multilinestring, separators=(",", ":")).encode("utf-8")
geojson_bytes_polygon = json.dumps(geojson_polygon, separators=(",", ":")).encode("utf-8")
geojson_bytes_multipolygon = json.dumps(geojson_multipolygon, separators=(",", ":")).encode("utf-8")
geojson_bytes_geometrycollection = json.dumps(geojson_geometrycollection, separators=(",", ":")).encode("utf-8")
geojson_bytes_feature = json.dumps(geojson_feature, separators=(",", ":")).encode("utf-8")
geojson_bytes_featurecollection = json.dumps(geojson_featurecollection, separators=(",", ":")).encode("utf-8")

# ------------------------- v1 point to bytes --------------------------------
data_point = geojson_to_bytes(geojson_point, srid=4326)
data_multipoint = geojson_to_bytes(geojson_multipoint, srid=4326)
data_linestring = geojson_to_bytes(geojson_linestring, srid=4326)
data_multilinestring = geojson_to_bytes(geojson_multilinestring, srid=4326)
data_polygon = geojson_to_bytes(geojson_polygon, srid=4326)
data_multipolygon = geojson_to_bytes(geojson_multipolygon, srid=4326)
data_geometrycollection = geojson_to_bytes(geojson_geometrycollection, srid=4326)
data_feature = geojson_to_bytes(geojson_feature, srid=4326)
data_featurecollection = geojson_to_bytes(geojson_featurecollection, srid=4326)

# ------------------------- v2 geojson to bytes --------------------------------
data_point_v2 = geojson_to_bytes_v2(geojson_point, srid=4326)
data_multipoint_v2 = geojson_to_bytes_v2(geojson_multipoint, srid=4326)
data_linestring_v2 = geojson_to_bytes_v2(geojson_linestring, srid=4326)
data_multilinestring_v2 = geojson_to_bytes_v2(geojson_multilinestring, srid=4326)
data_polygon_v2 = geojson_to_bytes_v2(geojson_polygon, srid=4326)
data_multipolygon_v2 = geojson_to_bytes_v2(geojson_multipolygon, srid=4326)
data_geometrycollection_v2 = geojson_to_bytes_v2(geojson_geometrycollection, srid=4326)
data_feature_v2 = geojson_to_bytes_v2(geojson_feature, srid=4326)
data_featurecollection_v2 = geojson_to_bytes_v2(geojson_featurecollection, srid=4326)

# -------------------------- v1 bytes to geojson -------------------------------
out_point = bytes_to_geojson(data_point)
out_multipoint = bytes_to_geojson(data_multipoint)
out_linestring = bytes_to_geojson(data_linestring)
out_multilinestring = bytes_to_geojson(data_multilinestring)
out_polygon = bytes_to_geojson(data_polygon)
out_multipolygon = bytes_to_geojson(data_multipolygon)
out_geometrycollection = bytes_to_geojson(data_geometrycollection)
out_feature = bytes_to_geojson(data_feature)
out_featurecollection = bytes_to_geojson(data_featurecollection)

# -------------------------- v2 bytes to geojson -------------------------------
out_point_v2 = bytes_to_geojson_v2(data_point_v2)
out_multipoint_v2 = bytes_to_geojson_v2(data_multipoint_v2)
out_linestring_v2 = bytes_to_geojson_v2(data_linestring_v2)
out_multilinestring_v2 = bytes_to_geojson_v2(data_multilinestring_v2)
out_polygon_v2 = bytes_to_geojson_v2(data_polygon_v2)
out_multipolygon_v2 = bytes_to_geojson_v2(data_multipolygon_v2)
out_geometrycollection_v2 = bytes_to_geojson_v2(data_geometrycollection_v2)
out_feature_v2 = bytes_to_geojson_v2(data_feature_v2)
out_featurecollection_v2 = bytes_to_geojson_v2(data_featurecollection_v2)

# GeoJSON → bytes fair  v1 comparison (compact, no whitespace)
geojson_bytes_point_fair = json.dumps(out_point, separators=(",", ":")).encode("utf-8")
geojson_bytes_multipoint_fair = json.dumps(out_multipoint, separators=(",", ":")).encode("utf-8")
geojson_bytes_linestring_fair = json.dumps(out_linestring, separators=(",", ":")).encode("utf-8")
geojson_bytes_multilinestring_fair = json.dumps(out_multilinestring, separators=(",", ":")).encode("utf-8")
geojson_bytes_polygon_fair = json.dumps(out_polygon, separators=(",", ":")).encode("utf-8")
geojson_bytes_multipolygon_fair = json.dumps(out_multipolygon, separators=(",", ":")).encode("utf-8")
geojson_bytes_geometrycollection_fair = json.dumps(out_geometrycollection, separators=(",", ":")).encode("utf-8")
geojson_bytes_feature_fair = json.dumps(out_feature, separators=(",", ":")).encode("utf-8")
geojson_bytes_featurecollection_fair = json.dumps(out_featurecollection, separators=(",", ":")).encode("utf-8")

# GeoJSON → bytes fair  v2 comparison (compact, no whitespace)
geojson_bytes_point_fair_v2 = json.dumps(out_point_v2, separators=(",", ":")).encode("utf-8")
geojson_bytes_multipoint_fair_v2 = json.dumps(out_multipoint_v2, separators=(",", ":")).encode("utf-8")
geojson_bytes_linestring_fair_v2 = json.dumps(out_linestring_v2, separators=(",", ":")).encode("utf-8")
geojson_bytes_multilinestring_fair_v2 = json.dumps(out_multilinestring_v2, separators=(",", ":")).encode("utf-8")
geojson_bytes_polygon_fair_v2 = json.dumps(out_polygon_v2, separators=(",", ":")).encode("utf-8")
geojson_bytes_multipolygon_fair_v2 = json.dumps(out_multipolygon_v2, separators=(",", ":")).encode("utf-8")
geojson_bytes_geometrycollection_fair_v2 = json.dumps(out_geometrycollection_v2, separators=(",", ":")).encode("utf-8")
geojson_bytes_feature_fair_v2 = json.dumps(out_feature_v2, separators=(",", ":")).encode("utf-8")
geojson_bytes_featurecollection_fair_v2 = json.dumps(out_featurecollection_v2, separators=(",", ":")).encode("utf-8")

print(" ======================= POINT ============================== ")
print("geojson point bytes length:", len(geojson_bytes_point))
print("protobuf v1 point bytes length:", len(data_point), "vs fair geojson bytes length:", len(geojson_bytes_point_fair))
print("protobuf v2 point bytes length:", len(data_point_v2), "vs fair geojson bytes length:", len(geojson_bytes_point_fair_v2))
print("out point v1 geojson:", json.dumps(out_point))
print("out point v2 geojson:", json.dumps(out_point_v2))
print("================================================")

print(" ======================= MULTI POINT ============================== ")
print("geojson multipoint bytes length", len(geojson_bytes_multipoint))
print("protobuf v1 multipoint bytes length", len(data_multipoint), "vs fair geojson bytes length:", len(geojson_bytes_multipoint_fair))
print("protobuf v2 multipoint bytes length:", len(data_multipoint_v2), "vs fair geojson bytes length:", len(geojson_bytes_multipoint_fair_v2))
print("out v1 multipoint:", json.dumps(out_multipoint))
print("out v2 multipoint:", json.dumps(out_multipoint_v2))
print("================================================")

print(" ======================= LINESTRING ============================== ")
print("geojson linestring bytes length", len(geojson_bytes_linestring))
print("protobuf v1 linestring bytes length", len(data_linestring), "vs fair geojson bytes length:", len(geojson_bytes_linestring_fair))
print("protobuf v2 linestring bytes length:", len(data_linestring_v2), "vs fair geojson bytes length:", len(geojson_bytes_linestring_fair_v2))
print("out v1 linestring:", json.dumps(out_linestring))
print("out v2 linestring:", json.dumps(out_linestring_v2))
print("================================================")

print(" ======================= MULTI LINESTRING ============================== ")
print("geojson multilinestring bytes length", len(geojson_bytes_multilinestring))
print("protobuf v1 multilinestring bytes length", len(data_multilinestring), "vs fair geojson bytes length:", len(geojson_bytes_multilinestring_fair))
print("protobuf v2 multilinestring bytes length:", len(data_multilinestring_v2), "vs fair geojson bytes length:", len(geojson_bytes_multilinestring_fair_v2))
print("out v1 multilinestring:", json.dumps(out_multilinestring))
print("out v2 multilinestring:", json.dumps(out_multilinestring_v2))
print("================================================")

print(" ======================= POLYGON ============================== ")
print("geojson polygon bytes length", len(geojson_bytes_polygon))
print("protobuf v1 polygon bytes length", len(data_polygon), "vs fair geojson bytes length:", len(geojson_bytes_polygon_fair))
print("protobuf v2 polygon bytes length:", len(data_polygon_v2), "vs fair geojson bytes length:", len(geojson_bytes_polygon_fair_v2))
print("out v1 polygon geojson:", json.dumps(out_polygon))
print("out v2 polygon geojson:", json.dumps(out_polygon_v2))
print("================================================")

print(" ======================= MULTI POLYGON ============================== ")
print("geojson multipolygon bytes length", len(geojson_bytes_multipolygon))
print("protobuf v1 multipolygon bytes length", len(data_multipolygon), "vs fair geojson bytes length:", len(geojson_bytes_multipolygon_fair))
print("protobuf v2 multipolygon bytes length", len(data_multipolygon_v2), "vs fair geojson bytes length:", len(geojson_bytes_multipolygon_fair_v2))
print("out v1 multipolygon geojson:", json.dumps(out_multipolygon))
print("out v2 multipolygon geojson:", json.dumps(out_multipolygon_v2))
print("================================================")

print(" ======================= GEOMETRY COLLECTION ============================== ")
print("geojson geometrycollection bytes length", len(geojson_bytes_geometrycollection))
print("protobuf v1 geometrycollection bytes length", len(data_geometrycollection), "vs fair geojson bytes length:", len(geojson_bytes_geometrycollection_fair))
print("protobuf v2 geometrycollection bytes length", len(data_geometrycollection_v2), "vs fair geojson bytes length:", len(geojson_bytes_geometrycollection_fair_v2))
print("out v1 geometrycollection geojson:", json.dumps(out_geometrycollection))
print("out v2 geometrycollection geojson:", json.dumps(out_geometrycollection_v2))
print("================================================")

print(" ======================= FEATURE ============================== ")
print("geojson feature bytes length", len(geojson_bytes_feature))
print("protobuf v1 feature bytes length", len(data_feature), "vs fair geojson bytes length:", len(geojson_bytes_feature_fair))
print("protobuf v2 feature bytes length:", len(data_feature_v2), "vs fair geojson bytes length:", len(geojson_bytes_feature_fair_v2))
print("out v1 feature geojson:", json.dumps(out_feature))
print("out v2 feature geojson:", json.dumps(out_feature_v2))
print("================================================")

print(" ======================= FEATURE COLLECTION ============================== ")
# protobuf_fc_bytes = sum(len(b) for b in data_featurecollection) # otherwise just first part
# protobuf_fc_bytes_v2 = sum(len(b) for b in data_featurecollection_v2)
print("geojson featurecollection bytes length", len(geojson_bytes_featurecollection))
print("protobuf v1 featurecollection bytes length", len(data_featurecollection), "vs fair geojson bytes length:", len(geojson_bytes_featurecollection_fair))
print("protobuf v2 featurecollection bytes length", len(data_featurecollection_v2), "vs fair geojson bytes length:", len(geojson_bytes_featurecollection_fair_v2))
print("out v1 featurecollection geojson:", json.dumps(out_featurecollection))
print("out v2 featurecollection geojson:", json.dumps(out_featurecollection_v2))
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

# ===================================================================================
# ================================ DATA LENGTH ======================================
# ===================================================================================
def data():
    geojson_bytes_point = json.dumps(geojson_point, separators=(",", ":")).encode("utf-8")
    geojson_bytes_multipoint = json.dumps(geojson_multipoint, separators=(",", ":")).encode("utf-8")
    geojson_bytes_linestring = json.dumps(geojson_linestring, separators=(",", ":")).encode("utf-8")
    geojson_bytes_multilinestring = json.dumps(geojson_multilinestring, separators=(",", ":")).encode("utf-8")
    geojson_bytes_polygon = json.dumps(geojson_polygon, separators=(",", ":")).encode("utf-8")
    geojson_bytes_multipolygon = json.dumps(geojson_multipolygon, separators=(",", ":")).encode("utf-8")
    geojson_bytes_geometrycollection = json.dumps(geojson_geometrycollection, separators=(",", ":")).encode("utf-8")
    geojson_bytes_feature = json.dumps(geojson_feature, separators=(",", ":")).encode("utf-8")
    geojson_bytes_featurecollection = json.dumps(geojson_featurecollection, separators=(",", ":")).encode("utf-8")
    return geojson_bytes_point,geojson_bytes_multipoint,geojson_bytes_linestring,geojson_bytes_multilinestring,geojson_bytes_polygon,geojson_bytes_multipolygon,geojson_bytes_geometrycollection,geojson_bytes_feature,geojson_bytes_featurecollection

geojson_bytes_point,geojson_bytes_multipoint,geojson_bytes_linestring,geojson_bytes_multilinestring,geojson_bytes_polygon,geojson_bytes_multipolygon,geojson_bytes_geometrycollection,geojson_bytes_feature,geojson_bytes_featurecollection = data()

# ===================================================================================
# ============================== GEOJSON -> BYTES ===================================
# ===================================================================================
# ------------------------- v1 geojson to bytes --------------------------------
def to_binary():
    data_point = geojson_to_bytes(geojson_point, srid=4326)
    data_multipoint = geojson_to_bytes(geojson_multipoint, srid=4326)
    data_linestring = geojson_to_bytes(geojson_linestring, srid=4326)
    data_multilinestring = geojson_to_bytes(geojson_multilinestring, srid=4326)
    data_polygon = geojson_to_bytes(geojson_polygon, srid=4326)
    data_multipolygon = geojson_to_bytes(geojson_multipolygon, srid=4326)
    data_geometrycollection = geojson_to_bytes(geojson_geometrycollection, srid=4326)
    data_feature = geojson_to_bytes(geojson_feature, srid=4326)
    data_featurecollection = geojson_to_bytes(geojson_featurecollection, srid=4326)
    return data_point,data_multipoint,data_linestring,data_multilinestring,data_polygon,data_multipolygon,data_geometrycollection,data_feature,data_featurecollection

data_point,data_multipoint,data_linestring,data_multilinestring,data_polygon,data_multipolygon,data_geometrycollection,data_feature,data_featurecollection = to_binary()

# ------------------------- v2 geojson to bytes --------------------------------
def to_binary_v2():
    data_point_v2 = geojson_to_bytes_v2(geojson_point, srid=4326)
    data_multipoint_v2 = geojson_to_bytes_v2(geojson_multipoint, srid=4326)
    data_linestring_v2 = geojson_to_bytes_v2(geojson_linestring, srid=4326)
    data_multilinestring_v2 = geojson_to_bytes_v2(geojson_multilinestring, srid=4326)
    data_polygon_v2 = geojson_to_bytes_v2(geojson_polygon, srid=4326)
    data_multipolygon_v2 = geojson_to_bytes_v2(geojson_multipolygon, srid=4326)
    data_geometrycollection_v2 = geojson_to_bytes_v2(geojson_geometrycollection, srid=4326)
    data_feature_v2 = geojson_to_bytes_v2(geojson_feature, srid=4326)
    data_featurecollection_v2 = geojson_to_bytes_v2(geojson_featurecollection, srid=4326)
    return data_point_v2,data_multipoint_v2,data_linestring_v2,data_multilinestring_v2,data_polygon_v2,data_multipolygon_v2,data_geometrycollection_v2,data_feature_v2,data_featurecollection_v2

data_point_v2,data_multipoint_v2,data_linestring_v2,data_multilinestring_v2,data_polygon_v2,data_multipolygon_v2,data_geometrycollection_v2,data_feature_v2,data_featurecollection_v2 = to_binary_v2()

# ------------------------- v4 geojson to bytes --------------------------------
data_point_v4 = geojson_to_bytes_v4(geojson_point, srid=4326)
data_feature_v4 = geojson_to_bytes_v4(geojson_feature, srid=4326)
data_featurecollection_v4 = geojson_to_bytes_v4(geojson_featurecollection, srid=4326)

# ------------------------- v5 geojson to bytes --------------------------------
data_point_v5 = geojson_to_bytes_v5(geojson_point, srid=4326)
data_feature_v5 = geojson_to_bytes_v5(geojson_feature, srid=4326)
data_featurecollection_v5 = geojson_to_bytes_v5(geojson_featurecollection, srid=4326)

# ===================================================================================
# ============================== BYTES -> GEOJSON ===================================
# ===================================================================================
# -------------------------- v1 bytes to geojson -------------------------------
def out_geom():
    out_point = bytes_to_geojson(data_point)
    out_multipoint = bytes_to_geojson(data_multipoint)
    out_linestring = bytes_to_geojson(data_linestring)
    out_multilinestring = bytes_to_geojson(data_multilinestring)
    out_polygon = bytes_to_geojson(data_polygon)
    out_multipolygon = bytes_to_geojson(data_multipolygon)
    out_geometrycollection = bytes_to_geojson(data_geometrycollection)
    out_feature = bytes_to_geojson(data_feature)
    out_featurecollection = bytes_to_geojson(data_featurecollection)
    return out_point,out_multipoint, out_linestring, out_multilinestring, out_polygon, out_multipolygon, out_geometrycollection, out_feature, out_featurecollection

out_point, out_multipoint, out_linestring, out_multilinestring, out_polygon, out_multipolygon, out_geometrycollection, out_feature, out_featurecollection = out_geom()

# -------------------------- v2 bytes to geojson -------------------------------
def out_geom2():
    out_point_v2 = bytes_to_geojson_v2(data_point_v2)
    out_multipoint_v2 = bytes_to_geojson_v2(data_multipoint_v2)
    out_linestring_v2 = bytes_to_geojson_v2(data_linestring_v2)
    out_multilinestring_v2 = bytes_to_geojson_v2(data_multilinestring_v2)
    out_polygon_v2 = bytes_to_geojson_v2(data_polygon_v2)
    out_multipolygon_v2 = bytes_to_geojson_v2(data_multipolygon_v2)
    out_geometrycollection_v2 = bytes_to_geojson_v2(data_geometrycollection_v2)
    out_feature_v2 = bytes_to_geojson_v2(data_feature_v2)
    out_featurecollection_v2 = bytes_to_geojson_v2(data_featurecollection_v2)
    return out_point_v2, out_multipoint_v2,out_linestring_v2, out_multilinestring_v2, out_polygon_v2, out_multipolygon_v2, out_geometrycollection_v2, out_feature_v2, out_featurecollection_v2

out_point_v2, out_multipoint_v2,out_linestring_v2, out_multilinestring_v2, out_polygon_v2, out_multipolygon_v2, out_geometrycollection_v2, out_feature_v2, out_featurecollection_v2 = out_geom2()

# -------------------------- v4 bytes to geojson -------------------------------
out_point_v4 = bytes_to_geojson_v4(data_point_v4)
out_feature_v4 = bytes_to_geojson_v4(data_feature_v4)
out_featurecollection_v4 = bytes_to_geojson_v4(data_featurecollection_v4)

# -------------------------- v5 bytes to geojson -------------------------------
out_point_v5 = bytes_to_geojson_v4(data_point_v5)
out_feature_v5 = bytes_to_geojson_v5(data_feature_v5)
out_featurecollection_v5 = bytes_to_geojson_v5(data_featurecollection_v5)

# ===================================================================================
# ============================ FAIR GEOJSON LENGTH ==================================
# ===================================================================================
# GeoJSON → bytes fair  v1 comparison (compact, no whitespace)
def fair_length():
    geojson_bytes_point_fair = json.dumps(out_point, separators=(",", ":")).encode("utf-8")
    geojson_bytes_multipoint_fair = json.dumps(out_multipoint, separators=(",", ":")).encode("utf-8")
    geojson_bytes_linestring_fair = json.dumps(out_linestring, separators=(",", ":")).encode("utf-8")
    geojson_bytes_multilinestring_fair = json.dumps(out_multilinestring, separators=(",", ":")).encode("utf-8")
    geojson_bytes_polygon_fair = json.dumps(out_polygon, separators=(",", ":")).encode("utf-8")
    geojson_bytes_multipolygon_fair = json.dumps(out_multipolygon, separators=(",", ":")).encode("utf-8")
    geojson_bytes_geometrycollection_fair = json.dumps(out_geometrycollection, separators=(",", ":")).encode("utf-8")
    geojson_bytes_feature_fair = json.dumps(out_feature, separators=(",", ":")).encode("utf-8")
    geojson_bytes_featurecollection_fair = json.dumps(out_featurecollection, separators=(",", ":")).encode("utf-8")
    return geojson_bytes_point_fair,geojson_bytes_multipoint_fair,geojson_bytes_linestring_fair,geojson_bytes_multilinestring_fair,geojson_bytes_polygon_fair,geojson_bytes_multipolygon_fair,geojson_bytes_geometrycollection_fair,geojson_bytes_feature_fair,geojson_bytes_featurecollection_fair

geojson_bytes_point_fair,geojson_bytes_multipoint_fair,geojson_bytes_linestring_fair,geojson_bytes_multilinestring_fair,geojson_bytes_polygon_fair,geojson_bytes_multipolygon_fair,geojson_bytes_geometrycollection_fair,geojson_bytes_feature_fair,geojson_bytes_featurecollection_fair = fair_length()

# GeoJSON → bytes fair  v2 comparison (compact, no whitespace)
def fair_length2():
    geojson_bytes_point_fair_v2 = json.dumps(out_point_v2, separators=(",", ":")).encode("utf-8")
    geojson_bytes_multipoint_fair_v2 = json.dumps(out_multipoint_v2, separators=(",", ":")).encode("utf-8")
    geojson_bytes_linestring_fair_v2 = json.dumps(out_linestring_v2, separators=(",", ":")).encode("utf-8")
    geojson_bytes_multilinestring_fair_v2 = json.dumps(out_multilinestring_v2, separators=(",", ":")).encode("utf-8")
    geojson_bytes_polygon_fair_v2 = json.dumps(out_polygon_v2, separators=(",", ":")).encode("utf-8")
    geojson_bytes_multipolygon_fair_v2 = json.dumps(out_multipolygon_v2, separators=(",", ":")).encode("utf-8")
    geojson_bytes_geometrycollection_fair_v2 = json.dumps(out_geometrycollection_v2, separators=(",", ":")).encode("utf-8")
    geojson_bytes_feature_fair_v2 = json.dumps(out_feature_v2, separators=(",", ":")).encode("utf-8")
    geojson_bytes_featurecollection_fair_v2 = json.dumps(out_featurecollection_v2, separators=(",", ":")).encode("utf-8")
    return geojson_bytes_point_fair_v2,geojson_bytes_multipoint_fair_v2,geojson_bytes_linestring_fair_v2,geojson_bytes_multilinestring_fair_v2,geojson_bytes_polygon_fair_v2,geojson_bytes_multipolygon_fair_v2,geojson_bytes_geometrycollection_fair_v2,geojson_bytes_feature_fair_v2,geojson_bytes_featurecollection_fair_v2

geojson_bytes_point_fair_v2,geojson_bytes_multipoint_fair_v2,geojson_bytes_linestring_fair_v2,geojson_bytes_multilinestring_fair_v2,geojson_bytes_polygon_fair_v2,geojson_bytes_multipolygon_fair_v2,geojson_bytes_geometrycollection_fair_v2,geojson_bytes_feature_fair_v2,geojson_bytes_featurecollection_fair_v2 = fair_length2()

# GeoJSON → bytes fair  v4 comparison (compact, no whitespace)
geojson_bytes_point_fair_v4 = json.dumps(out_point_v4, separators=(",", ":")).encode("utf-8")
geojson_bytes_feature_fair_v4 = json.dumps(out_feature_v4, separators=(",", ":")).encode("utf-8")
geojson_bytes_featurecollection_fair_v4 = json.dumps(out_featurecollection_v4, separators=(",", ":")).encode("utf-8")

# GeoJSON → bytes fair  v5 comparison (compact, no whitespace)
geojson_bytes_point_fair_v5 = json.dumps(out_point_v5, separators=(",", ":")).encode("utf-8")
geojson_bytes_feature_fair_v5 = json.dumps(out_feature_v5, separators=(",", ":")).encode("utf-8")
geojson_bytes_featurecollection_fair_v5 = json.dumps(out_featurecollection_v5, separators=(",", ":")).encode("utf-8")

# ===================================================================================
# =================================== PRINTING ======================================
# ===================================================================================
def printing():
    print(" ======================= POINT ============================== ")
    print("geojson point bytes length:", len(geojson_bytes_point))
    print("protobuf v1 point bytes length:", len(data_point), "vs fair geojson bytes length:", len(geojson_bytes_point_fair))
    print("protobuf v2 point bytes length:", len(data_point_v2), "vs fair geojson bytes length:", len(geojson_bytes_point_fair_v2))
    print("protobuf v4 point bytes length:", len(data_point_v4), "vs fair geojson bytes length:", len(geojson_bytes_point_fair_v4))
    print("protobuf v5 point bytes length:", len(data_point_v5), "vs fair geojson bytes length:", len(geojson_bytes_point_fair_v5))
    print("out point v1 geojson:", json.dumps(out_point))
    print("out point v2 geojson:", json.dumps(out_point_v2))
    print("out point v4 geojson:", json.dumps(out_point_v4))
    print("out point v5 geojson:", json.dumps(out_point_v4))
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
    print("protobuf v4 feature bytes length:", len(data_feature_v4), "vs fair geojson bytes length:", len(geojson_bytes_feature_fair_v4))
    print("protobuf v5 feature bytes length:", len(data_feature_v5), "vs fair geojson bytes length:", len(geojson_bytes_feature_fair_v5))
    print("out v1 feature geojson:", json.dumps(out_feature))
    print("out v2 feature geojson:", json.dumps(out_feature_v2))
    print("out v4 feature geojson:", json.dumps(out_feature_v4))
    print("out v5 feature geojson:", json.dumps(out_feature_v5))
    print("================================================")

    print(" ======================= FEATURE COLLECTION ============================== ")
    # protobuf_fc_bytes = sum(len(b) for b in data_featurecollection) # otherwise just first part
    # protobuf_fc_bytes_v2 = sum(len(b) for b in data_featurecollection_v2)
    print("geojson featurecollection bytes length", len(geojson_bytes_featurecollection))
    print("protobuf v1 featurecollection bytes length", len(data_featurecollection), "vs fair geojson bytes length:", len(geojson_bytes_featurecollection_fair))
    print("protobuf v2 featurecollection bytes length", len(data_featurecollection_v2), "vs fair geojson bytes length:", len(geojson_bytes_featurecollection_fair_v2))
    print("protobuf v4 featurecollection bytes length", len(data_featurecollection_v4), "vs fair geojson bytes length:", len(geojson_bytes_featurecollection_fair_v4))
    print("protobuf v5 featurecollection bytes length", len(data_featurecollection_v5), "vs fair geojson bytes length:", len(geojson_bytes_featurecollection_fair_v5))
    # print("out v1 featurecollection geojson:", json.dumps(out_featurecollection))
    # print("out v2 featurecollection geojson:", json.dumps(out_featurecollection_v2))
    # print("out v4 featurecollection geojson:", json.dumps(out_featurecollection_v4))
    # print("out v5 featurecollection geojson:", json.dumps(out_featurecollection_v5))

printing()
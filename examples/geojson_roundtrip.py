import json
# --------------------------------------- v1 ------------------------------------------
from sfproto.geojson.v1.geojson import geojson_to_bytes, bytes_to_geojson #hopefully rest can now be removed
# --------------------------------------- v2 ------------------------------------------
from sfproto.geojson.v2.geojson import geojson_to_bytes_v2, bytes_to_geojson_v2
# --------------------------------------- v4 ------------------------------------------
from sfproto.geojson.v4.geojson import geojson_to_bytes_v4, bytes_to_geojson_v4
# --------------------------------------- v5 ------------------------------------------
from sfproto.geojson.v5.geojson import geojson_to_bytes_v5, bytes_to_geojson_v5
# --------------------------------------- v6 ------------------------------------------
from sfproto.geojson.v6.geojson import geojson_to_bytes_v6, bytes_to_geojson_v6

from pathlib import Path
from pyproj import CRS

from typing import Any, Dict, Union

GeoJSON = Dict[str, Any]

DEFAULT_SRID = 4326

# function to load the geojson from the file
def load_geojson(relative_path):
    base_dir = Path(__file__).parent   # examples/
    path = base_dir / relative_path    # examples/data/Point.geojson
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

# =================================== DATA ==========================================
geojson_point = load_geojson('data/Point.geojson')
geojson_linestring = load_geojson('data/Linestring.geojson')
geojson_polygon = load_geojson('data/Polygon_with_holes.geojson')
geojson_multipoint = load_geojson('data/MultiPoint.geojson')
geojson_multilinestring = load_geojson('data/MultiLineString.geojson')
geojson_multipolygon = load_geojson('data/MultiPolygon.geojson')
geojson_geometrycollection = load_geojson('data/GeometryCollection.geojson')
geojson_feature = load_geojson('data/Feature.geojson')
geojson_featurecollection = load_geojson('data/FeatureCollection.geojson')
geojson_BAG = load_geojson('data/bag_pand_count_10000.geojson')


_geojson_input = geojson_BAG
_version = 5

# function to extract the geojson, if no geojson is present, then use default srid = 4326
def extract_srid_from_geojson(obj: Union[GeoJSON, str]) -> int:
    """
    Extract EPSG SRID from a GeoJSON object.
    Returns DEFAULT_SRID (4326) if no CRS is present or parsable.
    """
    if isinstance(obj, str):
        import json
        obj = json.loads(obj)

    # CRS may appear at FeatureCollection or Feature level
    crs_obj = obj.get("crs")
    if not isinstance(crs_obj, dict):
        return DEFAULT_SRID

    if crs_obj.get("type") != "name":
        return DEFAULT_SRID

    props = crs_obj.get("properties")
    if not isinstance(props, dict):
        return DEFAULT_SRID

    name = props.get("name")
    if not isinstance(name, str):
        return DEFAULT_SRID

    # Expect formats like:
    # "urn:ogc:def:crs:EPSG::28992"
    # "EPSG:28992"
    if "EPSG" not in name:
        return DEFAULT_SRID

    try:
        return int(name.split(":")[-1])
    except ValueError:
        return DEFAULT_SRID

_srid = extract_srid_from_geojson(_geojson_input)
print(f'srid = {_srid}')

# for EPSG[degree]: 1e7;    EPSG[m]: 100;   EPSG[foot]: 3048;
# all for cm accuracy
def get_scaler(srid:int) -> int:
    crs = CRS.from_epsg(srid)
    # Geographic CRS -> degrees
    if crs.is_geographic:
        return 10000000  # ~1 cm-ish at mid-latitudes, safe for int32 lon/lat

    # Projected CRS -> look at axis units
    if crs.is_projected:
        unit_name = (crs.axis_info[0].unit_name or "").lower()

        if "metre" in unit_name or "meter" in unit_name:
            print("it worked")
            return 100  # 1 cm
        if "foot" in unit_name or "feet" in unit_name:
            return 3048

    return 100 #fallback if unknown

_default_scaler = get_scaler(_srid)
print(f'default scaler: {_default_scaler}')

# ================================ ROUND TRIP ======================================
def roundtrip(input_geojson, version, print_):
    data_length = json.dumps(input_geojson, separators=(",", ":")).encode("utf-8")
    print(f'data length: = {len(data_length)}')
    if version == 1:
        binary_representation = geojson_to_bytes(input_geojson, srid=_srid)
        to_geojson = bytes_to_geojson(binary_representation)
    elif version == 2:
        binary_representation = geojson_to_bytes_v2(input_geojson, srid=_srid, scale=_default_scaler)
        to_geojson = bytes_to_geojson_v2(binary_representation)
    elif version == 4:
        binary_representation = geojson_to_bytes_v4(input_geojson, srid=_srid)
        to_geojson = bytes_to_geojson_v4(binary_representation)
    elif version == 5:
        binary_representation = geojson_to_bytes_v5(input_geojson, srid=_srid, scale=_default_scaler)
        to_geojson = bytes_to_geojson_v5(binary_representation)
    elif version == 6:
        binary_representation = geojson_to_bytes_v6(input_geojson, srid=_srid, scale=_default_scaler)
        to_geojson = bytes_to_geojson_v6(binary_representation)
    else:
        print(f'version = {version} does not exist')
        return
    geojson_bytes_fair = json.dumps(to_geojson, separators=(",", ":")).encode("utf-8")
    print(f'protobuf v{version} bytes length: {len(binary_representation)} vs fair geojson byte length: {len(geojson_bytes_fair)}')
    if print_:
        print(f'output geojson after roundtrip: {to_geojson}')

roundtrip(_geojson_input, 4, False)
roundtrip(_geojson_input, _version, False)
roundtrip(_geojson_input, 2, False)
roundtrip(_geojson_input, 6, False)

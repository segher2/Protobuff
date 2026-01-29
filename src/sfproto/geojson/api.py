from typing import Dict, Any, Union
from pyproj import CRS

from sfproto.geojson.v4.geojson import (
    geojson_to_bytes_v4,
    bytes_to_geojson_v4,
)
from sfproto.geojson.v7.geojson import (
    geojson_to_bytes_v7,
    bytes_to_geojson_v7,
)

GeoJSON = Dict[str, Any]

DEFAULT_SRID = 4326


def extract_srid(geojson: GeoJSON) -> int:
    crs = geojson.get("crs")
    if not isinstance(crs, dict):
        return DEFAULT_SRID

    if crs.get("type") != "name":
        return DEFAULT_SRID

    props = crs.get("properties", {})
    name = props.get("name")

    if not isinstance(name, str) or "EPSG" not in name:
        return DEFAULT_SRID

    try:
        return int(name.split(":")[-1])
    except ValueError:
        return DEFAULT_SRID


def get_scaler(srid: int) -> int:
    crs = CRS.from_epsg(srid)

    if crs.is_geographic:
        return 10_000_000

    if crs.is_projected:
        unit = (crs.axis_info[0].unit_name or "").lower()
        if "metre" in unit or "meter" in unit:
            return 100
        if "foot" in unit:
            return 3048

    return 100


def encode_geojson(
    geojson: GeoJSON,
    *,
    delta: bool = False,
) -> bytes:
    srid = extract_srid(geojson)

    if delta:
        scale = get_scaler(srid)
        return geojson_to_bytes_v7(geojson, srid=srid, scale=scale)

    return geojson_to_bytes_v4(geojson, srid=srid)


def decode_geojson(
    data: bytes,
    *,
    delta: bool = False,
) -> GeoJSON:
    if delta:
        return bytes_to_geojson_v7(data)

    return bytes_to_geojson_v4(data)

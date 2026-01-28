from __future__ import annotations

import json
import struct
from typing import Any, Dict, List, Tuple, Union, Callable

# Reuse v2 geometry codecs (no attributes in pure geometries)
from sfproto.geojson.v2.geojson_point import geojson_point_to_bytes_v2, bytes_to_geojson_point_v2
from sfproto.geojson.v2.geojson_multipoint import geojson_multipoint_to_bytes_v2, bytes_to_geojson_multipoint_v2
from sfproto.geojson.v2.geojson_linestring import geojson_linestring_to_bytes_v2, bytes_to_geojson_linestring_v2
from sfproto.geojson.v2.geojson_multilinestring import geojson_multilinestring_to_bytes_v2, bytes_to_geojson_multilinestring_v2
from sfproto.geojson.v2.geojson_polygon import geojson_polygon_to_bytes_v2, bytes_to_geojson_polygon_v2
from sfproto.geojson.v2.geojson_multipolygon import geojson_multipolygon_to_bytes_v2, bytes_to_geojson_multipolygon_v2
from sfproto.geojson.v2.geojson_feature import geojson_feature_to_bytes_v2, bytes_to_geojson_feature_v2

# v6 Feature codec (WITH properties)
from sfproto.geojson.v6.geojson_featurecollection import geojson_featurecollection_to_bytes_v6, bytes_to_geojson_featurecollection_v6
from sfproto.geojson.v6.geojson_geometrycollection import geojson_geometrycollection_to_bytes_v6, bytes_to_geojson_geometrycollection_v6

GeoJSON = Dict[str, Any]
GeoJSONInput = Union[GeoJSON, str]

DEFAULT_SCALE = 10_000_000 #parameter for accuacy
# -> strongly relies on which srid, formula to get 'cm' accuracy scaler is in geojson_roundtrip.py file

# during encoding, the protobuf recieves a tag, which stores which type is encoded
# input can be geoemtry (Point, MultiPoint, LineString, ...), geometrycollection, feature and featurecollection
_TAG_LEN = 4
_TAG_GEOM = b"GEOM"
_TAG_GCOL = b"GCOL"
_TAG_FEAT = b"FEAT"
_TAG_FCOL = b"FCOL"

# -------------------- helpers --------------------
# if input geojson is string, convert to dict
def _loads_if_needed(obj_or_json: GeoJSONInput) -> GeoJSON:
    return json.loads(obj_or_json) if isinstance(obj_or_json, str) else obj_or_json

# first 4 bytes are tag of what is the geojson input type, rest is the geojson itself
def _wrap(tag: bytes, payload: bytes) -> bytes:
    if len(tag) != _TAG_LEN:
        raise ValueError("tag must be 4 bytes")
    return tag + payload

# return first 4 bytes (type tag) and the rest (encoded GeoJSON)
def _unwrap(data: bytes) -> Tuple[bytes, bytes]:
    if len(data) < _TAG_LEN:
        raise ValueError("Invalid data: too short")
    return data[:_TAG_LEN], data[_TAG_LEN:]


def _pack_chunks(chunks: List[bytes]) -> bytes:
    out = bytearray()
    out += struct.pack(">I", len(chunks))
    for c in chunks:
        out += struct.pack(">I", len(c))
        out += c
    return bytes(out)


def _unpack_chunks(payload: bytes) -> List[bytes]:
    mv = memoryview(payload)
    if len(mv) < 4:
        raise ValueError("Invalid payload: too short")
    (n,) = struct.unpack(">I", mv[:4])
    offset = 4
    chunks: List[bytes] = []
    for _ in range(n):
        if offset + 4 > len(mv):
            raise ValueError("Invalid payload: truncated")
        (ln,) = struct.unpack(">I", mv[offset:offset + 4])
        offset += 4
        if offset + ln > len(mv):
            raise ValueError("Invalid payload: truncated chunk")
        chunks.append(bytes(mv[offset:offset + ln]))
        offset += ln
    if offset != len(mv):
        raise ValueError("Invalid payload: trailing bytes")
    return chunks


# -------------------- geometry dispatch (v2 geometry reused) --------------------
def _geometry_to_bytes_v2(geometry: GeoJSON, srid: int, scale: int) -> bytes:
    t = geometry.get("type")
    # use correct function for the input type (also with using scaling factor)
    if t == "Point":
        return geojson_point_to_bytes_v2(geometry, srid=srid, scale=scale)
    if t == "MultiPoint":
        return geojson_multipoint_to_bytes_v2(geometry, srid=srid, scale=scale)
    if t == "LineString":
        return geojson_linestring_to_bytes_v2(geometry, srid=srid, scale=scale)
    if t == "MultiLineString":
        return geojson_multilinestring_to_bytes_v2(geometry, srid=srid, scale=scale)
    if t == "Polygon":
        return geojson_polygon_to_bytes_v2(geometry, srid=srid, scale=scale)
    if t == "MultiPolygon":
        return geojson_multipolygon_to_bytes_v2(geometry, srid=srid, scale=scale)
    raise ValueError(f"Unsupported geometry type: {t!r}")


_GEOM_DECODERS_V2: Tuple[Callable[[bytes], GeoJSON], ...] = (
    bytes_to_geojson_point_v2,
    bytes_to_geojson_multipoint_v2,
    bytes_to_geojson_polygon_v2,
    bytes_to_geojson_multipolygon_v2,
    bytes_to_geojson_linestring_v2,
    bytes_to_geojson_multilinestring_v2,
)


def _bytes_to_geometry_v2(data: bytes) -> GeoJSON:
    for dec in _GEOM_DECODERS_V2:
        try:
            return dec(data)
        except Exception:
            pass
    raise ValueError("Bytes do not contain a supported v2 Geometry")


# -------------------- actually used functions v6 --------------------
def geojson_to_bytes_v6(obj_or_json: GeoJSONInput, srid: int = 0, scale: int = DEFAULT_SCALE) -> bytes:
    """
    Convert GeoJSON (Geometry | GeometryCollection | Feature | FeatureCollection) -> bytes.

    - Geometry encoding uses v2 encoders (quantized ints + delta).
    - Feature encoding uses v6 Feature message, preserving properties.
    - GeometryCollection/FeatureCollection are stored as chunk lists in an envelope.
    """
    obj = _loads_if_needed(obj_or_json)
    t = obj.get("type")

    # v6 stream containers
    if t == "FeatureCollection":
        payload = geojson_featurecollection_to_bytes_v6(obj, srid=srid, scale=scale)
        return _wrap(_TAG_FCOL, _pack_chunks([payload]))

    if t == "GeometryCollection":
        payload = geojson_geometrycollection_to_bytes_v6(obj, srid=srid, scale=scale)
        return _wrap(_TAG_GCOL, _pack_chunks([payload]))

    # otherwise: fall back to v2 standalone
    if t == "Feature":
        payload = geojson_feature_to_bytes_v2(obj, srid=srid, scale=scale)
        return _wrap(_TAG_FEAT, _pack_chunks([payload]))

    payload = _geometry_to_bytes_v2(obj, srid=srid, scale=scale)
    return _wrap(_TAG_GEOM, _pack_chunks([payload]))


def bytes_to_geojson_v6(data: bytes) -> GeoJSON:
    """
    Convert bytes -> GeoJSON (Geometry | GeometryCollection | Feature | FeatureCollection).
    """

    # get type from tag and input from payload of the encoded binary format
    tag, payload = _unwrap(data)
    chunks = _unpack_chunks(payload)

    # use tag to find use the correct decoder formula
    if tag == _TAG_GEOM:
        if len(chunks) != 1:
            raise ValueError("Invalid GEOM payload")
        return _bytes_to_geometry_v2(chunks[0])

    if tag == _TAG_FEAT:
        if len(chunks) != 1:
            raise ValueError("Invalid FEAT payload")
        return bytes_to_geojson_feature_v2(chunks[0])

    if tag == _TAG_FCOL:
        if len(chunks) != 1:
            raise ValueError("Invalid FCOL payload")
        return bytes_to_geojson_featurecollection_v6(chunks[0])

    if tag == _TAG_GCOL:
        if len(chunks) != 1:
            raise ValueError("Invalid GCOL payload")
        return bytes_to_geojson_geometrycollection_v6(chunks[0])

    raise ValueError(f"Unknown envelope tag: {tag!r}")

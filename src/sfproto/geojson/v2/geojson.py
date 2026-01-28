from __future__ import annotations

import json
import struct
from typing import Any, Dict, List, Tuple, Union, Callable

from sfproto.geojson.v2.geojson_point import geojson_point_to_bytes_v2, bytes_to_geojson_point_v2
from sfproto.geojson.v2.geojson_multipoint import geojson_multipoint_to_bytes_v2, bytes_to_geojson_multipoint_v2
from sfproto.geojson.v2.geojson_linestring import geojson_linestring_to_bytes_v2, bytes_to_geojson_linestring_v2
from sfproto.geojson.v2.geojson_multilinestring import geojson_multilinestring_to_bytes_v2, bytes_to_geojson_multilinestring_v2
from sfproto.geojson.v2.geojson_polygon import geojson_polygon_to_bytes_v2, bytes_to_geojson_polygon_v2
from sfproto.geojson.v2.geojson_multipolygon import geojson_multipolygon_to_bytes_v2, bytes_to_geojson_multipolygon_v2
from sfproto.geojson.v2.geojson_feature import geojson_feature_to_bytes_v2, bytes_to_geojson_feature_v2

GeoJSON = Dict[str, Any]
GeoJSONInput = Union[GeoJSON, str]

DEFAULT_SCALE = 1000 #parameter for accuacy
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
    if isinstance(obj_or_json, str):
        return json.loads(obj_or_json)
    return obj_or_json

# first 4 bytes are tag of what is the geojson input type, rest is the geojson itself
def _wrap(tag: bytes, payload: bytes) -> bytes:
    if len(tag) != _TAG_LEN:
        raise ValueError("Internal error: tag must be 4 bytes")
    return tag + payload

# return first 4 bytes (type tag) and the rest (encoded GeoJSON)
def _unwrap(data: bytes) -> Tuple[bytes, bytes]:
    if len(data) < _TAG_LEN:
        raise ValueError("Invalid data: too short for envelope tag")
    return data[:_TAG_LEN], data[_TAG_LEN:]


def _pack_chunks(chunks: List[bytes]) -> bytes:
    # u32 count, then repeated (u32 len, bytes)
    out = bytearray()
    out += struct.pack(">I", len(chunks))
    for c in chunks:
        out += struct.pack(">I", len(c))
        out += c
    return bytes(out)


def _unpack_chunks(payload: bytes) -> List[bytes]:
    mv = memoryview(payload)
    if len(mv) < 4:
        raise ValueError("Invalid chunk payload: too short")

    (n,) = struct.unpack(">I", mv[:4])
    offset = 4

    chunks: List[bytes] = []
    for _ in range(n):
        if offset + 4 > len(mv):
            raise ValueError("Invalid chunk payload: truncated length")
        (ln,) = struct.unpack(">I", mv[offset:offset + 4])
        offset += 4
        if offset + ln > len(mv):
            raise ValueError("Invalid chunk payload: truncated chunk")
        chunks.append(bytes(mv[offset:offset + ln]))
        offset += ln

    if offset != len(mv):
        raise ValueError("Invalid chunk payload: trailing bytes")
    return chunks


# -------------------- geometry dispatch (v2) --------------------
def _geometry_to_bytes(geometry: GeoJSON, srid: int = 0, scale: int = DEFAULT_SCALE) -> bytes:
    gtype = geometry.get("type")
    # use correct function for the input type (also with using scaling factor)
    if gtype == "Point":
        return geojson_point_to_bytes_v2(geometry, srid=srid, scale=scale)
    if gtype == "MultiPoint":
        return geojson_multipoint_to_bytes_v2(geometry, srid=srid, scale=scale)
    if gtype == "LineString":
        return geojson_linestring_to_bytes_v2(geometry, srid=srid, scale=scale)
    if gtype == "MultiLineString":
        return geojson_multilinestring_to_bytes_v2(geometry, srid=srid, scale=scale)
    if gtype == "Polygon":
        return geojson_polygon_to_bytes_v2(geometry, srid=srid, scale=scale)
    if gtype == "MultiPolygon":
        return geojson_multipolygon_to_bytes_v2(geometry, srid=srid, scale=scale)

    raise ValueError(f"Unsupported GeoJSON geometry type: {gtype!r}")


_GEOM_DECODERS: Tuple[Callable[[bytes], GeoJSON], ...] = (
    bytes_to_geojson_point_v2,
    bytes_to_geojson_multipoint_v2,
    bytes_to_geojson_polygon_v2,
    bytes_to_geojson_multipolygon_v2,
    bytes_to_geojson_linestring_v2,
    bytes_to_geojson_multilinestring_v2,
)


def _bytes_to_geometry(data: bytes) -> GeoJSON:
    for dec in _GEOM_DECODERS:
        try:
            return dec(data)
        except Exception:
            pass
    raise ValueError("Bytes do not contain a supported Geometry")


# -------------------- actually used functions v2 --------------------
def geojson_to_bytes_v2(obj_or_json: GeoJSONInput, srid: int = 0, scale: int = DEFAULT_SCALE) -> bytes:
    """
    Convert GeoJSON (Geometry | GeometryCollection | Feature | FeatureCollection) -> bytes.
    - Uses v2 encoders (quantized ints, delta encoding)
    """
    obj = _loads_if_needed(obj_or_json)
    t = obj.get("type")

    # if Feature -> give feature tag + rest as chunks
    if t == "Feature":
        payload = geojson_feature_to_bytes_v2(obj, srid=srid, scale=scale)
        return _wrap(_TAG_FEAT, _pack_chunks([payload]))

    # if FeatureCollection -> give featurecollection tag + rest as chunks
    if t == "FeatureCollection":
        feats = obj.get("features")
        if not isinstance(feats, list):
            raise ValueError("FeatureCollection.features must be a list")

        feat_bytes = [geojson_feature_to_bytes_v2(f, srid=srid, scale=scale) for f in feats]
        return _wrap(_TAG_FCOL, _pack_chunks(feat_bytes))

    # if GeometryCollection -> give geometrycollection tag + rest as chunks
    if t == "GeometryCollection":
        geoms = obj.get("geometries")
        if not isinstance(geoms, list):
            raise ValueError("GeometryCollection.geometries must be a list")

        geom_bytes = [_geometry_to_bytes(g, srid=srid, scale=scale) for g in geoms]
        return _wrap(_TAG_GCOL, _pack_chunks(geom_bytes))

    # If input is not Feature, FeatureCollection or GeometryCollection, give 'geometry tag'
    payload = _geometry_to_bytes(obj, srid=srid, scale=scale)
    return _wrap(_TAG_GEOM, _pack_chunks([payload]))


def bytes_to_geojson_v2(data: bytes) -> GeoJSON:
    """
    Convert bytes -> GeoJSON (Geometry | GeometryCollection | Feature | FeatureCollection).

    Note: decoding uses per-geometry v2 decoders, which read srid/scale from the protobuf payload.
    """

    # get type from tag and input from payload of the encoded binary format
    tag, payload = _unwrap(data)
    chunks = _unpack_chunks(payload)

    # use tag to find use the correct decoder formula
    if tag == _TAG_GEOM:
        if len(chunks) != 1:
            raise ValueError("Invalid GEOM payload: expected 1 chunk")
        return _bytes_to_geometry(chunks[0])

    if tag == _TAG_GCOL:
        geoms = [_bytes_to_geometry(c) for c in chunks]
        return {"type": "GeometryCollection", "geometries": geoms}

    if tag == _TAG_FEAT:
        if len(chunks) != 1:
            raise ValueError("Invalid FEAT payload: expected 1 chunk")
        return bytes_to_geojson_feature_v2(chunks[0])

    if tag == _TAG_FCOL:
        features = [bytes_to_geojson_feature_v2(c) for c in chunks]
        return {"type": "FeatureCollection", "features": features}

    raise ValueError(f"Unknown envelope tag: {tag!r}")

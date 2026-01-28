from __future__ import annotations

import json
import struct
from typing import Any, Dict, List, Tuple, Union, Callable

# --- v2 geometry codecs (standalone geometries) ---
from sfproto.geojson.v2.geojson_point import geojson_point_to_bytes_v2, bytes_to_geojson_point_v2
from sfproto.geojson.v2.geojson_multipoint import geojson_multipoint_to_bytes_v2, bytes_to_geojson_multipoint_v2
from sfproto.geojson.v2.geojson_linestring import geojson_linestring_to_bytes_v2, bytes_to_geojson_linestring_v2
from sfproto.geojson.v2.geojson_multilinestring import geojson_multilinestring_to_bytes_v2, bytes_to_geojson_multilinestring_v2
from sfproto.geojson.v2.geojson_polygon import geojson_polygon_to_bytes_v2, bytes_to_geojson_polygon_v2
from sfproto.geojson.v2.geojson_multipolygon import geojson_multipolygon_to_bytes_v2, bytes_to_geojson_multipolygon_v2

# --- v5 Feature fallback (optional but useful for Feature outside collections) ---
from sfproto.geojson.v5.geojson_feature import geojson_feature_to_bytes_v5, bytes_to_geojson_feature_v5
from sfproto.geojson.v5.geojson_featurecollection import (
    geojson_featurecollection_to_bytes_v5,
    bytes_to_geojson_featurecollection_v5,
)

# --- v7 stream containers (attributes + v6-style packed deltas) ---
from sfproto.geojson.v7.geojson_featurecollection import (
    geojson_featurecollection_to_bytes_v7,
    bytes_to_geojson_featurecollection_v7,
)
from sfproto.geojson.v7.geojson_geometrycollection import (
    geojson_geometrycollection_to_bytes_v7,
    bytes_to_geojson_geometrycollection_v7,
)

GeoJSON = Dict[str, Any]
GeoJSONInput = Union[GeoJSON, str]

DEFAULT_SCALE = 10_000_000  # 1e7

_TAG_LEN = 4

# Existing tags (from your previous versions)
_TAG_GEOM = b"GEOM"  # standalone geometry payload (v2)
_TAG_GCOL = b"GCOL"  # geometrycollection as chunked list of v2 geometries (legacy)
_TAG_FEAT = b"FEAT"  # standalone feature payload (v5)
_TAG_FCOL = b"FCOL"  # featurecollection payload (v5)

# New v7 tags
_TAG_GC7 = b"GCV7"   # GeometryCollection v7 (single protobuf payload)
_TAG_FC7 = b"FCV7"   # FeatureCollection v7 (single protobuf payload)
# (Optional) If you later make standalone Feature v7, add: _TAG_FT7 = b"FTV7"


# -------------------- helpers --------------------

def _loads_if_needed(obj_or_json: GeoJSONInput) -> GeoJSON:
    return json.loads(obj_or_json) if isinstance(obj_or_json, str) else obj_or_json


def _wrap(tag: bytes, payload: bytes) -> bytes:
    if len(tag) != _TAG_LEN:
        raise ValueError("tag must be 4 bytes")
    return tag + payload


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


# -------------------- v2 geometry dispatch (standalone) --------------------

def _geometry_to_bytes_v2(geometry: GeoJSON, srid: int, scale: int) -> bytes:
    t = geometry.get("type")
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


# -------------------- public API (v7) --------------------

def geojson_to_bytes_v7(obj_or_json: GeoJSONInput, srid: int = 0, scale: int = DEFAULT_SCALE) -> bytes:
    """
    Encode GeoJSON into bytes using v7 where applicable:
    - FeatureCollection -> v7 FeatureCollection (single protobuf payload)
    - GeometryCollection -> v7 GeometryCollection (single protobuf payload)
    - Feature -> fallback to v5 Feature (unless you implement standalone v7 Feature)
    - Geometry -> v2 standalone geometry
    """
    obj = _loads_if_needed(obj_or_json)
    t = obj.get("type")

    # v7 containers
    if t == "FeatureCollection":
        payload = geojson_featurecollection_to_bytes_v7(obj, srid=srid, scale=scale)
        return _wrap(_TAG_FC7, _pack_chunks([payload]))

    if t == "GeometryCollection":
        payload = geojson_geometrycollection_to_bytes_v7(obj, srid=srid, scale=scale)
        return _wrap(_TAG_GC7, _pack_chunks([payload]))

    # Feature: keep your existing v5 Feature codec (properties supported)
    if t == "Feature":
        payload = geojson_feature_to_bytes_v5(obj, srid=srid, scale=scale)
        return _wrap(_TAG_FEAT, _pack_chunks([payload]))

    # Otherwise: geometry as v2
    payload = _geometry_to_bytes_v2(obj, srid=srid, scale=scale)
    return _wrap(_TAG_GEOM, _pack_chunks([payload]))


def bytes_to_geojson_v7(data: bytes) -> GeoJSON:
    """
    Decode bytes into GeoJSON.
    Supports:
    - v7 tags (FCV7/GCV7)
    - legacy v5 tags (FEAT/FCOL)
    - legacy v2 tags (GEOM, GCOL)
    """
    tag, payload = _unwrap(data)
    chunks = _unpack_chunks(payload)

    # v7
    if tag == _TAG_FC7:
        if len(chunks) != 1:
            raise ValueError("Invalid FCV7 payload")
        return bytes_to_geojson_featurecollection_v7(chunks[0])

    if tag == _TAG_GC7:
        if len(chunks) != 1:
            raise ValueError("Invalid GCV7 payload")
        return bytes_to_geojson_geometrycollection_v7(chunks[0])

    # legacy v2 geometry
    if tag == _TAG_GEOM:
        if len(chunks) != 1:
            raise ValueError("Invalid GEOM payload")
        return _bytes_to_geometry_v2(chunks[0])

    if tag == _TAG_GCOL:
        # legacy: list of v2 geometry chunks
        geoms = [_bytes_to_geometry_v2(c) for c in chunks]
        return {"type": "GeometryCollection", "geometries": geoms}

    # legacy v5 feature/featurecollection
    if tag == _TAG_FEAT:
        if len(chunks) != 1:
            raise ValueError("Invalid FEAT payload")
        return bytes_to_geojson_feature_v5(chunks[0])

    if tag == _TAG_FCOL:
        if len(chunks) != 1:
            raise ValueError("Invalid FCOL payload")
        return bytes_to_geojson_featurecollection_v5(chunks[0])

    raise ValueError(f"Unknown envelope tag: {tag!r}")

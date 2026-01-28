from __future__ import annotations

import json
from typing import Any, Dict, List, Union, Tuple

from sfproto.geojson.v2.geojson_polygon import DEFAULT_SCALE
from sfproto.sf.v2 import geometry_pb2

GeoJSON = Dict[str, Any]

DEFAULT_SCALE = 1000 #parameter for accuacy
# -> strongly relies on which srid, formula to get 'cm' accuracy scaler is in geojson_roundtrip.py file

# scaler necessary for integer storing of coords
def _require_scale(scale: int) -> int:
    scale = int(scale)
    if scale <= 0:
        raise ValueError("scale must be a positive integer (e.g., 10000000)")
    return scale


def _quantize(v: float, scale: int) -> int:
    # multiply the floating number with scaler value and round to a integer
    return int(round(float(v) * scale))


def _dequantize(vi: int, scale: int) -> float:
    # divide by scaler to get 'normal' float number back again (less precision)
    return float(vi) / float(scale)


def _quantize_ring(ring: List[List[float]], scale: int) -> List[Tuple[int, int]]:
    if not isinstance(ring, (list, tuple)) or len(ring) < 4:
        raise ValueError("LinearRing must have at least 4 coordinates")

    q: List[Tuple[int, int]] = []
    for j, coord in enumerate(ring):
        if not (isinstance(coord, (list, tuple)) and len(coord) >= 2):
            raise ValueError(f"Polygon coordinates must be [x, y], got {coord!r} at index {j}")
        if coord[0] is None or coord[1] is None:
            raise ValueError(f"Polygon coordinates cannot be null, got {coord!r} at index {j}")
        q.append((_quantize(coord[0], scale), _quantize(coord[1], scale)))

    if q[0] != q[-1]:
        q.append(q[0])

    if len(q) < 4:
        raise ValueError("LinearRing must have at least 4 coordinates (after closure)")

    return q


def _fill_delta_ring(pb_ring: geometry_pb2.DeltaRing, q: List[Tuple[int, int]]) -> None:
    x0, y0 = q[0]
    pb_ring.start.x = int(x0)
    pb_ring.start.y = int(y0)

    prev_x, prev_y = x0, y0
    for (x, y) in q[1:]:
        pb_ring.dx.append(int(x - prev_x))
        pb_ring.dy.append(int(y - prev_y))
        prev_x, prev_y = x, y


def _decode_delta_ring(pb_ring: geometry_pb2.DeltaRing, scale: int) -> List[List[float]]:
    if len(pb_ring.dx) != len(pb_ring.dy):
        raise ValueError(f"DeltaRing dx/dy length mismatch: {len(pb_ring.dx)} vs {len(pb_ring.dy)}")

    coords: List[List[float]] = []
    x = int(pb_ring.start.x)
    y = int(pb_ring.start.y)
    coords.append([_dequantize(x, scale), _dequantize(y, scale)])

    for dx, dy in zip(pb_ring.dx, pb_ring.dy):
        x += int(dx)
        y += int(dy)
        coords.append([_dequantize(x, scale), _dequantize(y, scale)])

    # Ensure closed ring
    if coords[0] != coords[-1]:
        coords.append(coords[0])

    return coords


# ============================================================
# GeoJSON MultiPolygon -> Protobuf Geometry (v2)
# ============================================================

def geojson_multipolygon_to_pb(obj: GeoJSON, srid: int = 0, scale: int = DEFAULT_SCALE) -> geometry_pb2.Geometry:
    """
    Convert GeoJSON MultiPolygon -> Protobuf Geometry
    """
    if obj.get("type") != "MultiPolygon":
        raise ValueError(
            f"Expected GeoJSON type=MultiPolygon, got {obj.get('type')!r}"
        )

    polygons = obj.get("coordinates")
    if not isinstance(polygons, list):
        raise ValueError("MultiPolygon coordinates must be a list")

    scale = _require_scale(scale)

    g = geometry_pb2.Geometry()
    g.crs.srid = int(srid)
    g.crs.scale = int(scale)


    for p_i, poly in enumerate(polygons):
        if not isinstance(poly, list) or len(poly) == 0:
            raise ValueError(f"Polygon at index {p_i} must be a non-empty list of linear rings")

        pb_poly = g.multipolygon.polygons.add()

        for r_i, ring in enumerate(poly):
            q = _quantize_ring(ring, scale)
            pb_ring = pb_poly.rings.add()  # DeltaRing in v2
            _fill_delta_ring(pb_ring, q)

    return g


def pb_to_geojson_multipolygon(g: geometry_pb2.Geometry,) -> GeoJSON:
    """
    Convert Protobuf Geometry -> GeoJSON MultiPolygon
    """
    if not g.HasField("multipolygon"):
        raise ValueError(
            f"Expected Geometry.multipolygon, got {g.WhichOneof('geom')!r}"
        )

    scale = int(getattr(g.crs, "scale", 0)) or DEFAULT_SCALE
    scale = _require_scale(scale)

    coordinates: List[List[List[List[float]]]] = []

    for pb_poly in g.multipolygon.polygons:
        poly_coords: List[List[List[float]]] = []
        for pb_ring in pb_poly.rings:
            poly_coords.append(_decode_delta_ring(pb_ring, scale))
        coordinates.append(poly_coords)

    # output Multipolygon GeoJSON format
    return {"type": "MultiPolygon", "coordinates": coordinates}


def geojson_multipolygon_to_bytes_v2(obj_or_json: Union[GeoJSON, str], srid: int = 0, scale: int = DEFAULT_SCALE) -> bytes:
    """
    GeoJSON MultiPolygon (dict or JSON string) -> Protobuf bytes.
    """
    # if input geojson is string, convert to dict
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    # use message to encode to binary format
    msg = geojson_multipolygon_to_pb(obj, srid=srid, scale=scale)
    return msg.SerializeToString()


def bytes_to_geojson_multipolygon_v2(data: bytes) -> GeoJSON:
    """
    Protobuf-encoded bytes -> GeoJSON MultiPolygon dict.
    """
    # use message to decode to GeoJSON format
    msg = geometry_pb2.Geometry.FromString(data)
    return pb_to_geojson_multipolygon(msg)

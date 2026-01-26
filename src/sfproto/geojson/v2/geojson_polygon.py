from __future__ import annotations

import json
from typing import Any, Dict, List, Union, Tuple

from sfproto.sf.v2 import geometry_pb2

GeoJSON = Dict[str, Any]

DEFAULT_SCALE = 10000000 # 1e7 for cm accuracy

def _require_scale(scale: int) -> int:
    scale = int(scale)
    if scale <= 0:
        raise ValueError("scale must be a positive integer (e.g., 10000000)")
    return scale

def _quantize(v: float, scale: int) -> int:
    return int(round(float(v) * scale))


def _dequantize(vi: int, scale: int) -> float:
    return float(vi) / float(scale)


def _is_closed_ring(ring: List[List[float]]) -> bool:
    return (
        len(ring) >= 2
        and ring[0][0] == ring[-1][0]
        and ring[0][1] == ring[-1][1]
    )


def _quantize_ring(ring: List[List[float]], scale: int) -> List[Tuple[int, int]]:
    if not isinstance(ring, (list, tuple)) or len(ring) < 4:
        raise ValueError("LinearRing must have at least 4 coordinates")

    # Validate coords and quantize
    q: List[Tuple[int, int]] = []
    for j, coord in enumerate(ring):
        if not (isinstance(coord, (list, tuple)) and len(coord) >= 2):
            raise ValueError(f"Polygon coordinates must be [x, y], got {coord!r} at index {j}")
        if coord[0] is None or coord[1] is None:
            raise ValueError(f"Polygon coordinates cannot be null, got {coord!r} at index {j}")
        q.append((_quantize(coord[0], scale), _quantize(coord[1], scale)))

    # GeoJSON requires closed rings: ensure closure (in quantized space)
    if q[0] != q[-1]:
        q.append(q[0])

    # After closing, a valid ring must still have >= 4 positions
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

    # Ensure closed ring on output (GeoJSON requires this)
    if coords[0] != coords[-1]:
        coords.append(coords[0])

    return coords

# ============================================================
# GeoJSON Polygon -> Protobuf Geometry (v2: quantized + delta)
# ============================================================

def geojson_polygon_to_pb(obj: GeoJSON, srid: int = 0, scale: int = DEFAULT_SCALE) -> geometry_pb2.Geometry:
    """
    Convert a GeoJSON Polygon dict -> Protobuf Geometry message.
    """
    if obj.get("type") != "Polygon":
        raise ValueError(f"Expected GeoJSON type=Polygon, got: {obj.get('type')!r}")

    rings = obj.get("coordinates")
    if not isinstance(rings, list) or len(rings) == 0:
        raise ValueError("GeoJSON Polygon must have at least one linear ring")


    scale = _require_scale(scale)

    g = geometry_pb2.Geometry()
    g.crs.srid = int(srid)
    g.crs.scale = int(scale)

    for ring in rings:
        q = _quantize_ring(ring, scale)
        pb_ring = g.polygon.rings.add()  # DeltaRing
        _fill_delta_ring(pb_ring, q)

    return g


def pb_to_geojson_polygon(g: geometry_pb2.Geometry) -> GeoJSON:
    """
    Convert Protobuf Geometry message -> GeoJSON Polygon dict.
    """
    if not g.HasField("polygon"):
        raise ValueError(
            f"Expected Geometry.polygon, got oneof={g.WhichOneof('geom')!r}"
        )

    scale = int(getattr(g.crs, "scale", 0)) or DEFAULT_SCALE
    scale = _require_scale(scale)

    coordinates: List[List[List[float]]] = []

    for pb_ring in g.polygon.rings:
        coordinates.append(_decode_delta_ring(pb_ring, scale))

    return {"type": "Polygon", "coordinates": coordinates}

def geojson_polygon_to_bytes_v2(obj_or_json: Union[GeoJSON, str], srid: int = 0, scale: int = DEFAULT_SCALE) -> bytes:
    """
    Accepts a GeoJSON dict OR JSON string, returns Protobuf-encoded bytes.
    """
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    msg = geojson_polygon_to_pb(obj, srid=srid, scale=scale)
    return msg.SerializeToString()


def bytes_to_geojson_polygon_v2(data: bytes) -> GeoJSON:
    """
    Protobuf-encoded bytes -> GeoJSON Polygon dict.
    """
    msg = geometry_pb2.Geometry.FromString(data)
    return pb_to_geojson_polygon(msg)

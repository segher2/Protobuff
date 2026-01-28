from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple, Union

from sfproto.sf.v2 import geometry_pb2

GeoJSON = Dict[str, Any]
DEFAULT_SCALE = 1000  # 1e7 -> ~cm precision for lon/lat


def _require_scale(scale: int) -> int:
    scale = int(scale)
    if scale <= 0:
        raise ValueError("scale must be a positive integer (e.g., 10000000)")
    return scale


def _quantize(value: float, scale: int) -> int:
    return int(round(float(value) * scale))


def _dequantize(value_i: int, scale: int) -> float:
    return float(value_i) / float(scale)


def _quantize_line(line: List[List[float]], scale: int) -> List[Tuple[int, int]]:
    """Validate and quantize one LineString coordinate array."""
    if not isinstance(line, (list, tuple)) or len(line) < 2:
        raise ValueError("Each LineString must have at least two positions")

    out: List[Tuple[int, int]] = []
    for j, pair in enumerate(line):
        if (
            not isinstance(pair, (list, tuple))
            or len(pair) < 2
            or pair[0] is None
            or pair[1] is None
        ):
            raise ValueError(f"Invalid coordinate at index {j}: {pair!r}")
        out.append((_quantize(pair[0], scale), _quantize(pair[1], scale)))
    return out


def _fill_delta_line(pb_line: geometry_pb2.DeltaLineString, q: List[Tuple[int, int]]) -> None:
    """Fill a DeltaLineString from quantized points q."""
    x0, y0 = q[0]
    pb_line.start.x = int(x0)
    pb_line.start.y = int(y0)

    prev_x, prev_y = x0, y0
    for (x, y) in q[1:]:
        pb_line.dx.append(int(x - prev_x))
        pb_line.dy.append(int(y - prev_y))
        prev_x, prev_y = x, y


def _decode_delta_line(pb_line: geometry_pb2.DeltaLineString, scale: int) -> List[List[float]]:
    """Decode a DeltaLineString to GeoJSON coords (floats)."""
    if len(pb_line.dx) != len(pb_line.dy):
        raise ValueError(f"dx/dy length mismatch: {len(pb_line.dx)} vs {len(pb_line.dy)}")

    coords: List[List[float]] = []
    x = int(pb_line.start.x)
    y = int(pb_line.start.y)
    coords.append([_dequantize(x, scale), _dequantize(y, scale)])

    for dx, dy in zip(pb_line.dx, pb_line.dy):
        x += int(dx)
        y += int(dy)
        coords.append([_dequantize(x, scale), _dequantize(y, scale)])

    return coords


# ============================================================
# GeoJSON MultiLineString -> Protobuf Geometry (v2)
# ============================================================

def geojson_multilinestring_to_pb(
    obj: GeoJSON,
    srid: int = 0,
    scale: int = DEFAULT_SCALE,
) -> geometry_pb2.Geometry:
    if obj.get("type") != "MultiLineString":
        raise ValueError(f"Expected GeoJSON type=MultiLineString, got {obj.get('type')!r}")

    lines = obj.get("coordinates")
    if not isinstance(lines, (list, tuple)) or len(lines) == 0:
        raise ValueError("GeoJSON MultiLineString coordinates must be a non-empty list of LineStrings")

    scale = _require_scale(scale)

    g = geometry_pb2.Geometry()
    g.crs.srid = int(srid)
    g.crs.scale = int(scale)

    # IMPORTANT: fill g.multilinestring.line_strings (each is a DeltaLineString)
    for i, line in enumerate(lines):
        q = _quantize_line(line, scale)  # quantized points for THIS line only
        pb_line = g.multilinestring.line_strings.add()
        _fill_delta_line(pb_line, q)

    return g


# ============================================================
# Protobuf Geometry (v2) -> GeoJSON MultiLineString
# ============================================================

def pb_to_geojson_multilinestring(g: geometry_pb2.Geometry) -> GeoJSON:
    if not g.HasField("multilinestring"):
        raise ValueError(f"Expected Geometry.multilinestring, got oneof={g.WhichOneof('geom')!r}")

    scale = int(getattr(g.crs, "scale", 0)) or DEFAULT_SCALE
    scale = _require_scale(scale)

    coords_out: List[List[List[float]]] = []
    for pb_line in g.multilinestring.line_strings:
        coords_out.append(_decode_delta_line(pb_line, scale))

    return {"type": "MultiLineString", "coordinates": coords_out}


# ============================================================
# Bytes helpers
# ============================================================

def geojson_multilinestring_to_bytes_v2(obj_or_json: Union[GeoJSON, str],srid: int = 0,scale: int = DEFAULT_SCALE,) -> bytes:
    obj = json.loads(obj_or_json) if isinstance(obj_or_json, str) else obj_or_json
    msg = geojson_multilinestring_to_pb(obj, srid=srid, scale=scale)
    return msg.SerializeToString()


def bytes_to_geojson_multilinestring_v2(data: bytes) -> GeoJSON:
    msg = geometry_pb2.Geometry.FromString(data)
    return pb_to_geojson_multilinestring(msg)

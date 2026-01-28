from __future__ import annotations

import json
from typing import Any, Dict, Union

from sfproto.sf.v1 import geometry_pb2

GeoJSON = Dict[str, Any]


# ============================================================
# GeoJSON MultiLineString -> Protobuf Geometry
# ============================================================
def geojson_multilinestring_to_pb(obj: GeoJSON, srid: int = 0) -> geometry_pb2.Geometry:
    """
    Convert a GeoJSON MultiLineString dict -> Protobuf Geometry message.
    """
    if obj.get("type") != "MultiLineString":
        raise ValueError(
            f"Expected GeoJSON type=MultiLineString, got {obj.get('type')!r}"
        )

    lines = obj.get("coordinates")
    if not isinstance(lines, (list, tuple)) or len(lines) == 0:
        raise ValueError(
            "GeoJSON MultiLineString coordinates must be a non-empty list of LineStrings"
        )

    g = geometry_pb2.Geometry()
    g.crs.srid = int(srid)

    for i, line in enumerate(lines):
        if not isinstance(line, (list, tuple)) or len(line) < 2:
            raise ValueError(
                f"LineString at index {i} must have at least two points"
            )

        # use multilinestring, linestring and coord message to create the geometry message
        pb_line = g.multilinestring.line_strings.add()

        for j, pair in enumerate(line):
            if (
                not isinstance(pair, (list, tuple))
                or len(pair) < 2
                or pair[0] is None
                or pair[1] is None
            ):
                raise ValueError(
                    f"Invalid coordinate at line {i}, index {j}: {pair!r}"
                )

            x, y = pair
            p = pb_line.points.add()
            p.coord.x = float(x)
            p.coord.y = float(y)

    return g


# ============================================================
# Protobuf Geometry -> GeoJSON MultiLineString
# ============================================================

def pb_to_geojson_multilinestring(g: geometry_pb2.Geometry) -> GeoJSON:
    """
    Convert Protobuf Geometry message -> GeoJSON MultiLineString dict.
    """
    if not g.HasField("multilinestring"):
        raise ValueError(
            f"Expected Geometry.multilinestring, got oneof={g.WhichOneof('geom')!r}"
        )

    # output MultiLineString geometry format
    return {
        "type": "MultiLineString",
        "coordinates": [
            [
                [p.coord.x, p.coord.y]
                for p in line.points
            ]
            for line in g.multilinestring.line_strings
        ],
    }


# ============================================================
# Bytes helpers
# ============================================================

def geojson_multilinestring_to_bytes(obj_or_json: Union[GeoJSON, str], srid: int = 0) -> bytes:
    """
    GeoJSON MultiLineString (dict or JSON string) -> Protobuf bytes.
    """
    # if input geojson is string, convert to dict
    if isinstance(obj_or_json, str):
        obj = json.loads(obj_or_json)
    else:
        obj = obj_or_json

    # use message to encode to binary format
    msg = geojson_multilinestring_to_pb(obj, srid=srid)
    return msg.SerializeToString()


def bytes_to_geojson_multilinestring(data: bytes) -> GeoJSON:
    """
    Protobuf-encoded bytes -> GeoJSON MultiLineString dict.
    """
    # use message to decode to GeoJSON format
    msg = geometry_pb2.Geometry.FromString(data)
    return pb_to_geojson_multilinestring(msg)

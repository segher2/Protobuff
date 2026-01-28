from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple, Union, Optional

from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict

from sfproto.sf.v7 import geometry_pb2
from sfproto.geojson.v6.geojson_featurecollection import _flatten_geometry, _first_coord_of_geometry

GeoJSON = Dict[str, Any]
GeoJSONInput = Union[GeoJSON, str]

DEFAULT_SCALE = 10_000_000
_RESERVED_FCOL = {"type", "features", "bbox", "name", "crs"}

# ---------- struct helpers (same as v5) ----------

def _loads_if_needed(obj_or_json: GeoJSONInput) -> GeoJSON:
    return json.loads(obj_or_json) if isinstance(obj_or_json, str) else obj_or_json

def _dict_to_struct(d: Optional[Dict[str, Any]]) -> Struct:
    s = Struct()
    if d is None:
        return s
    s.update(d)
    return s

def _struct_to_dict(s: Struct) -> Dict[str, Any]:
    return MessageToDict(s)

def _extract_extra_fcol(obj: GeoJSON) -> Dict[str, Any]:
    return {k: v for k, v in obj.items() if k not in _RESERVED_FCOL}

# ---------- quantization helpers (same as you have) ----------
def _q(v: float, scale: int) -> int:
    return int(round(float(v) * scale))

def _uq(v: int, scale: int) -> float:
    return float(v) / float(scale)

# ---------- geometry flattening (reuse from v6) ----------
# _ring_drop_closure
# _first_coord_of_geometry
# _flatten_geometry

def _encode_stream_geometry(geom: GeoJSON, global_start_xy: Tuple[int, int], scale: int) -> geometry_pb2.StreamGeometry:
    gtype, flat_pts, part_sizes, poly_ring_counts = _flatten_geometry(geom)

    cursor_x, cursor_y = global_start_xy

    pb = geometry_pb2.StreamGeometry()
    pb.type = int(gtype)

    if part_sizes:
        pb.part_sizes.extend([int(x) for x in part_sizes])
    if poly_ring_counts:
        pb.poly_ring_counts.extend([int(x) for x in poly_ring_counts])

    for (x, y) in flat_pts:
        qx, qy = _q(x, scale), _q(y, scale)
        pb.dxy.append(int(qx - cursor_x))
        pb.dxy.append(int(qy - cursor_y))
        cursor_x, cursor_y = qx, qy

    return pb


def _decode_stream_geometry(pb: geometry_pb2.StreamGeometry, global_start_xy: Tuple[int, int], scale: int) -> GeoJSON:
    cursor_x, cursor_y = global_start_xy
    dxy = list(pb.dxy)
    if len(dxy) % 2 != 0:
        raise ValueError("Invalid StreamGeometry: dxy length must be even")

    qpts: List[Tuple[int, int]] = []
    for i in range(0, len(dxy), 2):
        cursor_x += int(dxy[i])
        cursor_y += int(dxy[i + 1])
        qpts.append((cursor_x, cursor_y))

    pts = [(_uq(x, scale), _uq(y, scale)) for (x, y) in qpts]

    t = int(pb.type)
    part_sizes = list(pb.part_sizes)
    poly_ring_counts = list(pb.poly_ring_counts)

    # same rebuild logic as your v6 decoder:
    if t == geometry_pb2.POINT:
        x, y = pts[0]
        return {"type": "Point", "coordinates": [x, y]}

    if t == geometry_pb2.MULTIPOINT:
        return {"type": "MultiPoint", "coordinates": [[x, y] for (x, y) in pts]}

    if t == geometry_pb2.LINESTRING:
        return {"type": "LineString", "coordinates": [[x, y] for (x, y) in pts]}

    if t == geometry_pb2.MULTILINESTRING:
        out_lines = []
        idx = 0
        for n in part_sizes:
            seg = pts[idx: idx + n]
            out_lines.append([[x, y] for (x, y) in seg])
            idx += n
        return {"type": "MultiLineString", "coordinates": out_lines}

    if t == geometry_pb2.POLYGON:
        out_rings = []
        idx = 0
        for n in part_sizes:
            ring = pts[idx: idx + n]
            idx += n
            ring_coords = [[x, y] for (x, y) in ring]
            if ring_coords:
                ring_coords.append(ring_coords[0])
            out_rings.append(ring_coords)
        return {"type": "Polygon", "coordinates": out_rings}

    if t == geometry_pb2.MULTIPOLYGON:
        out_polys = []
        idx = 0
        ring_size_idx = 0
        for ring_count in poly_ring_counts:
            poly = []
            for _ in range(ring_count):
                n = part_sizes[ring_size_idx]
                ring_size_idx += 1
                ring = pts[idx: idx + n]
                idx += n
                ring_coords = [[x, y] for (x, y) in ring]
                if ring_coords:
                    ring_coords.append(ring_coords[0])
                poly.append(ring_coords)
            out_polys.append(poly)
        return {"type": "MultiPolygon", "coordinates": out_polys}

    raise ValueError(f"Unsupported StreamGeometry type enum: {t}")


# ---------- public API ----------

def geojson_featurecollection_to_bytes_v7(obj_or_json: GeoJSONInput, srid: int, scale: int) -> bytes:
    obj = _loads_if_needed(obj_or_json)
    if obj.get("type") != "FeatureCollection":
        raise ValueError(f"Expected FeatureCollection, got {obj.get('type')!r}")

    feats = obj.get("features")
    if not isinstance(feats, list) or not feats:
        raise ValueError("FeatureCollection.features must be a non-empty list")

    fc = geometry_pb2.FeatureCollection()
    fc.crs.srid = int(srid)
    fc.crs.scale = int(scale)

    # global_start from first feature geometry
    first_geom = feats[0].get("geometry")
    if not isinstance(first_geom, dict):
        raise ValueError("First feature has no geometry object")

    x0, y0 = _first_coord_of_geometry(first_geom)
    fc.global_start.x = _q(x0, scale)
    fc.global_start.y = _q(y0, scale)
    global_start_xy = (int(fc.global_start.x), int(fc.global_start.y))

    # features
    for f in feats:
        if f.get("type") != "Feature":
            raise ValueError("FeatureCollection.features must contain Features")

        geom = f.get("geometry")
        if not isinstance(geom, dict):
            raise ValueError("Feature.geometry must be an object (not null)")

        feat_pb = geometry_pb2.Feature()
        feat_pb.geometry.CopyFrom(_encode_stream_geometry(geom, global_start_xy, scale))

        props = f.get("properties")
        if props is not None and not isinstance(props, dict):
            raise ValueError("Feature.properties must be an object or null")
        feat_pb.properties.CopyFrom(_dict_to_struct(props))

        fid = f.get("id")
        if fid is not None:
            feat_pb.id = str(fid)

        bbox = f.get("bbox")
        if isinstance(bbox, list) and len(bbox) in (4, 6) and all(isinstance(x, (int, float)) for x in bbox):
            feat_pb.bbox.extend([float(x) for x in bbox])

        # extra keys on Feature
        reserved = {"type", "geometry", "properties", "id", "bbox"}
        extra = {k: v for k, v in f.items() if k not in reserved}
        if extra:
            feat_pb.extra.CopyFrom(_dict_to_struct(extra))

        fc.features.append(feat_pb)

    # collection bbox/name/extra (like v5)
    bbox = obj.get("bbox")
    if isinstance(bbox, list) and len(bbox) in (4, 6) and all(isinstance(x, (int, float)) for x in bbox):
        fc.bbox.extend([float(x) for x in bbox])

    name = obj.get("name")
    if isinstance(name, str) and name:
        fc.name = name

    extra_top = _extract_extra_fcol(obj)
    if extra_top:
        fc.extra.CopyFrom(_dict_to_struct(extra_top))

    return fc.SerializeToString()


def bytes_to_geojson_featurecollection_v7(data: bytes) -> GeoJSON:
    fc = geometry_pb2.FeatureCollection.FromString(data)
    scale = int(fc.crs.scale)
    global_start_xy = (int(fc.global_start.x), int(fc.global_start.y))

    out: GeoJSON = {"type": "FeatureCollection", "features": []}

    for feat_pb in fc.features:
        geom = _decode_stream_geometry(feat_pb.geometry, global_start_xy, scale)

        props_dict = _struct_to_dict(feat_pb.properties)
        properties = None if props_dict == {} else props_dict

        feat: GeoJSON = {"type": "Feature", "geometry": geom, "properties": properties}

        if getattr(feat_pb, "id", ""):
            feat["id"] = feat_pb.id

        if getattr(feat_pb, "bbox", None) and len(feat_pb.bbox) in (4, 6):
            feat["bbox"] = list(feat_pb.bbox)

        extra = _struct_to_dict(feat_pb.extra) if hasattr(feat_pb, "extra") else {}
        for k, v in extra.items():
            if k not in feat:
                feat[k] = v

        out["features"].append(feat)

    if getattr(fc, "bbox", None) and len(fc.bbox) in (4, 6):
        out["bbox"] = list(fc.bbox)

    if getattr(fc, "name", ""):
        out["name"] = fc.name

    extra_top = _struct_to_dict(fc.extra) if hasattr(fc, "extra") else {}
    for k, v in extra_top.items():
        if k not in out:
            out[k] = v

    # optional: old-style GeoJSON crs member if you used that convention
    # if fc.crs.srid:
    #   out["crs"] = {"type":"name","properties":{"name": f"urn:ogc:def:crs:EPSG::{int(fc.crs.srid)}"}}

    return out

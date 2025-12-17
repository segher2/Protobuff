import json
from sfproto.geojson_point import geojson_point_to_bytes, bytes_to_geojson_point

geojson = {"type": "Point", "coordinates": [4.9, 52.37]}

data = geojson_point_to_bytes(geojson, srid=4326)
out = bytes_to_geojson_point(data)

print("bytes length:", len(data))
print("out:", json.dumps(out))

import json
from collections import Counter

# Path to your large GeoJSON file
GEOJSON_PATH = "examples/data/bag_pand_50k.geojson"

statuses = Counter()
gebruiksdoelen = Counter()

with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

for feature in data.get("features", []):
    props = feature.get("properties", {})

    status = props.get("status")
    gebruiksdoel = props.get("gebruiksdoel")

    # Normalize empty strings / nulls
    if status is None or status == "":
        statuses["<EMPTY>"] += 1
    else:
        statuses[status] += 1

    if gebruiksdoel is None or gebruiksdoel == "":
        gebruiksdoelen["<EMPTY>"] += 1
    else:
        gebruiksdoelen[gebruiksdoel] += 1


print("\nUnique pandstatus values:")
for k, v in statuses.items():
    print(f"- {k!r}: {v}")

print("\nUnique gebruiksdoel values:")
for k, v in gebruiksdoelen.items():
    print(f"- {k!r}: {v}")

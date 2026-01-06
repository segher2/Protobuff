# Protobuff
Create your own binary encoding of SimpleFeatures with Cap'n Proto or ProtoBuf. The main point of this project is to understand the properties of different data serialization frameworks and appreciate their pros, cons, and suitable use cases. You can try to encode BAG data or other 2D data with your own encoding scheme.

### 1. Install dependencies
```bash
pip install -r requirements.txt
pip install -e .
```
### 2. Generate Protobuff code
Only needed when geometry.proto changes
```bash
python scripts/gen_proto.py
```

### Run the example
```bash
python examples/geojson_roundtrip.py
```


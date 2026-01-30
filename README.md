# SFProto
SFProto is a research prototype that explores a custom Protocol Buffers (Protobuf)-based binary encoding for two-dimensional geospatial 
data following the Simple Features model. This project investigates how a general-purpose binary 
serialization framework compares to established geospatial formats such as GeoJSON and FlatGeobuf, with a 
particular focus on file size efficiency and performance.

This repository accompanies an MSc research project in Geomatics at TU Delft.

## Project overview 
Geospatial datasets are growing rapidly in size and complexity. While text-based formats such as 
GeoJSON are easy to use and widely supported, they often result in large file sizes. Binary formats 
can significantly reduce storage requirements and improve performance.

SFProto evaluates multiple Protobuf-based encoding variants that differ in:
- Attribute encoding and schema design (static vs. dynamic)
- Delta and integer geometry encoding

These variants are benchmarked against each other and against existing formats to analyze trade-offs between compactness,
flexibility, and performance.

## Installation
Clone the repository and install SFProto in editable mode: 
```bash
git clone https://github.com/segher2/SFProto.git
cd SFProto
python -m pip install -r requirements.txt
python -m pip install -e .
```

### Generate SFProto code
Only needed when geometry.proto changes
```bash
python scripts/gen_proto.py
```

## Repository Structure
```powershell
SFProto/
├── bench/
│   ├── bench_out_bag/
│   │    ├── figures/
│   │    │    └── ..
│   │    ├── results.csv
│   │    └── viz_bag.py
│   └── bench_out_osm/
│        ├── size/
│        │    └── figures_telling/
│        │         └── ..
│        ├── size_summary.csv
│        └── viz_size.py
├── proto/
│   └── sf/
│       ├── v1/
│       ├── v2/
│       ├── ..  
│       └── __init__.py
├── scripts/
│       ├── benchmark/
│       ├── download_data/
│       └── gen_proto.py
├── src/
│   └── sf/
│       └── v1/
│           ├── geometry_pb2.py
│           └── geometry_pb2.pyi
│       └── sfproto/            
│           ├── cli/
│           │    └── main.py
│           ├── geojson/
│           │    ├── v1
│           │    ├── ..
│           │    ├── __init__.py
│           │    └── api.py  
│           ├── sf/
│           │    ├── v1
│           │    ├── ..
│           │    └── __init__.py
│           └── __init__.py
├── tests/
│   ├── bag_roundtrip.py      
│   └── geojson_roundtrip.py                 
├── .gitignore             
├── pyproject.toml 
├── data.zip     
├── README.md
├── SFProto.pdf
└── requirements.txt
```

## Command Line Interface 
SFProto provides a command-line interface for encoding data into a custom Protobuf-based binary format and decoding it back to GeoJSON. 

### Installation
From the project root directory, install SFProto in editable mode: 
```bash
pip install -e .
```
This installs the project in editable mode, making it importable and executable as a package while running the code
directly from the source directory so that changes take effect immediately.

### Commands
- To encode GeoJSON to SFProto (.bin):
```bash
sfproto encode <input> -o <output> (--delta)
```
- To decode SFProto to GeoJSON (.geojson):
```bash
sfproto decode <input> -o <output> (--delta)
```
#### Arguments
- `<input>`: Path to the input .bin or .geojson file
- `-o <output>`: Write the output to the specified path as a .bin or .geojson file.
- `--delta`: Enables delta encoding with quantized coordinates (**v7**). If omitted, no delta encoding is used (**v4**). 

#### Example
```bash
sfproto encode "examples\data\bag_pand_50k.geojson" -o "examples\data\out_bag_pand_50k.sfproto" --delta
```

### Need help?
To display the available commands and options:
```bash
sfproto encode --help
```

## Test data
Test data is available via the following Google Drive link:  
https://drive.google.com/drive/folders/1oYb6kB4Xo0oaSMGJBJGEs5IFiBbD-t9c?usp=sharing

Please download the data and place it in the root directory of the SFProto repository.


### Unzipping the data archive
#### Option 1: Using WSL/Linux 
If you are working in WSL, navigate to the repository root and run:
```bash
unzip data.zip
```
If the unzip utility is not installed: 
```bash
sudo apt install unzip
```
The command above extracts the contents of `data.zip` into a `data\` directory. After extraction, 
the repository root should contain a directory similar to: 
```text
data/
├── ...
```
Ensure that the expected data files are present before proceeding.
#### Option 2: Using Windows (File Explorer)
If you are working directly in Windows: 
1. Right-click `data.zip`
2. Select **Extract All**
3. Click **Extract**
This will create a `data/` directory in the repository root.

#### Option 3: Windows (PowerShell)
From the repository root, run: 
```bash
Expand-Archive data.zip -DestinationPath .
```

### Notes 
- Do not rename the extracted `data\` directory.
- The contents of `data.zip` are assumed by the code to be located relative to the repository root.

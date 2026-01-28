"""
Convert a FlatGeobuf file to GeoJSON format using GDAL's ogr2ogr.

Requirements:
    - GDAL installed (conda install -c conda-forge gdal)
    - ogr2ogr available in PATH

Helpful commands (run in terminal):
    where ogr2ogr

Example usage:
    C:\Users\Julia\miniconda3\python.exe "C:\Users\Julia\Protobuff\scripts\convert_fgb_to_geojson.py"
"""

from pathlib import Path
import subprocess
import shutil

# define data trajectory
DATA_DIR = Path(__file__).resolve().parents[1] / "examples" / "data"

# input FlatGeobuf file and output GeoJSON file
input_fgb = DATA_DIR / "UScounties.fgb"
output_geojson = DATA_DIR / "UScounties.geojson"

# locate the ogr2ogr executable
# shutil.which searches for it in the system path
ogr2ogr = shutil.which("ogr2ogr")
if ogr2ogr is None:
    raise RuntimeError(
        "ogr2ogr not found. Run using your conda env (where GDAL is installed) "
        "or install GDAL: conda install -c conda-forge gdal"
    )

# run ogr2ogr to convert FlatGeobuf -> GeoJSON
# -f GeoJSON: specify output format
# check=True: raise an error if the command fails
subprocess.run(
    [
        ogr2ogr,
        "-f", "GeoJSON",
        str(output_geojson),
        str(input_fgb),
    ],
    check=True,
)

print("Wrote:", output_geojson)
"""
Convert a GeoJSON file to FlatGeobuf format using GDAL's ogr2ogr.

Requirements:
    - GDAL installed (conda install -c conda-forge gdal)
    - ogr2ogr available in PATH

Helpful commands (run in terminal):
    where ogr2ogr

Example usage:
    C:\Users\Julia\miniconda3\python.exe "C:\Users\Julia\Protobuff\scripts\convert_geojson_to_fgb.py"
"""

from pathlib import Path
import subprocess
import shutil

# define data trajectory
DATA_DIR = Path(__file__).resolve().parents[1] / "examples" / "data"

#input GeoJSON file and output FlatGeobuf file
input_geojson = DATA_DIR / "bag_pand_50k.geojson"
output_fgb = DATA_DIR / "bag_pand_50k.fgb"

#locate the ogr2ogr executable
#shutil.which searches for it in the system path
ogr2ogr = shutil.which("ogr2ogr")
if ogr2ogr is None:
    raise RuntimeError(
        "ogr2ogr not found. Run using your conda env (where GDAL is installed) "
        "or install GDAL: conda install -c conda-forge gdal"
    )

#run ogr2ogr to convert GeoJSON -> FlatGeobuf
# -f FlatGeobuf: specify output format
# check=True: raise an error if the command fails
subprocess.run(
    [
        ogr2ogr,
        "-f", "FlatGeobuf",
        str(output_fgb),
        str(input_geojson),
    ],
    check=True,
)

print("Wrote:", output_fgb)
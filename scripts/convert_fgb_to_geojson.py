from pathlib import Path
import subprocess
import shutil

BENCH_OUT = Path(__file__).resolve().parents[1] / "bench_out"
input_fgb = BENCH_OUT / "UScounties.fgb"
output_geojson = BENCH_OUT / "test_fgb.geojson"

ogr2ogr = shutil.which("ogr2ogr")
if ogr2ogr is None:
    raise RuntimeError(
        "ogr2ogr not found. Run using your conda env (where GDAL is installed) "
        "or install GDAL: conda install -c conda-forge gdal"
    )

subprocess.run(
    [
        ogr2ogr,
        "-f", "GeoJSON",
        "-lco", "RFC7946=YES",
        str(output_geojson),
        str(input_fgb),
    ],
    check=True,
)

print("Wrote:", output_geojson)
